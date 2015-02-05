// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

    // Information kept for every waiting writer
    struct DBImpl::Writer {
        Status status;
        WriteBatch* batch;
        bool sync;
        bool done;
        port::CondVar cv;

        explicit Writer(port::Mutex* mu) : cv(mu) { } //这里这个构造函数不只是简单的做类型转换 cv = mu; 而是调用cv的构造函数 CondVar::CondVar(Mutex* mu); 这个构造函数做的是对CondVar里面mu_赋值
    };

    struct DBImpl::CompactionState {
        Compaction* const compaction;

        // Sequence numbers < smallest_snapshot are not significant since we
        // will never have to service a snapshot below smallest_snapshot.
        // Therefore if we have seen a sequence number S <= smallest_snapshot,
        // we can drop all entries for the same key with sequence numbers < S.
        SequenceNumber smallest_snapshot;

        // Files produced by compaction
        struct Output {
            uint64_t number;
            uint64_t file_size;
            InternalKey smallest, largest;
        };
        std::vector<Output> outputs;

        // State kept for output being generated
        WritableFile* outfile;
        TableBuilder* builder;

        uint64_t total_bytes;

        Output* current_output() { return &outputs[outputs.size()-1]; }

        explicit CompactionState(Compaction* c)
            : compaction(c),
            outfile(NULL),
            builder(NULL),
            total_bytes(0) {
            }
    };

    // Fix user-supplied options to be reasonable
    template <class T,class V>
        static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
            if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
            if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
        }
    Options SanitizeOptions(const std::string& dbname,
            const InternalKeyComparator* icmp,
            const InternalFilterPolicy* ipolicy,
            const Options& src) {
        Options result = src;
        result.comparator = icmp;
        result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
        ClipToRange(&result.max_open_files,            20,     50000);
        ClipToRange(&result.write_buffer_size,         64<<10, 1<<30);
        ClipToRange(&result.block_size,                1<<10,  4<<20);
        if (result.info_log == NULL) {
            // Open a log file in the same directory as the db
            src.env->CreateDir(dbname);  // In case it does not exist
            src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
            Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
            if (!s.ok()) {
                // No place suitable for logging
                result.info_log = NULL;
            }
        }
        if (result.block_cache == NULL) {
            result.block_cache = NewLRUCache(8 << 20);
        }
        return result;
    }

    DBImpl::DBImpl(const Options& options, const std::string& dbname)
        : env_(options.env),
        internal_comparator_(options.comparator),
        internal_filter_policy_(options.filter_policy),
        options_(SanitizeOptions(
                    dbname, &internal_comparator_, &internal_filter_policy_, options)),
        owns_info_log_(options_.info_log != options.info_log),
        owns_cache_(options_.block_cache != options.block_cache),
        dbname_(dbname),
        db_lock_(NULL),
        shutting_down_(NULL),
        bg_cv_(&mutex_),
        mem_(new MemTable(internal_comparator_)),
        imm_(NULL),
        logfile_(NULL),
        logfile_number_(0),
        log_(NULL),
        tmp_batch_(new WriteBatch),
        bg_compaction_scheduled_(false),
        manual_compaction_(NULL) {
            mem_->Ref();
            has_imm_.Release_Store(NULL);

            // Reserve ten files or so for other uses and give the rest to TableCache.
            const int table_cache_size = options.max_open_files - 10;
            table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

            versions_ = new VersionSet(dbname_, &options_, table_cache_,
                    &internal_comparator_);
        }

    DBImpl::~DBImpl() {
        // Wait for background work to finish
        printf("call the DBImpl function\n");
        mutex_.Lock(); //这里尝试去锁住这个整个DB的mutex_, 如果这个时候有其他的线程在Compaction 或者在写入, 会阻塞在这里等待去抢占这个锁
        shutting_down_.Release_Store(this);  // Any non-NULL value is ok
        while (bg_compaction_scheduled_) {
            bg_cv_.Wait(); // 这里会放开上面的mutex_这个锁, 然后等待这个条件变量的signal().
        }
        mutex_.Unlock(); //TODO 为什么这里这个锁可以放开, 难道这个时候已经不可以有新的写入了么
        //到这里已经获得了这个唯一的这个锁. 不会再有数据写入了, 但是还可以读
        if (db_lock_ != NULL) {
            env_->UnlockFile(db_lock_); //这里把这个LOCK文件的锁放开
        }

        delete versions_;
        if (mem_ != NULL) mem_->Unref(); //对mem_ 和 imm_的引用的减去
        if (imm_ != NULL) imm_->Unref();
        delete tmp_batch_;
        delete log_;
        delete logfile_;
        delete table_cache_;

        if (owns_info_log_) {
            delete options_.info_log; // 把日志句柄删除
        }
        if (owns_cache_) {
            delete options_.block_cache; // 把block_cache句柄删除
        }
    }

    Status DBImpl::NewDB() {
        VersionEdit new_db;
        new_db.SetComparatorName(user_comparator()->Name());
        new_db.SetLogNumber(0);
        new_db.SetNextFile(2);
        new_db.SetLastSequence(0);

        const std::string manifest = DescriptorFileName(dbname_, 1);
        WritableFile* file;
        Status s = env_->NewWritableFile(manifest, &file);
        if (!s.ok()) {
            return s;
        }
        {
            log::Writer log(file);
            std::string record;
            new_db.EncodeTo(&record);
            s = log.AddRecord(record);
            if (s.ok()) {
                s = file->Close();
            }
        }
        delete file;
        if (s.ok()) {
            // Make "CURRENT" file that points to the new manifest file.
            s = SetCurrentFile(env_, dbname_, 1);
        } else {
            env_->DeleteFile(manifest);
        }
        return s;
    }

    void DBImpl::MaybeIgnoreError(Status* s) const {
        if (s->ok() || options_.paranoid_checks) {
            // No change needed
        } else {
            Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
            *s = Status::OK();
        }
    }

    void DBImpl::DeleteObsoleteFiles() {
        // Make a set of all of the live files
        // 这里初始化的live 的file是正在compaction 的file
        // 可以查看一下增加pending_outputs_的地方, 都是在versions_->NewFileNumber()之后
        // 也就是这个文件是在生成的过程中, 还没有加入到新的version里面
        std::set<uint64_t> live = pending_outputs_;
        // 这里就是遍历当前的version_set 链表, 把每一个version里面有的文件也添加到当前的
        // live中
        versions_->AddLiveFiles(&live);

        std::vector<std::string> filenames;
        env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type)) {
                bool keep = true;
                switch (type) {
                    case kLogFile:
                        // 如果是日志文件, 并且numer 是当前正在compaction的number 或者 比当前正在写的日志文件的LOG大
                        // 就保留下来
                        keep = ((number >= versions_->LogNumber()) ||
                                (number == versions_->PrevLogNumber()));
                        break;
                    case kDescriptorFile:
                        // Keep my manifest file, and any newer incarnations'
                        // (in case there is a race that allows other incarnations)
                        // 如果是manifestfile, 大于当前的Manifestfile 就保留
                        keep = (number >= versions_->ManifestFileNumber());
                        break;
                    case kTableFile:
                        // 如果是sst文件, 如果再这个live 列表里面, 那么就保留
                        // 具体看上面live是怎么生成的
                        keep = (live.find(number) != live.end());
                        break;
                    case kTempFile:
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs_, which is inserted into "live"
                        keep = (live.find(number) != live.end());
                        break;
                    case kCurrentFile:
                    case kDBLockFile:
                    case kInfoLogFile:
                        keep = true;
                        break;
                }

                if (keep == 0) {
                    if (type == kTableFile) {
                        // 如果删除的是sst文件, 那么也要把这个表从table_cache里面删除
                        table_cache_->Evict(number);
                    }
                    Log(options_.info_log, "Delete type=%d #%lld\n",
                            int(type),
                            static_cast<unsigned long long>(number));
                    env_->DeleteFile(dbname_ + "/" + filenames[i]);
                }
            }
        }
    }

    Status DBImpl::Recover(VersionEdit* edit) {
        mutex_.AssertHeld();

        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        env_->CreateDir(dbname_);
        assert(db_lock_ == NULL);
        Status s = env_->LockFile(LockFileName(dbname_), &db_lock_); // 尝试去锁住这个LOCK文件, 这个文件有且只有一个进程能持有, 这也是levelDB如何保持只有一个实例的方法.
        if (!s.ok()) {
            return s;
        }

        if (!env_->FileExists(CurrentFileName(dbname_))) {
            if (options_.create_if_missing) {
                s = NewDB();
                if (!s.ok()) {
                    return s;
                }
            } else {
                return Status::InvalidArgument(
                        dbname_, "does not exist (create_if_missing is false)");
            }
        } else {
            if (options_.error_if_exists) {
                return Status::InvalidArgument(
                        dbname_, "exists (error_if_exists is true)");
            }
        }

        s = versions_->Recover(); //这里是主要的一个Recover的过程, 从MANIFEST文件里面把所有的结果取出.并且会修改 pre_file_number_ 等也同时修改, 包括设定当前这个versions_ log_number 等一些参数
        // 因为MANIFEST记录的就是当前所有版本的信息. 所以这里就是根据这个MANIFEST文件建立versionSet的过程
        // 恢复完这个versionSet以后, 就是要处理这些log信息的过程

        if (s.ok()) {
            SequenceNumber max_sequence(0);

            // Recover from all newer log files than the ones named in the
            // descriptor (new log files may have been added by the previous
            // incarnation without registering them in the descriptor).
            //
            // Note that PrevLogNumber() is no longer used, but we pay
            // attention to it in case we are recovering a database
            // produced by an older version of leveldb.
            const uint64_t min_log = versions_->LogNumber();
            const uint64_t prev_log = versions_->PrevLogNumber();
            std::vector<std::string> filenames;
            s = env_->GetChildren(dbname_, &filenames);
            if (!s.ok()) {
                return s;
            }
            uint64_t number;
            FileType type;
            std::vector<uint64_t> logs;
            for (size_t i = 0; i < filenames.size(); i++) {
                if (ParseFileName(filenames[i], &number, &type)
                        && type == kLogFile
                        && ((number >= min_log) || (number == prev_log))) {
                    logs.push_back(number);
                }
            }

            // Recover in the order in which the logs were generated
            std::sort(logs.begin(), logs.end());
            for (size_t i = 0; i < logs.size(); i++) {
                s = RecoverLogFile(logs[i], edit, &max_sequence);

                // The previous incarnation may not have written any MANIFEST
                // records after allocating this log number.  So we manually
                // update the file number allocation counter in VersionSet.
                versions_->MarkFileNumberUsed(logs[i]);
            }

            if (s.ok()) {
                if (versions_->LastSequence() < max_sequence) {
                    versions_->SetLastSequence(max_sequence);
                }
            }
        }

        return s;
    }

    Status DBImpl::RecoverLogFile(uint64_t log_number,
            VersionEdit* edit,
            SequenceNumber* max_sequence) {
        struct LogReporter : public log::Reader::Reporter {
            Env* env;
            Logger* info_log;
            const char* fname;
            Status* status;  // NULL if options_.paranoid_checks==false
            virtual void Corruption(size_t bytes, const Status& s) {
                Log(info_log, "%s%s: dropping %d bytes; %s",
                        (this->status == NULL ? "(ignoring error) " : ""),
                        fname, static_cast<int>(bytes), s.ToString().c_str());
                if (this->status != NULL && this->status->ok()) *this->status = s;
            }
        };

        mutex_.AssertHeld();

        // Open the log file
        std::string fname = LogFileName(dbname_, log_number);
        SequentialFile* file;
        Status status = env_->NewSequentialFile(fname, &file);
        if (!status.ok()) {
            MaybeIgnoreError(&status);
            return status;
        }

        // Create the log reader.
        LogReporter reporter;
        reporter.env = env_;
        reporter.info_log = options_.info_log;
        reporter.fname = fname.c_str();
        reporter.status = (options_.paranoid_checks ? &status : NULL);
        // We intentially make log::Reader do checksumming even if
        // paranoid_checks==false so that corruptions cause entire commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).
        log::Reader reader(file, &reporter, true/*checksum*/,
                0/*initial_offset*/);
        Log(options_.info_log, "Recovering log #%llu",
                (unsigned long long) log_number);

        // Read all the records and add to a memtable
        std::string scratch;
        Slice record;
        WriteBatch batch;
        MemTable* mem = NULL;
        while (reader.ReadRecord(&record, &scratch) &&
                status.ok()) {
            if (record.size() < 12) {
                reporter.Corruption(
                        record.size(), Status::Corruption("log record too small"));
                continue;
            }
            WriteBatchInternal::SetContents(&batch, record);

            if (mem == NULL) {
                mem = new MemTable(internal_comparator_);
                mem->Ref();
            }
            status = WriteBatchInternal::InsertInto(&batch, mem);
            MaybeIgnoreError(&status);
            if (!status.ok()) {
                break;
            }
            const SequenceNumber last_seq =
                WriteBatchInternal::Sequence(&batch) +
                WriteBatchInternal::Count(&batch) - 1;
            if (last_seq > *max_sequence) {
                *max_sequence = last_seq;
            }

            if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
                status = WriteLevel0Table(mem, edit, NULL);
                if (!status.ok()) {
                    // Reflect errors immediately so that conditions like full
                    // file-systems cause the DB::Open() to fail.
                    break;
                }
                mem->Unref();
                mem = NULL;
            }
        }

        if (status.ok() && mem != NULL) {
            status = WriteLevel0Table(mem, edit, NULL);
            // Reflect errors immediately so that conditions like full
            // file-systems cause the DB::Open() to fail.
        }

        if (mem != NULL) mem->Unref();
        delete file;
        return status;
    }

    Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
            Version* base) {
        mutex_.AssertHeld();
        const uint64_t start_micros = env_->NowMicros();
        FileMetaData meta;
        meta.number = versions_->NewFileNumber();
        pending_outputs_.insert(meta.number);
        Iterator* iter = mem->NewIterator();
        Log(options_.info_log, "Level-0 table #%llu: started",
                (unsigned long long) meta.number);

        Status s; 
        {
            //这里把锁放开, 同样是因为这个iterator已经获得, 固定下来了. 所以不在乎这个有更新
            mutex_.Unlock();
            s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
            mutex_.Lock();
        }

        Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
                (unsigned long long) meta.number,
                (unsigned long long) meta.file_size,
                s.ToString().c_str());
        delete iter;
        pending_outputs_.erase(meta.number);


        // Note that if file_size is zero, the file has been deleted and
        // should not be added to the manifest.
        int level = 0;
        if (s.ok() && meta.file_size > 0) {
            const Slice min_user_key = meta.smallest.user_key();
            const Slice max_user_key = meta.largest.user_key();
            if (base != NULL) {
                level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
            }
            edit->AddFile(level, meta.number, meta.file_size,
                    meta.smallest, meta.largest);
        }

        CompactionStats stats;
        stats.micros = env_->NowMicros() - start_micros;
        stats.bytes_written = meta.file_size;
        stats_[level].Add(stats);
        return s;
    }

    Status DBImpl::CompactMemTable() {
        mutex_.AssertHeld();
        assert(imm_ != NULL);

        // Save the contents of the memtable as a new Table
        VersionEdit edit;
        Version* base = versions_->current();
        base->Ref();
        Status s = WriteLevel0Table(imm_, &edit, base);
        base->Unref();

        if (s.ok() && shutting_down_.Acquire_Load()) {
            s = Status::IOError("Deleting DB during memtable compaction");
        }

        // Replace immutable memtable with the generated Table
        if (s.ok()) {
            edit.SetPrevLogNumber(0);
            edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
            s = versions_->LogAndApply(&edit, &mutex_);
        }

        if (s.ok()) {
            // Commit to the new state
            imm_->Unref();
            imm_ = NULL;
            has_imm_.Release_Store(NULL);
            DeleteObsoleteFiles();
        }

        return s;
    }

    void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
        int max_level_with_files = 1;
        {
            MutexLock l(&mutex_);
            Version* base = versions_->current();
            for (int level = 1; level < config::kNumLevels; level++) {
                if (base->OverlapInLevel(level, begin, end)) {
                    max_level_with_files = level;
                }
            }
        }
        TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
        for (int level = 0; level < max_level_with_files; level++) {
            TEST_CompactRange(level, begin, end);
        }
    }

    void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
        assert(level >= 0);
        assert(level + 1 < config::kNumLevels);

        InternalKey begin_storage, end_storage;

        ManualCompaction manual;
        manual.level = level;
        manual.done = false;
        if (begin == NULL) {
            manual.begin = NULL;
        } else {
            begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
            manual.begin = &begin_storage;
        }
        if (end == NULL) {
            manual.end = NULL;
        } else {
            end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
            manual.end = &end_storage;
        }

        MutexLock l(&mutex_);
        while (!manual.done) {
            while (manual_compaction_ != NULL) {
                bg_cv_.Wait();
            }
            manual_compaction_ = &manual;
            MaybeScheduleCompaction();
            while (manual_compaction_ == &manual) {
                bg_cv_.Wait();
            }
        }
    }

    Status DBImpl::TEST_CompactMemTable() {
        // NULL batch means just wait for earlier writes to be done
        Status s = Write(WriteOptions(), NULL);
        if (s.ok()) {
            // Wait until the compaction completes
            MutexLock l(&mutex_);
            while (imm_ != NULL && bg_error_.ok()) {
                bg_cv_.Wait();
            }
            if (imm_ != NULL) {
                s = bg_error_;
            }
        }
        return s;
    }

    void DBImpl::MaybeScheduleCompaction() {
        mutex_.AssertHeld();
        // 如果后台有Compaction 线程, 那么直接退出
        if (bg_compaction_scheduled_) {
            // Already scheduled
            // 如果db 要被 shut_down, 直接退出
        } else if (shutting_down_.Acquire_Load()) {
            // DB is being deleted; no more background compactions
            // 如果 imm_ 这个文件还是空的, 并且是manual_compaction是空的, 这里
            // TODO
        } else if (imm_ == NULL &&
                manual_compaction_ == NULL &&
                !versions_->NeedsCompaction()) {
            // No work to be done
        } else {
            // 设置这个后台有compaction 线程已经启动
            bg_compaction_scheduled_ = true;
            env_->Schedule(&DBImpl::BGWork, this); //调用下面的 BGWork函数. 这里虽然是env_, 当时这env_里面会调用这个函数指针, 调用DBImpl::BGWork 这个函数
        }
    }

    void DBImpl::BGWork(void* db) {
        reinterpret_cast<DBImpl*>(db)->BackgroundCall(); //这里真正调用BackgroundCall
    }

    void DBImpl::BackgroundCall() {
        MutexLock l(&mutex_);
        assert(bg_compaction_scheduled_);
        if (!shutting_down_.Acquire_Load()) {
            Status s = BackgroundCompaction();
            if (s.ok()) {
                // Success
            } else if (shutting_down_.Acquire_Load()) {
                // Error most likely due to shutdown; do not wait
            } else {
                // Wait a little bit before retrying background compaction in
                // case this is an environmental problem and we do not want to
                // chew up resources for failed compactions for the duration of
                // the problem.
                bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
                Log(options_.info_log, "Waiting after background compaction error: %s",
                        s.ToString().c_str());
                mutex_.Unlock();
                env_->SleepForMicroseconds(1000000);
                mutex_.Lock();
            }
        }

        bg_compaction_scheduled_ = false;

        // Previous compaction may have produced too many files in a level,
        // so reschedule another compaction if needed.
        MaybeScheduleCompaction();
        bg_cv_.SignalAll();
    }

    Status DBImpl::BackgroundCompaction() {
        mutex_.AssertHeld();

        if (imm_ != NULL) { // 如果imm_非空, 则优先compaction immutable memtable
            return CompactMemTable();
        }

        Compaction* c;
        // 判断是否是手动控制要 compaction 否则就是从某一个级别里面挑选出来进行Compaction
        bool is_manual = (manual_compaction_ != NULL);
        InternalKey manual_end;
        if (is_manual) {
            ManualCompaction* m = manual_compaction_;
            c = versions_->CompactRange(m->level, m->begin, m->end);
            m->done = (c == NULL);
            if (c != NULL) {
                manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
            }
            Log(options_.info_log,
                    "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
                    m->level,
                    (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
                    (m->end ? m->end->DebugString().c_str() : "(end)"),
                    (m->done ? "(end)" : manual_end.DebugString().c_str()));
        } else {
            // 这里就是具体生成的Compaction, 具体的Compaction 操作在下面完成
            c = versions_->PickCompaction();
        }

        Status status;
        if (c == NULL) {
            // Nothing to do
        } else if (!is_manual && c->IsTrivialMove()) {
            // Move file to next level
            // 这个是做直接将当前这个级别移动的grandfather级别的Compaction 操作
            // 
            assert(c->num_input_files(0) == 1);
            FileMetaData* f = c->input(0, 0);
            c->edit()->DeleteFile(c->level(), f->number);
            c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                    f->smallest, f->largest);
            status = versions_->LogAndApply(c->edit(), &mutex_);
            VersionSet::LevelSummaryStorage tmp;
            Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
                    static_cast<unsigned long long>(f->number),
                    c->level() + 1,
                    static_cast<unsigned long long>(f->file_size),
                    status.ToString().c_str(),
                    versions_->LevelSummary(&tmp));
        } else {
            CompactionState* compact = new CompactionState(c);
            // 这个DoCompactionWork 是做最正常的compact
            status = DoCompactionWork(compact);
            CleanupCompaction(compact);
            c->ReleaseInputs();
            DeleteObsoleteFiles();
        }
        delete c;

        if (status.ok()) {
            // Done
        } else if (shutting_down_.Acquire_Load()) {
            // Ignore compaction errors found during shutting down
        } else {
            Log(options_.info_log,
                    "Compaction error: %s", status.ToString().c_str());
            if (options_.paranoid_checks && bg_error_.ok()) {
                bg_error_ = status;
            }
        }

        if (is_manual) {
            ManualCompaction* m = manual_compaction_;
            if (!status.ok()) {
                m->done = true;
            }
            if (!m->done) {
                // We only compacted part of the requested range.  Update *m
                // to the range that is left to be compacted.
                m->tmp_storage = manual_end;
                m->begin = &m->tmp_storage;
            }
            manual_compaction_ = NULL;
        }
        return status;
    }

    void DBImpl::CleanupCompaction(CompactionState* compact) {
        mutex_.AssertHeld();
        if (compact->builder != NULL) {
            // May happen if we get a shutdown call in the middle of compaction
            compact->builder->Abandon();
            delete compact->builder;
        } else {
            assert(compact->outfile == NULL);
        }
        delete compact->outfile;
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output& out = compact->outputs[i];
            pending_outputs_.erase(out.number);
        }
        delete compact;
    }

    Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
        assert(compact != NULL);
        assert(compact->builder == NULL);
        uint64_t file_number;
        {
            // 获得这个当前这个Compaction 生成的FileNumber的地方需要锁住这个DB级别的锁
            mutex_.Lock();
            file_number = versions_->NewFileNumber();
            pending_outputs_.insert(file_number);
            CompactionState::Output out;
            out.number = file_number;
            out.smallest.Clear();
            out.largest.Clear();
            compact->outputs.push_back(out);
            mutex_.Unlock();
        }

        // Make the output file
        std::string fname = TableFileName(dbname_, file_number);
        Status s = env_->NewWritableFile(fname, &compact->outfile);
        if (s.ok()) {
            compact->builder = new TableBuilder(options_, compact->outfile);
        }
        return s;
    }

    Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
            Iterator* input) {
        assert(compact != NULL);
        assert(compact->outfile != NULL);
        assert(compact->builder != NULL);

        const uint64_t output_number = compact->current_output()->number;
        assert(output_number != 0);

        // Check for iterator errors
        Status s = input->status();
        const uint64_t current_entries = compact->builder->NumEntries();
        if (s.ok()) {
            s = compact->builder->Finish();
        } else {
            compact->builder->Abandon();
        }
        const uint64_t current_bytes = compact->builder->FileSize();
        compact->current_output()->file_size = current_bytes;
        compact->total_bytes += current_bytes;
        delete compact->builder;
        compact->builder = NULL;

        // Finish and check for file errors
        if (s.ok()) {
            s = compact->outfile->Sync();
        }
        if (s.ok()) {
            s = compact->outfile->Close();
        }
        delete compact->outfile;
        compact->outfile = NULL;

        // 这里先生成一个文件, 直接输出
        if (s.ok() && current_entries > 0) {
            // Verify that the table is usable
            Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                    output_number,
                    current_bytes);
            s = iter->status();
            delete iter;
            if (s.ok()) {
                Log(options_.info_log,
                        "Generated table #%llu: %lld keys, %lld bytes",
                        (unsigned long long) output_number,
                        (unsigned long long) current_entries,
                        (unsigned long long) current_bytes);
            }
        }
        return s;
    }


    Status DBImpl::InstallCompactionResults(CompactionState* compact) {
        mutex_.AssertHeld();
        Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
                compact->compaction->num_input_files(0),
                compact->compaction->level(),
                compact->compaction->num_input_files(1),
                compact->compaction->level() + 1,
                static_cast<long long>(compact->total_bytes));

        // Add compaction outputs
        compact->compaction->AddInputDeletions(compact->compaction->edit());
        const int level = compact->compaction->level();
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output& out = compact->outputs[i];
            compact->compaction->edit()->AddFile(
                    level + 1,
                    out.number, out.file_size, out.smallest, out.largest);
        }
        return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
    }

    Status DBImpl::DoCompactionWork(CompactionState* compact) {
        const uint64_t start_micros = env_->NowMicros();
        int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

        Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
                compact->compaction->num_input_files(0),
                compact->compaction->level(),
                compact->compaction->num_input_files(1),
                compact->compaction->level() + 1);

        assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
        assert(compact->builder == NULL);
        assert(compact->outfile == NULL);
        // 获得当前的版本号
        if (snapshots_.empty()) {
            compact->smallest_snapshot = versions_->LastSequence();
        } else {
            compact->smallest_snapshot = snapshots_.oldest()->number_;
        }

        // Release mutex while we're actually doing the compaction work
        // 在我们进行compaction的时候, 把锁放开
        mutex_.Unlock();

        //这里获得了level 和 level + 1 的一个指针, 指向所有需要合并的Block
        Iterator* input = versions_->MakeInputIterator(compact->compaction);
        input->SeekToFirst();
        Status status;
        ParsedInternalKey ikey;
        std::string current_user_key;
        bool has_current_user_key = false;
        SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
        for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
            // Prioritize immutable compaction work
            if (has_imm_.NoBarrier_Load() != NULL) {
                const uint64_t imm_start = env_->NowMicros();
                mutex_.Lock();
                if (imm_ != NULL) {
                    CompactMemTable();
                    bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
                }
                mutex_.Unlock();
                imm_micros += (env_->NowMicros() - imm_start);
            }

            Slice key = input->key();
            if (compact->compaction->ShouldStopBefore(key) &&
                    compact->builder != NULL) {
                status = FinishCompactionOutputFile(compact, input);
                if (!status.ok()) {
                    break;
                }
            }

            // Handle key/value, add to state, etc.
            // 这里判断这个key 是否要抛弃掉
            bool drop = false;
            if (!ParseInternalKey(key, &ikey)) {
                // Do not hide error keys
                current_user_key.clear();
                has_current_user_key = false;
                last_sequence_for_key = kMaxSequenceNumber;
            } else {
                // 这里has_current_user_key 表示这个key以前是否出现, 如果已经出现过了就不会再进行处理
                // 因为leveldb 里面对相同的key是进行过排序的. 默认squencenumber 最大的排在最前面, 
                // 也就是最新的数据排在最前面.
                if (!has_current_user_key ||
                        user_comparator()->Compare(ikey.user_key,
                            Slice(current_user_key)) != 0) {
                    // First occurrence of this user key
                    current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                    has_current_user_key = true;
                    last_sequence_for_key = kMaxSequenceNumber;
                }

                if (last_sequence_for_key <= compact->smallest_snapshot) {
                    // Hidden by an newer entry for same user key
                    drop = true;    // (A)
                } else if (ikey.type == kTypeDeletion &&
                        ikey.sequence <= compact->smallest_snapshot &&
                        compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
                    // For this user key:
                    // (1) there is no data in higher levels
                    // (2) data in lower levels will have larger sequence numbers
                    // (3) data in layers that are being compacted here and have
                    //     smaller sequence numbers will be dropped in the next
                    //     few iterations of this loop (by rule (A) above).
                    // Therefore this deletion marker is obsolete and can be dropped.
                    drop = true;
                }

                last_sequence_for_key = ikey.sequence;
            }
#if 0
            Log(options_.info_log,
                    "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
                    "%d smallest_snapshot: %d",
                    ikey.user_key.ToString().c_str(),
                    (int)ikey.sequence, ikey.type, kTypeValue, drop,
                    compact->compaction->IsBaseLevelForKey(ikey.user_key),
                    (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

            // 如果不丢掉, 则进行插入
            if (drop == false) {
                // Open output file if necessary
                if (compact->builder == NULL) {
                    status = OpenCompactionOutputFile(compact);
                    if (!status.ok()) {
                        break;
                    }
                }
                if (compact->builder->NumEntries() == 0) {
                    compact->current_output()->smallest.DecodeFrom(key);
                }
                compact->current_output()->largest.DecodeFrom(key);
                compact->builder->Add(key, input->value());

                // Close output file if it is big enough
                if (compact->builder->FileSize() >=
                        compact->compaction->MaxOutputFileSize()) {
                    // 这里不断的生成新的文件, 所以一次Compaction 会生成多个文件
                    status = FinishCompactionOutputFile(compact, input);
                    if (!status.ok()) {
                        break;
                    }
                }
            }

            input->Next();
        }

        if (status.ok() && shutting_down_.Acquire_Load()) {
            status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != NULL) {
            status = FinishCompactionOutputFile(compact, input);
        }
        if (status.ok()) {
            status = input->status();
        }
        delete input;
        input = NULL;

        CompactionStats stats;
        stats.micros = env_->NowMicros() - start_micros - imm_micros;
        for (int which = 0; which < 2; which++) {
            for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
                stats.bytes_read += compact->compaction->input(which, i)->file_size;
            }
        }
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            stats.bytes_written += compact->outputs[i].file_size;
        }

        mutex_.Lock();
        stats_[compact->compaction->level() + 1].Add(stats);

        if (status.ok()) {
            status = InstallCompactionResults(compact);
        }
        VersionSet::LevelSummaryStorage tmp;
        Log(options_.info_log,
                "compacted to: %s", versions_->LevelSummary(&tmp));
        return status;
    }

    namespace {
        struct IterState {
            port::Mutex* mu;
            Version* version;
            MemTable* mem;
            MemTable* imm;
        };

        static void CleanupIteratorState(void* arg1, void* arg2) {
            IterState* state = reinterpret_cast<IterState*>(arg1);
            state->mu->Lock();
            state->mem->Unref();
            if (state->imm != NULL) state->imm->Unref();
            state->version->Unref();
            state->mu->Unlock();
            delete state;
        }
    }  // namespace

    Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
            SequenceNumber* latest_snapshot) {
        IterState* cleanup = new IterState;
        mutex_.Lock();
        *latest_snapshot = versions_->LastSequence();

        // Collect together all needed child iterators
        std::vector<Iterator*> list;
        list.push_back(mem_->NewIterator());
        mem_->Ref();
        if (imm_ != NULL) {
            list.push_back(imm_->NewIterator());
            imm_->Ref();
        }
        versions_->current()->AddIterators(options, &list);
        Iterator* internal_iter =
            NewMergingIterator(&internal_comparator_, &list[0], list.size());
        versions_->current()->Ref();

        cleanup->mu = &mutex_;
        cleanup->mem = mem_;
        cleanup->imm = imm_;
        cleanup->version = versions_->current();
        internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

        mutex_.Unlock();
        return internal_iter;
    }

    Iterator* DBImpl::TEST_NewInternalIterator() {
        SequenceNumber ignored;
        return NewInternalIterator(ReadOptions(), &ignored);
    }

    int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
        MutexLock l(&mutex_);
        return versions_->MaxNextLevelOverlappingBytes();
    }

    Status DBImpl::Get(const ReadOptions& options,
            const Slice& key,
            std::string* value) {
        Status s;
        MutexLock l(&mutex_); //加锁操作
        SequenceNumber snapshot; //这里就是定义一个最新的一个操作号, 有一个全局唯一的SequenceNumber
        if (options.snapshot != NULL) { //如果要求取的是某一个版本的数据
            snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
        } else {
            snapshot = versions_->LastSequence(); //否则就是最新的数据, 也就是当前versions_里面最大的SequenceNumber的数据
        }

        MemTable* mem = mem_; //mem table
        MemTable* imm = imm_; //imm table
        Version* current = versions_->current(); //当前的version
        mem->Ref(); //对mem的引用+1, 这个ref主要是用来删除文件的时候判断, 如果这个ref引用为0了, 那么就可以删除掉.
        if (imm != NULL) imm->Ref(); 
        current->Ref(); //同样对当前版本的ref引用+1

        bool have_stat_update = false; //用来是否有更新, 如果有更新再判断是否启动compaction线程
        Version::GetStats stats;

        // Unlock while reading from files and memtables
        {
            mutex_.Unlock(); //把锁放开, 可以看到 正真加锁部分只有获得当前版本号, 以及获得当前最新的版本这一部分, 也就是说在具体的get操作之前就已经可以支持多个线程进行读取了. 为什么可以这么做呢? 首先获得了当前最新的current以后, 并把这个current 的引用+1, 就可以保证当前的这个version 是不会被删除的, 同样对于当前的这个imm, mem ref+1 以后可以保证是不会呗删除掉得. 所以只要在这段时间锁住就可以. 如果这个时候又有新的key写入, 那么他这个时候写入的key 是一个新的SequenceNumber. 不会影响我们接下来读的结果.


            // First look in the memtable, then in the immutable memtable (if any).
            LookupKey lkey(key, snapshot);
            if (mem->Get(lkey, value, &s)) { //从mem里面读取这个key的 value  这里要注意memtable里面的kv 是如何排序的. 这里面key的排序是 首先按照SquenceNumber排序, 然后是操作类型(删除排在最前面), 然后是key的大小(具体再看一下Compaction里面)
                // Done
            } else if (imm != NULL && imm->Get(lkey, value, &s)) {
                // Done
            } else {
                s = current->Get(options, lkey, value, &stats); //如果mem 和 imm 都找不到, 那么这个时候我们要从一个一个level里面去找.
                have_stat_update = true;
            }
            mutex_.Lock();
        }

        if (have_stat_update && current->UpdateStats(stats)) { // 这里如果有更新, 那么会判断是否启动后台的Compaction() 进程
            MaybeScheduleCompaction();
        }
        mem->Unref(); //分别把 mem, imm, current 的ref - 1
        if (imm != NULL) imm->Unref();
        current->Unref();
        return s;
    }

    Iterator* DBImpl::NewIterator(const ReadOptions& options) {
        SequenceNumber latest_snapshot;
        Iterator* internal_iter = NewInternalIterator(options, &latest_snapshot);
        return NewDBIterator(
                &dbname_, env_, user_comparator(), internal_iter,
                (options.snapshot != NULL
                 ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
                 : latest_snapshot));
    }

    const Snapshot* DBImpl::GetSnapshot() {
        MutexLock l(&mutex_);
        return snapshots_.New(versions_->LastSequence());
    }

    void DBImpl::ReleaseSnapshot(const Snapshot* s) {
        MutexLock l(&mutex_);
        snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
    }

    // Convenience methods
    Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
        return DB::Put(o, key, val);
    }

    Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
        return DB::Delete(options, key);
    }

    Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
        // 这里用到的就是标准的 condition variable 配合 mutex 使用的例子, 
        // 这里在这个while 里面添加的 w != writes_.front() 同时又保证了只有一个写
        // 标准的例子里面, 可以同时有多个写
        Writer w(&mutex_); // 这个w锁是一个条件变量, 传入的mutex_是交给条件变量里面的mu_的
        w.batch = my_batch;
        w.sync = options.sync;
        w.done = false;

        // NICE
        // 这里写的也很精妙, 之所以用MutexLock 来实现, 是因为这样只要在中途退出就会自动
        // 触发这个MutexLock的析构函数, 析构函数里面写了unLock这个锁的操作, 那么就可以不用在
        // 每个中间的return 前面都加上这个l->unLock()操作
        MutexLock l(&mutex_); // 这里的操作是在做pthread_cond_wait之前把mutex_锁住的操作, 这样保证pthread_cond_wait的时候不会死锁
        writers_.push_back(&w);
        while (!w.done && &w != writers_.front()) { //这里用一个队列, 并且只有在队列最头部的那个writeBatch才会被写. 
            w.cv.Wait(); //这里是condition varaible, 这里wait 的时候会同时把mu_这个锁放开
        }
        if (w.done) {
            return w.status;
        }
        // 接下来处理的就是这个writers_ 里面最头的那个的信息

        // May temporarily unlock and wait.
        // 这里是检查memtable有没有空间可以写入, 如果没有就换一个buffer 和 compaction等操作
        Status status = MakeRoomForWrite(my_batch == NULL);
        uint64_t last_sequence = versions_->LastSequence();
        Writer* last_writer = &w;
        if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
            WriteBatch* updates = BuildBatchGroup(&last_writer);
            WriteBatchInternal::SetSequence(updates, last_sequence + 1);
            last_sequence += WriteBatchInternal::Count(updates);

            // Add to log and apply to memtable.  We can release the lock
            // during this phase since &w is currently responsible for logging
            // and protects against concurrent loggers and concurrent writes
            // into mem_.
            {
                //因为到这里的时候 只有一个writers_里面的一个能到达这里. 所以这里可以保证这有一个线程到了可以AddRecord这一步了.
                //所以这里把锁release掉

                mutex_.Unlock();
                status = log_->AddRecord(WriteBatchInternal::Contents(updates));
                if (status.ok() && options.sync) {
                    status = logfile_->Sync();
                }
                if (status.ok()) {
                    status = WriteBatchInternal::InsertInto(updates, mem_);
                }
                mutex_.Lock();
            }
            if (updates == tmp_batch_) tmp_batch_->Clear();

            versions_->SetLastSequence(last_sequence);
        }

        while (true) {
            Writer* ready = writers_.front();
            writers_.pop_front();
            if (ready != &w) {
                ready->status = status;
                ready->done = true;
                ready->cv.Signal();
            }
            if (ready == last_writer) break;
        }

        // Notify new head of write queue
        if (!writers_.empty()) {
            writers_.front()->cv.Signal();
        }

        return status;
    }

    // REQUIRES: Writer list must be non-empty
    // REQUIRES: First writer must have a non-NULL batch
    WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
        assert(!writers_.empty());
        Writer* first = writers_.front();
        WriteBatch* result = first->batch;
        assert(result != NULL);

        size_t size = WriteBatchInternal::ByteSize(first->batch);

        // Allow the group to grow up to a maximum size, but if the
        // original write is small, limit the growth so we do not slow
        // down the small write too much.
        size_t max_size = 1 << 20;
        if (size <= (128<<10)) {
            max_size = size + (128<<10);
        }

        *last_writer = first;
        std::deque<Writer*>::iterator iter = writers_.begin();
        ++iter;  // Advance past "first"
        for (; iter != writers_.end(); ++iter) {
            Writer* w = *iter;
            if (w->sync && !first->sync) {
                // Do not include a sync write into a batch handled by a non-sync write.
                break;
            }

            if (w->batch != NULL) {
                size += WriteBatchInternal::ByteSize(w->batch);
                if (size > max_size) {
                    // Do not make batch too big
                    break;
                }

                // Append to *reuslt
                if (result == first->batch) {
                    // Switch to temporary batch instead of disturbing caller's batch
                    result = tmp_batch_;
                    assert(WriteBatchInternal::Count(result) == 0);
                    WriteBatchInternal::Append(result, first->batch);
                }
                WriteBatchInternal::Append(result, w->batch);
            }
            *last_writer = w;
        }
        return result;
    }

    // REQUIRES: mutex_ is held
    // REQUIRES: this thread is currently at the front of the writer queue
    Status DBImpl::MakeRoomForWrite(bool force) { //在这里的时候, 这个线程在这个Write queue的最前面
        mutex_.AssertHeld(); //这个函数什么都没做....-_-#
        assert(!writers_.empty());
        bool allow_delay = !force;
        Status s;
        while (true) {
            if (!bg_error_.ok()) {
                // Yield previous error
                s = bg_error_;
                break;
            } else if (
                    allow_delay &&
                    versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
                // 这里我们检查一下0层的文件有多少个, 如果这里0层文件的个数大于这个0层文件的个数
                // 那么这里肯定就会触发Compaction这个操作, 所以这个时候就把锁放开, 让其他
                // 的线程优先去进行操作. 所以这里这个线程会sleep 1000ms, 然后在获得这个
                // 锁. 这里只能这样操作一次, 不然这个level 0 层总被停止,让给别人效果肯定
                // 也是不好的
                //
                // We are getting close to hitting a hard limit on the number of
                // L0 files.  Rather than delaying a single write by several
                // seconds when we hit the hard limit, start delaying each
                // individual write by 1ms to reduce latency variance.  Also,
                // this delay hands over some CPU to the compaction thread in
                // case it is sharing the same core as the writer.
                mutex_.Unlock();
                env_->SleepForMicroseconds(1000);
                allow_delay = false;  // Do not delay a single write more than once
                mutex_.Lock();
            } else if (!force &&
                    (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
                // There is room in current memtable
                // 如果当前的memtable 有空间, 那么就可以直接写入
                break;
            } else if (imm_ != NULL) { // ask:: 这里immutable memtable 正在compacted. 
                //所以必须等待她compacted 完才可以. 这里为什么要这样? 如果只是往memtable
                //写入数据, 应该不会影响才是呀.
                //ans:: 因为走到这里可以看出上面的 memtable 已经没有空间了, 而immutable 
                //缺还没compaction 完, 所以必须得wait
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                bg_cv_.Wait();
            } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
                // 这种情况是第一次允许delay以后, 就直接把锁占住, 然后在这边等待它Trigger
                // There are too many level-0 files.
                Log(options_.info_log, "waiting...\n");
                bg_cv_.Wait();
            } else {
                // Attempt to switch to a new memtable and trigger compaction of old
                // 这里做的是如果level 0 文件数比较少, memtable 没有足够的空间, ummutable
                // memtable 还没开始compacted 的时候这里触发 immutable => level 0 文件
                // 的过程
                assert(versions_->PrevLogNumber() == 0);
                uint64_t new_log_number = versions_->NewFileNumber();
                WritableFile* lfile = NULL;
                // 这里levelDB 对env进行了封装, 可以将想要的输出写在任意的文件系统上
                // 这里是新建立了一个level 0 的文件
                s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
                if (!s.ok()) {
                    // Avoid chewing through file number space in a tight loop.
                    versions_->ReuseFileNumber(new_log_number);
                    break;
                }
                delete log_;
                delete logfile_;
                logfile_ = lfile;
                logfile_number_ = new_log_number;
                log_ = new log::Writer(lfile);  //log 指向这个新建立的文件
                imm_ = mem_; //immutable 指向memtable的位置
                has_imm_.Release_Store(imm_);
                mem_ = new MemTable(internal_comparator_); //memtable 指向一块新的内存
                mem_->Ref(); //memtable 的引用+1
                force = false;   // Do not force another compaction if have room
                MaybeScheduleCompaction(); //这里检查以下是否需要compaction, 因为这里有可
                // 能level 0的文件过多.
            }
        }
        return s;
    }

    bool DBImpl::GetProperty(const Slice& property, std::string* value) {
        value->clear();

        MutexLock l(&mutex_);
        Slice in = property;
        Slice prefix("leveldb.");
        if (!in.starts_with(prefix)) return false;
        in.remove_prefix(prefix.size());

        if (in.starts_with("num-files-at-level")) {
            in.remove_prefix(strlen("num-files-at-level"));
            uint64_t level;
            bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
            if (!ok || level >= config::kNumLevels) {
                return false;
            } else {
                char buf[100];
                snprintf(buf, sizeof(buf), "%d",
                        versions_->NumLevelFiles(static_cast<int>(level)));
                *value = buf;
                return true;
            }
        } else if (in == "stats") {
            char buf[200];
            snprintf(buf, sizeof(buf),
                    "                               Compactions\n"
                    "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                    "--------------------------------------------------\n"
                    );
            value->append(buf);
            for (int level = 0; level < config::kNumLevels; level++) {
                int files = versions_->NumLevelFiles(level);
                if (stats_[level].micros > 0 || files > 0) {
                    snprintf(
                            buf, sizeof(buf),
                            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                            level,
                            files,
                            versions_->NumLevelBytes(level) / 1048576.0,
                            stats_[level].micros / 1e6,
                            stats_[level].bytes_read / 1048576.0,
                            stats_[level].bytes_written / 1048576.0);
                    value->append(buf);
                }
            }
            return true;
        } else if (in == "sstables") {
            *value = versions_->current()->DebugString();
            return true;
        }

        return false;
    }

    void DBImpl::GetApproximateSizes(
            const Range* range, int n,
            uint64_t* sizes) {
        // TODO(opt): better implementation
        Version* v;
        {
            MutexLock l(&mutex_);
            versions_->current()->Ref();
            v = versions_->current();
        }

        for (int i = 0; i < n; i++) {
            // Convert user_key into a corresponding internal key.
            InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
            InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
            uint64_t start = versions_->ApproximateOffsetOf(v, k1);
            uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
            sizes[i] = (limit >= start ? limit - start : 0);
        }

        {
            MutexLock l(&mutex_);
            v->Unref();
        }
    }

    // Default implementations of convenience methods that subclasses of DB
    // can call if they wish
    Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
        WriteBatch batch;
        batch.Put(key, value);
        return Write(opt, &batch);
    }

    Status DB::Delete(const WriteOptions& opt, const Slice& key) {
        WriteBatch batch;
        batch.Delete(key);
        return Write(opt, &batch);
    }

    DB::~DB() { }

    Status DB::Open(const Options& options, const std::string& dbname,
            DB** dbptr) {
        *dbptr = NULL;

        DBImpl* impl = new DBImpl(options, dbname);
        impl->mutex_.Lock();
        VersionEdit edit;
        Status s = impl->Recover(&edit); // Handles create_if_missing, error_if_exists
        if (s.ok()) {
            uint64_t new_log_number = impl->versions_->NewFileNumber();
            WritableFile* lfile;
            s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                    &lfile);
            if (s.ok()) {
                edit.SetLogNumber(new_log_number);
                impl->logfile_ = lfile;
                impl->logfile_number_ = new_log_number;
                impl->log_ = new log::Writer(lfile);
                s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
            }
            if (s.ok()) {
                impl->DeleteObsoleteFiles();
                impl->MaybeScheduleCompaction();
            }
        }
        impl->mutex_.Unlock();
        if (s.ok()) {
            *dbptr = impl;
        } else {
            delete impl;
        }
        return s;
    }

    Snapshot::~Snapshot() {
    }

    Status DestroyDB(const std::string& dbname, const Options& options) {
        Env* env = options.env;
        std::vector<std::string> filenames;
        // Ignore error in case directory does not exist
        env->GetChildren(dbname, &filenames);
        if (filenames.empty()) {
            return Status::OK();
        }

        FileLock* lock;
        const std::string lockname = LockFileName(dbname);
        Status result = env->LockFile(lockname, &lock);
        if (result.ok()) {
            uint64_t number;
            FileType type;
            for (size_t i = 0; i < filenames.size(); i++) {
                if (ParseFileName(filenames[i], &number, &type) &&
                        type != kDBLockFile) {  // Lock file will be deleted at end
                    Status del = env->DeleteFile(dbname + "/" + filenames[i]);
                    if (result.ok() && !del.ok()) {
                        result = del;
                    }
                }
            }
            env->UnlockFile(lock);  // Ignore error since state is already gone
            env->DeleteFile(lockname);
            env->DeleteDir(dbname);  // Ignore error in case dir contains other files
        }
        return result;
    }

}  // namespace leveldb
