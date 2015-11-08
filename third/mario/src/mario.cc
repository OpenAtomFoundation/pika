#include "mario.h"
#include "producer.h"
#include "consumer.h"
#include "version.h"
#include "mutexlock.h"
#include "port.h"
#include "filename.h"
#include "signal.h"

#include <iostream>
#include <string>
#include <stdint.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

namespace mario {

struct Mario::Writer {
    Status status;
    bool done;
    port::CondVar cv;

    explicit Writer(port::Mutex* mu) : cv(mu) { }
};

Mario::Mario(const char* mario_path, int32_t retry)
    : consumer_num_(0),
    item_num_(0),
    env_(Env::Default()),
    writefile_(NULL),
    versionfile_(NULL),
    version_(NULL),
//    bg_cv_(&mutex_),
    pronum_(0),
    retry_(retry),
    arg_({NULL, NULL}),
    pool_(NULL),
    exit_all_consume_(false),
    mario_path_(mario_path)
{
//    env_->set_thread_num(consumer_num_);
    filename_ = mario_path_ + kWrite2file;
    const std::string manifest = mario_path_ + kManifest;
    std::string profile;
    env_->CreateDir(mario_path_);
    Status s;
    if (!env_->FileExists(manifest)) {
        log_info("Manifest file not exist");
        profile = NewFileName(filename_, pronum_);
        env_->NewWritableFile(profile, &writefile_);
        env_->NewRWFile(manifest, &versionfile_);
        if (versionfile_ == NULL) {
            log_warn("new versionfile error");
        }
        version_ = new Version(versionfile_);
        version_->StableSave();
    } else {
        log_info("Find the exist file ");
        s = env_->NewRWFile(manifest, &versionfile_);
        if (s.ok()) {
            version_ = new Version(versionfile_);
            version_->InitSelf();
            pronum_ = version_->pronum();
            version_->debug();
        } else {
            log_warn("new REFile error");
        }
        profile = NewFileName(filename_, pronum_);
        log_info("profile %s", profile.c_str());
        env_->AppendWritableFile(profile, &writefile_, version_->pro_offset());
        uint64_t filesize = writefile_->Filesize();
        log_info("filesize %" PRIu64 "", filesize);
    }

    producer_ = new Producer(writefile_, version_);
    //env_->StartThread(&Mario::SplitLogWork, this);

}

Mario::~Mario()
{
//    mutex_.Lock();
    exit_all_consume_ = true;
//    bg_cv_.SignalAll();
//    while (consumer_num_ > 0) {
//        bg_cv_.Wait();
//        std::cout<<"wait thread exit"<<std::endl;
//        sleep(1);
//    }
    env_->WaitForJoin();
//    mutex_.Unlock();

    delete producer_;
  //  std::list<ConsumerItem*>::iterator iter;
  //  for (iter = consumers_.begin(); iter != consumers_.end(); iter++) {
  //      delete (*iter);
  //  }

    delete version_;
    delete versionfile_;

    delete writefile_;
    // delete env_;
}

Status Mario::AddConsumer(uint32_t filenum, uint64_t con_offset, Consumer::Handler* h, int fd) {
    std::string confile = NewFileName(filename_, filenum);
    SequentialFile *readfile;
    Status s = env_->AppendSequentialFile(confile, &readfile);
    if (!s.ok()){
        return s;
    }
    Consumer* consumer = new Consumer(readfile, h, con_offset, filenum);
    if (consumer->trim() == 0) {
        
        ConsumerItem* consumer_item = new ConsumerItem(readfile, consumer, h, fd);
        //env_->set_thread_num(consumer_num_);
        arg_ = { this, consumer_item };
        pthread_t tid = env_->StartThread(&Mario::BGWork, (void*)&arg_);
        consumer_item->tid_ = tid;

        mutex_.Lock();
        consumer_num_++;
        consumers_.push_back(consumer_item);
        mutex_.Unlock();
        return Status::OK();
    } else {
        return Status::NotFound("bad consumer");
    }
}

Status Mario::RemoveConsumer(int fd) {
    std::list<ConsumerItem *>::iterator it;

    for (it = consumers_.begin(); it != consumers_.end(); it++) {
      if ((*it)->fd_ == fd) {
          (*it)->SetExit();

          void* pret;

          int err = pthread_join((*it)->tid_, &pret);
          if (err != 0) {
              std::string msg = "can't join thread " + std::string(strerror(err));
              return Status::Corruption(msg);
          }
          mutex_.Lock();
          consumer_num_--;
          consumers_.erase(it);
          mutex_.Unlock();

          return Status::OK();
      }
    }
    return Status::NotFound("");
}

// TODO Skip con_offset
Status Mario::SetConsumer(int fd, uint32_t filenum, uint64_t con_offset) {
    std::list<ConsumerItem *>::iterator it;
    for (it = consumers_.begin(); it != consumers_.end(); it++) {
      if ((*it)->fd_ == fd) {
        ConsumerItem *c = *it;
        std::string confile = NewFileName(filename_, filenum);

        //if (env_->FileExists(confile)) {
            SequentialFile *readfile;
            Status s = env_->AppendSequentialFile(confile, &readfile);
            if (!s.ok()){
                return s;
            }

            mutex_.Lock();
            delete c->readfile_;
            c->readfile_ = readfile;

            delete c->consumer_;
            c->consumer_ = new Consumer(c->readfile_, c->h_, 0, filenum);
            int ret = c->consumer_->trim();
            mutex_.Unlock();

            if (ret != 0) {
                return Status::InvalidArgument("invalid offset");
            }
            return Status::OK();
        //} else {
        //    return Status::InvalidArgument();
        //}
      }
    }
    return Status::NotFound("");
}

Status Mario::GetConsumerStatus(int fd, uint32_t *filenum, uint64_t *con_offset) {
    std::list<ConsumerItem *>::iterator it;
    for (it = consumers_.begin(); it != consumers_.end(); it++) {
      if ((*it)->fd_ == fd) {
          *filenum = (*it)->consumer_->filenum();
          *con_offset = (*it)->consumer_->con_offset();

          return Status::OK();
      }
    }
    return Status::NotFound("");
}

Status Mario::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset) {
    MutexLock l(&mutex_);
    *filenum = version_->pronum();
    *pro_offset = version_->pro_offset();

    return Status::OK();
}

Status Mario::AppendBlank(WritableFile *file, uint64_t len) {
    uint64_t pos = 0;
    std::string blank(kBlockSize, ' ');
    for (; pos + kBlockSize < len; pos += kBlockSize) {
        file->Append(Slice(blank.data(), blank.size()));
    }

    // Append a msg which occupy the remain part of the last block
    uint32_t n = (uint32_t) ((len % kBlockSize) - kHeaderSize);

    char buf[kBlockSize];
    buf[0] = static_cast<char>(n & 0xff);
    buf[1] = static_cast<char>((n & 0xff00) >> 8);
    buf[2] = static_cast<char>(n >> 16);
    buf[3] = static_cast<char>(kFullType);

    Status s = file->Append(Slice(buf, kHeaderSize));
    if (s.ok()) {
        s = file->Append(Slice(blank.data(), n));
        if (s.ok()) {
            s = file->Flush();
        }
    }
    return s;
}

Status Mario::SetProducerStatus(uint32_t pronum, uint64_t pro_offset) {
    MutexLock l(&mutex_);

    std::string profile = NewFileName(filename_, pronum);

    if (writefile_ != NULL) {
        delete writefile_;
    }

    if (!env_->FileExists(profile)) {
        env_->NewWritableFile(profile, &writefile_);
        Mario::AppendBlank(writefile_, pro_offset);
        //std::string blank(pro_offset, ' ');
        //writefile_->Append(Slice(blank.data(), blank.size()));
    } else {
        env_->AppendWritableFile(profile, &writefile_, pro_offset);
    }

    version_->set_pronum(pronum);
    version_->set_pro_offset(pro_offset);

    version_->StableSave();

    if (producer_ != NULL) {
        delete producer_;
    }
    producer_ = new Producer(writefile_, version_);
    return Status::OK();
}

void Mario::SplitLogWork(void *m)
{
    reinterpret_cast<Mario*>(m)->SplitLogCall();
}

void Mario::SplitLogCall()
{
    Status s;
    std::string profile;
    while (1) {
        if (exit_all_consume_) {
            pthread_exit(NULL);
        }


        uint64_t filesize;
        {
            MutexLock l(&mutex_);
            filesize = writefile_->Filesize();
        }
        // log_info("filesize %llu kMmapSize %llu", filesize, kMmapSize);
        if (filesize > kMmapSize) {
            {
            MutexLock l(&mutex_);
            delete producer_;
            delete writefile_;
            pronum_++;
            profile = NewFileName(filename_, pronum_);
            env_->NewWritableFile(profile, &writefile_);
            version_->set_pro_offset(0);
            version_->set_pronum(pronum_);
            version_->StableSave();
            version_->debug();
            producer_ = new Producer(writefile_, version_);

            }
        }
        usleep(10);
    }
}


void Mario::BGWork(void *m)
{
//    reinterpret_cast<Mario*>(m)->BackgroundCall();
    void* ptr = ((ThreadArg*)m)->ptr;
    ConsumerItem* consumer_item = ((ThreadArg*)m)->consumer_item;
    reinterpret_cast<Mario*>(ptr)->BackgroundCall(consumer_item);
}

void Mario::BackgroundCall(ConsumerItem* consumer_item)
{
    std::string scratch("");
    Status s;
    while (!consumer_item->IsExit()) {
      {
        mutex_.Lock();
        bool flag = false;
        while (consumer_item->consumer_->filenum() == version_->pronum() &&
                consumer_item->consumer_->con_offset() == version_->pro_offset()) {
            if (consumer_item->IsExit() || exit_all_consume_) {
                flag = true;
                mutex_.Unlock();
                break;
            }
            mutex_.Unlock();
            sleep(1);
            mutex_.Lock();
        }
        if (flag) break;

//        log_info("filenum: %ld, con_offset: %ld", consumer_item->consumer_->filenum(), consumer_item->consumer_->con_offset());
//        std::cout<<"filenum: "<<consumer_item->consumer_->filenum()<< ", con_offset: "<<consumer_item->consumer_->con_offset()<<std::endl;
        //std::cout<<"1 --> con_offset: "<<consumer_item->consumer_->con_offset()<<" pro_offset: "<<version_->pro_offset()<<std::endl;
        scratch = "";
        s = consumer_item->consumer_->Consume(scratch);
        while (!consumer_item->IsExit() && !s.ok()) {
            std::string confile = NewFileName(filename_, consumer_item->consumer_->filenum() + 1);
            if (s.IsEndFile() && env_->FileExists(confile)) {
//                log_info("end of file");
                delete consumer_item->readfile_;
                env_->AppendSequentialFile(confile, &(consumer_item->readfile_));
                uint32_t last_filenum = consumer_item->consumer_->filenum();
                delete consumer_item->consumer_;
                consumer_item->consumer_ = new Consumer(consumer_item->readfile_, consumer_item->h_, 0, last_filenum+1);
                s = consumer_item->consumer_->Consume(scratch);
                break;
            } else {
                mutex_.Unlock();
                sleep(1);
                mutex_.Lock();
            }
            s = consumer_item->consumer_->Consume(scratch);
        }
        mutex_.Unlock();
        if (retry_ == -1) {
            while (!consumer_item->IsExit() && consumer_item->h_->processMsg(scratch)) {
            }
        } else {
            int retry = retry_ - 1;
            while (!consumer_item->IsExit() && !consumer_item->h_->processMsg(scratch) && retry--) {
            }
            if (retry <= 0) {
                log_warn("message retry %d time still error %s", retry_, scratch.c_str());
            }
        }
      }
    }

    delete consumer_item;
    pthread_exit(NULL);
}

Status Mario::Put(const std::string &item)
{
    Status s;

    {
    MutexLock l(&mutex_);

    /* Check to roll log file */
    uint64_t filesize = writefile_->Filesize();
    //log_info("filesize %llu kMmapSize %llu\n", filesize, kMmapSize);
    if (filesize > kMmapSize) {
        //log_info("roll file filesize %llu kMmapSize %llu\n", filesize, kMmapSize);
        delete producer_;
        delete writefile_;
        pronum_++;
        std::string profile = NewFileName(filename_, pronum_);
        env_->NewWritableFile(profile, &writefile_);
        version_->set_pro_offset(0);
        version_->set_pronum(pronum_);
        version_->StableSave();
        version_->debug();
        producer_ = new Producer(writefile_, version_);
    }

    s = producer_->Produce(Slice(item.data(), item.size()));
    if (s.ok()) {
        version_->plus_item_num();
        version_->StableSave();
    }

    }
//    bg_cv_.Signal();
    return s;
}

Status Mario::Put(const char* item, int len)
{
    Status s;

    {
    MutexLock l(&mutex_);

    /* Check to roll log file */
    uint64_t filesize = writefile_->Filesize();
    if (filesize > kMmapSize) {
        //log_info("roll file filesize %llu kMmapSize %llu\n", filesize, kMmapSize);
        delete producer_;
        delete writefile_;
        pronum_++;
        std::string profile = NewFileName(filename_, pronum_);
        env_->NewWritableFile(profile, &writefile_);
        version_->set_pro_offset(0);
        version_->set_pronum(pronum_);
        version_->StableSave();
        version_->debug();
        producer_ = new Producer(writefile_, version_);
    }


    s = producer_->Produce(Slice(item, len));
    if (s.ok()) {
        version_->plus_item_num();
        version_->StableSave();

    }

    }
//    bg_cv_.Signal();
    return s;
}


} // namespace mario
