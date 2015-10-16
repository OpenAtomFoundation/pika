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

Mario::Mario(int32_t retry)
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
    mario_path_("./log")
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
    env_->StartThread(&Mario::SplitLogWork, this);

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
    std::vector<ConsumerItem*>::iterator iter;
    for (iter = consumers_.begin(); iter != consumers_.end(); iter++) {
        delete (*iter);
    }

    delete version_;
    delete versionfile_;

    delete writefile_;
    // delete env_;
}

Status Mario::AddConsumer(uint32_t filenum, uint64_t con_offset, Consumer::Handler* h) {
    std::string confile = NewFileName(filename_, filenum);
    SequentialFile *readfile;
    Status s = env_->AppendSequentialFile(confile, &readfile);
    if (!s.ok()){
        return s;
    }
    Consumer* consumer = new Consumer(readfile, h, con_offset, filenum);
    if (consumer->trim() == 0) {
        ConsumerItem* consumer_item = new ConsumerItem(readfile, consumer, h);
        consumers_.push_back(consumer_item);
        mutex_.Lock();
        consumer_num_++;
        mutex_.Unlock();
        env_->set_thread_num(consumer_num_);
        arg_ = { this, consumer_item };
        env_->StartThread(&Mario::BGWork, (void*)&arg_);
        return Status::OK();
    } else {
        return Status::NotFound("bad consumer");
    }
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
        uint64_t filesize = writefile_->Filesize();
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
        sleep(1);
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
    while (1) {
        {
        mutex_.Lock();
        while (consumer_item->consumer_->filenum() == version_->pronum() &&
                consumer_item->consumer_->con_offset() == version_->pro_offset()) {
            if (exit_all_consume_) {
                consumer_num_--;
                mutex_.Unlock();
                pthread_exit(NULL);
            }
            mutex_.Unlock();
            sleep(1);
            mutex_.Lock();
        }
//        log_info("filenum: %ld, con_offset: %ld", consumer_item->consumer_->filenum(), consumer_item->consumer_->con_offset());
//        std::cout<<"filenum: "<<consumer_item->consumer_->filenum()<< ", con_offset: "<<consumer_item->consumer_->con_offset()<<std::endl;
        //std::cout<<"1 --> con_offset: "<<consumer_item->consumer_->con_offset()<<" pro_offset: "<<version_->pro_offset()<<std::endl;
        scratch = "";
        s = consumer_item->consumer_->Consume(scratch);
        while (!s.ok()) {
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
            while (consumer_item->h_->processMsg(scratch)) {
            }
        } else {
            int retry = retry_ - 1;
            while (!consumer_item->h_->processMsg(scratch) && retry--) {
            }
            if (retry <= 0) {
                log_warn("message retry %d time still error %s", retry_, scratch.c_str());
            }
        }

        }
    }
    return ;
}

Status Mario::Put(const std::string &item)
{
    Status s;

    {
    MutexLock l(&mutex_);
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
