// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "binlog.h"

#include <iostream>
#include <string>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>

#include <glog/logging.h>

#include "slash/include/slash_mutex.h"


/*
 * Binlog
 */
Binlog::Binlog(const std::string& binlog_path, const int file_size) :
    version_(NULL),
    versionfile_(NULL),
    pro_num_(0),
    binlog_path_(binlog_path),
    file_size_(file_size) {

  // To intergrate with old version, we don't set mmap file size to 100M;
  //slash::SetMmapBoundSize(file_size);
  //slash::kMmapBoundSize = 1024 * 1024 * 100;

  Status s;

  slash::CreateDir(binlog_path_);

  filename = binlog_path_ + kBinlogPrefix;
  const std::string manifest = binlog_path_ + kManifest;
  std::string profile;

  if (!slash::FileExists(manifest)) {
    DLOG(INFO) << "Binlog: Manifest file not exist";

    s = slash::NewRWFile(manifest, &versionfile_);
    if (!s.ok()) {
      LOG(WARNING) << "Binlog: new versionfile error " << s.ToString();
    }

    version_ = new Version(versionfile_);
    version_->StableSave();
  } else {

    s = slash::NewRWFile(manifest, &versionfile_);
    if (s.ok()) {
      version_ = new Version(versionfile_);
      version_->Init();
      pro_num_ = version_->pro_num_;

      // Debug
      //version_->debug();
    } else {
      LOG(WARNING) << "Binlog: open versionfile error";
    }

  }

}

Binlog::~Binlog() {
  delete version_;
  delete versionfile_;
}


Status Binlog::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset) {
  slash::RWLock(&(version_->rwlock_), false);

  *filenum = version_->pro_num_;
  *pro_offset = version_->pro_offset_;
  //*filenum = version_->pro_num();
  //*pro_offset = version_->pro_offset();

  return Status::OK();
}


std::string NewFileName(const std::string name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
}

/*
 * Version
 */
Version::Version(slash::RWFile *save)
  : pro_offset_(0),
    pro_num_(0),
    save_(save) {
  assert(save_ != NULL);

  pthread_rwlock_init(&rwlock_, NULL);
}

Version::~Version() {
  StableSave();
  pthread_rwlock_destroy(&rwlock_);
}

Status Version::StableSave() {
  char *p = save_->GetData();
  memcpy(p, &pro_offset_, sizeof(uint64_t));
  p += 20;
  //memcpy(p, &con_offset_, sizeof(uint64_t));
  //p += 8;
  //memcpy(p, &item_num_, sizeof(uint32_t));
  //p += 4;
  memcpy(p, &pro_num_, sizeof(uint32_t));
  //p += 4;
  //memcpy(p, &con_num_, sizeof(uint32_t));
  //p += 4;
  return Status::OK();
}

Status Version::Init() {
  Status s;
  if (save_->GetData() != NULL) {
    memcpy((char*)(&pro_offset_), save_->GetData(), sizeof(uint64_t));
    //memcpy((char*)(&con_offset_), save_->GetData() + 8, sizeof(uint64_t));
    memcpy((char*)(&item_num_), save_->GetData() + 16, sizeof(uint32_t));
    memcpy((char*)(&pro_num_), save_->GetData() + 20, sizeof(uint32_t));
    //memcpy((char*)(&con_num_), save_->GetData() + 24, sizeof(uint32_t));
    // DLOG(INFO) << "Version Init pro_offset "<< pro_offset_ << " itemnum " << item_num << " pro_num " << pro_num_ << " con_num " << con_num_;
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
}



