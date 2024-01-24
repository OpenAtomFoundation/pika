//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <thread>

#include "pstd/include/pika_conf.h"
#include "src/lock_mgr.h"
#include "src/mutex_impl.h"

using namespace storage;

std::unique_ptr<PikaConf> g_pika_conf;

void Func(LockMgr* mgr, int id, const std::string& key) {
  mgr->TryLock(key);
  printf("thread %d TryLock %s success\n", id, key.c_str());
  std::this_thread::sleep_for(std::chrono::seconds(3));
  mgr->UnLock(key);
  printf("thread %d UnLock %s\n", id, key.c_str());
}

int main() {
  std::string pika_conf_path = "./pika.conf";
#ifdef PIKA_ROOT_DIR
  pika_conf_path = PIKA_ROOT_DIR;
  pika_conf_path += "/tests/conf/pika.conf";
#endif
  LOG(WARNING) << "pika_conf_path: " << pika_conf_path;
  g_pika_conf = std::make_unique<PikaConf>(pika_conf_path);
  if (g_pika_conf->Load()) {
    printf("pika load conf error\n");
    return 0;
  }
  std::shared_ptr<MutexFactory> factory = std::make_shared<MutexFactoryImpl>();
  LockMgr mgr(1, 3, factory);

  std::thread t1(Func, &mgr, 1, "key_1");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::thread t2(Func, &mgr, 2, "key_2");
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::thread t3(Func, &mgr, 3, "key_3");
  std::thread t4(Func, &mgr, 4, "key_4");

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto s = mgr.TryLock("key_1");
  printf("thread main TryLock key_1 ret %s\n", s.ToString().c_str());
  mgr.UnLock("key_1");
  printf("thread main UnLock key_1\n");

  t1.join();
  t2.join();
  t3.join();
  t4.join();
  return 0;
}
