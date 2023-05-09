//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "chrono"
#include "iostream"
#include "unistd.h"

#include "blackwidow/blackwidow.h"

#include "progress_thread.h"
#include "scan_thread.h"
#include "write_thread.h"

int32_t scan_batch_limit = 256;
std::string blackwidow_db_path;
std::string target_file;

using std::chrono::high_resolution_clock;

void PrintInfo(const std::time_t& now) {
  std::cout << "===================== Pika To Txt ======================" << std::endl;
  std::cout << "Blackwidow_db_path : " << blackwidow_db_path << std::endl;
  std::cout << "Target_file_path : " << target_file << std::endl;
  std::cout << "Scan_batch_limit : " << scan_batch_limit << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now));
  std::cout << "========================================================" << std::endl;
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "\tPika_To_Txt reads kv data from Blackwidow DB and write to file" << std::endl;
  std::cout << "\t-h    -- displays this help information and exits" << std::endl;
  std::cout << "\t-b    -- the upper limit for each scan, default = 256" << std::endl;
  std::cout << "\texample: ./pika_to_txt ./blackwidow_db ./data.txt" << std::endl;
}

int main(int argc, char** argv) {
  if (argc != 3 && argc != 5) {
    Usage();
    exit(-1);
  }

  blackwidow_db_path = std::string(argv[1]);
  target_file = std::string(argv[2]);

  if (argc >= 5) {
    if (std::string(argv[3]) == "-b") {
      scan_batch_limit = atoi(argv[4]);
    } else {
      Usage();
      exit(-1);
    }
  }

  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  // Init db
  rocksdb::Status status;
  blackwidow::BlackwidowOptions bw_option;
  bw_option.options.create_if_missing = true;
  bw_option.options.write_buffer_size = 256 * 1024 * 1024;     // 256M
  bw_option.options.target_file_size_base = 20 * 1024 * 1024;  // 20M
  blackwidow::BlackWidow* blackwidow_db = new blackwidow::BlackWidow();
  if (blackwidow_db != NULL && (status = blackwidow_db->Open(bw_option, blackwidow_db_path)).ok()) {
    std::cout << "Open BlackWidow db success..." << std::endl;
  } else {
    std::cout << "Open BlackWidow db failed..." << std::endl;
    return -1;
  }

  std::cout << "Start migrating data from Blackwidow db to " << target_file << "..." << std::endl;

  WriteThread* write_thread = new WriteThread(target_file);
  ScanThread* scan_thread = new ScanThread(write_thread, blackwidow_db);
  ProgressThread* progress_thread = new ProgressThread(scan_thread);

  write_thread->StartThread();
  // wait for write thread open file success
  sleep(1);
  scan_thread->StartThread();
  progress_thread->StartThread();

  progress_thread->JoinThread();
  scan_thread->JoinThread();
  write_thread->Stop();
  write_thread->JoinThread();

  delete blackwidow_db;
  delete write_thread;
  delete scan_thread;
  delete progress_thread;

  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  std::cout << "Finish Time : " << asctime(localtime(&now));

  auto hours = std::chrono::duration_cast<std::chrono::hours>(end_time - start_time).count();
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(end_time - start_time).count();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

  std::cout << "Total Time Cost : " << hours << " hours " << minutes % 60 << " minutes " << seconds % 60 << " seconds "
            << std::endl;

  return 0;
}
