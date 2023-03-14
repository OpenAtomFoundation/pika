#include "chrono"
#include "ctime"
#include "iomanip"
#include "iostream"

#include <glog/logging.h>

#include "nemo.h"
#include "slash/include/env.h"
#include "blackwidow/blackwidow.h"

#include "migrator.h"
#include "progress_thread.h"
#include "classify_thread.h"

int32_t thread_num = 6;
int32_t need_write_log = 0;
int32_t max_batch_limit = 512;
std::string nemo_db_path;
std::string blackwidow_db_path;

slash::Mutex mutex;
ProgressThread* progress_thread;
std::vector<Migrator*> migrators;
std::vector<ClassifyThread*> classify_threads;

void PrintInfo(const std::time_t& now) {
  std::cout << "================== Nemo To Blackwidow ==================" << std::endl;
  std::cout << "Thread_num : " << thread_num << std::endl;
  std::cout << "Need write log : " << (need_write_log ? "yes" : "no") << std::endl;
  std::cout << "Max batch limit : " << max_batch_limit << std::endl;
  std::cout << "Nemo_db_path : " << nemo_db_path << std::endl;
  std::cout << "Blackwidow_db_path : " << blackwidow_db_path << std::endl;
  std::cout << "Startup Time : " << asctime(localtime(&now));
  std::cout << "========================================================" << std::endl;
}

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "\tNemo_to_Blackwidow reads data from Nemo DB and send to Blackwidow DB" << std::endl;
  std::cout << "\t-h    -- displays this help information and exits" << std::endl;
  std::cout << "\t-n    -- numbers of migrator, default = 6" << std::endl;
  std::cout << "\t-l    -- whether write log, default = 0"<< std::endl;
  std::cout << "\t-b    -- number of members in multiple data structures per migration, default = 512" << std::endl;
  std::cout << "\texample: ./nemo_to_blackwidow ./nemo_db ./blackwidow_db -n 10 -l 0 -b 512" << std::endl;
}

static void GlogInit() {
  if (!slash::FileExists("./log")) {
    slash::CreatePath("./log");
  }

  FLAGS_log_dir = "./log";
  FLAGS_max_log_size = 2048;   // log file 2GB
  ::google::InitGoogleLogging("nemo_to_blackwidow");
}

int main(int argc, char **argv) {
  if (argc != 3
    && argc != 5
    && argc != 7
    && argc != 9) {
    Usage();
    exit(-1);
  }

  nemo_db_path = std::string(argv[1]);
  blackwidow_db_path = std::string(argv[2]);

  if (argc >= 5) {
    if (std::string(argv[3]) == "-n") {
      thread_num = atoi(argv[4]);
    } else {
      Usage();
      exit(-1);
    }
  }
  if (argc >= 7) {
    if (std::string(argv[5]) == "-l") {
      need_write_log = atoi(argv[6]);
    } else {
      Usage();
      exit(-1);
    }
  }
  if (argc >= 9) {
    if (std::string(argv[7]) == "-b") {
      max_batch_limit = atoi(argv[8]);
    } else {
      Usage();
      exit(-1);
    }
  }

  if (need_write_log) {
    GlogInit();
  }

  std::chrono::system_clock::time_point start_time = std::chrono::system_clock::now();
  std::time_t now = std::chrono::system_clock::to_time_t(start_time);
  PrintInfo(now);

  // Init nemo db
  nemo::Options nemo_option;
  nemo_option.create_if_missing = false;
  nemo_option.write_buffer_size = 256 * 1024 * 1024;           // 256M
  nemo_option.target_file_size_base = 20 * 1024 * 1024;        // 20M
  nemo::Nemo* nemo_db = new nemo::Nemo(nemo_db_path, nemo_option);
  if (nemo_db != NULL) {
    std::cout << "Open Nemo db success..." << std::endl;
  } else {
    std::cout << "Open Nemo db failed..." << std::endl;
    return -1;
  }

  // Init blackwidow db
  rocksdb::Status status;
  blackwidow::BlackwidowOptions bw_option;
  bw_option.options.create_if_missing = true;
  bw_option.options.write_buffer_size = 256 * 1024 * 1024;     // 256M
  bw_option.options.target_file_size_base = 20 * 1024 * 1024;  // 20M
  blackwidow::BlackWidow* blackwidow_db = new blackwidow::BlackWidow();
  if (blackwidow_db != NULL
    && (status = blackwidow_db->Open(bw_option, blackwidow_db_path)).ok()) {
    std::cout << "Open BlackWidow db success..." << std::endl;
  } else {
    std::cout << "Open BlackWidow db failed..." << std::endl;
    return -1;
  }


  for (size_t idx = 0; idx < static_cast<size_t>(thread_num); ++idx) {
    migrators.push_back(new Migrator(idx, nemo_db, blackwidow_db));
  }

  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::KV_DB));
  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::HASH_DB));
  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::LIST_DB));
  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::SET_DB));
  classify_threads.push_back(new ClassifyThread(nemo_db, migrators, nemo::ZSET_DB));

  progress_thread = new ProgressThread(&classify_threads);

  std::cout << "Start migrating data from Nemo to Blackwidow..." << std::endl;
  for (size_t idx = 0; idx < static_cast<size_t>(thread_num); ++idx) {
    migrators[idx]->StartThread();
  }

  for (size_t idx = 0; idx < classify_threads.size(); ++idx) {
    classify_threads[idx]->StartThread();
  }

  progress_thread->StartThread();
  progress_thread->JoinThread();
  delete progress_thread;

  for (size_t idx = 0; idx < classify_threads.size(); ++idx) {
    classify_threads[idx]->JoinThread();
    delete classify_threads[idx];
  }

  for (size_t idx = 0; idx < static_cast<size_t>(thread_num); ++idx) {
    migrators[idx]->SetShouldExit();
  }

  for (size_t idx = 0; idx < static_cast<size_t>(thread_num); ++idx) {
    migrators[idx]->JoinThread();
    delete migrators[idx];
  }

  delete nemo_db;
  delete blackwidow_db;

  std::chrono::system_clock::time_point end_time = std::chrono::system_clock::now();
  now = std::chrono::system_clock::to_time_t(end_time);
  std::cout << "Finish Time : " << asctime(localtime(&now));

  auto hours = std::chrono::duration_cast<std::chrono::hours>(end_time - start_time).count();
  auto minutes = std::chrono::duration_cast<std::chrono::minutes>(end_time - start_time).count();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

  std::cout << "Total Time Cost : "
            << hours << " hours "
            << minutes % 60 << " minutes "
            << seconds % 60 << " seconds "
            << std::endl;
  return 0;
}
