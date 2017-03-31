#include <iostream>
#include <thread>

#include "SSDB_client.h"
#include "nemo.h"

const int kBatchLen = 1000;
const int kSplitNum = 500000;

void MigrateKv(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db,
    const std::string& start = "", const std::string& end = "") {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Kv client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Kv client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::cout << std::this_thread::get_id() << ", Kv client start to migrate, from " << start << " to " << end << std::endl;

  std::vector<std::string> kvs;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  std::string prev_start = start;
  while (true) {
    kvs.clear();
    status_ssdb = client->scan(prev_start, end, kBatchLen, &kvs);

    if(!status_ssdb.ok() && kvs.size() % 2 != 0) {
      std::cout << "kv client scan error" << std::endl;
      break;
    }
    if (kvs.size() == 0) {
      break;
    }
    for (auto iter = kvs.begin(); iter != kvs.end(); iter+=2) {
//      std::cout << "set " << *iter << " " << *(iter+1) << std::endl;
      status_nemo = db->Set(*iter, *(iter+1));
      if (!status_nemo.ok()) {
        std::cout << "Kv client set error, key: " << *iter << std::endl;
      }
    }
    prev_start = kvs[kvs.size() - 2];
  }

  std::cout << std::this_thread::get_id() << ", Kv client done" << std::endl;
  
}

void MigrateHash(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db,
    std::vector<std::string> keys) {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Hash client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Hash client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::cout << std::this_thread::get_id() << ", Hash client start to migrate, " << keys.size() << " keys, from " << keys.front() << " to " << keys.back() << std::endl;

  std::vector<std::string> fvs;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  std::string prev_start_field = "";
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    prev_start_field = "";
    while (true) {
      fvs.clear();
      status_ssdb = client->hscan(*iter, prev_start_field, "", kBatchLen, &fvs);
      if (!status_ssdb.ok() || fvs.size() % 2 != 0) {
        std::cout << "Hash client hscan error" << std::endl;
        delete client;
        return;
      }
      if (fvs.empty()) {
        break;
      }
      for (auto it = fvs.begin(); it != fvs.end(); it+=2) {
//        std::cout << "hset " << *iter << " " << *it << " " << *(it+1) << std::endl;
        status_nemo = db->HSet(*iter, *it, *(it+1));
        if (!status_nemo.ok()) {
          std::cout << "Hash client hset error, key: " << *iter << std::endl;
          delete client;
          return;
        }
      }
      prev_start_field = fvs[fvs.size() - 2];
    }
  }
  std::cout << std::this_thread::get_id() << ", Hash client done" << std::endl;
}

void MigrateQueue(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db,
    std::vector<std::string> keys) {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Queue client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Queue client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::cout << std::this_thread::get_id() << ", Queue client start to migrate, " << keys.size() << " keys, from " << keys.front() << " to " << keys.back() << std::endl;

  std::vector<std::string> fs;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  int64_t start = 0;
  int64_t len = 0;
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    start = 0;
    while (true) {
      fs.clear();
      status_ssdb = client->qrange(*iter, start, kBatchLen, &fs);
      if (!status_ssdb.ok()) {
        std::cout << "Queue client range error" << std::endl;
        delete client;
        return;
      }
      if (fs.empty()) {
        break;
      }
      for (auto it = fs.begin(); it != fs.end(); it++) {
//        std::cout << "rpush " << *iter << " " << *it << " " << std::endl;
        status_nemo = db->RPush(*iter, *it, &len);
        if (!status_nemo.ok()) {
          std::cout << "Queue client rpush error, key: " << *iter << std::endl;
          delete client;
          return;
        }
      }
      start += fs.size();
    }
  }
  std::cout << std::this_thread::get_id() << ", Queue client done" << std::endl;
}

void MigrateZset(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db,
    std::vector<std::string> keys) {
  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Zset client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Zset client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::cout << std::this_thread::get_id() << ", Zset client start to migrate, " << keys.size() << " keys, from " << keys.front() << " to " << keys.back() << std::endl;

  std::vector<std::string> sms;
  ssdb::Status status_ssdb;
  nemo::Status status_nemo;

  std::string prev_start_member = "";
  int64_t zadd_res;
  for (auto iter = keys.begin(); iter != keys.end(); iter++) {
    prev_start_member = "";
    while (true) {
      sms.clear();
      status_ssdb = client->zscan(*iter, prev_start_member, NULL, NULL, kBatchLen, &sms);
      if (!status_ssdb.ok() || sms.size() % 2 != 0) {
        std::cout << "Zset client zscan error" << std::endl;
        delete client;
        return;
      }
      if (sms.empty()) {
        break;
      }
      for (auto it = sms.begin(); it != sms.end(); it+=2) {
//        std::cout << "zadd " << *iter << " " << *it << " " << *(it+1) << std::endl;
        status_nemo = db->ZAdd(*iter, stod(*(it+1)), *it, &zadd_res);
        if (!status_nemo.ok()) {
          std::cout << "Zadd client zadd error, key: " << *iter << std::endl;
          delete client;
          return;
        }
      }
      prev_start_member = sms[sms.size() - 2];
    }
  }
  std::cout << std::this_thread::get_id() << ", Zset client done" << std::endl;
}

void DoKv(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Kv center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Kv center client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::string start = "";
  std::string end = "";
  
  ssdb::Status status_ssdb;
  std::vector<std::string> keys;
  std::thread *threads[1000];
  int thread_num = 0;
  std::string prev_start = "";
  while (true) {
    keys.clear();
    end = "";
    status_ssdb = client->keys(start, end, kSplitNum, &keys);
    if (!status_ssdb.ok()) {
      std::cout << "Kv center client keys error" << std::endl;
      delete client;
      break;
    }
    if (keys.empty()) {
      std::cout << "Kv center client dispatch keys done, thread_num: " << thread_num << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(MigrateKv, ip, port,
        password, db, prev_start, keys.back());
    thread_num++;
    start = prev_start = keys.back();
  }

  for (int i = 0; i < thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }

  std::cout << "Kv migrate done" << std::endl;
}

void DoHash(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Hash center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Hash center client auth error" << std::endl;
      delete client;
      return;
    }
  }
  std::string start = "";
  std::string end = "";
  
  ssdb::Status status_ssdb;
  std::thread *threads[1000];
  int thread_num = 0;
  std::string prev_start = "";
  std::vector<std::string> keys;
  while (true) {
    keys.clear();
    end = "";
    resp = NULL;
    resp = client->request("hlist", start,
        end, std::to_string(kSplitNum));
    if (!resp || resp->front() != "ok") {
      std::cout << "Hash center client keys error" << std::endl;
      delete client;
      break;
    }
    keys.assign(resp->begin() + 1, resp->end());

    if (keys.empty()) {
      std::cout << "Hash center client dispatch keys done, thread_num: " << thread_num << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(MigrateHash, ip, port,
        password, db, keys);
    thread_num++;
    start = prev_start = resp->back();
  }

  for (int i = 0; i< thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }

  std::cout << "Hash migrate done" << std::endl;
}

void DoZset(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Zset center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
    resp = client->request("auth", password);
    if (!resp || resp->empty() || resp->front() != "ok") {
      std::cout << "Zset center client auth error" << std::endl;
      delete client;
      return;
    }
  }

  std::string start = "";
  std::string end = "";
  
  ssdb::Status status_ssdb;
  std::thread *threads[1000];
  int thread_num = 0;
  std::string prev_start = "";
  std::vector<std::string> keys;
  while (true) {
    keys.clear();
    end = "";
    resp = NULL;
    resp = client->request("zlist", start,
        end, std::to_string(kSplitNum));
    if (!resp || resp->front() != "ok") {
      std::cout << "Zset center client keys error" << std::endl;
      delete client;
      break;
    }
    keys.assign(resp->begin() + 1, resp->end());

    if (keys.empty()) {
      std::cout << "Zset center client dispatch keys done, thread_num: " << thread_num << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(MigrateZset, ip, port,
        password, db, keys);
    thread_num++;
    start = prev_start = resp->back();
  }

  for (int i = 0; i< thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }

  std::cout << "Zset migrate done" << std::endl;
}

void DoQueue(const std::string& ip, const int port,
    const std::string& password, nemo::Nemo* db) {

  ssdb::Client *client = ssdb::Client::connect(ip, port);
  if (client == NULL) {
    std::cout << "Queue center client failed to connect to ssdb" << std::endl;
    return;
  }
  const std::vector<std::string> *resp;
  if (password != "") {
  resp = client->request("auth", password);
  if (!resp || resp->empty() || resp->front() != "ok") {
    std::cout << "Queue center client auth error" << std::endl;
    delete client;
    return;
  }
  }

  std::string start = "";
  std::string end = "";
  
  ssdb::Status status_ssdb;
  std::thread *threads[1000];
  int thread_num = 0;
  std::string prev_start = "";
  std::vector<std::string> keys;
  while (true) {
    keys.clear();
    end = "";
    resp = NULL;
    resp = client->request("qlist", start,
        end, std::to_string(kSplitNum));
    if (!resp || resp->front() != "ok") {
      std::cout << "Queue center client keys error" << std::endl;
      delete client;
      break;
    }
    keys.assign(resp->begin() + 1, resp->end());

    if (keys.empty()) {
      std::cout << "Queue center client dispatch keys done, thread_num: " << thread_num << std::endl;
      delete client;
      break;
    }

    threads[thread_num] = new std::thread(MigrateQueue, ip, port,
        password, db, keys);
    thread_num++;
    start = prev_start = resp->back();
  }

  for (int i = 0; i< thread_num; i++) {
    threads[i]->join();
    delete threads[i];
  }

  std::cout << "Queue migrate done" << std::endl;
}

void Usage() {
  std::cout << "usage:" << std::endl
    << "./ssdb2pika ssdb_server_ip ssdb_server_port pika_db_path [ssdb_server_passwd]"
    << std::endl;
}

int main(int argc, char** argv) {
  if (argc != 4 && argc != 5) {
    Usage();
    return -1;
  }

  const char *ip = argv[1];
  int port = atoi(argv[2]);
  std::string nemo_path = argv[3];
  std::string password = "";
  if (argc == 5) {
    password = argv[4];
  }

  auto start = std::chrono::steady_clock::now();
  nemo::Options option;
  option.write_buffer_size = 268435456; //256M
  option.target_file_size_base = 20971520; //20M
  option.max_background_flushes = 4;
  option.max_background_compactions = 4;
  nemo::Nemo* db = new nemo::Nemo(nemo_path, option);
  std::thread thread_kv = std::thread(DoKv, ip, port, password, db);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::thread thread_hash = std::thread(DoHash, ip, port, password, db);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::thread thread_zset = std::thread(DoZset, ip, port, password, db);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::thread thread_queue = std::thread(DoQueue, ip, port, password, db);

  thread_kv.join();
  thread_hash.join();
  thread_zset.join();
  thread_queue.join();

  delete db;
  auto end = std::chrono::steady_clock::now();
  std::cout << "Migrate used " << std::chrono::duration_cast<std::chrono::seconds>(end - start).count() << "s" << std::endl;
  return 0;
}
