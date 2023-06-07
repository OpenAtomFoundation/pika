#include <glog/logging.h>

#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/pika_migrate_thread.h"
#include "include/pika_server.h"
#include "include/pika_slot_command.h"

#define min(a, b) (((a) > (b)) ? (b) : (a))

const int32_t MAX_MEMBERS_NUM = 512;
const std::string INVALID_STR = "NL";

extern std::unique_ptr<PikaServer> g_pika_server;
extern std::unique_ptr<PikaConf> g_pika_conf;

// do migrate key to dest pika server
static int doMigrate(net::NetCli *cli, std::string send_str) {
  pstd::Status s;
  s = cli->Send(&send_str);
  if (!s.ok()) {
    LOG(WARNING) << "Slot Migrate Send error: " << s.ToString();
    return -1;
  }
  return 1;
}

// do migrate cli auth
static int doAuth(net::NetCli *cli) {
  net::RedisCmdArgsType argv;
  std::string wbuf_str;
  std::string requirepass = g_pika_conf->requirepass();
  if (requirepass != "") {
    argv.push_back("auth");
    argv.push_back(requirepass);
  } else {
    argv.push_back("ping");
  }
  net::SerializeRedisCommand(argv, &wbuf_str);

  pstd::Status s;
  s = cli->Send(&wbuf_str);
  if (!s.ok()) {
    LOG(WARNING) << "Slot Migrate auth Send error: " << s.ToString();
    return -1;
  }
  // Recv
  s = cli->Recv(&argv);
  if (!s.ok()) {
    LOG(WARNING) << "Slot Migrate auth Recv error: " << s.ToString();
    return -1;
  }
  pstd::StringToLower(argv[0]);
  if (argv[0] != "ok" && argv[0] != "pong" && argv[0].find("no password") == std::string::npos) {
    LOG(WARNING) << "Slot Migrate auth error: " << argv[0];
    return -1;
  }
  return 0;
}

// get kv key value
static int kvGet(const std::string key, std::string &value, std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->Get(key, &value);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      value = "";
      LOG(WARNING) << "Get kv key: " << key << " not found ";
      return 0;
    } else {
      value = "";
      LOG(WARNING) << "Get kv key: " << key << " error: " << s.ToString();
      return -1;
    }
  }
  return 0;
}

static int migrateKeyTTl(net::NetCli *cli, const std::string key, std::shared_ptr<Slot> slot) {
  net::RedisCmdArgsType argv;
  std::string send_str;
  std::map<storage::DataType, rocksdb::Status> type_status;
  std::map<storage::DataType, int64_t> type_timestamp;
  type_timestamp = slot->db()->TTL(key, &type_status);

  for (const auto &item : type_timestamp) {
    // mean operation exception errors happen in database
    if (item.second == -3) {
      return 0;
    }
  }

  int64_t ttl = 0;
  if (type_timestamp[storage::kStrings] != -2) {
    ttl = type_timestamp[storage::kStrings];
  } else if (type_timestamp[storage::kHashes] != -2) {
    ttl = type_timestamp[storage::kHashes];
  } else if (type_timestamp[storage::kLists] != -2) {
    ttl = type_timestamp[storage::kLists];
  } else if (type_timestamp[storage::kSets] != -2) {
    ttl = type_timestamp[storage::kSets];
  } else if (type_timestamp[storage::kZSets] != -2) {
    ttl = type_timestamp[storage::kZSets];
  } else {
    // mean this key not exist
    return 0;
  }

  if (ttl > 0) {
    argv.push_back("expire");
    argv.push_back(key);
    argv.push_back(std::to_string(ttl));
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0) {
      return -1;
    }
    return 1;
  }
  return 0;
}

// get set key all values
static int setGetall(const std::string key, std::vector<std::string> *members, std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->SMembers(key, members);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "Set get key: " << key << " value not found ";
      return 0;
    } else {
      LOG(WARNING) << "Set get key: " << key << " value error: " << s.ToString();
      return -1;
    }
  }
  return 1;
}

// get one zset key all values
static int zsetGetall(const std::string key, std::vector<storage::ScoreMember> *score_members,
                      std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->ZRange(key, 0, -1, score_members);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "zset get key: " << key << " not found ";
      return 0;
    } else {
      LOG(WARNING) << "zset get key: " << key << " value error: " << s.ToString();
      return -1;
    }
  }
  return 1;
}

// migrate zset key
static int migrateZset(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot> slot) {
  int r, ret = 0;
  std::vector<storage::ScoreMember> score_members;
  if (zsetGetall(key, &score_members, slot) < 0) {
    return -1;
  }
  size_t keySize = score_members.size();
  if (keySize == 0) {
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  for (size_t i = 0; i <= keySize / MaxKeySendSize; ++i) {
    if (i > 0) {
      LOG(WARNING) << "Migrate big key: " << key << " size: " << keySize
                   << " migrated value: " << min((i + 1) * MaxKeySendSize, keySize);
    }
    argv.clear();
    send_str = "";
    argv.push_back("zadd");
    argv.push_back(key);
    for (size_t j = i * MaxKeySendSize; j < (i + 1) * MaxKeySendSize && j < keySize; ++j) {
      argv.push_back(std::to_string(score_members[j].score));
      argv.push_back(score_members[j].member);
    }
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0) {
      return -1;
    } else {
      ret++;
    }
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0) {
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    KeyDelete(key, 'z', slot);
  }
  return ret;
}

// migrate one kv key
static int migrateKv(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot> slot) {
  int r, ret = 0;
  std::string value;
  if (kvGet(key, value, slot) < 0) {
    return -1;
  }
  if (value == "") {
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  argv.push_back("set");
  argv.push_back(key);
  argv.push_back(value);
  net::SerializeRedisCommand(argv, &send_str);

  if (doMigrate(cli, send_str) < 0) {
    return -1;
  } else {
    ret++;
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0) {
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    KeyDelete(key, 'k', slot);  // key already been migrated successfully, del error doesn't matter
  }
  return ret;
}

// get all hash field and values
static int hashGetall(const std::string key, std::vector<storage::FieldValue> *fvs, std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->HGetall(key, fvs);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "HGetall key: " << key << " not found ";
      return 0;
    } else {
      LOG(WARNING) << "HGetall key: " << key << " error: " << s.ToString();
      return -1;
    }
  }
  return 1;
}

// migrate one hash key
static int migrateHash(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot> slot) {
  int r, ret = 0;
  std::vector<storage::FieldValue> fvs;
  if (hashGetall(key, &fvs, slot) < 0) {
    return -1;
  }
  size_t keySize = fvs.size();
  if (keySize == 0) {
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  for (size_t i = 0; i <= keySize / MaxKeySendSize; ++i) {
    if (i > 0) {
      LOG(WARNING) << "Migrate big key: " << key << " size: " << keySize
                   << " migrated value: " << min((i + 1) * MaxKeySendSize, keySize);
    }
    argv.clear();
    send_str = "";
    argv.push_back("hmset");
    argv.push_back(key);
    for (size_t j = i * MaxKeySendSize; j < (i + 1) * MaxKeySendSize && j < keySize; ++j) {
      argv.push_back(fvs[j].field);
      argv.push_back(fvs[j].value);
    }
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0) {
      return -1;
    } else {
      ret++;
    }
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0) {
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    KeyDelete(key, 'h', slot);  // key already been migrated successfully, del error doesn't matter
  }
  return ret;
}

// migrate one set key
static int migrateSet(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot> slot) {
  int r, ret = 0;
  std::vector<std::string> members;
  if (setGetall(key, &members, slot) < 0) {
    return -1;
  }
  size_t keySize = members.size();
  if (keySize == 0) {
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  for (size_t i = 0; i <= keySize / MaxKeySendSize; ++i) {
    if (i > 0) {
      LOG(WARNING) << "Migrate big key: " << key << " size: " << keySize
                   << " migrated value: " << min((i + 1) * MaxKeySendSize, keySize);
    }
    argv.clear();
    send_str = "";
    argv.push_back("sadd");
    argv.push_back(key);
    for (size_t j = i * MaxKeySendSize; j < (i + 1) * MaxKeySendSize && j < keySize; ++j) {
      argv.push_back(members[j]);
    }
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0) {
      return -1;
    } else {
      ret++;
    }
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0) {
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    KeyDelete(key, 's', slot);  // key already been migrated successfully, del error doesn't matter
  }
  return ret;
}

// get list key all values
static int listGetall(const std::string key, std::vector<std::string> *values, std::shared_ptr<Slot> slot) {
  rocksdb::Status s = slot->db()->LRange(key, 0, -1, values);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(WARNING) << "List get key: " << key << " value not found ";
      return 0;
    } else {
      LOG(WARNING) << "List get key: " << key << " value error: " << s.ToString();
      return -1;
    }
  }
  return 1;
}

// migrate one list key
static int migrateList(net::NetCli *cli, const std::string key, bool async, std::shared_ptr<Slot> slot) {
  int r, ret = 0;
  std::vector<std::string> values;
  if (listGetall(key, &values, slot) < 0) {
    return -1;
  }
  size_t keySize = values.size();
  if (keySize == 0) {
    return 0;
  }

  net::RedisCmdArgsType argv;
  std::string send_str;
  for (size_t i = 0; i <= keySize / MaxKeySendSize; ++i) {
    if (i > 0) {
      LOG(WARNING) << "Migrate big key: " << key << " size: " << keySize
                   << " migrated value: " << min((i + 1) * MaxKeySendSize, keySize);
    }
    argv.clear();
    send_str = "";
    argv.push_back("lpush");
    argv.push_back(key);
    for (size_t j = i * MaxKeySendSize; j < (i + 1) * MaxKeySendSize && j < keySize; ++j) {
      argv.push_back(values[j]);
    }
    net::SerializeRedisCommand(argv, &send_str);
    if (doMigrate(cli, send_str) < 0) {
      return -1;
    } else {
      ret++;
    }
  }

  if ((r = migrateKeyTTl(cli, key, slot)) < 0) {
    return -1;
  } else {
    ret += r;
  }

  if (!async) {
    KeyDelete(key, 'l', slot);  // key already been migrated successfully, del error doesn't matter
  }
  return ret;
}

std::string GetSlotsSlotKey(int slot) { return SlotKeyPrefix + std::to_string(slot); }

// do migrate key to dest pika server
static int DoMigrate(net::NetCli *cli, std::string send_str) {
  pstd::Status s;
  s = cli->Send(&send_str);
  if (!s.ok()) {
    LOG(WARNING) << "Slot Migrate Send error: " << strerror(errno);
    return -1;
  }
  return 1;
}

PikaParseSendThread::PikaParseSendThread(PikaMigrateThread *migrate_thread, std::shared_ptr<Slot> slot)
    : dest_ip_("none"),
      dest_port_(-1),
      timeout_ms_(3000),
      mgrtkeys_num_(64),
      should_exit_(false),
      migrate_thread_(migrate_thread),
      slot_(slot),
      cli_(NULL) {}

PikaParseSendThread::~PikaParseSendThread() {
  if (is_running()) {
    should_exit_ = true;
    StopThread();
  }

  if (cli_) {
    delete cli_;
    cli_ = NULL;
  }
}

bool PikaParseSendThread::Init(const std::string &ip, int64_t port, int64_t timeout_ms, int64_t mgrtkeys_num) {
  dest_ip_ = ip;
  dest_port_ = port;
  timeout_ms_ = timeout_ms;
  mgrtkeys_num_ = mgrtkeys_num;

  cli_ = net::NewRedisCli();
  cli_->set_connect_timeout(timeout_ms_);
  cli_->set_send_timeout(timeout_ms_);
  cli_->set_recv_timeout(timeout_ms_);
  LOG(INFO) << "PikaParseSendThread init cli_, dest_ip_: " << dest_ip_ << " ,dest_port_: " << dest_port_;
  pstd::Status result = cli_->Connect(dest_ip_, dest_port_, g_pika_server->host());
  if (!result.ok()) {
    LOG(INFO) << "PikaParseSendThread::Init failed. Connect server(" << dest_ip_ << ":" << dest_port_ << ") "
              << result.ToString();
    return false;
  }

  // do auth
  if (doAuth(cli_) < 0) {
    LOG(WARNING) << "PikaParseSendThread::Init do auth failed !!";
    cli_->Close();
    return false;
  }

  return true;
}

void PikaParseSendThread::ExitThread(void) { should_exit_ = true; }

int PikaParseSendThread::MigrateOneKey(net::NetCli *cli, const std::string key, const char key_type, bool async) {
  int ret;
  switch (key_type) {
    case 'k':
      if ((ret = migrateKv(cli, key, async, slot_)) < 0) {
        SlotKeyAdd("k", key, slot_);
        return -1;
      }
      break;
    case 'h':
      if ((ret = migrateHash(cli, key, async, slot_)) < 0) {
        SlotKeyAdd("h", key, slot_);
        return -1;
      }
      break;
    case 'l':
      if ((ret = migrateList(cli, key, async, slot_)) < 0) {
        SlotKeyAdd("l", key, slot_);
        return -1;
      }
      break;
    case 's':
      if ((ret = migrateSet(cli, key, async, slot_)) < 0) {
        SlotKeyAdd("s", key, slot_);
        return -1;
      }
      break;
    case 'z':
      if ((ret = migrateZset(cli, key, async, slot_)) < 0) {
        SlotKeyAdd("z", key, slot_);
        return -1;
      }
      break;
    default:
      return -1;
      break;
  }
  return ret;
}

void PikaParseSendThread::DelKeysAndWriteBinlog(std::deque<std::pair<const char, std::string>> &send_keys,
                                                std::shared_ptr<Slot> slot) {
  for (auto iter = send_keys.begin(); iter != send_keys.end(); ++iter) {
    KeyDelete(iter->second, iter->first, slot);
    // todo add to binlog
//    WriteDelKeyToBinlog(iter->second, slot);
  }
}

bool PikaParseSendThread::CheckMigrateRecv(int64_t need_receive_num) {
  net::RedisCmdArgsType argv;
  for (int64_t i = 0; i < need_receive_num; ++i) {
    pstd::Status s;
    s = cli_->Recv(&argv);
    if (!s.ok()) {
      LOG(ERROR) << "PikaParseSendThread::CheckMigrateRecv Recv error: " << s.ToString();
      return false;
    }

    // set   return ok
    // zadd  return number
    // hset  return 0 or 1
    // hmset return ok
    // sadd  return number
    // rpush return length
    std::string reply = argv[0];
    int64_t ret;
    if (1 == argv.size() &&
        (kInnerReplOk == pstd::StringToLower(reply) || pstd::string2int(reply.data(), reply.size(), &ret))) {
      continue;
    } else {
      LOG(ERROR) << "PikaParseSendThread::CheckMigrateRecv reply error: " << reply;
      return false;
    }
  }
  return true;
}

void *PikaParseSendThread::ThreadMain() {
  while (!should_exit_) {
    std::deque<std::pair<const char, std::string>> send_keys;
    {
      std::unique_lock<std::mutex> lq(migrate_thread_->mgrtkeys_queue_mutex_);
      while (!should_exit_ && 0 >= migrate_thread_->mgrtkeys_queue_.size()) {
        migrate_thread_->mgrtkeys_cond_.wait(lq);
      }

      if (should_exit_) {
        LOG(INFO) << "PikaParseSendThread::ThreadMain :" << pthread_self() << " exit !!!";
        return NULL;
      }

      migrate_thread_->IncWorkingThreadNum();
      for (int32_t i = 0; i < mgrtkeys_num_; ++i) {
        if (migrate_thread_->mgrtkeys_queue_.empty()) {
          break;
        }
        send_keys.push_back(migrate_thread_->mgrtkeys_queue_.front());
        migrate_thread_->mgrtkeys_queue_.pop_front();
      }
    }

    int64_t send_num = 0;
    int64_t need_receive_num = 0;
    int32_t migrate_keys_num = 0;
    for (auto iter = send_keys.begin(); iter != send_keys.end(); ++iter) {
      if (0 > (send_num = MigrateOneKey(cli_, iter->second, iter->first, false))) {
        LOG(WARNING) << "PikaParseSendThread::ThreadMain MigrateOneKey: " << iter->second << " failed !!!";
        migrate_thread_->TaskFailed();
        migrate_thread_->DecWorkingThreadNum();
        return NULL;
      } else {
        need_receive_num += send_num;
        ++migrate_keys_num;
      }
    }

    // check response
    if (!CheckMigrateRecv(need_receive_num)) {
      LOG(INFO) << "PikaMigrateThread::ThreadMain CheckMigrateRecv failed !!!";
      migrate_thread_->TaskFailed();
      migrate_thread_->DecWorkingThreadNum();
      return NULL;
    } else {
      DelKeysAndWriteBinlog(send_keys, slot_);
    }

    migrate_thread_->AddResponseNum(migrate_keys_num);
    migrate_thread_->DecWorkingThreadNum();
  }

  return NULL;
}

PikaMigrateThread::PikaMigrateThread()
    : net::Thread(),
      dest_ip_("none"),
      dest_port_(-1),
      timeout_ms_(3000),
      slot_num_(-1),
      keys_num_(-1),
      is_migrating_(false),
      should_exit_(false),
      is_task_success_(true),
      send_num_(0),
      response_num_(0),
      moved_num_(0),
      request_migrate_(false),
      workers_num_(8),
      working_thread_num_(0),
      cursor_(0) {}

PikaMigrateThread::~PikaMigrateThread() {
  LOG(INFO) << "PikaMigrateThread::~PikaMigrateThread";

  if (is_running()) {
    should_exit_ = true;
    NotifyRequestMigrate();
    workers_cond_.notify_all();
    StopThread();
  }
}

bool PikaMigrateThread::ReqMigrateBatch(const std::string &ip, int64_t port, int64_t time_out, int64_t slot_num,
                                        int64_t keys_num, std::shared_ptr<Slot> slot) {
  if (migrator_mutex_.try_lock()) {
    if (is_migrating_) {
      if (dest_ip_ != ip || dest_port_ != port || slot_num_ != slot_num) {
        LOG(INFO) << "PikaMigrateThread::ReqMigrate current: " << dest_ip_ << ":" << dest_port_ << " slot[" << slot_num_
                  << "]"
                  << "request: " << ip << ":" << port << " slot[" << slot << "]";
        migrator_mutex_.unlock();
        return false;
      }
      slot_ = slot;
      timeout_ms_ = time_out;
      keys_num_ = keys_num;
      NotifyRequestMigrate();
      migrator_mutex_.unlock();
      return true;
    } else {
      dest_ip_ = ip;
      dest_port_ = port;
      timeout_ms_ = time_out;
      slot_num_ = slot_num;
      keys_num_ = keys_num;
      should_exit_ = false;
      slot_ = slot;

      ResetThread();
      int ret = StartThread();
      if (0 != ret) {
        LOG(ERROR) << "PikaMigrateThread::ReqMigrateBatch StartThread failed. "
                   << " ret=" << ret;
        is_migrating_ = false;
        StopThread();
      } else {
        LOG(INFO) << "PikaMigrateThread::ReqMigrateBatch slot: " << slot_num;
        is_migrating_ = true;
        NotifyRequestMigrate();
      }
      migrator_mutex_.unlock();
      return true;
    }
  }
  return false;
}

int PikaMigrateThread::ReqMigrateOne(const std::string &key) {
  std::unique_lock lm(migrator_mutex_);

  int slot_num = SlotNum(key);
  std::string type_str;
  char key_type;
  rocksdb::Status s = slot_->db()->Type(key, &type_str);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << "PikaMigrateThread::ReqMigrateOne key: " << key << " not found";
      return 0;
    } else {
      LOG(WARNING) << "PikaMigrateThread::ReqMigrateOne key: " << key << " error: " << strerror(errno);
      return -1;
    }
  }

  if (type_str == "string") {
    key_type = 'k';
  } else if (type_str == "hash") {
    key_type = 'h';
  } else if (type_str == "list") {
    key_type = 'l';
  } else if (type_str == "set") {
    key_type = 's';
  } else if (type_str == "zset") {
    key_type = 'z';
  } else if (type_str == "none") {
    return 0;
  } else {
    LOG(WARNING) << "PikaMigrateThread::ReqMigrateOne key: " << key << " type: " << type_str << " is  illegal";
    return -1;
  }

  if (slot_num != slot_num_) {
    LOG(WARNING) << "PikaMigrateThread::ReqMigrateOne Slot : " << slot_num
                 << " is not the migrating slot:" << slot_num_;
    return -1;
  }

  // if the migrate thread exit, start it
  if (!is_migrating_) {
    ResetThread();
    int ret = StartThread();
    if (0 != ret) {
      LOG(ERROR) << "PikaMigrateThread::ReqMigrateOne StartThread failed. "
                 << " ret=" << ret;
      is_migrating_ = false;
      StopThread();
    } else {
      LOG(INFO) << "PikaMigrateThread::ReqMigrateOne StartThread";
      is_migrating_ = true;
      usleep(100);
    }
  } else {
    // check the key is migrating
    std::pair<const char, std::string> kpair = std::make_pair(key_type, key);
    if (IsMigrating(kpair)) {
      LOG(INFO) << "PikaMigrateThread::ReqMigrateOne key: " << key << " is migrating ! ";
      return 1;
    } else {
      std::unique_lock lo(mgrtone_queue_mutex_);
      mgrtone_queue_.push_back(kpair);
      NotifyRequestMigrate();
    }
  }

  return 1;
}

void PikaMigrateThread::GetMigrateStatus(std::string *ip, int64_t *port, int64_t *slot, bool *migrating, int64_t *moved,
                                         int64_t *remained) {
  std::unique_lock lm(migrator_mutex_);
  *ip = dest_ip_;
  *port = dest_port_;
  *slot = slot_num_;
  *migrating = is_migrating_;
  *moved = moved_num_;
  std::unique_lock lq(mgrtkeys_queue_mutex_);
  int64_t migrating_keys_num = mgrtkeys_queue_.size();
  std::string slotKey = GetSlotsSlotKey(slot_num_);  // SlotKeyPrefix + std::to_string(slot_num_);
  int32_t slot_size = 0;
  rocksdb::Status s = slot_->db()->SCard(slotKey, &slot_size);
  if (s.ok()) {
    *remained = slot_size + migrating_keys_num;
  } else {
    *remained = migrating_keys_num;
  }
}

void PikaMigrateThread::CancelMigrate(void) {
  LOG(INFO) << "PikaMigrateThread::CancelMigrate";

  if (is_running()) {
    should_exit_ = true;
    NotifyRequestMigrate();
    workers_cond_.notify_all();
    StopThread();
  }
}

void PikaMigrateThread::IncWorkingThreadNum(void) { ++working_thread_num_; }

void PikaMigrateThread::DecWorkingThreadNum(void) {
  std::unique_lock lw(workers_mutex_);
  --working_thread_num_;
  workers_cond_.notify_all();
}

void PikaMigrateThread::TaskFailed() {
  LOG(ERROR) << "PikaMigrateThread::TaskFailed !!!";
  is_task_success_ = false;
}

void PikaMigrateThread::AddResponseNum(int32_t response_num) { response_num_ += response_num; }

void PikaMigrateThread::ResetThread(void) {
  if (0 != thread_id()) {
    JoinThread();
  }
}

void PikaMigrateThread::DestroyThread(bool is_self_exit) {
  std::unique_lock lm(migrator_mutex_);
  LOG(INFO) << "PikaMigrateThread::DestroyThread";

  // Destroy work threads
  DestroyParseSendThreads();

  if (is_self_exit) {
    set_is_running(false);
  }

  {
    std::unique_lock lq(mgrtkeys_queue_mutex_);
    std::unique_lock lm(mgrtkeys_map_mutex_);
    std::deque<std::pair<const char, std::string>>().swap(mgrtkeys_queue_);
    std::map<std::pair<const char, std::string>, std::string>().swap(mgrtkeys_map_);
  }

  cursor_ = 0;
  is_migrating_ = false;
  is_task_success_ = true;
  moved_num_ = 0;
}

void PikaMigrateThread::NotifyRequestMigrate(void) {
  std::unique_lock lr(request_migrate_mutex_);
  request_migrate_ = true;
  request_migrate_cond_.notify_all();
}

bool PikaMigrateThread::IsMigrating(std::pair<const char, std::string> &kpair) {
  std::unique_lock lo(mgrtone_queue_mutex_);
  std::unique_lock lm(mgrtkeys_map_mutex_);

  for (auto iter = mgrtone_queue_.begin(); iter != mgrtone_queue_.end(); ++iter) {
    if (iter->first == kpair.first && iter->second == kpair.second) {
      return true;
    }
  }

  auto iter = mgrtkeys_map_.find(kpair);
  if (iter != mgrtkeys_map_.end()) {
    return true;
  }

  return false;
}

void PikaMigrateThread::ReadSlotKeys(const std::string &slotKey, int64_t need_read_num, int64_t &real_read_num,
                                     int32_t *finish) {
  real_read_num = 0;
  std::string key;
  char key_type;
  int32_t is_member = 0;
  std::vector<std::string> members;

  rocksdb::Status s = slot_->db()->SScan(slotKey, cursor_, "*", need_read_num, &members, &cursor_);
  if (s.ok() && 0 < members.size()) {
    for (const auto &member : members) {
      slot_->db()->SIsmember(slotKey, member, &is_member);
      if (is_member) {
        key = member;
        key_type = key.at(0);
        key.erase(key.begin());
        std::pair<const char, std::string> kpair = std::make_pair(key_type, key);
        if (mgrtkeys_map_.find(kpair) == mgrtkeys_map_.end()) {
          mgrtkeys_queue_.push_back(kpair);
          mgrtkeys_map_[kpair] = INVALID_STR;
          ++real_read_num;
        }
      } else {
        LOG(INFO) << "PikaMigrateThread::ReadSlotKeys key " << member << " not found in" << slotKey;
      }
    }
  }

  *finish = (0 == cursor_) ? 1 : 0;
}

bool PikaMigrateThread::CreateParseSendThreads(int32_t dispatch_num) {
  workers_num_ = g_pika_conf->slotmigrate_thread_num();
  for (int32_t i = 0; i < workers_num_; ++i) {
    PikaParseSendThread *worker = new PikaParseSendThread(this, slot_);
    if (!worker->Init(dest_ip_, dest_port_, timeout_ms_, dispatch_num)) {
      delete worker;
      DestroyParseSendThreads();
      return false;
    } else {
      int ret = worker->StartThread();
      if (0 != ret) {
        LOG(INFO) << "PikaMigrateThread::CreateParseSendThreads start work thread failed ret=" << ret;
        delete worker;
        DestroyParseSendThreads();
        return false;
      } else {
        workers_.push_back(worker);
      }
    }
  }
  return true;
}

void PikaMigrateThread::DestroyParseSendThreads(void) {
  if (!workers_.empty()) {
    for (auto iter = workers_.begin(); iter != workers_.end(); ++iter) {
      (*iter)->ExitThread();
    }

    {
      std::unique_lock lm(mgrtkeys_queue_mutex_);
      mgrtkeys_cond_.notify_all();
    }

    for (auto iter = workers_.begin(); iter != workers_.end(); ++iter) {
      delete *iter;
    }
    workers_.clear();
  }
}

void *PikaMigrateThread::ThreadMain() {
  LOG(INFO) << "PikaMigrateThread::ThreadMain Start";

  // Create parse_send_threads
  int32_t dispatch_num = g_pika_conf->thread_migrate_keys_num();
  if (!CreateParseSendThreads(dispatch_num)) {
    LOG(INFO) << "PikaMigrateThread::ThreadMain CreateParseSendThreads failed !!!";
    DestroyThread(true);
    return NULL;
  }

  std::string slotKey = GetSlotsSlotKey(slot_num_);
  int32_t slot_size = 0;
  slot_->db()->SCard(slotKey, &slot_size);

  while (!should_exit_) {
    // Waiting migrate task
    {
      std::unique_lock<std::mutex> lm(request_migrate_mutex_);
      while (!request_migrate_) {
        LOG(INFO) << "request_migrate_cond_ 进入等待新的迁移工作！";
        request_migrate_cond_.wait(lm);
      }
      request_migrate_ = false;

      if (should_exit_) {
        LOG(INFO) << "PikaMigrateThread::ThreadMain :" << pthread_self() << " exit1 !!!";
        DestroyThread(false);
        return NULL;
      }
    }

    // read keys form slot and push to mgrtkeys_queue_
    int64_t round_remained_keys = keys_num_;
    int64_t real_read_num = 0;
    int32_t is_finish = 0;
    send_num_ = 0;
    response_num_ = 0;
    do {
      std::unique_lock lq(mgrtkeys_queue_mutex_);
      std::unique_lock lo(mgrtone_queue_mutex_);
      std::unique_lock lm(mgrtkeys_map_mutex_);

      // first check whether need migrate one key
      if (!mgrtone_queue_.empty()) {
        while (!mgrtone_queue_.empty()) {
          mgrtkeys_queue_.push_front(mgrtone_queue_.front());
          mgrtkeys_map_[mgrtone_queue_.front()] = INVALID_STR;
          mgrtone_queue_.pop_front();
          ++send_num_;
        }
      } else {
        int64_t need_read_num = (0 < round_remained_keys - dispatch_num) ? dispatch_num : round_remained_keys;
        ReadSlotKeys(slotKey, need_read_num, real_read_num, &is_finish);
        round_remained_keys -= need_read_num;
        send_num_ += real_read_num;
      }
      mgrtkeys_cond_.notify_all();

    } while (0 < round_remained_keys && !is_finish);

    LOG(INFO) << "PikaMigrateThread:: wait ParseSenderThread finish";
    // wait all ParseSenderThread finish
    {
      std::unique_lock lw(workers_mutex_);
      while (!should_exit_ && is_task_success_ && send_num_ != response_num_) {
        workers_cond_.wait(lw);
      }
    }
    LOG(INFO) << "PikaMigrateThread::ThreadMain send_num:" << send_num_ << " response_num:" << response_num_;

    if (should_exit_) {
      LOG(INFO) << "PikaMigrateThread::ThreadMain :" << pthread_self() << " exit2 !!!";
      DestroyThread(false);
      return NULL;
    }

    // check one round migrate task success
    if (!is_task_success_) {
      LOG(ERROR) << "PikaMigrateThread::ThreadMain one round migrate task failed !!!";
      DestroyThread(true);
      return NULL;
    } else {
      moved_num_ += response_num_;

      std::unique_lock lm(mgrtkeys_map_mutex_);
      std::map<std::pair<const char, std::string>, std::string>().swap(mgrtkeys_map_);
    }

    // check slot migrate finish
    int32_t slot_remained_keys = 0;
    slot_->db()->SCard(slotKey, &slot_remained_keys);
    if (0 == slot_remained_keys) {
      LOG(INFO) << "PikaMigrateThread::ThreadMain slot_size:" << slot_size << " moved_num:" << moved_num_;
      if (slot_size != moved_num_) {
        LOG(ERROR) << "PikaMigrateThread::ThreadMain moved_num != slot_size !!!";
      }
      DestroyThread(true);
      return NULL;
    }
  }

  return NULL;
}

/* EOF */
