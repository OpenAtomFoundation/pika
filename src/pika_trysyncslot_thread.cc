#include <fstream>
#include <glog/logging.h>
#include <poll.h>
#include "pika_slaveping_thread.h"
#include "pika_trysyncslot_thread.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "env.h"
#include "rsync.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

static const int RAW_ARGS_LEN = 1024 * 1024; 

PikaTrysyncSlotThread::~PikaTrysyncSlotThread() {
  should_exit_ = true;
  pthread_join(thread_id(), NULL);
  slash::StopRsync(g_pika_conf->db_sync_path());
  delete cli_;
  LOG(INFO) << " TrysyncSlot thread " << pthread_self() << " exit!!!";
}

bool PikaTrysyncSlotThread::Send() {
  pink::RedisCmdArgsType argv;
  std::string wbuf_str;
  std::string requirepass = g_pika_conf->requirepass();
  if (requirepass != "") {
    argv.push_back("auth");
    argv.push_back(requirepass);
    pink::RedisCli::SerializeCommand(argv, &wbuf_str);
  }

  argv.clear();
  std::string tbuf_str;
  argv.push_back("trysyncslot");
  argv.push_back(g_pika_server->host());
  argv.push_back(std::to_string(g_pika_server->port()));
  argv.push_back(std::to_string(g_pika_server->slot_sync()));
  pink::RedisCli::SerializeCommand(argv, &tbuf_str);

  wbuf_str.append(tbuf_str);
  LOG(INFO) << "===============" << wbuf_str;

  pink::Status s;
  s = cli_->Send(&wbuf_str);
  if (!s.ok()) {
    LOG(WARNING) << "Connect master, Send, error: " <<strerror(errno);
    return false;
  }
  return true;
}

bool PikaTrysyncSlotThread::RecvProc() {
  bool should_auth = g_pika_conf->requirepass() == "" ? false : true;
  bool is_authed = false;
  pink::Status s;
  std::string reply;

  while (1) {
    s = cli_->Recv(NULL);
    if (!s.ok()) {
      LOG(WARNING) << "Connect master, Recv, error: " <<strerror(errno);
      return false;
    }

    reply = cli_->argv_[0];
    LOG(WARNING) << "Reply from master after trysync: " << reply;
    if (!is_authed && should_auth) {
      if (kInnerReplOk != slash::StringToLower(reply)) {
        LOG(WARNING) << "auth with master, error, come in SyncError stage";
        g_pika_server->SyncError();
        return false;
      }
      is_authed = true;
    } else {
      if (cli_->argv_.size() == 1 &&
          slash::string2l(reply.data(), reply.size(), &sid_)) {
        // Luckly, I got your point, the sync is comming
        LOG(INFO) << "Recv sid from master: " << sid_;
        break;
      }
      // Failed

      if (kInnerReplWait == reply) {
        // You can't sync this time, but may be different next time,
        // This may happened when 
        // 1, Master do bgsave first.
        // 2, Master waiting for an existing bgsaving process
        // 3, Master do dbsyncing
        LOG(INFO) << "Need wait to sync";
        g_pika_server->SlotNeedWaitDBSync();
      } else {
        LOG(WARNING) << "something wrong with sync, come in SyncError stage";
        g_pika_server->SyncError();
      }
      return false;
    }
  }
  return true;
}

// Try to update master offset
// This may happend when dbsync from master finished
// Here we do:
// 1, Check dbsync finished, got the new binlog offset
// 2, Replace the old db
// 3, Update master offset, and the PikaTrysyncSlotThread cron will connect and do slaveof task with master
bool PikaTrysyncSlotThread::TryUpdateMasterOffset() {
  // Check dbsync finished
  std::string info_path = g_pika_conf->db_sync_path() + kBgsaveInfoFile;
  if (!slash::FileExists(info_path)) {
    return false;
  }

  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Failed to open info file after db sync";
    return false;
  }
  std::string line, master_ip;
  int lineno = 0;
  int64_t slot = 0, tmp = 0, master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 5) {
      if (!slash::string2l(line.data(), line.size(), &tmp) || tmp < 0) {
        LOG(WARNING) << "Format of info file after db sync error, line : " << line;
        is.close();
        return false;
      }
      if (lineno == 3) { 
		  master_port = tmp; 
	  } else if (lineno == 4) { 
		  slot = tmp; 
	  }
    } else if (lineno > 4) {
      LOG(WARNING) << "Format of info file after db sync error, line : " << line;
      is.close();
      return false;
    }
  }
  is.close();
  LOG(INFO) << "Information from dbsync info. master_ip: " << master_ip
    << ", master_port: " << master_port
    << ", slot: " << slot;

  // Sanity check
  if (master_ip != g_pika_server->master_ip() ||
      master_port != g_pika_server->master_port()) {
    LOG(WARNING) << "Error master ip port: " << master_ip << ":" << master_port;
    return false;
  }

  // Replace the old db
  slash::StopRsync(g_pika_conf->db_sync_path());
  slash::DeleteFile(info_path);

  //open the slot db
  nemo::Options option;
  option.write_buffer_size = g_pika_conf->write_buffer_size();
  option.target_file_size_base = g_pika_conf->target_file_size_base();
  option.max_background_flushes = g_pika_conf->max_background_flushes();
  option.max_background_compactions = g_pika_conf->max_background_compactions();
  option.max_open_files = g_pika_conf->max_cache_files();
  if (g_pika_conf->compression() == "none") {
		 option.compression = false;
  }


  std::shared_ptr<nemo::Nemo> slot_db = std::shared_ptr<nemo::Nemo>(new nemo::Nemo(g_pika_conf->db_sync_path(), option));
  assert(slot_db);

  LOG(INFO) << "slot db path:" << g_pika_conf->db_sync_path();
  std::vector<std::string> keys;
  slot_db->Keys("*", keys);
  std::cout << "key size:" << keys.size() << std::endl;

  //TODO:ttl
  //backup kv
  std::vector<std::string> kv_keys;
  slot_db->KvKeys("*", kv_keys);
  LOG(INFO) << "kv keys:[" << kv_keys.size() << "]";

  for (size_t i = 0; i < kv_keys.size(); ++i){
	  std::string key =  kv_keys[i];
      std::string value;

      int64_t ttl = 0;
      nemo::Status ttl_s = slot_db->TTL(key, &ttl);
      if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
    	  return false;
      }

      nemo::Status s = slot_db->Get(key, &value);
	  if (s.ok()){
		  //db
          g_pika_server->db()->Set(key, value, ttl);
		  //log
		  std::string raw_args;
		  raw_args.reserve(RAW_ARGS_LEN);
		
		  PikaCmdArgsType argv_;
		  argv_.push_back("SET");
		  argv_.push_back(key);
		  argv_.push_back(value);
		  RedisAppendLen(raw_args, argv_.size(), "*");
		  PikaCmdArgsType::const_iterator it = argv_.begin();
		  for ( ; it != argv_.end(); ++it) {
			  RedisAppendLen(raw_args, (*it).size(), "$");
			  RedisAppendContent(raw_args, *it);
		  }
		  pthread_rwlock_rdlock(g_pika_server->rwlock());
		  g_pika_server->logger_->Lock();
		  g_pika_server->logger_->Put(raw_args);
		  g_pika_server->logger_->Unlock();
		  pthread_rwlock_unlock(g_pika_server->rwlock());

		  if (ttl > 0) {
			std::string raw_args;
			raw_args.reserve(RAW_ARGS_LEN);
			PikaCmdArgsType argv_;
			raw_args.clear();
			argv_.clear();
			char buf[sizeof(int64_t)];
    		slash::d2string(buf, sizeof(buf), ttl);
			argv_.push_back("EXPIRE");
			argv_.push_back(key);
			argv_.push_back(buf);
    	  	int64_t res;
			//db
    	  	g_pika_server->db()->Expire(key, ttl, &res);
			//log
			pthread_rwlock_rdlock(g_pika_server->rwlock());
		    g_pika_server->logger_->Lock();
		    g_pika_server->logger_->Put(raw_args);
		    g_pika_server->logger_->Unlock();
		    pthread_rwlock_unlock(g_pika_server->rwlock());
		  }
          LOG(INFO) << "kv key:[" << key << "] value:[" << value << "] ttl:[" << ttl << "]";
      }else if (s.IsNotFound()) {

      }else{
    	  return false;
      }
  }
  LOG(INFO) << "migrate insert kv key finished!";

  //backup list
  std::vector<std::string> list_keys;
  slot_db->ListKeys("*", list_keys);
  for (size_t i = 0; i < list_keys.size(); ++i){
      std::string key =  list_keys[i];
      int64_t ttl = 0;
      nemo::Status ttl_s = slot_db->TTL(key, &ttl);
      if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
    	  return false;
      }

      std::vector<nemo::IV> ivs;
      nemo::Status s = slot_db->LRange(key, 0, -1, ivs);
      std::string val_list;
      int64_t len;
      if (s.ok()){
    	  std::vector<nemo::IV>::iterator iter;
    	  for (iter = ivs.begin(); iter != ivs.end(); iter++) {
    		  val_list.append(iter->val);
    		  val_list.append(",");
			  //db
			  g_pika_server->db()->RPush(key, iter->val, &len);
			  //log
			  std::string raw_args;
			  raw_args.reserve(RAW_ARGS_LEN);
			  PikaCmdArgsType argv_;
			  argv_.push_back("RPUSH");
			  argv_.push_back(key);
			  argv_.push_back(iter->val);
			  RedisAppendLen(raw_args, argv_.size(), "*");
			  PikaCmdArgsType::const_iterator it = argv_.begin();
			  for ( ; it != argv_.end(); ++it) {
				  RedisAppendLen(raw_args, (*it).size(), "$");
				  RedisAppendContent(raw_args, *it);
			  }
			  pthread_rwlock_rdlock(g_pika_server->rwlock());
			  g_pika_server->logger_->Lock();
			  g_pika_server->logger_->Put(raw_args);
			  g_pika_server->logger_->Unlock();
			  pthread_rwlock_unlock(g_pika_server->rwlock());
    	  }    
		  if (ttl > 0) {
			std::string raw_args;
			raw_args.reserve(RAW_ARGS_LEN);
			PikaCmdArgsType argv_;
			raw_args.clear();
			argv_.clear();
			char buf[sizeof(int64_t)];
    		slash::d2string(buf, sizeof(buf), ttl);
			argv_.push_back("EXPIRE");
			argv_.push_back(key);
			argv_.push_back(buf);
    	  	int64_t res;
			//db
    	  	g_pika_server->db()->Expire(key, ttl, &res);
			//log
			pthread_rwlock_rdlock(g_pika_server->rwlock());
		    g_pika_server->logger_->Lock();
		    g_pika_server->logger_->Put(raw_args);
		    g_pika_server->logger_->Unlock();
		    pthread_rwlock_unlock(g_pika_server->rwlock());
		  }
      }else if (s.IsNotFound()) {

      }else{
    	return false;
      }
      //LOG(INFO) << "list key:[" << key << "] val_list:[" << val_list << "]";
  }
  LOG(INFO) << "migrate insert list key finished!";
  //backup hash
  std::vector<std::string> hash_keys;
  slot_db->HashKeys("*", hash_keys);
  for (size_t i = 0; i < hash_keys.size(); ++i){
      std::string key = hash_keys[i];

      int64_t ttl = 0;
      nemo::Status ttl_s = slot_db->TTL(key, &ttl);
      if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
    	  return false;
      }

      std::vector<nemo::FV> fvs;
      nemo::Status s = slot_db->HGetall(key, fvs);
      std::string fv;
      if (s.ok()){
    	  std::vector<nemo::FV>::const_iterator iter;
    	  for (iter = fvs.begin(); iter != fvs.end(); iter++) {
    		  fv.append(iter->field);
    		  fv.append("=");
    		  fv.append(iter->val);
    		  fv.append(",");
              //db
			  g_pika_server->db()->HSet(key, iter->field, iter->val);
			  //log
			  std::string raw_args;
			  raw_args.reserve(RAW_ARGS_LEN);
			  PikaCmdArgsType argv_;
			  argv_.push_back("HSET");
			  argv_.push_back(key);
			  argv_.push_back(iter->field);
			  argv_.push_back(iter->val);
			  RedisAppendLen(raw_args, argv_.size(), "*");
			  PikaCmdArgsType::const_iterator it = argv_.begin();
			  for ( ; it != argv_.end(); ++it) {
				  RedisAppendLen(raw_args, (*it).size(), "$");
				  RedisAppendContent(raw_args, *it);
			  }
			  pthread_rwlock_rdlock(g_pika_server->rwlock());
			  g_pika_server->logger_->Lock();
			  g_pika_server->logger_->Put(raw_args);
			  g_pika_server->logger_->Unlock();
			  pthread_rwlock_unlock(g_pika_server->rwlock());
    	  }
		  if (ttl > 0) {
			std::string raw_args;
			raw_args.reserve(RAW_ARGS_LEN);
			PikaCmdArgsType argv_;
			raw_args.clear();
			argv_.clear();
			char buf[sizeof(int64_t)];
    		slash::d2string(buf, sizeof(buf), ttl);
			argv_.push_back("EXPIRE");
			argv_.push_back(key);
			argv_.push_back(buf);
    	  	int64_t res;
			//db
    	  	g_pika_server->db()->Expire(key, ttl, &res);
			//log
			pthread_rwlock_rdlock(g_pika_server->rwlock());
		    g_pika_server->logger_->Lock();
		    g_pika_server->logger_->Put(raw_args);
		    g_pika_server->logger_->Unlock();
		    pthread_rwlock_unlock(g_pika_server->rwlock());
		  }
      }else if (s.IsNotFound()) {

      }else{
    	return false;
      }
      //LOG(INFO) << "hash key:[" << key << "] fvs:[" << fv << "]";
  }
  LOG(INFO) << "migrate insert hash key finished!";
  //backup set
  std::vector<std::string> set_keys;
  slot_db->SetKeys("*", set_keys);
  for (size_t i = 0; i < set_keys.size(); ++i){
      std::string key =  set_keys[i];
      int64_t ttl = 0;
      nemo::Status ttl_s = slot_db->TTL(key, &ttl);
      if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
    	  return false;
      }

      std::vector<std::string> members;
      nemo::Status s = slot_db->SMembers(key, members);
      std::string vals;
      int64_t len;
      if (s.ok()){
    	  for (size_t i = 0; i < members.size(); ++i){
    		  vals.append(members[i]);
    		  vals.append(",");
			  //db
    		  g_pika_server->db()->SAdd(key, members[i], &len);
			  //log
			  std::string raw_args;
			  raw_args.reserve(RAW_ARGS_LEN);
			  PikaCmdArgsType argv_;
			  argv_.push_back("SADD");
			  argv_.push_back(key);
			  argv_.push_back(members[i]);
			  RedisAppendLen(raw_args, argv_.size(), "*");
			  PikaCmdArgsType::const_iterator it = argv_.begin();
			  for ( ; it != argv_.end(); ++it) {
				  RedisAppendLen(raw_args, (*it).size(), "$");
				  RedisAppendContent(raw_args, *it);
			  }
			  pthread_rwlock_rdlock(g_pika_server->rwlock());
			  g_pika_server->logger_->Lock();
			  g_pika_server->logger_->Put(raw_args);
			  g_pika_server->logger_->Unlock();
			  pthread_rwlock_unlock(g_pika_server->rwlock());
    	  }
		  if (ttl > 0) {
			std::string raw_args;
			raw_args.reserve(RAW_ARGS_LEN);
			PikaCmdArgsType argv_;
			raw_args.clear();
			argv_.clear();
			char buf[sizeof(int64_t)];
    		slash::d2string(buf, sizeof(buf), ttl);
			argv_.push_back("EXPIRE");
			argv_.push_back(key);
			argv_.push_back(buf);
    	  	int64_t res;
			//db
    	  	g_pika_server->db()->Expire(key, ttl, &res);
			//log
			pthread_rwlock_rdlock(g_pika_server->rwlock());
		    g_pika_server->logger_->Lock();
		    g_pika_server->logger_->Put(raw_args);
		    g_pika_server->logger_->Unlock();
		    pthread_rwlock_unlock(g_pika_server->rwlock());
		  }
      }else if (s.IsNotFound()) {

      }else{
    	return false;
      }
      //LOG(INFO) << "set key:[" << key << "] members:[" << vals << "]";
  }
  LOG(INFO) << "migrate insert set key finished!";
  //backup zset
  std::vector<std::string> zset_keys;
  slot_db->ZsetKeys("*", zset_keys);

  for (size_t i = 0; i < zset_keys.size(); ++i){
      std::string key = zset_keys[i];
      int64_t ttl = 0;
      nemo::Status ttl_s = slot_db->TTL(key, &ttl);
      if (!(ttl_s.ok() || ttl_s.IsNotFound())) {
    	  return false;
      }

      std::vector<nemo::SM> sms_v;
      nemo::Status s = slot_db->ZRange(key, 0, -1, sms_v);
      std::string vals;
      char buf[32];
      if (s.ok()) {
    	  std::vector<nemo::SM>::const_iterator iter = sms_v.begin();
    	  int64_t res;
    	  for (; iter != sms_v.end(); iter++) {
    		  vals.append(iter->member);
    		  vals.append("=");
    		  slash::d2string(buf, sizeof(buf), iter->score);
    		  vals.append(buf);
    		  g_pika_server->db()->ZAdd(key, iter->score, iter->member, &res);
			  //log
			  std::string raw_args;
			  raw_args.reserve(RAW_ARGS_LEN);
			  PikaCmdArgsType argv_;
			  argv_.push_back("ZADD");
			  argv_.push_back(key);
			  argv_.push_back(buf);
			  argv_.push_back(iter->member);
			  RedisAppendLen(raw_args, argv_.size(), "*");
			  PikaCmdArgsType::const_iterator it = argv_.begin();
			  for ( ; it != argv_.end(); ++it) {
				  RedisAppendLen(raw_args, (*it).size(), "$");
				  RedisAppendContent(raw_args, *it);
			  }
			  pthread_rwlock_rdlock(g_pika_server->rwlock());
			  g_pika_server->logger_->Lock();
			  g_pika_server->logger_->Put(raw_args);
			  g_pika_server->logger_->Unlock();
			  pthread_rwlock_unlock(g_pika_server->rwlock());
    	  }
		  if (ttl > 0) {
			std::string raw_args;
			raw_args.reserve(RAW_ARGS_LEN);
			PikaCmdArgsType argv_;
			raw_args.clear();
			argv_.clear();
			char buf[sizeof(int64_t)];
    		slash::d2string(buf, sizeof(buf), ttl);
			argv_.push_back("EXPIRE");
			argv_.push_back(key);
			argv_.push_back(buf);
    	  	int64_t res;
			//db
    	  	g_pika_server->db()->Expire(key, ttl, &res);
			//log
			pthread_rwlock_rdlock(g_pika_server->rwlock());
		    g_pika_server->logger_->Lock();
		    g_pika_server->logger_->Put(raw_args);
		    g_pika_server->logger_->Unlock();
		    pthread_rwlock_unlock(g_pika_server->rwlock());
		  }
      }else if (s.IsNotFound()) {

      }else{
    	return false;
      }
      //LOG(INFO) << "zset key:[" << key << "] vals:[" << vals << "]";
  }
  LOG(INFO) << "migrate insert zset key finished!";
  // Update master offset
  g_pika_server->SlotWaitDBSyncFinish();
  return true;
}

void PikaTrysyncSlotThread::PrepareRsync() {
  std::string db_sync_path = g_pika_conf->db_sync_path();
  slash::StopRsync(db_sync_path);
  slash::CreatePath(db_sync_path + "kv");
  slash::CreatePath(db_sync_path + "hash");
  slash::CreatePath(db_sync_path + "list");
  slash::CreatePath(db_sync_path + "set");
  slash::CreatePath(db_sync_path + "zset");
}

// TODO maybe use RedisCli
void* PikaTrysyncSlotThread::ThreadMain() {
  while (!should_exit_) {
    sleep(1);
    if (g_pika_server->SlotWaitingDBSync()) {
      //Try to update offset by db sync
      if (TryUpdateMasterOffset()) {
        LOG(INFO) << "Success Update Master Offset";
      }
    }

    if (!g_pika_server->SlotShouldConnectMaster()) {
      continue;
    }
    sleep(2);
    LOG(INFO) << "Should connect master";
    
    std::string master_ip = g_pika_server->master_ip();
    int master_port = g_pika_server->master_port();
    
    // Start rsync
    std::string dbsync_path = g_pika_conf->db_sync_path();
    PrepareRsync();
    std::string ip_port = slash::IpPortString(master_ip, master_port);
    // We append the master ip port after module name
    // To make sure only data from current master is received
    int ret = slash::StartRsync(dbsync_path, kDBSyncModule + "_" + ip_port, g_pika_server->host(), g_pika_conf->port() + 3000);
    if (0 != ret) {
      LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path << " error : " << ret;
    }
    LOG(INFO) << "====================Finish to start rsync, path:" << dbsync_path;


    if ((cli_->Connect(master_ip, master_port, g_pika_server->host())).ok()) {
      cli_->set_send_timeout(5000);
      cli_->set_recv_timeout(5000);
      if (Send() && RecvProc()) {
        g_pika_server->SlotConnectMasterDone();
        // Stop rsync, binlog sync with master is begin
        slash::StopRsync(dbsync_path);
        g_pika_server->SlotMigrateFinished();
        LOG(INFO) << "TrysyncSlot success";
      }
      cli_->Close();
    } else {
      LOG(WARNING) << "Failed to connect to master, " << master_ip << ":" << master_port;
    }
  }
  return NULL;
}
