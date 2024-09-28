#include "proxy_cmd_kv.h"
#include "base_cmd.h"
#include <memory>
#include <utility>
#include "pikiwidb.h"

// proxy_cmd_kv.cc
namespace pikiwidb {

std::string SetProxyCmd::GetCommand() {
  return "set " + key_ + " " + value_;
}

bool SetProxyCmd::DoInitial(PClient* client) {
 // client 
 client_.reset(client);
}
  
// 
void SetProxyCmd::Execute() {
  // TODO: route, (leave interface for mget or mset,
  // split task and combine callback)
  // Codis: add sub task for mget or mset

  // route class: manage all brpc_redis
  // task -> route -> brpc_redis_
  // Commit might be launch from timer (better) or route
  // route::forward(cmd)

  ROUTER.forward(shared_from_this());
} 

void SetProxyCmd::CallBack() {
  // same as cmd_kv.cc
  // after DoCmd ?
  
  client_->SetRes(CmdRes::kOK);
}

} // namespace pikiwidb

// TODO:
// 1. 解析 config 文件，知道后台有多少 pikiwidb 
// 2. flag 以 proxy 模式启动
// 3. client