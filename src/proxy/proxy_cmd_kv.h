#pragma once
#include "proxy_base_cmd.h"
#include "brpc_redis.h"
#include "router.h"

namespace pikiwidb {

class SetProxyCmd : public ProxyBaseCmd {
 public:
  enum SetCondition { kNONE, kNX, kXX, kEXORPX };
  SetProxyCmd(std::string key, std::string value) : key_(key), value_(value) {};
  std::shared_ptr<PClient> Client() { return client_; }
  
 protected:
  void Execute() override;
  void CallBack() override;
  bool DoInitial(PClient* client) override;
  std::string GetCommand() override;
  
 private:
  std::string key_;
  std::string value_;
  int64_t sec_ = 0;
  std::shared_ptr<PClient> client_; // TODO: need to discuss
  Router* router_;  
  
  SetProxyCmd::SetCondition condition_{kNONE};

  std::unique_ptr<BrpcRedis> brpc_redis_;
};
  

} // namespace pikiwidb