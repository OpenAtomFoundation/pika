#include "brpc_redis.h"
#include <bthread/bthread.h>
#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include "config.h"
#include "proxy_base_cmd.h"

namespace pikiwidb {

void* thread_entry(void* arg) {
  auto func = static_cast<std::function<void()>*>(arg);
  (*func)();
  delete func;
  return nullptr;
}

void BrpcRedis::Open() {

}

void BrpcRedis::PushRedisTask(const std::shared_ptr<ProxyBaseCmd>& task) {
  std::lock_guard<std::mutex> lock(lock__);
  tasks_.push_back(task);
}

void SetResponse(const brpc::RedisResponse& response, const std::shared_ptr<ProxyBaseCmd>& task, size_t index) {
  // TODO: write callback
  LOG(INFO) << response.reply(index);
  


}

void BrpcRedis::Commit() {
  brpc::RedisRequest request;
  brpc::RedisResponse response;
  brpc::Controller cntl;
  std::vector<std::shared_ptr<ProxyBaseCmd>> task_batch;
  size_t batch_size = std::min((size_t) tasks_.size(), batch_size_);

  {
    std::lock_guard<std::mutex> lock(lock__);
    task_batch.assign(tasks_.begin(), tasks_.begin() + batch_size);
    tasks_.erase(tasks_.begin(), tasks_.begin() + batch_size);
  }

  for (auto& task : task_batch) {
    request.AddCommand(task->GetCommand());
  } 
  
  std::function<void()> callback = [&]() {
    channel_.CallMethod(nullptr, &cntl, &request, &response, nullptr);
    for (size_t i = 0; i < task_batch.size(); i++) {
      SetResponse(response, task_batch[i], i);
    }
  };

  callback();
}
  
} // namespace pikiwidb