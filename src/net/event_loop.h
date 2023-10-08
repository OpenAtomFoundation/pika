#pragma once

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "event_obj.h"
#include "http_client.h"
#include "http_server.h"
#include "pipe_obj.h"
#include "reactor.h"
#include "tcp_listener.h"
#include "tcp_connection.h"

namespace pikiwidb {
/// EventLoop is a wrapper for reactor and running its loop,
/// one thread should at most has one EventLoop object,
class EventLoop {
 public:
  EventLoop() = default;
  ~EventLoop() = default;

  EventLoop(const EventLoop&) = delete;
  void operator=(const EventLoop&) = delete;

  // Init loop
  void Init();
  // Run in a specific thread
  void Run();
  // Stop loop
  void Stop();

  // Exec func in loop thread, it's thread-safe
  template <typename F, typename... Args>
  auto Execute(F&&, Args&&...) -> std::future<typename std::invoke_result<F, Args...>::type>;

  // Exec func every some time, it's thread-safe
  template <typename Duration, typename F, typename... Args>
  TimerId ScheduleRepeatedly(const Duration& period, F&& f, Args&&... args);
  template <typename F, typename... Args>
  TimerId ScheduleRepeatedly(int period_ms, F&& f, Args&&... args);

  // Exec func after some time, it's thread-safe
  template <typename Duration, typename F, typename... Args>
  TimerId ScheduleLater(const Duration& delay, F&& f, Args&&... args);
  template <typename F, typename... Args>
  TimerId ScheduleLater(int delay_ms, F&& f, Args&&... args);

  // cancel timer
  std::future<bool> Cancel(TimerId id);

  // check if loop running in the caller's thread
  bool InThisLoop() const;

  // the backend reactor
  Reactor* GetReactor() const { return reactor_.get(); }

  // TCP server
  bool Listen(const char* ip, int port, NewTcpConnectionCallback ccb, EventLoopSelector selector);

  // TCP client
  std::shared_ptr<TcpConnection> Connect(const char* ip, int port, NewTcpConnectionCallback ccb, TcpConnectionFailCallback fcb);

  // HTTP server
  std::shared_ptr<HttpServer> ListenHTTP(const char* ip, int port, EventLoopSelector selector,
                                         HttpServer::OnNewClient cb = HttpServer::OnNewClient());

  // HTTP client
  std::shared_ptr<HttpClient> ConnectHTTP(const char* ip, int port);

  bool Register(std::shared_ptr<EventObject> src, int events);
  bool Modify(std::shared_ptr<EventObject> src, int events);
  void Unregister(std::shared_ptr<EventObject> src);

  void SetName(std::string name) { name_ = std::move(name); }
  const std::string& GetName() const { return name_; }

  static EventLoop* Self();

  // for unittest only
  void Reset();

 private:
  std::unique_ptr<Reactor> reactor_;
  std::unordered_map<int, std::shared_ptr<EventObject>> objects_;
  std::shared_ptr<internal::PipeObject> notifier_;

  std::mutex task_mutex_;
  std::vector<std::function<void()>> tasks_;

  std::string name_;  // for top command
  std::atomic<bool> running_{true};

  static std::atomic<int> obj_id_generator_;
  static std::atomic<TimerId> timerid_generator_;
};

template <typename F, typename... Args>
auto EventLoop::Execute(F&& f, Args&&... args) -> std::future<typename std::invoke_result<F, Args...>::type> {
  using resultType = typename std::invoke_result<F, Args...>::type;

  auto task =
      std::make_shared<std::packaged_task<resultType()>>(std::bind(std::forward<F>(f), std::forward<Args>(args)...));
  std::future<resultType> fut = task->get_future();

  if (InThisLoop()) {
    (*task)();
  } else {
    std::unique_lock<std::mutex> guard(task_mutex_);
    tasks_.emplace_back([task]() { (*task)(); });
    notifier_->Notify();
  }

  return fut;
}

template <typename Duration, typename F, typename... Args>
TimerId EventLoop::ScheduleRepeatedly(const Duration& period, F&& f, Args&&... args) {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;

  auto period_ms = std::max(milliseconds(1), duration_cast<milliseconds>(period));
  int ms = static_cast<int>(period_ms.count());
  return ScheduleRepeatedly(ms, std::forward<F>(f), std::forward<Args>(args)...);
}

template <typename F, typename... Args>
TimerId EventLoop::ScheduleRepeatedly(int period_ms, F&& f, Args&&... args) {
  std::function<void()> cb(std::bind(std::forward<F>(f), std::forward<Args>(args)...));
  auto id = timerid_generator_.fetch_add(1) + 1;

  if (InThisLoop()) {
    reactor_->ScheduleRepeatedly(id, period_ms, std::move(cb));
  } else {
    Execute([this, id, period_ms, cb]() { reactor_->ScheduleRepeatedly(id, period_ms, std::move(cb)); });
  }

  return id;
}

template <typename Duration, typename F, typename... Args>
TimerId EventLoop::ScheduleLater(const Duration& delay, F&& f, Args&&... args) {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;

  auto delay_ms = std::max(milliseconds(1), duration_cast<milliseconds>(delay));
  int ms = static_cast<int>(delay_ms.count());
  return ScheduleLater(ms, std::forward<F>(f), std::forward<Args>(args)...);
}

template <typename F, typename... Args>
TimerId EventLoop::ScheduleLater(int delay_ms, F&& f, Args&&... args) {
  std::function<void()> cb(std::bind(std::forward<F>(f), std::forward<Args>(args)...));
  auto id = timerid_generator_.fetch_add(1) + 1;

  if (InThisLoop()) {
    reactor_->ScheduleLater(id, delay_ms, std::move(cb));
  } else {
    Execute([this, id, delay_ms, cb]() { reactor_->ScheduleLater(id, delay_ms, std::move(cb)); });
  }

  return id;
}

}  // namespace pikiwidb
