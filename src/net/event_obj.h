#pragma once

#include <cstdio>
#include <functional>
#include <memory>

namespace pikiwidb {
enum EventType {
  kEventNone = 0,
  kEventRead = 0x1 << 0,
  kEventWrite = 0x1 << 1,
  kEventError = 0x1 << 2,
};

class EventLoop;
// choose a loop for load balance
using EventLoopSelector = std::function<EventLoop*()>;

/// Event object base class.
class EventObject : public std::enable_shared_from_this<EventObject> {
 public:
  /// Constructor, printf is for debug, you can comment it
  EventObject() {}
  /// Destructor, printf is for debug, you can comment it
  virtual ~EventObject() { }

  EventObject(const EventObject&) = delete;
  void operator=(const EventObject&) = delete;

  // Return socket fd
  virtual int Fd() const = 0;
  // When read event occurs
  virtual bool HandleReadEvent() { return false; }
  // When write event occurs
  virtual bool HandleWriteEvent() { return false; }
  // When error event occurs
  virtual void HandleErrorEvent() {}

  // set event loop selector
  virtual void SetEventLoopSelector(EventLoopSelector cb) final { loop_selector_ = std::move(cb); }

  // The unique id, it'll not repeat in one thread.
  int GetUniqueId() const { return unique_id_; }
  // Set the unique id, it's called by library.
  void SetUniqueId(int id) { unique_id_ = id; }

 protected:
  EventLoopSelector loop_selector_;

 private:
  int unique_id_ = -1;  // set by eventloop
};

}  // namespace pikiwidb
