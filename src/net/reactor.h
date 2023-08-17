#pragma once

#include <stdio.h>

#include <memory>

//#include "util.h"

namespace pikiwidb {
typedef int64_t TimerId;

class EventObject;
/// Reactor interface
class Reactor {
 public:
  Reactor() = default;
  virtual ~Reactor() = default;

  Reactor(const Reactor&) = delete;
  void operator=(const Reactor&) = delete;

  // backend
  virtual void* Backend() = 0;

  // Register event
  virtual bool Register(EventObject* obj, int events) = 0;
  // Unregister event
  virtual void Unregister(EventObject* obj) = 0;
  // Modify event
  virtual bool Modify(EventObject* obj, int events) = 0;
  // Poll events
  virtual bool Poll() = 0;
  // timer function
  virtual void ScheduleRepeatedly(TimerId id, int period_ms, std::function<void()> f) = 0;
  virtual void ScheduleLater(TimerId id, int delay_ms, std::function<void()> f) = 0;

  // cancel timer
  virtual bool Cancel(TimerId id) = 0;
};

}  // namespace pikiwidb
