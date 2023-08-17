#pragma once

#include <cassert>
#include <functional>
#include <memory>
#include <unordered_map>

#include "event2/bufferevent.h"
#include "event2/event.h"
#include "reactor.h"

namespace pikiwidb {
class EventObject;
namespace internal {

// Libevent Reactor
class LibeventReactor : public Reactor {
 public:
  LibeventReactor();
  virtual ~LibeventReactor() {}

  bool Register(EventObject* obj, int events) override;
  void Unregister(EventObject* obj) override;
  bool Modify(EventObject* obj, int events) override;
  bool Poll() override;

  void ScheduleRepeatedly(TimerId id, int period_ms, std::function<void()> f) override;
  void ScheduleLater(TimerId id, int delay_ms, std::function<void()> f) override;

  bool Cancel(TimerId id) override;
  void* Backend() override;

 private:
  struct EventDeleter {
    void operator()(struct event* ev) {
      if (ev) {
        event_free(ev);
      }
    }
  };

  struct Timer : public std::enable_shared_from_this<Timer> {
    Timer() = default;
    ~Timer();

    Timer(const Timer&) = delete;
    void operator=(const Timer&) = delete;

    TimerId id;
    bool repeat;
    void* ctx = nullptr;
    struct event* event = nullptr;
    std::function<void()> callback;
  };

  struct Object {
    explicit Object(EventObject* evobj) : ev_obj(evobj) {}
    ~Object() = default;

    bool IsReadEnabled() const { return !!read_event.get(); }
    bool IsWriteEnabled() const { return !!write_event.get(); }

    std::unique_ptr<event, EventDeleter> read_event;
    std::unique_ptr<event, EventDeleter> write_event;
    EventObject* const ev_obj;
  };

  void Schedule(TimerId id, int period_ms, std::function<void()>, bool repeat);
  static void TimerCallback(evutil_socket_t, int16_t, void*);

  std::unique_ptr<event_base, decltype(event_base_free)*> event_base_;
  std::unordered_map<int, std::unique_ptr<Object>> objects_;

  // use shared ptr because callback will hold timer
  std::unordered_map<TimerId, std::shared_ptr<Timer>> timers_;

  // for wake up loop
  std::unique_ptr<event, EventDeleter> wakeup_event_;
};

}  // end namespace internal
}  // namespace pikiwidb
