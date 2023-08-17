#include "libevent_reactor.h"

#include <cassert>

#include "event_obj.h"

namespace pikiwidb {
namespace internal {

LibeventReactor::LibeventReactor() : event_base_(event_base_new(), event_base_free) {
  auto base = event_base_.get();

  // to wakeup loop atmost every 10 ms
  struct timeval timeout;
  timeout.tv_sec = 0;
  timeout.tv_usec = 10 * 1000;

  wakeup_event_.reset(event_new(
      base, -1, EV_PERSIST, [](evutil_socket_t, int16_t, void*) {}, this));
  event_add(wakeup_event_.get(), &timeout);
}

static void OnReadable(evutil_socket_t socket, short events, void* ctx) {
  EventObject* obj = reinterpret_cast<EventObject*>(ctx);
  if (!obj->HandleReadEvent()) {
    obj->HandleErrorEvent();
  }
}

static void OnWritable(evutil_socket_t socket, short events, void* ctx) {
  EventObject* obj = reinterpret_cast<EventObject*>(ctx);
  if (!obj->HandleWriteEvent()) {
    obj->HandleErrorEvent();
  }
}

bool LibeventReactor::Register(EventObject* evobj, int events) {
  if (!evobj) {
    return false;
  }

  if (events == 0) {
    return true;  // evobj can manage events by itself
  }

  // check if repeat
  int id = evobj->GetUniqueId();
  assert(id >= 0);
  auto it = objects_.find(id);
  if (it != objects_.end()) {
    return false;
  }

  // register events
  std::unique_ptr<Object> obj(new Object(evobj));
  if (events & kEventRead) {
    auto base = event_base_.get();
    obj->read_event.reset(event_new(base, evobj->Fd(), EV_READ | EV_PERSIST, OnReadable, evobj));
    event_add(obj->read_event.get(), nullptr);
  }
  if (events & kEventWrite) {
    auto base = event_base_.get();
    obj->write_event.reset(event_new(base, evobj->Fd(), EV_WRITE | EV_PERSIST, OnWritable, evobj));
    event_add(obj->write_event.get(), nullptr);
  }

  // success, keep it
  objects_.insert({id, std::move(obj)});
  return true;
}

void LibeventReactor::Unregister(EventObject* evobj) {
  if (!evobj) {
    return;
  }

  // check if exist
  int id = evobj->GetUniqueId();
  auto it = objects_.find(id);
  if (it == objects_.end()) {
    return;
  }

  auto obj = it->second.get();
  obj->read_event.reset();
  obj->write_event.reset();
  objects_.erase(it);
}

bool LibeventReactor::Modify(EventObject* evobj, int events) {
  if (!evobj) {
    return false;
  }

  int id = evobj->GetUniqueId();
  auto it = objects_.find(id);
  if (it == objects_.end()) {
    return false;
  }

  auto base = event_base_.get();
  auto obj = it->second.get();
  assert(obj->ev_obj == evobj);
  if (events & kEventRead) {
    if (!obj->read_event) {
      obj->read_event.reset(event_new(base, evobj->Fd(), EV_READ | EV_PERSIST, OnReadable, evobj));
      event_add(obj->read_event.get(), nullptr);
    }
  } else {
    obj->read_event.reset();
  }

  if (events & kEventWrite) {
    if (!obj->write_event) {
      obj->write_event.reset(event_new(base, evobj->Fd(), EV_WRITE | EV_PERSIST, OnWritable, evobj));
      event_add(obj->write_event.get(), nullptr);
    }
  } else {
    obj->write_event.reset();
  }

  return true;
}

bool LibeventReactor::Poll() { return event_base_loop(event_base_.get(), EVLOOP_ONCE) != -1; }

void LibeventReactor::ScheduleRepeatedly(TimerId id, int period_ms, std::function<void()> f) {
  Schedule(id, period_ms, std::move(f), true);
}

void LibeventReactor::ScheduleLater(TimerId id, int delay_ms, std::function<void()> f) {
  Schedule(id, delay_ms, std::move(f), false);
}

void LibeventReactor::Schedule(TimerId id, int period_ms, std::function<void()> f, bool repeat) {
  auto timer = std::make_shared<Timer>();
  timer->id = id;
  timer->callback = std::move(f);
  timer->repeat = repeat;
  timer->ctx = this;

  int flag = (repeat ? EV_PERSIST : 0);
  struct event* ev = event_new(event_base_.get(), -1, flag, &LibeventReactor::TimerCallback, timer.get());
  timer->event = ev;

  struct timeval timeout;
  timeout.tv_sec = period_ms / 1000;
  timeout.tv_usec = 1000 * (period_ms % 1000);
  int err = event_add(ev, &timeout);
  assert(err == 0);

  timers_[id] = std::move(timer);
}

bool LibeventReactor::Cancel(TimerId id) {
  auto it = timers_.find(id);
  if (it == timers_.end()) {
    return false;
  }

  timers_.erase(it);
  return true;
}

LibeventReactor::Timer::~Timer() {
  if (event) {
    event_free(event);
  }
}

// static
void LibeventReactor::TimerCallback(evutil_socket_t, int16_t, void* ctx) {
  auto timer = reinterpret_cast<Timer*>(ctx)->shared_from_this();
  timer->callback();
  if (!timer->repeat) {
    auto reactor = reinterpret_cast<LibeventReactor*>(timer->ctx);
    reactor->Cancel(timer->id);
  }
}

void* LibeventReactor::Backend() { return event_base_.get(); }

}  // namespace internal
}  // namespace pikiwidb
