#pragma once

#include "event_obj.h"

namespace pikiwidb {
namespace internal {

class PipeObject : public EventObject {
 public:
  PipeObject();
  ~PipeObject();

  PipeObject(const PipeObject&) = delete;
  void operator=(const PipeObject&) = delete;

  int Fd() const override;
  bool HandleReadEvent() override;
  bool HandleWriteEvent() override;
  void HandleErrorEvent() override;

  bool Notify();

 private:
  int read_fd_;
  int write_fd_;
};

}  // end namespace internal
}  // namespace pikiwidb
