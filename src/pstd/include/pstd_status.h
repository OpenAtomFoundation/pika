#ifndef __PSTD_STATUS_H__
#define __PSTD_STATUS_H__

#include <string>
#include "pstd/include/pstd_slice.h"

namespace pstd {

class Status {
 public:
  // Create a success status.
  Status() = default;
  ~Status() { delete[] state_; }

  // Copy the specified status.
  Status(const Status& s);
  void operator=(const Status& s);

  // Return a success status.
  static Status OK() { return {}; }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kNotFound, msg, msg2); }
  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kCorruption, msg, msg2); }
  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kNotSupported, msg, msg2); }
  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) { return {kInvalidArgument, msg, msg2}; }
  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kIOError, msg, msg2); }
  static Status EndFile(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kEndFile, msg, msg2); }

  static Status Incomplete(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kIncomplete, msg, msg2); }

  static Status Complete(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kComplete, msg, msg2); }

  static Status Timeout(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kTimeout, msg, msg2); }

  static Status AuthFailed(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kAuthFailed, msg, msg2); }

  static Status Busy(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kBusy, msg, msg2); }

  static Status ItemNotExist(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kItemNotExist, msg, msg2); }

  static Status Error(const Slice& msg, const Slice& msg2 = Slice()) { return Status(kError, msg, msg2); }

  // Returns true if the status indicates success.
  bool ok() const { return !state_; }

  // Returns true if the status indicates a NotFound error.
  bool IsNotFound() const { return code() == kNotFound; }

  // Returns true if the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; }

  // Returns true if the status indicates a NotSupported error.
  bool IsNotSupported() const { return code() == kNotSupported; }

  // Returns true if the status indicates an IOError.
  bool IsIOError() const { return code() == kIOError; }

  // Returns true if the status indicates an EOF.
  bool IsEndFile() const { return code() == kEndFile; }

  // Returns true if the status is complete.
  bool IsComplete() const { return code() == kComplete; }

  // Returns true if the status is Incomplete
  bool IsIncomplete() const { return code() == kIncomplete; }

  // Returns true if the status is InvalidArgument
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }

  // Returns true if the status is Timeout
  bool IsTimeout() const { return code() == kTimeout; }

  // Returns true if the status is AuthFailed
  bool IsAuthFailed() const { return code() == kAuthFailed; }

  // Return true if the status is Busy
  bool IsBusy() const { return code() == kBusy; }

  bool IsError() const { return code() == kError; }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

 private:
  // OK status has a null state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
  const char* state_{nullptr};

  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kEndFile = 6,
    kIncomplete = 7,
    kComplete = 8,
    kTimeout = 9,
    kAuthFailed = 10,
    kBusy = 11,
    kItemNotExist = 12,
    kError = 13
  };

  Code code() const { return !state_ ? kOk : static_cast<Code>(state_[4]); }

  Status(Code code, const Slice& msg, const Slice& msg2);
  static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) { state_ = !s.state_ ? nullptr : CopyState(s.state_); }
inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (&s != this && state_ != s.state_) {
    delete[] state_;
    state_ = !s.state_ ? nullptr : CopyState(s.state_);
  }
}

}  // namespace pstd

#endif  // __PSTD_STATUS_H__
