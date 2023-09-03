// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef __PSTD_DEFER_H__
#define __PSTD_DEFER_H__

#include <functional>
#include <type_traits>

// CTAD: See https://en.cppreference.com/w/cpp/language/class_template_argument_deduction
#if __cpp_deduction_guides >= 201606

template <class F>
class ExecuteOnScopeExit {
 public:
  ExecuteOnScopeExit(F&& f) : func_(std::move(f)) {}
  ExecuteOnScopeExit(const F& f) : func_(f) {}
  ~ExecuteOnScopeExit() { func_(); }

  ExecuteOnScopeExit(const ExecuteOnScopeExit& e) = delete;
  ExecuteOnScopeExit& operator=(const ExecuteOnScopeExit& f) = delete;

 private:
  F func_;
};

#else

class ExecuteOnScopeExit {
 public:
  ExecuteOnScopeExit() = default;

  // movable
  ExecuteOnScopeExit(ExecuteOnScopeExit&&) = default;
  ExecuteOnScopeExit& operator=(ExecuteOnScopeExit&&) = default;

  // non copyable
  ExecuteOnScopeExit(const ExecuteOnScopeExit& e) = delete;
  void operator=(const ExecuteOnScopeExit& f) = delete;

  template <typename F>
  ExecuteOnScopeExit(F&& f) : func_(std::forward<F>(f)) {}

  ~ExecuteOnScopeExit() noexcept {
    if (func_) func_();
  }

 private:
  std::function<void()> func_;
};

#endif

#define _CONCAT(a, b) a##b
#define _MAKE_DEFER_(line) ExecuteOnScopeExit _CONCAT(defer, line) = [&]()

// !!! DEFER
#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__)

#endif  // __PSTD_DEFER_H__
