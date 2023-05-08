// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef __PSTD_DEFER_H__
#define __PSTD_DEFER_H__

#include <functional>
#include <type_traits>

namespace pstd {

// The defer class for C++11
//
// Usage:
// void f() {
//    FILE* fp = fopen(xxx);
//    if (!fp) return;
//
//    DEFER {
//        // it'll be executed on f() exiting.
//        fclose(fp);
//    }
//
//    ... // Do your business
// }
//
// An example for statics function time cost:
//
// #define STAT_FUNC_COST
//     // !!! omits std::chrono namespace
//     auto _start_ = steady_clock::now();
//     DEFER {
//          auto end = steady_clock::now();
//          cout << "Used:" << duration_cast<milliseconds>(end-_start_).count();
//     }
//
// // Insert into your function at first line.
// void f() {
//     STAT_FUNC_COST;
//     // when f() exit, will print its running time.
// }
//

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

}  // namespace pstd

#define _CONCAT(a, b) a##b
#define _MAKE_DEFER_(line) pstd::ExecuteOnScopeExit _CONCAT(defer, line) = [&]()

// !!! DEFER
#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__)

#endif  // __PSTD_DEFER_H__
