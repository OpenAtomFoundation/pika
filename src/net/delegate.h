/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <functional>
#include <list>

template <typename T>
class Delegate;

template <typename... Args>
class Delegate<void(Args...)> {
 public:
  typedef Delegate<void(Args...)> Self;

  Delegate() = default;

  Delegate(const Self&) = delete;
  Self& operator=(const Self&) = delete;

  template <typename F>
  Delegate(F&& f) {
    connect(std::forward<F>(f));
  }

  Delegate(Self&& other) : funcs_(std::move(other.funcs_)) {}

  template <typename F>
  Self& operator+=(F&& f) {
    connect(std::forward<F>(f));
    return *this;
  }

  template <typename F>
  Self& operator-=(F&& f) {
    disconnect(std::forward<F>(f));
    return *this;
  }

  void operator()(Args&&... args) { call(std::forward<Args>(args)...); }

 private:
  std::list<std::function<void(Args...)> > funcs_;

  template <typename F>
  void connect(F&& f) {
    funcs_.emplace_back(std::forward<F>(f));
  }

  template <typename F>
  void disconnect(F&& f) {
    // std::cerr << "&f = " << typeid(&f).name() << ", and target = " << typeid(decltype(std::addressof(f))).name() <<
    // std::endl;
    for (auto it(funcs_.begin()); it != funcs_.end(); ++it) {
      const auto& target = it->template target<decltype(std::addressof(f))>();
      if (target) {
        if (*target == &f) {
          funcs_.erase(it);
          return;
        }
      } else {
        const auto& target2 = it->template target<typename std::remove_reference<decltype(f)>::type>();

        // the function object must implement operator ==
        if (target2 && *target2 == f) {
          funcs_.erase(it);
          return;
        }
      }
    }
  }

  void call(Args&&... args) {
    // But what if rvalue args? FIXME
    for (const auto& f : funcs_) {
      f(std::forward<Args>(args)...);
    }
  }
};
