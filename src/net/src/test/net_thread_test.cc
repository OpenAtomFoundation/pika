// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/net_thread.h"

#include <stdlib.h>
#include <string>

#include "gmock/gmock.h"

using ::testing::AtLeast;
using ::testing::Invoke;

class MockThread : public net::Thread {
 public:
  MOCK_METHOD0(ThreadMain, void*());

  void* thread_loop() {
    while (!should_stop()) {
      usleep(500);
    }
    return nullptr;
  }
};

TEST(NetThreadTest, ThreadOps) {
  MockThread t;
  EXPECT_CALL(t, ThreadMain()).Times(AtLeast(1));

  ON_CALL(t, ThreadMain()).WillByDefault(Invoke(&t, &MockThread::thread_loop));

  EXPECT_EQ(0, t.StartThread());

  EXPECT_EQ(true, t.is_running());

  EXPECT_EQ(false, t.should_stop());

  EXPECT_EQ(0, t.StopThread());

  EXPECT_EQ(true, t.should_stop());

  EXPECT_EQ(false, t.is_running());
}
