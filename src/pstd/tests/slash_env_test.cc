// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "gtest/gtest.h"
#include "pstd/include/env.h"
#include "pstd/include/testutil.h"

namespace pstd {

class EnvTest : public ::testing::Test {};

TEST_F(EnvTest, SetMaxFileDescriptorNum) {
  ASSERT_EQ(0, SetMaxFileDescriptorNum(10));
  //ASSERT_NE(0, SetMaxFileDescriptorNum(2147483647));
}

TEST_F(EnvTest, FileOps) {
  std::string tmp_dir;
  GetTestDirectory(&tmp_dir);

  ASSERT_TRUE(DeleteDirIfExist(tmp_dir));
  ASSERT_TRUE(!FileExists(tmp_dir));
  ASSERT_EQ(-1, DeleteDir(tmp_dir));
  //ASSERT_NE(0, SetMaxFileDescriptorNum(2147483647));
}

}  // namespace pstd
