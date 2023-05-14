// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "pstd/include/base_conf.h"
#include "pstd/include/env.h"
#include "pstd/include/testutil.h"

namespace pstd {

class BaseConfTest : public ::testing::Test {
 public:
  BaseConfTest() {
    GetTestDirectory(&tmpdir_);
    DeleteDirIfExist(tmpdir_);
    CreateDir(tmpdir_);
    test_conf_ = tmpdir_ + "/test.conf";
  }

  Status CreateSampleConf() {
    std::vector<std::string> sample_conf = {
        "test_int : 1\n",
        "test_str : abkxk\n",
        "test_vec : four, five, six\n",
        "test_bool : yes\n",
    };

    std::unique_ptr<WritableFile> write_file;
    Status ret = NewWritableFile(test_conf_, write_file);
    if (!ret.ok()) return ret;
    for (std::string& item : sample_conf) {
      write_file->Append(item);
    }

    return Status::OK();
  }

  void ASSERT_OK(const Status& s) { ASSERT_TRUE(s.ok()); }

 protected:
  std::string tmpdir_;
  std::string test_conf_;
};

TEST_F(BaseConfTest, WriteReadConf) {
  ASSERT_OK(CreateSampleConf());
  auto conf = std::make_unique<BaseConf>(test_conf_);
  ASSERT_EQ(conf->LoadConf(), 0);

  // Write configuration
  ASSERT_TRUE(conf->SetConfInt("test_int", 1345));
  ASSERT_TRUE(conf->SetConfStr("test_str", "kdkbixk"));
  ASSERT_TRUE(conf->SetConfStr("test_vec", "one, two, three"));
  ASSERT_TRUE(conf->SetConfBool("test_bool", false));
  // Cover test
  ASSERT_TRUE(conf->SetConfInt("test_int", 13985));
  ASSERT_TRUE(conf->WriteBack());

  // Read configuration
  int test_int;
  std::string test_str;
  bool test_bool;
  std::vector<std::string> values;
  ASSERT_TRUE(conf->GetConfInt("test_int", &test_int));
  ASSERT_EQ(test_int, 13985);
  ASSERT_TRUE(conf->GetConfStr("test_str", &test_str));
  ASSERT_EQ(test_str, "kdkbixk");
  ASSERT_TRUE(conf->GetConfBool("test_bool", &test_bool));
  ASSERT_EQ(test_bool, false);
  ASSERT_TRUE(conf->GetConfStrVec("test_vec", &values));
  ASSERT_EQ(values[0], "one");
  ASSERT_EQ(values[1], "two");
  ASSERT_EQ(values[2], "three");
}

}  // namespace pstd
