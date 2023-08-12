#include <gtest/gtest.h>
#include <iostream>
#include "include/pika_stream_util.h"

TEST(StreamUtilTest, String2Uint64) {
  uint64_t value;

  // Test with valid input
  ASSERT_TRUE(StreamUtil::string2uint64("1234567890", value));
  ASSERT_EQ(value, 1234567890);

  // Test with invalid input (non-numeric string)
  ASSERT_FALSE(StreamUtil::string2uint64("abc", value));

  // Test with empty string
  ASSERT_FALSE(StreamUtil::string2uint64("", value));

  // Test with nullptr
  ASSERT_FALSE(StreamUtil::string2uint64(nullptr, value));

  // Test with string representing a number beyond the range of uint64_t
  ASSERT_FALSE(StreamUtil::string2uint64("18446744073709551616", value));  // 2^64
}

TEST(StreamUtilTest, String2Int64) {
  int64_t value;

  // Test with valid input
  ASSERT_TRUE(StreamUtil::string2int64("1234567890", value));
  ASSERT_EQ(value, 1234567890);

  // Test with invalid input (non-numeric string)
  ASSERT_FALSE(StreamUtil::string2int64("abc", value));

  // Test with empty string
  ASSERT_FALSE(StreamUtil::string2int64("", value));

  // Test with nullptr
  ASSERT_FALSE(StreamUtil::string2int64(nullptr, value));

  // Test with string representing a number beyond the range of int64_t
  ASSERT_FALSE(StreamUtil::string2int64("9223372036854775808", value));  // 2^63
}

TEST(StreamUtilTest, String2Int32) {
  int32_t value;

  // Test with valid input
  ASSERT_TRUE(StreamUtil::string2int32("1234567890", value));
  ASSERT_EQ(value, 1234567890);

  // Test with invalid input (non-numeric string)
  ASSERT_FALSE(StreamUtil::string2int32("abc", value));

  // Test with empty string
  ASSERT_FALSE(StreamUtil::string2int32("", value));

  // Test with nullptr
  ASSERT_FALSE(StreamUtil::string2int32(nullptr, value));

  // Test with string representing a number beyond the range of int32_t
  ASSERT_FALSE(StreamUtil::string2int32("2147483648", value));  // 2^31
}


TEST(StreamUtilTest, StreamID2String) {
  StreamUtil util;
  streamID id(12345, 67890);
  std::string serialized_id;

  bool result = StreamUtil::StreamID2String(id, serialized_id);
  
  EXPECT_TRUE(result);

  streamID deserialized_id;
  memcpy(&deserialized_id, serialized_id.data(), sizeof(streamID));
  
  EXPECT_EQ(deserialized_id.ms, id.ms);
  EXPECT_EQ(deserialized_id.seq, id.seq);
}

TEST(StreamUtilTest, SerializeMessage) {
  StreamUtil util;
  std::vector<std::string> field_values = {"Hello", "World", "Test"};
  std::string message;
  int field_pos = 0;

  bool result = StreamUtil::SerializeMessage(field_values, message, field_pos);

  EXPECT_TRUE(result);
  EXPECT_NE(message, "");  
}

TEST(StreamUtilTest, DeserializeMessage) {
  StreamUtil util;
  std::string message;
  std::vector<std::string> field_values = {"Hello", "World", "Test"};
  int field_pos = 0;

  StreamUtil::SerializeMessage(field_values, message, field_pos);

  std::vector<std::string> parsed_message;

  bool result = StreamUtil::DeserializeMessage(message, parsed_message);

  EXPECT_TRUE(result);
  EXPECT_EQ(field_values, parsed_message);
}

TEST(StreamUtilTest, DeserializeMessage_Fail) {
  StreamUtil util;
  std::string message = "Invalid";
  std::vector<std::string> parsed_message;

  bool result = StreamUtil::DeserializeMessage(message, parsed_message);

  EXPECT_FALSE(result);
}

// Test StreamParseID
TEST(StreamUtilTest, StreamParseID) {
  StreamUtil util;
  streamID id;
  uint64_t missing_seq = 0;

  // Test valid ID
  CmdRes res = util.StreamParseID("12345-67890", id, missing_seq);
  EXPECT_TRUE(res.ok());
  EXPECT_EQ(id.ms, 12345);
  EXPECT_EQ(id.seq, 67890);

  // Test special ID: "+"
  res = util.StreamParseID("+", id, missing_seq);
  EXPECT_TRUE(res.ok());
  EXPECT_EQ(id.ms, STREAMID_MAX.ms);
  EXPECT_EQ(id.seq, STREAMID_MAX.seq);

  // Test special ID: "-"
  res = util.StreamParseID("-", id, missing_seq);
  EXPECT_TRUE(res.ok());
  EXPECT_EQ(id.ms, 0);
  EXPECT_EQ(id.seq, missing_seq);

  // Test invalid format
  res = util.StreamParseID("12345", id, missing_seq);
  EXPECT_TRUE(res.ok());
  
  // Test empty string
  res = util.StreamParseID("", id, missing_seq);
  EXPECT_FALSE(res.ok());
}

// Test StreamParseStrictID
TEST(StreamUtilTest, StreamParseStrictID) {
  StreamUtil util;
  streamID id;
  uint64_t missing_seq = 0;
  bool seq_given;

  // Test valid ID
  CmdRes res = util.StreamParseStrictID("12345-67890", id, missing_seq, &seq_given);
  EXPECT_TRUE(res.ok());
  EXPECT_EQ(id.ms, 12345);
  EXPECT_EQ(id.seq, 67890);
  EXPECT_TRUE(seq_given);

  // Test invalid format
  res = util.StreamParseStrictID("12345", id, missing_seq, &seq_given);
  EXPECT_TRUE(res.ok());

  // Test special ID: "+"
  res = util.StreamParseStrictID("+", id, missing_seq, &seq_given);
  EXPECT_FALSE(res.ok());

  // Test special ID: "-"
  res = util.StreamParseStrictID("-", id, missing_seq, &seq_given);
  EXPECT_FALSE(res.ok());
  
  // Test empty string
  res = util.StreamParseStrictID("", id, missing_seq, &seq_given);
  EXPECT_FALSE(res.ok());
}

// Test StreamParseIntervalId
TEST(StreamUtilTest, StreamParseIntervalId) {
  StreamUtil util;
  streamID id;
  uint64_t missing_seq = 0;
  bool exclude;

  // Test valid closed interval
  CmdRes res = util.StreamParseIntervalId("12345-67890", id, nullptr, missing_seq);
  EXPECT_TRUE(res.ok());
  EXPECT_EQ(id.ms, 12345);
  EXPECT_EQ(id.seq, 67890);

  // Test valid open interval
  res = util.StreamParseIntervalId("(12345-67890", id, &exclude, missing_seq);
  EXPECT_TRUE(res.ok());
  EXPECT_EQ(id.ms, 12345);
  EXPECT_EQ(id.seq, 67890);
  EXPECT_TRUE(exclude);

  // Test invalid format
  res = util.StreamParseIntervalId("12345", id, nullptr, missing_seq);
  EXPECT_FALSE(res.ok());
  
  // Test empty string
  res = util.StreamParseIntervalId("", id, &exclude, missing_seq);
  EXPECT_FALSE(res.ok());
  
  // Test open interval with empty string
  res = util.StreamParseIntervalId("(", id, &exclude, missing_seq);
  EXPECT_FALSE(res.ok());
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
