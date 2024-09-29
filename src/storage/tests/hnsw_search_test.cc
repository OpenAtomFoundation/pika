#include <gtest/gtest.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include <cstdint>
#include <cstdio>
#include <unordered_set>
#include <vector>
#include "pstd/env.h"
#include "pstd/log.h"
#include "src/base_value_format.h"
#include "src/search_format.h"
#include "storage/storage.h"

class LogIniter {
 public:
  LogIniter() {
    logger::Init("./hnsw_search_test.log");
    spdlog::set_level(spdlog::level::info);
  }
};

LogIniter log_initer;

class HnswTest : public ::testing::Test {
 public:
  HnswTest() = default;
  ~HnswTest() override = default;

  void SetUp() override {
    pstd::DeleteDirIfExist(db_path);
    mkdir(db_path.c_str(), 0755);
    options.options.create_if_missing = true;
    options.options.create_missing_column_families = true;
    options.options.max_background_jobs = 10;
    options.db_instance_num = 1;
    auto s = db.Open(options, db_path);
    ASSERT_TRUE(s.ok());
  }

  void TearDown() override { db.Close(); }

  std::string db_path{"./test_db/hnsw_test"};
  storage::StorageOptions options;
  storage::Storage db;
  storage::Status s;
};

TEST_F(HnswTest, PutAndGetHnswIndexMeta) {
  uint16_t layer = 0;

  std::string i1 = "index1";
  std::string i2 = "index2";
  std::string i3 = "index3";

  storage::HnswMetaValue meta1;
  db.PutHnswIndexMetaData(i1, meta1);
  storage::HnswMetaValue parsed_meta1;
  db.GetHnswIndexMetaData(i1, parsed_meta1);
  ASSERT_EQ(parsed_meta1.GetCapacity(), 500000);
  ASSERT_EQ(parsed_meta1.GetEfConstruction(), 200);
  ASSERT_EQ(parsed_meta1.GetEfRuntime(), 10);
  ASSERT_EQ(parsed_meta1.GetEpislon(), 0.01);
  ASSERT_EQ(parsed_meta1.GetNumLevel(), 0);

  storage::HnswMetaValue meta2(storage::DataType::kSearch, storage::VectorType::FLOAT64, storage::DataType::kHashes,
                               11451, storage::DistanceMetric::COSINE, 102400, 100, 30, 0.02, 3);
  db.PutHnswIndexMetaData(i2, meta2);
  storage::HnswMetaValue parsed_meta2;
  db.GetHnswIndexMetaData(i2, parsed_meta2);
  ASSERT_EQ(parsed_meta2.GetDim(), 11451);
  ASSERT_EQ(parsed_meta2.GetCapacity(), 102400);
  ASSERT_EQ(parsed_meta2.GetEfConstruction(), 100);
  ASSERT_EQ(parsed_meta2.GetEfRuntime(), 30);
  ASSERT_EQ(parsed_meta2.GetEpislon(), 0.02);
  ASSERT_EQ(parsed_meta2.GetNumLevel(), 3);

  storage::HnswMetaValue meta3(storage::DataType::kSearch, storage::VectorType::FLOAT64, storage::DataType::kHashes,
                               12354, storage::DistanceMetric::L2, 9527, 120, 40, 0.31, 5);
  db.PutHnswIndexMetaData(i3, meta3);
  storage::HnswMetaValue parsed_meta3;
  db.GetHnswIndexMetaData(i3, parsed_meta3);
  ASSERT_EQ(parsed_meta3.GetDim(), 12354);
  ASSERT_EQ(parsed_meta3.GetCapacity(), 9527);
  ASSERT_EQ(parsed_meta3.GetEfConstruction(), 120);
  ASSERT_EQ(parsed_meta3.GetEfRuntime(), 40);
  ASSERT_EQ(parsed_meta3.GetEpislon(), 0.31);
  ASSERT_EQ(parsed_meta3.GetNumLevel(), 5);

  parsed_meta3.SetNumLevel(16);
  parsed_meta3.SetDim(42);
  db.PutHnswIndexMetaData(i3, parsed_meta3);
  storage::HnswMetaValue updated_meta3;
  ASSERT_EQ(parsed_meta3.GetNumLevel(), 16);
  ASSERT_EQ(parsed_meta3.GetDim(), 42);
}

TEST_F(HnswTest, PutAndGetHnswNodeMetaData) {
  std::string n1 = "node1";
  std::string n2 = "node2";
  std::string n3 = "node3";

  storage::HnswNodeMetaData meta1(0, {1, 2, 3, 4, 5});
  storage::HnswNodeMetaData meta2(0, {6, 7, 8, 9, 10});
  storage::HnswNodeMetaData meta3(0, {11, 12, 13, 14, 15});

  db.PutHnswNodeMetaData(n1, 0, meta1);
  db.PutHnswNodeMetaData(n2, 0, meta2);
  db.PutHnswNodeMetaData(n3, 0, meta3);

  storage::HnswNodeMetaData parsed_meta1;
  db.GetHnswNodeMetaData(n1, 0, parsed_meta1);
  ASSERT_EQ(parsed_meta1.num_neighbours, 0);
  ASSERT_EQ(parsed_meta1.vector, std::vector<double>({1, 2, 3, 4, 5}));

  storage::HnswNodeMetaData parsed_meta2;
  db.GetHnswNodeMetaData(n2, 0, parsed_meta2);
  ASSERT_EQ(parsed_meta2.num_neighbours, 0);
  ASSERT_EQ(parsed_meta2.vector, std::vector<double>({6, 7, 8, 9, 10}));

  storage::HnswNodeMetaData parsed_meta3;
  db.GetHnswNodeMetaData(n3, 0, parsed_meta3);
  ASSERT_EQ(parsed_meta3.num_neighbours, 0);
  ASSERT_EQ(parsed_meta3.vector, std::vector<double>({11, 12, 13, 14, 15}));

  auto edge1 = storage::ConstructHnswEdge(0, n1, n2);
  auto edge2 = storage::ConstructHnswEdge(0, n2, n1);
  auto edge3 = storage::ConstructHnswEdge(0, n1, n3);
  auto edge4 = storage::ConstructHnswEdge(0, n3, n1);
  auto edge5 = storage::ConstructHnswEdge(0, n2, n3);
  auto edge6 = storage::ConstructHnswEdge(0, n3, n2);

  db.AddHnswEdge(edge1);
  db.AddHnswEdge(edge2);
  db.AddHnswEdge(edge3);
  db.AddHnswEdge(edge4);
  db.AddHnswEdge(edge5);
  db.AddHnswEdge(edge6);

  std::vector<std::string> node1_neighbours;
  db.HnswNodeDecodeNeighbours(n1, 0, node1_neighbours);
  EXPECT_EQ(node1_neighbours.size(), 2);
  std::unordered_set<std::string> expected_node1_neighbours = {"node2", "node3"};
  std::unordered_set<std::string> actual_node1_neighbours(node1_neighbours.begin(), node1_neighbours.end());
  EXPECT_EQ(actual_node1_neighbours, expected_node1_neighbours);

  std::vector<std::string> node2_neighbours;
  db.HnswNodeDecodeNeighbours(n2, 0, node2_neighbours);
  EXPECT_EQ(node2_neighbours.size(), 2);
  std::unordered_set<std::string> expected_node2_neighbours = {"node1", "node3"};
  std::unordered_set<std::string> actual_node2_neighbours(node2_neighbours.begin(), node2_neighbours.end());
  EXPECT_EQ(actual_node2_neighbours, expected_node2_neighbours);

  std::vector<std::string> node3_neighbours;
  db.HnswNodeDecodeNeighbours(n3, 0, node3_neighbours);
  EXPECT_EQ(node3_neighbours.size(), 2);
  std::unordered_set<std::string> expected_node3_neighbours = {"node1", "node2"};
  std::unordered_set<std::string> actual_node3_neighbours(node3_neighbours.begin(), node3_neighbours.end());
  EXPECT_EQ(actual_node3_neighbours, expected_node3_neighbours);
}

TEST_F(HnswTest, ModifyNeighbours) {
  std::string n1 = "node1";
  std::string n2 = "node2";
  std::string n3 = "node3";
  std::string n4 = "node4";

  storage::HnswNodeMetaData meta1(0, {1, 2, 3, 4, 5});
  storage::HnswNodeMetaData meta2(0, {6, 7, 8, 9, 10});
  storage::HnswNodeMetaData meta3(0, {11, 12, 13, 14, 15});
  storage::HnswNodeMetaData meta4(0, {16, 17, 18, 19, 20});

  db.PutHnswNodeMetaData(n1, 1, meta1);
  db.PutHnswNodeMetaData(n2, 1, meta2);
  db.PutHnswNodeMetaData(n3, 1, meta3);
  db.PutHnswNodeMetaData(n4, 1, meta4);

  auto s1 = db.HnswNodeAddNeighbour(n1, 1, n2);
  ASSERT_TRUE(s1.ok());
  auto s2 = db.HnswNodeAddNeighbour(n2, 1, n1);
  ASSERT_TRUE(s2.ok());
  auto s3 = db.HnswNodeAddNeighbour(n2, 1, n3);
  ASSERT_TRUE(s3.ok());
  auto s4 = db.HnswNodeAddNeighbour(n3, 1, n2);
  ASSERT_TRUE(s4.ok());

  std::vector<std::string> node1_neighbours;
  db.HnswNodeDecodeNeighbours(n1, 1, node1_neighbours);
  EXPECT_EQ(node1_neighbours.size(), 1);
  EXPECT_EQ(node1_neighbours[0], "node2");

  std::vector<std::string> node2_neighbours;
  db.HnswNodeDecodeNeighbours(n2, 1, node2_neighbours);
  EXPECT_EQ(node2_neighbours.size(), 2);
  std::unordered_set<std::string> expected_node2_neighbours = {"node1", "node3"};
  std::unordered_set<std::string> actual_node2_neighbours(node2_neighbours.begin(), node2_neighbours.end());
  EXPECT_EQ(actual_node2_neighbours, expected_node2_neighbours);

  std::vector<std::string> node3_neighbours;
  db.HnswNodeDecodeNeighbours(n3, 1, node3_neighbours);
  EXPECT_EQ(node3_neighbours.size(), 1);
  EXPECT_EQ(node3_neighbours[0], "node2");

  db.HnswNodeRemoveNeighbour(n2, 1, n3);
  node2_neighbours.clear();
  db.HnswNodeDecodeNeighbours(n2, 1, node2_neighbours);
  EXPECT_EQ(node2_neighbours.size(), 1);
  EXPECT_EQ(node2_neighbours[0], "node1");
}