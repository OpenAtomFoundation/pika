#include <unistd.h>

#include <memory>

#include "gtest/gtest.h"
#include "storage/backupable.h"
#include "storage/storage.h"
#include "storage/util.h"

using namespace storage;

static const std::string path = "./db/keys";

class BackupableTest : public ::testing::Test {
 public:
  BackupableTest() = default;
  ~BackupableTest() = default;

  void SetUp() {
    if (access(path.c_str(), F_OK)) {
      mkdir(path.c_str(), 0755);
    }
    StorageOptions storage_options;
    storage_options.options.create_if_missing = true;
    db_ = std::make_shared<Storage>();
    Status s = db_->Open(storage_options, path);
    ASSERT_TRUE(s.ok()) << "db open fail";
  }

  void TearDown() { DeleteFiles(path.c_str()); }

  std::shared_ptr<Storage> db_;
};

TEST_F(BackupableTest, OpenTest) {
  BackupEngine* engine;
  Status s = BackupEngine::Open(db_, &engine);
  ASSERT_TRUE(s.ok()) << "backup engine open fail!";
  delete engine;
}

TEST_F(BackupableTest, CreateNewBackupSpecifyTest) {
  std::string backup_path = "./db/backup";
  if (access(backup_path.c_str(), F_OK)) {
    mkdir(backup_path.c_str(), 0755);
  }
  BackupEngine* engine;
  Status s = BackupEngine::Open(db_, &engine);
  ASSERT_TRUE(s.ok()) << "backup engine open fail!";

  {
    s = db_->Set("STRING_KEY", "VALUE");
    ASSERT_TRUE(s.ok());
  }

  {
    int32_t ret;
    s = db_->HSet("HEXIST_KEY", "HEXIST_FIELD", "HEXIST_VALUE", &ret);
    ASSERT_TRUE(s.ok());
  }

  {
    int32_t ret = 0;
    std::vector<std::string> members1{"a", "b", "c"};
    s = db_->SAdd("SADD_KEY", members1, &ret);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(ret, 3);
  }

  std::string types[] = {STRINGS_DB, HASHES_DB, LISTS_DB, ZSETS_DB, SETS_DB};
  for (const auto& type : types) {
    s = engine->CreateNewBackupSpecify(backup_path, type);
    ASSERT_TRUE(s.ok()) << "create new backup fail, type:" << type;
  }

  delete engine;
  DeleteFiles(backup_path.c_str()); 
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}