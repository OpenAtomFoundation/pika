#include "pstd/include/env.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <cassert>

#include <fstream>
#include <sstream>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "pstd/include/xdebug.h"

namespace pstd {

/*
 *  Set the resource limits of a process
 */

/*
 *  0: success.
 * -1: set failed.
 * -2: get resource limits failed.
 */
const size_t kPageSize = getpagesize();

int SetMaxFileDescriptorNum(int64_t max_file_descriptor_num) {
  // Try to Set the number of file descriptor
  struct rlimit limit;
  if (getrlimit(RLIMIT_NOFILE, &limit) != -1) {
    if (limit.rlim_cur < static_cast<rlim_t>(max_file_descriptor_num)) {
      // rlim_cur could be set by any user while rlim_max are
      // changeable only by root.
      limit.rlim_cur = max_file_descriptor_num;
      if (limit.rlim_cur > limit.rlim_max) {
        limit.rlim_max = max_file_descriptor_num;
      }
      if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
        return 0;
      } else {
        return -1;
      };
    } else {
      return 0;
    }
  } else {
    return -2;
  }
}

/*
 * size of initial mmap size
 */
size_t kMmapBoundSize = 1024 * 1024 * 4;

void SetMmapBoundSize(size_t size) { kMmapBoundSize = size; }

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

int CreateDir(const std::string& path) {
  int res = 0;

  if ((res = mkdir(path.c_str(), 0755)) != 0) {
    LOG(WARNING) << "mkdir error is " << strerror(errno);
  }
  return res;
}

bool FileExists(const std::string& path) { return access(path.c_str(), F_OK) == 0; }

Status DeleteFile(const std::string& fname) {
  Status result;
  if (unlink(fname.c_str()) != 0) {
    result = IOError(fname, errno);
  }
  return result;
}

int DoCreatePath(const char* path, mode_t mode) {
  struct stat st;
  int status = 0;

  if (stat(path, &st) != 0) {
    /* Directory does not exist. EEXIST for race
     * condition */
    if (mkdir(path, mode) != 0 && errno != EEXIST) { status = -1;
}
  } else if (!S_ISDIR(st.st_mode)) {
    errno = ENOTDIR;
    status = -1;
  }

  return (status);
}

/**
 ** CreatePath - ensure all directories in path exist
 ** Algorithm takes the pessimistic view and works top-down to ensure
 ** each directory in path exists, rather than optimistically creating
 ** the last element and working backwards.
 */
int CreatePath(const std::string& path, mode_t mode) {
  char* pp;
  char* sp;
  int status;
  char* copypath = strdup(path.c_str());

  status = 0;
  pp = copypath;
  while (status == 0 && (sp = strchr(pp, '/')) != nullptr) {
    if (sp != pp) {
      /* Neither root nor double pstd in path */
      *sp = '\0';
      status = DoCreatePath(copypath, mode);
      *sp = '/';
    }
    pp = sp + 1;
  }
  if (status == 0) {
    status = DoCreatePath(path.c_str(), mode);
  }
  free(copypath);
  return (status);
}

static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;  // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

Status LockFile(const std::string& fname, FileLock** lock) {
  *lock = nullptr;
  Status result;
  int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    result = IOError(fname, errno);
  } else if (LockOrUnlock(fd, true) == -1) {
    result = IOError("lock " + fname, errno);
    close(fd);
  } else {
    auto my_lock = new FileLock;
    my_lock->fd_ = fd;
    my_lock->name_ = fname;
    *lock = my_lock;
  }
  return result;
}

Status UnlockFile(FileLock* lock) {
  Status result;
  if (LockOrUnlock(lock->fd_, false) == -1) {
    result = IOError("unlock", errno);
  }
  close(lock->fd_);
  delete lock;
  return result;
}

int GetChildren(const std::string& dir, std::vector<std::string>& result) {
  int res = 0;
  result.clear();
  DIR* d = opendir(dir.c_str());
  if (!d) {
    return errno;
  }
  struct dirent* entry;
  while ((entry = readdir(d)) != nullptr) {
    if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
      continue;
    }
    result.emplace_back(entry->d_name);
  }
  closedir(d);
  return res;
}

bool GetDescendant(const std::string& dir, std::vector<std::string>& result) {
  DIR* d = opendir(dir.c_str());
  if (!d) {
    return false;
  }
  struct dirent* entry;
  std::string fname;
  while ((entry = readdir(d)) != nullptr) {
    if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
      continue;
    }
    fname = dir + "/" + entry->d_name;
    if (0 == IsDir(fname)) {
      if (!GetDescendant(fname, result)) {
        return false;
      }
    } else {
      result.push_back(fname);
    }
  }
  closedir(d);
  return true;
}

int RenameFile(const std::string& oldname, const std::string& newname) {
  return rename(oldname.c_str(), newname.c_str());
}

int IsDir(const std::string& path) {
  struct stat buf;
  int ret = stat(path.c_str(), &buf);
  if (0 == ret) {
    if ((buf.st_mode & S_IFDIR) != 0) {
      // folder
      return 0;
    } else {
      // file
      return 1;
    }
  }
  return -1;
}

int DeleteDir(const std::string& path) {
  char chBuf[256];
  DIR* dir = nullptr;
  struct dirent* ptr;
  int ret = 0;
  dir = opendir(path.c_str());
  if (nullptr == dir) {
    return -1;
  }
  while ((ptr = readdir(dir)) != nullptr) {
    ret = strcmp(ptr->d_name, ".");
    if (0 == ret) {
      continue;
    }
    ret = strcmp(ptr->d_name, "..");
    if (0 == ret) {
      continue;
    }
    snprintf(chBuf, 256, "%s/%s", path.c_str(), ptr->d_name);
    ret = IsDir(chBuf);
    if (0 == ret) {
      // is dir
      ret = DeleteDir(chBuf);
      if (0 != ret) {
        return -1;
      }
    } else if (1 == ret) {
      // is file
      ret = remove(chBuf);
      if (0 != ret) {
        return -1;
      }
    }
  }
  (void)closedir(dir);
  ret = remove(path.c_str());
  if (0 != ret) {
    return -1;
  }
  return 0;
}

bool DeleteDirIfExist(const std::string& path) {
  return !(IsDir(path) == 0 && DeleteDir(path) != 0);
}

uint64_t Du(const std::string& filename) {
  struct stat statbuf;
  uint64_t sum;
  if (lstat(filename.c_str(), &statbuf) != 0) {
    return 0;
  }
  if (S_ISLNK(statbuf.st_mode) && stat(filename.c_str(), &statbuf) != 0) {
    return 0;
  }
  sum = statbuf.st_size;
  if (S_ISDIR(statbuf.st_mode)) {
    DIR* dir = nullptr;
    struct dirent* entry;
    std::string newfile;

    dir = opendir(filename.c_str());
    if (!dir) {
      return sum;
    }
    while ((entry = readdir(dir)) != nullptr) {
      if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0) {
        continue;
      }
      newfile = filename + "/" + entry->d_name;
      sum += Du(newfile);
    }
    closedir(dir);
  }
  return sum;
}

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void SleepForMicroseconds(int micros) { usleep(micros); }

SequentialFile::~SequentialFile() = default;

class PosixSequentialFile : public SequentialFile {
 private:
  std::string filename_;
  FILE* file_ = nullptr;

 public:
  virtual void setUnBuffer() { setbuf(file_, nullptr); }

  PosixSequentialFile(std::string  fname, FILE* f) : filename_(std::move(fname)), file_(f) { setbuf(file_, nullptr); }

  ~PosixSequentialFile() override {
    if (file_) {
      fclose(file_);
    }
  }

  Status Read(size_t n, Slice* result, char* scratch) override {
    Status s;
    size_t r = fread(scratch, 1, n, file_);

    *result = Slice(scratch, r);

    if (r < n) {
      if (feof(file_) != 0) {
        s = Status::EndFile(filename_, "end file");
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  Status Skip(uint64_t n) override {
    if (fseek(file_, n, SEEK_CUR) != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  char* ReadLine(char* buf, int n) override { return fgets(buf, n, file_); }

  virtual Status Close() {
    if (fclose(file_) != 0) {
      return IOError(filename_, errno);
    }
    file_ = nullptr;
    return Status::OK();
  }
};

WritableFile::~WritableFile() = default;

// We preallocate up to an extra megabyte and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
class PosixMmapFile : public WritableFile {
 private:
  std::string filename_;
  int fd_ = -1;
  size_t page_size_      = 0;
  size_t map_size_       = 0;       // How much extra memory to map at a time
  char* base_            = nullptr; // The mapped region
  char* limit_           = nullptr; // Limit of the mapped region
  char* dst_             = nullptr; // Where to write next  (in range [base_,limit_])
  char* last_sync_       = nullptr; // Where have we synced up to
  uint64_t file_offset_  = 0;       // Offset of base_ in file
  uint64_t write_len_    = 0;       // The data that written in the file

  // Have we done an munmap of unsynced data?
  bool pending_sync_ = false;

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

  static size_t TrimDown(size_t x, size_t y) { return (x / y) * y; }
  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  bool UnmapCurrentRegion() {
    bool result = true;
    if (base_) {
      if (last_sync_ < limit_) {
        // Defer syncing this data until next Sync() call, if any
        pending_sync_ = true;
      }
      if (munmap(base_, limit_ - base_) != 0) {
        result = false;
      }
      file_offset_ += limit_ - base_;
      base_ = nullptr;
      limit_ = nullptr;
      last_sync_ = nullptr;
      dst_ = nullptr;

      // Increase the amount we map the next time, but capped at 1MB
      if (map_size_ < (1 << 20)) {
        map_size_ *= 2;
      }
    }
    return result;
  }

  bool MapNewRegion() {
    assert(base_ == nullptr);
#if defined(__APPLE__)
    if (ftruncate(fd_, file_offset_ + map_size_) != 0) {
#else
    if (posix_fallocate(fd_, file_offset_, map_size_) != 0) {
#endif
      LOG(WARNING) << "ftruncate error";
      return false;
    }
    void* ptr = mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, file_offset_);
    if (ptr == MAP_FAILED) {  // NOLINT
      LOG(WARNING) << "mmap failed";
      return false;
    }
    base_ = reinterpret_cast<char*>(ptr);
    limit_ = base_ + map_size_;
    dst_ = base_ + write_len_;
    write_len_ = 0;
    last_sync_ = base_;
    return true;
  }

 public:
  PosixMmapFile(std::string  fname, int fd, size_t page_size, uint64_t write_len = 0)
      : filename_(std::move(fname)),
        fd_(fd),
        page_size_(page_size),
        map_size_(Roundup(kMmapBoundSize, page_size)),

        write_len_(write_len)
        {
    if (write_len_ != 0) {
      while (map_size_ < write_len_) {
        map_size_ += (1024 * 1024);
      }
    }
    assert((page_size & (page_size - 1)) == 0);
  }

  ~PosixMmapFile() override {
    if (fd_ >= 0) {
      PosixMmapFile::Close();
    }
  }

  Status Append(const Slice& data) override {
    const char* src = data.data();
    size_t left = data.size();
    while (left > 0) {
      assert(base_ <= dst_);
      assert(dst_ <= limit_);
      size_t avail = limit_ - dst_;
      if (!avail) {
        if (!UnmapCurrentRegion() || !MapNewRegion()) {
          return IOError(filename_, errno);
        }
      }
      size_t n = (left <= avail) ? left : avail;
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
    }
    return Status::OK();
  }

  Status Close() override {
    Status s;
    size_t unused = limit_ - dst_;
    if (!UnmapCurrentRegion()) {
      s = IOError(filename_, errno);
    } else if (unused > 0) {
      // Trim the extra space at the end of the file
      if (ftruncate(fd_, file_offset_ - unused) < 0) {
        s = IOError(filename_, errno);
      }
    }

    if (close(fd_) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }

    fd_ = -1;
    base_ = nullptr;
    limit_ = nullptr;
    return s;
  }

  Status Flush() override { return Status::OK(); }

  Status Sync() override {
    Status s;

    if (pending_sync_) {
      // Some unmapped data was not synced
      pending_sync_ = false;
#if defined(__APPLE__)
      if (fsync(fd_) < 0) {
#else
      if (fdatasync(fd_) < 0) {
#endif
        s = IOError(filename_, errno);
      }
    }

    if (dst_ > last_sync_) {
      // Find the beginnings of the pages that contain the first and last
      // bytes to be synced.
      size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
      size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
      last_sync_ = dst_;
      if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
        s = IOError(filename_, errno);
      }
    }

    return s;
  }

  Status Trim(uint64_t target) override {
    if (!UnmapCurrentRegion()) {
      return IOError(filename_, errno);
    }

    file_offset_ = target;

    if (!MapNewRegion()) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  uint64_t Filesize() override { return write_len_ + file_offset_ + (dst_ - base_); }
};

RWFile::~RWFile() = default;

class MmapRWFile : public RWFile {
 public:
  MmapRWFile(std::string  fname, int fd, size_t page_size)
      : filename_(std::move(fname)), fd_(fd), page_size_(page_size), map_size_(Roundup(65536, page_size)) {
    DoMapRegion();
  }

  ~MmapRWFile() override {
    if (fd_ >= 0) {
      munmap(base_, map_size_);
    }
  }

  bool DoMapRegion() {
#if defined(__APPLE__)
    if (ftruncate(fd_, map_size_) != 0) {
#else
    if (posix_fallocate(fd_, 0, map_size_) != 0) {
#endif
      return false;
    }
    void* ptr = mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (ptr == MAP_FAILED) {  // NOLINT
      return false;
    }
    base_ = reinterpret_cast<char*>(ptr);
    return true;
  }

  char* GetData() override { return base_; }
  char* base() { return base_; }

 private:
  static size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }
  std::string filename_;
  int fd_ = -1;
  size_t page_size_[[maybe_unused]] = 0;
  size_t map_size_ = 0;
  char* base_ = nullptr;
};

class PosixRandomRWFile : public RandomRWFile {
 private:
  const std::string filename_;
  int fd_ = -1;
  bool pending_sync_ = false;
  bool pending_fsync_ = false;
  // bool fallocate_with_keep_size_;

 public:
  PosixRandomRWFile(std::string fname, int fd)
      : filename_(std::move(fname)), fd_(fd) {
    // fallocate_with_keep_size_ = options.fallocate_with_keep_size;
  }

  ~PosixRandomRWFile() override {
    if (fd_ >= 0) {
      // TODO(clang-tidy): Call virtual method during destruction bypasses virtual dispatch
      // So I disabled next line clang-tidy check simply temporarily.
      Close();  // NOLINT
    }
  }

  Status Write(uint64_t offset, const Slice& data) override {
    const char* src = data.data();
    size_t left = data.size();
    Status s;
    pending_sync_ = true;
    pending_fsync_ = true;

    while (left != 0) {
      ssize_t done = pwrite(fd_, src, left, offset);
      if (done < 0) {
        if (errno == EINTR) {
          continue;
        }
        return IOError(filename_, errno);
      }

      left -= done;
      src += done;
      offset += done;
    }

    return Status::OK();
  }

  Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override {
    Status s;
    ssize_t r = -1;
    size_t left = n;
    char* ptr = scratch;
    while (left > 0) {
      r = pread(fd_, ptr, left, static_cast<off_t>(offset));
      if (r <= 0) {
        if (errno == EINTR) {
          continue;
        }
        break;
      }
      ptr += r;
      offset += r;
      left -= r;
    }
    *result = Slice(scratch, (r < 0) ? 0 : n - left);
    if (r < 0) {
      s = IOError(filename_, errno);
    }
    return s;
  }

  Status Close() override {
    Status s = Status::OK();
    if (fd_ >= 0 && close(fd_) < 0) {
      s = IOError(filename_, errno);
    }
    fd_ = -1;
    return s;
  }

  Status Sync() override {
#if defined(__APPLE__)
    if (pending_sync_ && fsync(fd_) < 0) {
#else
    if (pending_sync_ && fdatasync(fd_) < 0) {
#endif
      return IOError(filename_, errno);
    }
    pending_sync_ = false;
    return Status::OK();
  }

  Status Fsync() override {
    if (pending_fsync_ && fsync(fd_) < 0) {
      return IOError(filename_, errno);
    }
    pending_fsync_ = false;
    pending_sync_ = false;
    return Status::OK();
  }

  //  virtual Status Allocate(off_t offset, off_t len) override {
  //    TEST_KILL_RANDOM(rocksdb_kill_odds);
  //    int alloc_status = fallocate(
  //        fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0, offset, len);
  //    if (alloc_status == 0) {
  //      return Status::OK();
  //    } else {
  //      return IOError(filename_, errno);
  //    }
  //  }
};

Status NewSequentialFile(const std::string& fname, std::unique_ptr<SequentialFile>& result) {
  FILE* f = fopen(fname.c_str(), "r");
  if (!f) {
    return IOError(fname, errno);
  } else {
    result = std::make_unique<PosixSequentialFile>(fname, f);
    return Status::OK();
  }
}

Status NewWritableFile(const std::string& fname, std::unique_ptr<WritableFile>& result) {
  Status s;
  const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    s = IOError(fname, errno);
  } else {
    result = std::make_unique<PosixMmapFile>(fname, fd, kPageSize);
  }
  return s;
}

Status NewRWFile(const std::string& fname, std::unique_ptr<RWFile>& result) {
  Status s;
  const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    s = IOError(fname, errno);
  } else {
    result = std::make_unique<MmapRWFile>(fname, fd, kPageSize);
  }
  return s;
}

Status AppendWritableFile(const std::string& fname, std::unique_ptr<WritableFile>& result, uint64_t write_len) {
  Status s;
  const int fd = open(fname.c_str(), O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    s = IOError(fname, errno);
  } else {
    result = std::make_unique<PosixMmapFile>(fname, fd, kPageSize, write_len);
  }
  return s;
}

Status NewRandomRWFile(const std::string& fname, std::unique_ptr<RandomRWFile>& result) {
  Status s;
  const int fd = open(fname.c_str(), O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    s = IOError(fname, errno);
  } else {
    result = std::make_unique<PosixRandomRWFile>(fname, fd);
  }
  return s;
}

}  // namespace pstd
