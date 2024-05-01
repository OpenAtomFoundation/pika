#ifndef __PSTD_ENV_H__
#define __PSTD_ENV_H__


#include <unistd.h>
#include <string>
#include <vector>
#include <memory>

#include "pstd/include/pstd_status.h"
#include "pstd/include/noncopyable.h"

namespace pstd {

class WritableFile;
class SequentialFile;
class RWFile;
class RandomRWFile;

/*
 *  Set the resource limits of a process
 */
int SetMaxFileDescriptorNum(int64_t max_file_descriptor_num);

/*
 * Set size of initial mmap size
 */
void SetMmapBoundSize(size_t size);

extern const size_t kPageSize;

/*
 * File Operations
 */
int IsDir(const std::string& path);
int DeleteDir(const std::string& path);
bool DeleteDirIfExist(const std::string& path);
int CreateDir(const std::string& path);
int CreatePath(const std::string& path, mode_t mode = 0755);
uint64_t Du(const std::string& filename);

/*
 * Whether the file is exist
 * If exist return true, else return false
 */
bool FileExists(const std::string& path);

bool DeleteFile(const std::string& fname);

int RenameFile(const std::string& oldname, const std::string& newname);

class FileLock : public pstd::noncopyable {
 public:
  FileLock() = default;
  virtual ~FileLock()= default;;

  int fd_ = -1;
  std::string name_;
};

int GetChildren(const std::string& dir, std::vector<std::string>& result);
void GetDescendant(const std::string& dir, std::vector<std::string>& result);

uint64_t NowMicros();
void SleepForMicroseconds(int micros);

Status NewSequentialFile(const std::string& fname, std::unique_ptr<SequentialFile>& result);

Status NewWritableFile(const std::string& fname, std::unique_ptr<WritableFile>& result);

Status NewRWFile(const std::string& fname, std::unique_ptr<RWFile>& result);

Status AppendSequentialFile(const std::string& fname, SequentialFile** result);

Status AppendWritableFile(const std::string& fname, std::unique_ptr<WritableFile>& result, uint64_t write_len = 0);

Status NewRandomRWFile(const std::string& fname, std::unique_ptr<RandomRWFile>& result);

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class WritableFile : public pstd::noncopyable {
 public:
  WritableFile() = default;
  virtual ~WritableFile();

  virtual Status Append(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual Status Flush() = 0;
  virtual Status Sync() = 0;
  virtual Status Trim(uint64_t offset) = 0;
  virtual uint64_t Filesize() = 0;
};

// A abstract for the sequential readable file
class SequentialFile {
 public:
  SequentialFile()= default;;
  virtual ~SequentialFile();
  // virtual Status Read(size_t n, char *&result, char *scratch) = 0;
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;
  virtual Status Skip(uint64_t n) = 0;
  // virtual Status Close() = 0;
  virtual char* ReadLine(char* buf, int n) = 0;
};

class RWFile : public pstd::noncopyable {
 public:
  RWFile() = default;
  virtual ~RWFile();
  virtual char* GetData() = 0;
};

// A file abstraction for random reading and writing.
class RandomRWFile : public pstd::noncopyable {
 public:
  RandomRWFile() = default;
  virtual ~RandomRWFile() = default;

  // Write data from Slice data to file starting from offset
  // Returns IOError on failure, but does not guarantee
  // atomicity of a write.  Returns OK status on success.
  //
  // Safe for concurrent use.
  virtual Status Write(uint64_t offset, const Slice& data) = 0;
  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const = 0;
  virtual Status Close() = 0;  // closes the file
  virtual Status Sync() = 0;   // sync data

  /*
   * Sync data and/or metadata as well.
   * By default, sync only data.
   * Override this method for environments where we need to sync
   * metadata as well.
   */
  virtual Status Fsync() { return Sync(); }

  /*
   * Pre-allocate space for a file.
   */
  virtual Status Allocate(off_t offset, off_t len) {
    (void)offset;
    (void)len;
    return Status::OK();
  }
};
}  // namespace pstd
#endif  // __PSTD_ENV_H__
