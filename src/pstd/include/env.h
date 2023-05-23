#ifndef __PSTD_ENV_H__
#define __PSTD_ENV_H__

#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <functional>
#include <string>
#include <vector>
#include <memory>
#include "pstd/include/pstd_status.h"

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
uint64_t Du(const std::string& path);

/*
 * Whether the file is exist
 * If exist return true, else return false
 */
bool FileExists(const std::string& path);

Status DeleteFile(const std::string& fname);

int RenameFile(const std::string& oldname, const std::string& newname);

class FileLock {
 public:
  FileLock() {}
  virtual ~FileLock(){};

  int fd_ = -1;
  std::string name_;

 private:
  // No copying allowed
  FileLock(const FileLock&);
  void operator=(const FileLock&);
};

Status LockFile(const std::string& f, FileLock** l);
Status UnlockFile(FileLock* l);

int GetChildren(const std::string& dir, std::vector<std::string>& result);
bool GetDescendant(const std::string& dir, std::vector<std::string>& result);

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
class WritableFile {
 public:
  WritableFile() {}
  virtual ~WritableFile();

  virtual Status Append(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual Status Flush() = 0;
  virtual Status Sync() = 0;
  virtual Status Trim(uint64_t offset) = 0;
  virtual uint64_t Filesize() = 0;

 private:
  // No copying allowed
  WritableFile(const WritableFile&);
  void operator=(const WritableFile&);
};

// A abstract for the sequential readable file
class SequentialFile {
 public:
  SequentialFile(){};
  virtual ~SequentialFile();
  // virtual Status Read(size_t n, char *&result, char *scratch) = 0;
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;
  virtual Status Skip(uint64_t n) = 0;
  // virtual Status Close() = 0;
  virtual char* ReadLine(char* buf, int n) = 0;
};

class RWFile {
 public:
  RWFile() {}
  virtual ~RWFile();
  virtual char* GetData() = 0;

 private:
  // No copying allowed
  RWFile(const RWFile&);
  void operator=(const RWFile&);
};

// A file abstraction for random reading and writing.
class RandomRWFile {
 public:
  RandomRWFile() {}
  virtual ~RandomRWFile() {}

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

 private:
  // No copying allowed
  RandomRWFile(const RandomRWFile&);
  void operator=(const RandomRWFile&);
};


typedef struct {
  std::string task_name;
  uint32_t event_type;
  std::function<void()> fun;
} TimedTask;

class TimedTaskManager {
 public:
  TimedTaskManager(int epoll_fd) : epoll_fd_(epoll_fd) {}
  ~TimedTaskManager() {
    for (auto &pair: tasks_) {
      epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, pair.first, nullptr);
      close(pair.first);
    }
  }
  /**
   * @param task_name name of the timed task
   * @param interval  exec time interval of the timed task
   * @param f addr of a function whose exec is the task itself
   * @param args parameters of the function
   * @return fd that related with the task, return -1 if the interval is invalid
   */
  template <class F, class... Args>
  int AddTimedTask(const std::string& task_name, int32_t interval, F&& f, Args&&... args) {
    int task_fd = CreateTimedfd(interval);
    if (task_fd == -1) {
      return -1;
    }

    std::function<void()> new_task = [f = std::forward<F>(f), args = std::make_tuple(std::forward<Args>(args)...), task_fd] {
      std::apply(f, args);
      uint64_t time_now;
      read(task_fd, &time_now, sizeof(time_now));
    };

    tasks_[task_fd] = {task_name, EPOLLIN, new_task};
    epoll_event event;
    event.data.fd = task_fd;
    event.events = EPOLLIN;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, task_fd, &event);
    return task_fd;
  }

  int CreateTimedfd(int32_t interval) {
    if (interval <= 0) {
      return -1;
    }
    int fd = timerfd_create(CLOCK_REALTIME, 0);
    int sec = interval / 1000;
    int ms = interval % 1000;
    timespec current_time;
    clock_gettime(CLOCK_REALTIME, &current_time);
    itimerspec timer_spec;
    timer_spec.it_value = current_time;
    timer_spec.it_interval = {sec, ms * 1000000};
    timerfd_settime(fd, TFD_TIMER_ABSTIME, &timer_spec, nullptr);
    return fd;
  }

  /**
   * @param fd the fd that fetchd from epoll_wait.
   * @return if this fd is bind to a timed task and which got executed, false if this fd dose not bind to a timed task.
   */
  bool TryToExecTimedTask(int fd, int32_t event_type) {
    auto it = tasks_.find(fd);
    if (it == tasks_.end() || it->second.event_type != event_type) {
      return false;
    }
    it->second.fun();
    return true;
  }

  int GetTaskfdByTaskName(const std::string& task_name) {
    std::vector<int> fds;
    for (auto& pair : tasks_) {
      if (task_name == pair.second.task_name) {
        return pair.first;
      }
    }
  }
  void EraseTask(int task_fd) {
    auto it = tasks_.find(task_fd);
    if (it == tasks_.end()) {
      return;
    }
    tasks_.erase(task_fd);
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, task_fd, nullptr);
    close(task_fd);
  }

  //no copying allowed
  TimedTaskManager(const TimedTaskManager& other) = delete;
  TimedTaskManager& operator=(const TimedTaskManager& other) = delete;
 private:
  int epoll_fd_;
  std::unordered_map<int, TimedTask> tasks_;
};

}  // namespace pstd
#endif  // __PSTD_ENV_H__
