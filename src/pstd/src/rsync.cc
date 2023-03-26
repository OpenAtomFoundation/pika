#include <fstream>
#include <sstream>
#include <string>

#include "pstd/include/env.h"
#include "pstd/include/rsync.h"
#include "pstd/include/xdebug.h"

namespace pstd {
// Clean files for rsync info, such as the lock, log, pid, conf file
static bool CleanRsyncInfo(const std::string& path) {
  return pstd::DeleteDirIfExist(path + kRsyncSubDir);
}

int StartRsync(const std::string& raw_path,
               const std::string& module,
               const std::string& ip,
               const int port,
               const std::string& passwd) {
  // Sanity check  
  if (raw_path.empty() || module.empty() || passwd.empty()) {
    return -1;
  }
  std::string path(raw_path);
  if (path.back() != '/') {
    path += "/";
  }
  std::string rsync_path = path + kRsyncSubDir + "/";
  CreatePath(rsync_path);

  // Generate secret file
  std::string secret_file(rsync_path + kRsyncSecretFile);
  std::ofstream secret_stream(secret_file.c_str());
  if (!secret_stream) {
    log_warn("Open rsync secret file failed!");
    return -1;
  }
  secret_stream << kRsyncUser << ":" << passwd;
  secret_stream.close();

  // Generate conf file
  std::string conf_file(rsync_path + kRsyncConfFile);
  std::ofstream conf_stream(conf_file.c_str());
  if (!conf_stream) {
    log_warn("Open rsync conf file failed!");
    return -1;
  }

  if (geteuid() == 0) {
    conf_stream << "uid = root" << std::endl;
    conf_stream << "gid = root" << std::endl;
  }
  conf_stream << "use chroot = no" << std::endl;
  conf_stream << "max connections = 10" << std::endl;
  conf_stream << "lock file = " << rsync_path + kRsyncLockFile << std::endl;
  conf_stream << "log file = " << rsync_path + kRsyncLogFile << std::endl;
  conf_stream << "pid file = " << rsync_path + kRsyncPidFile << std::endl;
  conf_stream << "list = no" << std::endl;
  conf_stream << "strict modes = no" << std::endl;
  conf_stream << "auth users = " << kRsyncUser << std::endl;
  conf_stream << "secrets file = " << secret_file << std::endl;
  conf_stream << "[" << module << "]" << std::endl;
  conf_stream << "path = " << path << std::endl;
  conf_stream << "read only = no" << std::endl;
  conf_stream.close();

  // Execute rsync command
  std::stringstream ss;
  ss << "rsync --daemon --config=" << conf_file;
  ss << " --address=" << ip;
  if (port != 873) {
    ss << " --port=" << port;
  }
  std::string rsync_start_cmd = ss.str();
  int ret = system(rsync_start_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    return 0;
  }
  log_warn("Start rsync deamon failed : %d!", ret);
  return ret;
}

int StopRsync(const std::string& raw_path) {
  // Sanity check  
  if (raw_path.empty()) {
    log_warn("empty rsync path!");
    return -1;
  }
  std::string path(raw_path);
  if (path.back() != '/') {
    path += "/";
  }

  std::string pid_file(path + kRsyncSubDir + "/" + kRsyncPidFile);
  if (!FileExists(pid_file)) {
    log_warn("no rsync pid file found");
    return 0; // Rsync deamon is not exist
  }

  // Kill Rsync
  SequentialFile *sequential_file;
  if (!NewSequentialFile(pid_file, &sequential_file).ok()) {
    log_warn("no rsync pid file found");
    return 0;
  };

  char line[32];
  if (sequential_file->ReadLine(line, 32) == NULL) {
    log_warn("read rsync pid file err");
    delete sequential_file;
    return 0;
  };

  delete sequential_file;
  
  pid_t pid = atoi(line);

  if (pid <= 1) {
    log_warn("read rsync pid err");
    return 0;
  }

  std::string rsync_stop_cmd = "kill -- -$(ps -o pgid= " + std::to_string(pid) + ")";
  int ret = system(rsync_stop_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    log_info("Stop rsync success!");
  } else {
    log_warn("Stop rsync deamon failed : %d!", ret);
  }
  CleanRsyncInfo(path);
  return ret;
}

int RsyncSendFile(const std::string& local_file_path,
                  const std::string& remote_file_path,
                  const std::string& secret_file_path,
                  const RsyncRemote& remote) {
  std::stringstream ss;
  ss << """rsync -avP --bwlimit=" << remote.kbps
    << " --password-file=" << secret_file_path
    << " --port=" << remote.port
    << " " << local_file_path
    << " " << kRsyncUser << "@" << remote.host
    << "::" << remote.module << "/" << remote_file_path;
  std::string rsync_cmd = ss.str();
  int ret = system(rsync_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    return 0;
  }
  log_warn("Rsync send file failed : %d!", ret);
  return ret;
}

int RsyncSendClearTarget(const std::string& local_dir_path,
                         const std::string& remote_dir_path,
                         const std::string& secret_file_path,
                         const RsyncRemote& remote) {
  if (local_dir_path.empty() || remote_dir_path.empty()) {
    return -2;
  }
  std::string local_dir(local_dir_path), remote_dir(remote_dir_path);
  if (local_dir_path.back() != '/') {
    local_dir.append("/");
  }
  if (remote_dir_path.back() != '/') {
    remote_dir.append("/");
  }
  std::stringstream ss;
  ss << "rsync -avP --delete --port=" << remote.port
    << " --password-file=" << secret_file_path
    << " " << local_dir
    << " " << kRsyncUser << "@" << remote.host
    << "::" << remote.module << "/" << remote_dir;
  std::string rsync_cmd = ss.str();
  int ret = system(rsync_cmd.c_str());
  if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
    return 0;
  }
  log_warn("Rsync send file failed : %d!", ret);
  return ret;
}

}  // namespace pstd
