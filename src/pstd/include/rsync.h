#ifndef __PSTD_RSYNC_H__
#define __PSTD_RSYNC_H__

#include <string>

namespace pstd {
const std::string kRsyncSecretFile = "pstd_rsync.secret";
const std::string kRsyncConfFile = "pstd_rsync.conf";
const std::string kRsyncLogFile = "pstd_rsync.log";
const std::string kRsyncPidFile = "pstd_rsync.pid";
const std::string kRsyncLockFile = "pstd_rsync.lock";
const std::string kRsyncSubDir = "rsync";
const std::string kRsyncUser = "rsync_user";
struct RsyncRemote {
  std::string host;
  int port;
  std::string module;
  int kbps; //speed limit
  RsyncRemote(const std::string& _host, const int _port,
      const std::string& _module, const int _kbps)
  : host(_host), port(_port), module(_module), kbps(_kbps) {}
};

int StartRsync(const std::string& rsync_path, const std::string& module, const std::string& ip, const int port, const std::string& passwd);
int StopRsync(const std::string& path);
int RsyncSendFile(const std::string& local_file_path, const std::string& remote_file_path,
    const std::string& secret_file_path, const RsyncRemote& remote);
int RsyncSendClearTarget(const std::string& local_dir_path, const std::string& remote_dir_path,
    const std::string& secret_file_path, const RsyncRemote& remote);

}
#endif
