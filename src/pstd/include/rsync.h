#ifndef __PSTD_RSYNC_H__
#define __PSTD_RSYNC_H__

#include <string>
#include <utility>

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
  int32_t port;
  std::string module;
  int32_t kbps;  // speed limit
  RsyncRemote(std::string  _host, const int32_t _port, std::string  _module, const int32_t _kbps)
      : host(std::move(_host)), port(_port), module(std::move(_module)), kbps(_kbps) {}
};

int32_t StartRsync(const std::string& raw_path, const std::string& module, const std::string& ip, int32_t port,
               const std::string& passwd);
int32_t StopRsync(const std::string& path);
int32_t RsyncSendFile(const std::string& local_file_path, const std::string& remote_file_path,
                  const std::string& secret_file_path, const RsyncRemote& remote);
int32_t RsyncSendClearTarget(const std::string& local_dir_path, const std::string& remote_dir_path,
                         const std::string& secret_file_path, const RsyncRemote& remote);

}  // namespace pstd
#endif
