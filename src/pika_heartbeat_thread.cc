#include <glog/logging.h>
#include "pika_heartbeat_thread.h"
#include "pika_heartbeat_conn.h"
#include "slash_mutex.h"
#include "pika_server.h"

extern PikaServer* g_pika_server;

PikaHeartbeatThread::PikaHeartbeatThread(int port, int cron_interval) :
  HolyThread::HolyThread(port, cron_interval) {
}

PikaHeartbeatThread::~PikaHeartbeatThread() {

}

void PikaHeartbeatThread::CronHandle() {
/*
 *	find out timeout slave and kill them 
 */
	struct timeval now;
	gettimeofday(&now, NULL);
  {
	slash::RWLock l(&rwlock_, true); // Use WriteLock to iterate the conns_
	std::map<int, void*>::iterator iter = conns_.begin();
  while (iter != conns_.end()) {
    if (now.tv_sec - static_cast<PikaHeartbeatConn*>(iter->second)->last_interaction().tv_sec > 30) {
      DLOG(INFO) << "Find Timeout Slave: " << static_cast<PikaHeartbeatConn*>(iter->second)->ip_port();
			close(iter->first);
			//	erase item in slaves_
			g_pika_server->DeleteSlave(iter->first);

			delete(static_cast<PikaHeartbeatConn*>(iter->second));
			iter = conns_.erase(iter);


			continue;
    } 
		iter++;
  }
  }

/*
 * find out: 1. stay STAGE_ONE to lone
 *					 2. the hb_fd have already be deleted
 * erase it in slaves_;
 */
	{
		slash::MutexLock l(&g_pika_server->slave_mutex_);
		std::vector<SlaveItem>::iterator iter = g_pika_server->slaves_.begin();
		while (iter != g_pika_server->slaves_.end()) {
			if ((iter->stage = SLAVE_ITEM_STAGE_ONE && now.tv_sec - iter->create_time.tv_sec > 30)
				|| (iter->stage == SLAVE_ITEM_STAGE_TWO && !FindSlave(iter->hb_fd))) {
				//pthread_kill(iter->tid);
        
        // Kill BinlogSender
				iter = g_pika_server->slaves_.erase(iter);
				continue;
			} 
			iter++;
		}
	}
}

bool PikaHeartbeatThread::AccessHandle(const std::string& ip_port) {
//  if (/* ip_port is not in slaves_ */) {
//    return false;
//  }
  return true;
}

bool PikaHeartbeatThread::FindSlave(int fd) {
  slash::RWLock(&rwlock_, false);
  std::map<int, void*>::iterator iter;
  for (iter = conns_.begin(); iter != conns_.end(); iter++) {
    if (iter->first != fd) {
      return true;
    }
  }
  return false;
}

