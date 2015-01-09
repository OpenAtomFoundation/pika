#include "tick_thread.h"
#include "mutexlock.h"
#include "port.h"
#include "tick_epoll.h"
#include "tick_define.h"
#include "csapp.h"
#include "tick_item.h"
#include "tick_conf.h"
#include <glog/logging.h>
#include "status.h"

extern TickConf* g_tickConf;

TickThread::TickThread()
{
    thread_id_ = pthread_self();

    /*
     * inital the tickepoll object, add the notify_receive_fd to epoll
     */
    tickEpoll_ = new TickEpoll();
    int fds[2];
    if (pipe(fds)) {
        LOG(FATAL) << "Can't create notify pipe";
    }
    notify_receive_fd_ = fds[0];
    notify_send_fd_ = fds[1];
    tickEpoll_->TickAddEvent(notify_receive_fd_, EPOLLIN | EPOLLERR | EPOLLHUP);

    /*
     * initial the qbus client
     */
    qbus_cluster_ = g_tickConf->qbus_cluster();
    qbus_conf_path_ = g_tickConf->qbus_conf_path();
    qbus_topic_ = g_tickConf->qbus_topic();
    producer_ = KafkaProducer::getInstance(qbus_cluster_, qbus_conf_path_, false);
    if (producer_ == NULL) {
        LOG(FATAL) << "KafkaProducer getInstance error";
    }

    producer_->setSendTimeout(5);
    producer_->setRecvTimeout(2);
    producer_->setConnMax(4);
}

TickThread::~TickThread()
{
    producer_ = NULL;
    delete(tickEpoll_);
    mutex_.Unlock();
    close(notify_send_fd_);
    close(notify_receive_fd_);
}

bool TickThread::SendMessage(const char *msg, int32_t len)
{
    std::vector<std::string> msgs(1, std::string(msg, len));
    std::string errstr("");
    bool ret = producer_->send(msgs, qbus_topic_, errstr, KafkaConstDef::MESSAGE_RANDOM_SEND);
    return ret;
}


void TickThread::RunProcess()
{
    thread_id_ = pthread_self();
    int nfds;
    TickFiredEvent *tfe = NULL;
    char bb[1];
    TickItem *ti;
    for (;;) {
        nfds = tickEpoll_->TickPoll();
        tfe = tickEpoll_->firedevent();
        for (int i = 0; i < nfds; i++) {
            if ((tfe + i)->mask_ & EPOLLIN) {
                read(notify_receive_fd_, bb, 1);
                {
                MutexLock l(&mutex_);
                ti = conn_queue_.front();
                conn_queue_.pop();
                }
                SendMessage(ti->msg(), ti->len());
                /*
                 * After consume the item, you need delete it
                 */
                delete(ti);
                LOG(WARNING) << "Sent one message to QBus";
            }
        }
    }
}
