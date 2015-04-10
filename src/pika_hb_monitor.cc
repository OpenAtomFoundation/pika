#include "pika_hb_monitor.h"
#include "xdebug.h"
#include <algorithm>

PikaHbMonitor::PikaHbMonitor(std::vector<PikaNode>* cur) :
    cur_(cur)
{
    thread_id_ = pthread_self();
    pre.clear();
}

void PikaHbMonitor::CreateHbMonitor(pthread_t* pid, PikaHbMonitor* pikaHbMonitor)
{
    pthread_create(pid, NULL, &(PikaHbMonitor::StartHbMonitor), pikaHbMonitor);
    return;
}

void* PikaHbMonitor::StartHbMonitor(void* arg)
{
    reinterpret_cast<PikaHbMonitor*>(arg)->RunProcess();
    return NULL;
}

void PikaHbMonitor::RunProcess()
{
    std::vector<PikaNode>::iterator iter;
    std::vector<PikaNode>::iterator preIter;
    while (1) {
        for (iter = cur_->begin(); iter != cur_->end(); iter++) {
            log_info("compare pre cur host %s port %d", (*iter).host()->c_str(), (*iter).port());
            if (std::find(pre.begin(), pre.end(), *iter) == cur_->end()) {
                /*
                 * This node can't found in the pre node list, this mean the node havn't 
                 * added into the metadata before. so we need add to the
                 * metadata
                 */

            }
        }
        for (preIter = pre.begin(); preIter != pre.end(); preIter++) {
            log_info("compare cur cur host %s port %d", (*preIter).host()->c_str(), (*preIter).port());
            /*
             * if we find this node in the cur node list, this mean we have add this node to the metadata
             */
            if (std::find(cur_->begin(), cur_->end(), *preIter) == pre.end()) {
                /*
                 * This node can't found in the cur_ node list, this mean the node is
                 * disconnected, so we need delete the node from the metadata 
                 */
            }
        }

        sleep(3);
    }
}
