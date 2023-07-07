#ifndef THROTTLE_H_
#define THROTTLE_H_
#include <atomic>

class Throttle {
public:
    Throttle(size_t throttle_throughput_bytes, size_t check_cycle);
    ~Throttle();
    size_t ThrottledByThroughput(size_t bytes);
    void ReturnUnusedThroughput(
             size_t acquired, size_t consumed, size_t elaspe_time_us);
private:
    std::atomic<size_t> throttle_throughput_bytes_;
    // the num of tasks doing install_snapshot
    std::atomic<size_t> last_throughput_check_time_us_;
    std::atomic<size_t> cur_throughput_bytes_;
    // user defined check cycles of throughput per second
    size_t check_cycle_;
};

#endif