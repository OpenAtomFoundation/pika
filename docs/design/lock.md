     
pika作为类redis的存储系统，为了弥补在性能上的不足，在整个系统中大量使用[多线程的结构](https://github.com/Qihoo360/pika/wiki/pika-%E7%BA%BF%E7%A8%8B%E6%A8%A1%E5%9E%8B)，涉及到多线程编程，势必需要为线程加锁来保证数据访问的一致性和有效性。其中主要用到了三种锁

1. 互斥锁
1. 读写锁
2. 行锁

## 读写锁
### 应用场景
应用挂起指令，在挂起指令的执行中，会添加写锁，以确保，此时没有其他指令执行。其他的普通指令在会添加读锁，可以并行访问。
其中挂起指令有：
1. trysync
2. bgsave
3. flushall
4. readonly

#### 作用和意义
保证当前服务器在执行挂起指令时，起到阻写作用。

## 行锁
行锁，用于对一个key加锁，保证同一时间只有一个线程对一个key进行操作。

### 作用和意义：
pika中存取的数据都是类key，value数据，不同key所对应的数据完全独立，所以只需要对key加锁可以保证数据在并发访问时的一致性，行锁相对来说，锁定粒度小，也可以保证数据访问的高效性。

### 应用场景
在pika系统中，对于数据库的操作都需要添加行锁，主要在应用于两个地方，在系统上层指令过程中和在数据引擎层面。在pika系统中，对于写指令(会改变数据状态，如SET,HSET)需要除了更新数据库状态，还涉及到pika的[增量同步](https://github.com/Qihoo360/pika/wiki/pika-%E4%B8%BB%E4%BB%8E%E5%90%8C%E6%AD%A5%E5%8A%9F%E8%83%BD),需要在binlog中添加所执行的写指令，用于保证master和slave的数据库状态一致。故一条写指令的执行，主要有两个部分：

1. 更改数据库状态
2. 将指令添加到binlog中

其加锁情况，如下图：
![](http://ww4.sinaimg.cn/large/c2cd4307jw1f6no7d5557j20fa0ma74x.jpg)

#### 设计的平衡
在图中可以看到，对同一个key，加了两次行锁，在实际应用中，pika上所加的锁就已经能够保证数据访问的正确性。如果只是为了pika所需要的业务，blackwidow层面使用行锁是多余的，但是[blackwidow的设计](https://github.com/Qihoo360/pika/wiki/pika-blackwidow引擎数据存储格式)初衷就是通过对rocksdb的改造和封装提供一套完整的类redis数据访问的解决方案，而不仅仅是为pika提供数据库引擎。这种设计思路也是秉承了Unix中的设计原则：Write programs that do one thing and do it well。

这样设计大大降低了pika与blackwidow之间的耦合，也使得blackwidow可以被单独拿出来测试和使用，在pika中的[数据迁移工具](https://github.com/Qihoo360/pika/wiki/pika%E5%88%B0redis%E8%BF%81%E7%A7%BB%E5%B7%A5%E5%85%B7)就是完全使用blackwidow来完成，不必依赖任何pika相关的东西。另外对于blackwidow感兴趣或者有需求的团队也可以直接将blackwidow作为数据库引擎而不需要修改任何代码就能使用完整的数据访问功能。


### 具体实现
在pika系统中，一把行锁就可以维护所有key。在行锁的实现上是将一个key与一把互斥锁相绑定，并将其放入哈希表中维护，来保证每次访问key的线程只有一个，但是不可能也不需要为每一个key保留一把互斥锁，只需要当有多条线程访问同一个key时才需要锁，在所有线程都访问结束之后，就可以销毁这个绑定key的互斥锁，释放资源。具体实现如下：
``` C++
class RecordLock {
 public:
  RecordLock(port::RecordMutex *mu, const std::string& key)
      : mu_(mu), key_(key) {
        mu_->Lock(key_);
      }
  ~RecordLock() { mu_->Unlock(key_); }

 private:
  port::RecordMutex *const mu_;
  std::string key_;

  // No copying allowed
  RecordLock(const RecordLock&);
  void operator=(const RecordLock&);
};

void RecordMutex::Lock(const std::string& key) {
  mutex_.Lock();
  std::unordered_map<std::string, RefMutex *>::const_iterator it = records_.find(key);

  if (it != records_.end()) {
    //log_info ("tid=(%u) >Lock key=(%s) exist, map_size=%u", pthread_self(), key.c_str(), records_.size());
    RefMutex *ref_mutex = it->second;
    ref_mutex->Ref();
    mutex_.Unlock();

    ref_mutex->Lock();
    //log_info ("tid=(%u) <Lock key=(%s) exist", pthread_self(), key.c_str());
  } else {
    //log_info ("tid=(%u) >Lock key=(%s) new, map_size=%u ++", pthread_self(), key.c_str(), records_.size());
    RefMutex *ref_mutex = new RefMutex();

    records_.insert(std::make_pair(key, ref_mutex));
    ref_mutex->Ref();
    mutex_.Unlock();

    ref_mutex->Lock();
    //log_info ("tid=(%u) <Lock key=(%s) new", pthread_self(), key.c_str());
  }
}

```
完整代码可参考：[pstd_mutex.cc](https://github.com/OpenAtomFoundation/pika/blob/unstable/src/pstd/src/pstd_mutex.cc) [pstd_mutex.h](https://github.com/OpenAtomFoundation/pika/blob/unstable/src/pstd/include/pstd_mutex.h)
