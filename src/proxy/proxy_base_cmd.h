#pragma once 

#include <atomic>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "proxy_base_cmd.h"
#include "client.h"
#include "store.h"

namespace pikiwidb {

const std::string kTaskNamePing = "ping";

enum TaskFlags {
  kTaskFlagsWrite = (1 << 0),
  
};

class ProxyBaseCmd : public std::enable_shared_from_this<ProxyBaseCmd> {
public:
  enum class OpType {
    kGet,
    kSet,
    kDel,
    kIncr,
    kDecr,
    kUnknown,
  };

  enum class KeyType {
    kGKey,
    kMKey,
    kUnknown,
  };
  
  enum class TaskType {
    kExecute,
    kCallback,
    kUnknown,
  };

  ProxyBaseCmd() = default;
  virtual ~ProxyBaseCmd() = default;

  virtual void Execute() = 0;
  virtual void CallBack() = 0;
  
  virtual std::string GetCommand() = 0;

  OpType GetOpType() const { return op_type_; }
  KeyType GetKeyType() const { return key_type_; }
  TaskType GetTaskType() const { return task_type_; }
  virtual std::shared_ptr<PClient> Client() = 0;
  virtual std::string GetKey() = 0; 

protected:
  OpType op_type_ = OpType::kUnknown;
  KeyType key_type_ = KeyType::kUnknown;
  TaskType task_type_ = TaskType::kUnknown;

  // TODO(Tangruilin): counter for mget or mset

private:
  virtual bool DoInitial(PClient* client) = 0;
};

}