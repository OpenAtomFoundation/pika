
#ifndef PIKA_STREAM_H_
#define PIKA_STREAM_H_

#include <cstdint>
#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "include/pika_stream_types.h"
#include "storage/storage.h"

/*
 * list
 */
class XAddCmd : public Cmd {
 public:
  XAddCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new XAddCmd(*this); }

 private:
  std::string key_;
  std::vector<std::pair<std::string, std::string>> filed_values_;
  /* XADD options */
  streamID id;         /* User-provided ID, for XADD only. */
  int id_given = 0;    /* Was an ID different than "*" specified? for XADD only. */
  int seq_given = 0;   /* Was an ID different than "ms-*" specified? for XADD only. */
  int no_mkstream = 0; /* if set to 1 do not create new stream */

  /* XADD + XTRIM common options */
  StreamTrimStrategy trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_NONE; /* TRIM_STRATEGY_* */
  int trim_strategy_arg_idx = 0; /* Index of the count in MAXLEN/MINID, for rewriting. */
  int approx_trim = 0;           /* If 1 only delete whole radix tree nodes, so
                                  * the trim argument is not applied verbatim. */
  uint64_t limit = 0;            /* Maximum amount of entries to trim. If 0, no limitation
                                  * on the amount of trimming work is enforced. */
  /* TRIM_STRATEGY_MAXLEN options */
  uint64_t maxlen = 0; /* After trimming, leave stream at this length . */
  /* TRIM_STRATEGY_MINID options */
  streamID minid; /* Trim by ID (No stream entries with ID < 'minid' will remain) */

  void DoInitial() override;
  void Clear() override { filed_values_.clear(); }
};

#endif
