
#include "include/pika_stream_util.h"
#include <cstdint>
#include <memory>
#include "include/pika_command.h"
#include "include/pika_stream_meta_value.h"
#include "rocksdb/status.h"

// void TreeIDGenerator::TryToFetchLastIdFromStorage() {

// }

// static std::unique_ptr<ParsedStreamMetaFiledValue> GetStreamMeta(const std::string &key,
//                                                                  const std::shared_ptr<Slot> &slot) {
//   std::string value;
//   rocksdb::Status s = slot->db()->HGet(STREAM_META_HASH_KEY, key, &value);
//   return std::make_unique<ParsedStreamMetaFiledValue>(&value);
// }

CmdRes StreamUtil::StreamGenericParseIDOrReply(const std::string &var, streamID *id, uint64_t missing_seq, bool strict,
                                               int *seq_given) {
  CmdRes res;
  char buf[128];
  if (var.size() > sizeof(buf) - 1) {
    res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return res;
  }

  memcpy(buf, var.data(), var.size());
  buf[var.size()] = '\0';

  if (strict && (buf[0] == '-' || buf[0] == '+') && buf[1] == '\0') {
    res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return res;
  }

  if (seq_given != nullptr) {
    *seq_given = 1;
  }

  /* Handle the "-" and "+" special cases. */
  if (buf[0] == '-' && buf[1] == '\0') {
    id->ms = 0;
    id->seq = 0;
    res.SetRes(CmdRes::kOk);
    return res;
  } else if (buf[0] == '+' && buf[1] == '\0') {
    id->ms = UINT64_MAX;
    id->seq = UINT64_MAX;
    res.SetRes(CmdRes::kOk);
    return res;
  }

  /* Parse <ms>-<seq> form. */
  uint64_t ms;
  uint64_t seq;
  char *dot = strchr(buf, '-');
  if (dot) {
    *dot = '\0';
  }
  if (!string2uint64(buf, ms)) {
    res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return res;
  };
  if (dot) {
    size_t seqlen = strlen(dot + 1);
    if (seq_given != nullptr && seqlen == 1 && *(dot + 1) == '*') {
      /* Handle the <ms>-* form. */
      seq = 0;
      *seq_given = 0;
    } else if (!string2uint64(dot + 1, seq)) {
      res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
      return res;
    }
  } else {
    seq = missing_seq;
  }
  id->ms = ms;
  id->seq = seq;
  res.SetRes(CmdRes::kOk);
  return res;
}

bool StreamUtil::string2uint64(const char *s, uint64_t &value) {
  char *end;
  uint64_t tmp = strtoull(s, &end, 10);

  if (*end) {
    return false;
  }

  value = tmp;
  return true;
}

