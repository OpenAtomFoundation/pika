
#include "include/pika_stream_util.h"
#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "include/pika_command.h"
#include "include/pika_stream_meta_value.h"
#include "rocksdb/status.h"

bool StreamUtil::is_stream_meta_hash_created_ = false;

CmdRes ParseAddOrTrimArgs(const PikaCmdArgsType &argv, StreamAddTrimArgs &args, int &idpos, bool is_xadd) {
  CmdRes res;
  int i = 2;
  bool limit_given = false;
  for (; i < argv.size(); ++i) {
    int moreargs = argv.size() - 1 - i;
    const std::string &opt = argv[i];
    if (is_xadd && opt == "*" && opt.size() == 1) {
      // case: XADD mystream * field value [field value ...]
      break;
    } else if (opt == "maxlen" && moreargs) {
      if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return res;
      }
      args.approx_trim = false;
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && next == "~") {
        // case: XADD mystream MAXLEN ~ <count> * field value [field value ...]
        args.approx_trim = true;
        i++;
      } else if (moreargs >= 2 && next == "=") {
        // case: XADD mystream MAXLEN = <count> * field value [field value ...]
        i++;
      }
      if (!StreamUtil::string2uint64(argv[i + 1].c_str(), args.maxlen)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid MAXLEN argument");
      }
      i++;
      args.trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_MAXLEN;
      args.trim_strategy_arg_idx = i;
    } else if (opt == "minid" && moreargs) {
      if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return res;
      }
      args.approx_trim = false;
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && next == "~" && next.size() == 1) {
        // case: XADD mystream MINID ~ <id> * field value [field value ...]
        args.approx_trim = true;
        i++;
      } else if (moreargs >= 2 && next == "=" && next.size() == 1) {
        // case: XADD mystream MINID ~ <id> = field value [field value ...]
        i++;
      }
      auto ret = StreamUtil::StreamParseStrictID(argv[i + 1], args.minid, 0, nullptr);
      if (!ret.ok()) {
        res = ret;
        return res;
      }
      i++;
      args.trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_MINID;
      args.trim_strategy_arg_idx = i;
    } else if (opt == "limit" && moreargs) {
      // case: XADD mystream ... LIMIT ...
      if (!StreamUtil::string2uint64(argv[i + 1].c_str(), args.limit)) {
        res.SetRes(CmdRes::kInvalidParameter);
        return res;
      }
      limit_given = true;
      i++;
    } else if (is_xadd && opt == "nomkstream") {
      // case: XADD mystream ... NOMKSTREAM ...
      args.no_mkstream = 1;
    } else if (is_xadd) {
      // case: XADD mystream <ID or *> field value [field value ...]
      auto ret = StreamUtil::StreamParseStrictID(argv[i], args.id, 0, nullptr);
      if (!ret.ok()) {
        res = ret;
        return res;
      }
      break;
    } else {
      res.SetRes(CmdRes::kSyntaxErr);
      return res;
    }
  }  // end for

  if (args.limit && args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_NONE) {
    res.SetRes(CmdRes::kSyntaxErr, "syntax error, LIMIT cannot be used without specifying a trimming strategy");
    return res;
  }

  if (!is_xadd && args.trim_strategy == StreamTrimStrategy::TRIM_STRATEGY_NONE) {
    res.SetRes(CmdRes::kSyntaxErr, "syntax error, XTRIM must be called with a trimming strategy");
    return res;
  }

  // FIXME: figure out what is mustObeyClient() means in redis
  // if (mustObeyClient(c)) {
  //   args->limit = 0;
  // } else {

  if (limit_given) {
    if (!args.approx_trim) {
      res.SetRes(CmdRes::kSyntaxErr, "syntax error, LIMIT cannot be used without the special ~ option");
      return res;
    }
  } else {
    // if limit given but not give
    if (args.approx_trim) {
      // FIXME: let limit can be defined in config
      args.limit = 100 * 10000;
      if (args.limit <= 0) {
        args.limit = KSTREAM_MIN_LIMIT;
      }
      if (args.limit > kSTREAM_MAX_LIMIT) {
        args.limit = kSTREAM_MAX_LIMIT;
      }
    } else {
      args.limit = 0;
    }
  }

  idpos = i;
  return res;
}

// Korpse TODO: unit test
CmdRes StreamUtil::GetStreamMeta(const std::string &key, std::string &value, const std::shared_ptr<Slot> &slot) {
  rocksdb::Status s;
  CmdRes res;
  s = slot->db()->HGet(STREAM_META_HASH_KEY, key, &value);

  // check is_stream_meta_hash_created_ flag only when the key is not found
  if (!s.ok() && !is_stream_meta_hash_created_) {
    s = CheckAndCreateStreamMetaHash(key, value, slot);
    if (!s.ok()) {  // unknown error
      res.SetRes(CmdRes::kErrOther, "Failed to create stream meta hash: " + s.ToString());
      return res;
    }
    // retry to get the value
    s = slot->db()->HGet(STREAM_META_HASH_KEY, key, &value);
  }

  if (!s.ok()) {
    res.SetRes(CmdRes::kNotFound);
    return res;
  }

  res.SetRes(CmdRes::kOk);
  return res;
}

// Korpse TODO: unit test
CmdRes StreamUtil::StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
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

  if (buf[0] == '-' && buf[1] == '\0') {
    id.ms = 0;
    id.seq = 0;
    res.SetRes(CmdRes::kOk);
    return res;
  } else if (buf[0] == '+' && buf[1] == '\0') {
    id.ms = UINT64_MAX;
    id.seq = UINT64_MAX;
    res.SetRes(CmdRes::kOk);
    return res;
  }

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
      seq = 0;
      *seq_given = 0;
    } else if (!string2uint64(dot + 1, seq)) {
      res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
      return res;
    }
  } else {
    seq = missing_seq;
  }
  id.ms = ms;
  id.seq = seq;
  res.SetRes(CmdRes::kOk);
  return res;
}

CmdRes StreamUtil::StreamParseID(const std::string &var, streamID &id, uint64_t missing_seq) {
  return StreamGenericParseID(var, id, missing_seq, false, nullptr);
}

CmdRes StreamUtil::StreamParseStrictID(const std::string &var, streamID &id, uint64_t missing_seq, int *seq_given) {
  return StreamGenericParseID(var, id, missing_seq, true, seq_given);
}

// Korpse TODO: unit test
bool StreamUtil::string2uint64(const char *s, uint64_t &value) {
  if (!s) {
    return false;
  }

  char *end;
  uint64_t tmp = strtoull(s, &end, 10);

  if (*end) {
    return false;
  }

  value = tmp;
  return true;
}

// Korpse TODO: unit test
rocksdb::Status StreamUtil::CreateStreamMetaHash(const std::string &key, std::string &value,
                                                         const std::shared_ptr<Slot> &slot) {
  std::unique_lock<std::mutex> lg(create_stream_meta_hash_mutex_);

  rocksdb::Status s;

  // recheck if the stream meta key exists, it must be checked in the exclusion zone:
  // 1. in case of the key is created by other thread at the same time
  // 2. the db has rebooted and is_stream_meta_hash_created_ flag is not set
  std::map<storage::DataType, rocksdb::Status> tmp_type_status;
  tmp_type_status[storage::DataType::kHashes] = s;
  (void)slot->db()->Exists(std::vector<std::string>{STREAM_META_HASH_KEY}, &tmp_type_status);
  if (tmp_type_status[storage::DataType::kHashes].ok()) {
    is_stream_meta_hash_created_ = true;
    return s;
  }

  // other wise, create the stream meta hash
  int32_t res{0};
  s = slot->db()->HSet(STREAM_META_HASH_KEY, key, value, &res);
  (void)res;
  if (!s.ok()) {
    LOG(FATAL) << "Failed to create stream meta hash: " << s.ToString();
    return s;
  }

  is_stream_meta_hash_created_ = true;
  return s;
}
