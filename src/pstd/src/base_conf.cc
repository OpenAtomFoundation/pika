// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pstd/include/base_conf.h"

#include <sys/stat.h>
#include <algorithm>

#include <glog/logging.h>

#include "pstd/include/env.h"
#include "pstd/include/pstd_string.h"
#include "pstd/include/xdebug.h"

namespace pstd {

static const int kConfItemLen = 1024 * 1024;

BaseConf::BaseConf(const std::string& path) : rep_(new Rep(path)) {}

BaseConf::~BaseConf() { delete rep_; }

int BaseConf::LoadConf() {
  if (!FileExists(rep_->path)) {
    return -1;
  }
  SequentialFile* sequential_file;
  NewSequentialFile(rep_->path, &sequential_file);

  // read conf items

  char line[kConfItemLen];
  char name[kConfItemLen], value[kConfItemLen];
  int line_len = 0;
  int name_len = 0, value_len = 0;
  int sep_sign = 0;
  Rep::ConfType type = Rep::kConf;

  while (sequential_file->ReadLine(line, kConfItemLen) != nullptr) {
    sep_sign = 0;
    name_len = 0;
    value_len = 0;
    type = Rep::kComment;
    line_len = strlen(line);
    for (int i = 0; i < line_len; i++) {
      if (i == 0 && line[i] == COMMENT) {
        type = Rep::kComment;
        break;
      }
      switch (line[i]) {
        case SPACE:
        case '\r':
        case '\n':
          break;
        case COLON:
          if (!sep_sign) {
            type = Rep::kConf;
            sep_sign = 1;
            break;
          }
        default:
          if (sep_sign == 0) {
            name[name_len++] = line[i];
          } else {
            value[value_len++] = line[i];
          }
      }
    }

    if (type == Rep::kConf) {
      rep_->item.push_back(Rep::ConfItem(Rep::kConf, std::string(name, name_len), std::string(value, value_len)));
    } else {
      rep_->item.push_back(Rep::ConfItem(Rep::kComment, std::string(line, line_len)));
    }
  }

  // sequential_file->Close();
  delete sequential_file;
  return 0;
}

int BaseConf::ReloadConf() {
  Rep* rep = rep_;
  rep_ = new Rep(rep->path);
  if (LoadConf() == -1) {
    delete rep_;
    rep_ = rep;
    return -1;
  }
  delete rep;
  return 0;
}

bool BaseConf::GetConfInt(const std::string& name, int* value) const {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      (*value) = atoi(rep_->item[i].value.c_str());
      return true;
    }
  }
  return false;
}

bool BaseConf::GetConfIntHuman(const std::string& name, int* value) const {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      auto c_str = rep_->item[i].value.c_str();
      (*value) = strtoll(c_str, nullptr, 10);
      char last = c_str[rep_->item[i].value.size() - 1];
      if (last == 'K' || last == 'k') {
        (*value) *= (1 << 10);
      } else if (last == 'M' || last == 'm') {
        (*value) *= (1 << 20);
      } else if (last == 'G' || last == 'g') {
        (*value) *= (1 << 30);
      }
      return true;
    }
  }
  return false;
}

bool BaseConf::GetConfInt64Human(const std::string& name, int64_t* value) const {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      auto c_str = rep_->item[i].value.c_str();
      (*value) = strtoll(c_str, nullptr, 10);
      char last = c_str[rep_->item[i].value.size() - 1];
      if (last == 'K' || last == 'k') {
        (*value) *= (1 << 10);
      } else if (last == 'M' || last == 'm') {
        (*value) *= (1 << 20);
      } else if (last == 'G' || last == 'g') {
        (*value) *= (1 << 30);
      }
      return true;
    }
  }
  return false;
}

bool BaseConf::GetConfInt64(const std::string& name, int64_t* value) const {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      (*value) = strtoll(rep_->item[i].value.c_str(), nullptr, 10);
      return true;
    }
  }
  return false;
}

bool BaseConf::GetConfStr(const std::string& name, std::string* val) const {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == 1) {
      continue;
    }
    if (name == rep_->item[i].name) {
      (*val) = rep_->item[i].value;
      return true;
    }
  }
  return false;
}

bool BaseConf::GetConfStrVec(const std::string& name, std::vector<std::string>* value) const {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      std::string val_str = rep_->item[i].value;
      std::string::size_type pos;
      while (true) {
        pos = val_str.find(",");
        if (pos == std::string::npos) {
          value->push_back(StringTrim(val_str));
          break;
        }
        value->push_back(StringTrim(val_str.substr(0, pos)));
        val_str = val_str.substr(pos + 1);
      }
      return true;
    }
  }
  return false;
}

bool BaseConf::GetConfBool(const std::string& name, bool* value) const {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      if (rep_->item[i].value == "true" || rep_->item[i].value == "1" || rep_->item[i].value == "yes") {
        (*value) = true;
      } else if (rep_->item[i].value == "false" || rep_->item[i].value == "0" || rep_->item[i].value == "no") {
        (*value) = false;
      }
      return true;
    }
  }
  return false;
}

bool BaseConf::SetConfInt(const std::string& name, const int value) {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      rep_->item[i].value = std::to_string(value);
      return true;
    }
  }
  return false;
}

bool BaseConf::SetConfInt64(const std::string& name, const int64_t value) {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      rep_->item[i].value = std::to_string(value);
      return true;
    }
  }
  return false;
}

bool BaseConf::SetConfStr(const std::string& name, const std::string& value) {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      rep_->item[i].value = value;
      return true;
    }
  }
  return false;
}

bool BaseConf::SetConfBool(const std::string& name, const bool value) {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      if (value == true) {
        rep_->item[i].value = "true";
      } else {
        rep_->item[i].value = "false";
      }
      return true;
    }
  }
  return false;
}

bool BaseConf::SetConfStrVec(const std::string& name, const std::vector<std::string>& value) {
  std::string value_str = StringConcat(value, COMMA);
  return SetConfStr(name, value_str);
}

bool BaseConf::CheckConfExist(const std::string& name) const {
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kComment) {
      continue;
    }
    if (name == rep_->item[i].name) {
      return true;
    }
  }
  return false;
}

void BaseConf::DumpConf() const {
  int cnt = 1;
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kConf) {
      char buf[256];
      int len = snprintf(buf, sizeof(buf), "%2d %s %s\n", cnt++, rep_->item[i].name.c_str(), rep_->item[i].value.c_str());
      LOG(INFO) << buf;
    }
  }
}

bool BaseConf::WriteBack() {
  WritableFile* write_file;
  std::string tmp_path = rep_->path + ".tmp";
  Status ret = NewWritableFile(tmp_path, &write_file);
  log_info("ret %s", ret.ToString().c_str());
  if (!write_file) {
    return false;
  }
  std::string tmp;
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kConf) {
      tmp = rep_->item[i].name + " : " + rep_->item[i].value + "\n";
      write_file->Append(tmp);
    } else {
      write_file->Append(rep_->item[i].value);
    }
  }
  DeleteFile(rep_->path);
  RenameFile(tmp_path, rep_->path);
  delete write_file;
  return true;
}

void BaseConf::WriteSampleConf() const {
  WritableFile* write_file;
  std::string sample_path = rep_->path + ".sample";
  Status ret = NewWritableFile(sample_path, &write_file);
  std::string tmp;
  for (size_t i = 0; i < rep_->item.size(); i++) {
    if (rep_->item[i].type == Rep::kConf) {
      tmp = rep_->item[i].name + " :\n";
      write_file->Append(tmp);
    } else {
      write_file->Append(rep_->item[i].value);
    }
  }
  delete write_file;
  return;
}

void BaseConf::PushConfItem(const Rep::ConfItem& item) { rep_->item.push_back(item); }

}  // namespace pstd
