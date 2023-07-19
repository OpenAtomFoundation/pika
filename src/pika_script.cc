#include <fmt/core.h>

extern "C" {
#include "include/rand.h"
#include "include/sha1.h"
}

#include "include/pika_command.h"
#include "include/pika_script.h"
#include "include/pika_server.h"

#include <fmt/core.h>
#include <numeric>

#include "pstd/include/pstd_string.h"

extern PikaServer *g_pika_server;

void sha1hex(char *digest, char *script, std::size_t len) {
  SHA1_CTX ctx;
  unsigned char hash[20];
  char *cset = const_cast<char*>("0123456789abcdef");
  int j;

  SHA1Init(&ctx);
  SHA1Update(&ctx, (unsigned char *)script, len);
  SHA1Final(hash, &ctx);

  for (j = 0; j < 20; j++) {
    digest[j * 2] = cset[((hash[j] & 0xF0) >> 4)];
    digest[j * 2 + 1] = cset[(hash[j] & 0xF)];
  }
  digest[40] = '\0';
}

/* We replace math.random() with our implementation that is not affected
 * by specific libc random() implementations and will output the same sequence
 * (for the same seed) in every arch. */

/* The following implementation is the one shipped with Lua itself but with
 * rand() replaced by redisLrand48(). */
int redis_math_random(lua_State *L) {
  /* the `%' avoids the (rare) case of r==1, and is needed also because on
     some systems (SunOS!) `rand()' may return a value larger than RAND_MAX */
  lua_Number r = (lua_Number)(redisLrand48() % REDIS_LRAND48_MAX) / (lua_Number)REDIS_LRAND48_MAX;
  switch (lua_gettop(L)) {  /* check number of arguments */
    case 0: {               /* no arguments */
      lua_pushnumber(L, r); /* Number between 0 and 1 */
      break;
    }
    case 1: { /* only upper limit */
      int u = luaL_checkint(L, 1);
      luaL_argcheck(L, 1 <= u, 1, "interval is empty");
      lua_pushnumber(L, floor(r * u) + 1); /* int between 1 and `u' */
      break;
    }
    case 2: { /* lower and upper limits */
      int l = luaL_checkint(L, 1);
      int u = luaL_checkint(L, 2);
      luaL_argcheck(L, l <= u, 2, "interval is empty");
      lua_pushnumber(L, floor(r * (u - l + 1)) + l); /* int between `l' and `u' */
      break;
    }
    default:
      return luaL_error(L, "wrong number of arguments");
  }
  return 1;
}

int redis_math_randomseed(lua_State *L) {
  redisSrand48(luaL_checkint(L, 1));
  return 0;
}

std::pair<sol::object, char *> RedisProtocolToLuaType_Int(sol::state_view &lua, char *reply) {
  char *p = strchr(reply + 1, '\r');
  long long value;

  pstd::string2int(reply + 1, p - reply - 1, &value);
  return {sol::make_object(lua, value), p + 2};
}

std::pair<sol::object, char *> RedisProtocolToLuaType_Bulk(sol::state_view &lua, char *reply) {
  char *p = strchr(reply + 1, '\r');
  long long bulklen;

  pstd::string2int(reply + 1, p - reply - 1, &bulklen);
  if (bulklen == -1) {
    return {sol::make_object(lua, false), p + 2};
  } else {
    return {sol::make_object(lua, std::string(p + 2, bulklen)), p + 2 + bulklen + 2};
  }
}

std::pair<sol::object, char *> RedisProtocolToLuaType_Status(sol::state_view &lua, char *reply) {
  char *p = strchr(reply + 1, '\r');
  return {lua.create_table_with("ok", std::string(reply + 1, p - reply - 1)), p + 2};
}

std::pair<sol::object, char *> RedisProtocolToLuaType_Error(sol::state_view &lua, char *reply) {
  char *p = strchr(reply + 1, '\r');
  return {lua.create_table_with("err", std::string(reply + 1, p - reply - 1)), p + 2};
}

std::pair<sol::object, char *> RedisProtocolToLuaType(sol::state_view &lua, char *reply);

std::pair<sol::object, char *> RedisProtocolToLuaType_MultiBulk(sol::state_view &lua, char *reply) {
  char *p = strchr(reply + 1, '\r');
  long long mbulklen;
  int j = 0;

  pstd::string2int(reply + 1, p - reply - 1, &mbulklen);
  p += 2;
  if (mbulklen == -1) {
    return {sol::make_object(lua, false), p};
  }
  sol::table tb = lua.create_table();
  for (j = 0; j < mbulklen; j++) {
    auto res = RedisProtocolToLuaType(lua, p);
    tb[j + 1] = res.first;
    p = res.second;
  }
  return {tb, p};
}

std::pair<sol::object, char *> RedisProtocolToLuaType(sol::state_view &lua, char *reply) {
  LOG(INFO) << "redisProtocolToLuaType: " << reply;
  char *p = reply;
  std::pair<sol::object, char *> res;

  switch (*p) {
    case ':':
      res = RedisProtocolToLuaType_Int(lua, reply);
      break;
    case '$':
      res = RedisProtocolToLuaType_Bulk(lua, reply);
      break;
    case '+':
      res = RedisProtocolToLuaType_Status(lua, reply);
      break;
    case '-':
      res = RedisProtocolToLuaType_Error(lua, reply);
      break;
    case '*':
      res = RedisProtocolToLuaType_MultiBulk(lua, reply);
      break;
  }
  return res;
}

sol::table LuaPushError(sol::state_view &lua, const char *error) { return lua.create_table_with("err", error); }

// a lua hook function run every xxx instructions
void LuaMaskCountHook(lua_State *lua, lua_Debug *ar) {
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() -
                                                                       g_pika_server->lua_time_start_);
  // check whether the script is timeout
  if (elapsed >= g_pika_server->lua_time_limit_ && g_pika_server->lua_timedout_ == false) {
    LOG(WARNING) << "Lua slow script detected: still in execution after " << elapsed.count()
                 << " milliseconds. You can try killing the script using the SCRIPT KILL command.";
    g_pika_server->lua_timedout_ = true;
  }
  // receive a script kill command
  if (g_pika_server->lua_kill_) {
    // set false to accept other command
    g_pika_server->lua_timedout_ = false;
    LOG(WARNING) << "Lua script killed by user with SCRIPT KILL.";
    lua_pushstring(lua, "Script killed by user with SCRIPT KILL...");
    lua_error(lua);
  }
}

/* Set an array of Redis String Objects as a Lua array (table) stored into a
 * global variable. */
void LuaSetGlobalArray(sol::state_view &lua, const std::string &var, const std::vector<std::string> &elev) {
  sol::table tb = lua.create_table();
  int j = 1;
  for (const auto &e : elev) {
    tb[j++] = e;
  }
  lua[var] = tb;
}

struct CreateLuaFuncRes {
  enum class CreateLuaFuncStatus { compile_err, run_err, ok };
  CreateLuaFuncStatus status_;
  std::string err_;
};

CreateLuaFuncRes LuaCreateFunction(sol::state_view &lua, const std::string &funcname, const std::string &body) {
  std::string funcdef = fmt::format("function {}() {} end", funcname, body);
  sol::load_result script = lua.load(funcdef, "@user_script");
  if (!script.valid()) {
    return {CreateLuaFuncRes::CreateLuaFuncStatus::compile_err, static_cast<sol::error>(script).what()};
  }
  sol::protected_function_result res = script();
  if (!res.valid()) {
    return {CreateLuaFuncRes::CreateLuaFuncStatus::run_err, static_cast<sol::error>(script).what()};
  }
  /* We also save a SHA1 -> Original script map in a dictionary
   * so that we can replicate / write in the AOF all the
   * EVALSHA commands as EVAL using the original script. */
  g_pika_server->lua_scripts_[funcname.substr(2)] = body;
  return {CreateLuaFuncRes::CreateLuaFuncStatus::ok, ""};
}

/* Sort the array currently in the stack. We do this to make the output
 * of commands like KEYS or SMEMBERS something deterministic when called
 * from Lua (to play well with AOf/replication).
 *
 * The array is sorted using table.sort itself, and assuming all the
 * list elements are strings. */
void LuaSortArray(sol::state_view &lua, sol::table arr) {
  sol::protected_function arr_sort = lua["table"]["sort"];
  sol::protected_function_result res = arr_sort(arr);
  if (!res.valid()) {
    /* We are not interested in the error, we assume that the problem is
     * that there are 'false' elements inside the array, so we try
     * again with a slower function but able to handle this case, that
     * is: table.sort(table, __redis__compare_helper) */
    arr_sort(arr, lua["__redis__compare_helper"]);
  }
}

void LuaLogCommand(sol::this_state l, sol::variadic_args va) {
  sol::state_view lua(l);
  if (va.size() < 2) {
    LuaPushError(lua, "redis.log() requires two arguments or more.");
    return;
  } else if (va.get_type(0) != sol::type::number) {
    LuaPushError(lua, "First argument must be a number (log level).");
    return;
  }
  int level = va.get<int>(0);
  if (level < google::INFO || level > google::FATAL) {
    LuaPushError(lua, "Invalid log level.");
    return;
  }

  /* Glue together all the arguments */
  std::string log_str;
  for (auto iter = va.begin()+1; iter != va.end(); iter++) {
    log_str.append(const_cast<char*>(lua_tostring(iter->lua_state(), iter->stack_index())));
    log_str.push_back(' ');
  }
  log_str.pop_back();

  switch (level) {
    case google::INFO:
      LOG(INFO) << log_str;
      break;
    case google::WARNING:
      LOG(WARNING) << log_str;
      break;
    case google::ERROR:
      LOG(ERROR) << log_str;
      break;
    case google::FATAL:
      LOG(FATAL) << log_str;
      break;
  }
}

/* This adds redis.sha1hex(string) to Lua scripts using the same hashing
* function used for sha1ing lua scripts. */
std::string LuaRedisSha1hexCommand(sol::this_state l, sol::variadic_args va) {
  sol::state_view lua(l);
  if (va.size() != 1) {
    LuaPushError(lua, "wrong number of arguments");
  }
  size_t len;
  // we can accept arg that can be converted to string
  char *s = const_cast<char*>(lua_tolstring(lua, va.stack_index(), &len));
  std::string digest(40, '\0');
  sha1hex(digest.data(), s, len);
  return digest;
}

/* Returns a table with a single field 'field' set to the string value
* passed as argument. This helper function is handy when returning
* a Redis Protocol error or status reply from Lua:
*
* return redis.error_reply("ERR Some Error")
* return redis.status_reply("ERR Some Error")
*/
sol::table LuaRedisReturnSingeFieldTable(sol::state_view& lua, sol::variadic_args va, const std::string &field) {
  if (va.size() != 1 || va.get_type(0) != sol::type::string) {
    return LuaPushError(lua, "wrong number or type of arguments");
  }
  return lua.create_table_with(field, va.get<std::string>(0));
}

sol::table LuaRedisErrorReplyCommand(sol::this_state l, sol::variadic_args va) {
  sol::state_view lua(l);
  return LuaRedisReturnSingeFieldTable(lua, va, "err");
}

sol::table LuaRedisStatusReplyCommand(sol::this_state l, sol::variadic_args va) {
  sol::state_view lua(l);
  return LuaRedisReturnSingeFieldTable(lua, va, "ok");
}

sol::object LuaRedisGenericCommand(sol::state_view &lua, bool raise_error, sol::variadic_args va) {
  if (va.size() == 0) {
    return LuaPushError(lua, "Please specify at least one argument for redis.call()");
  }

  // extract command args from lua
  net::RedisCmdArgsType argv;
  for (auto v : va) {
    if (!lua_isstring(v.lua_state(), v.stack_index())) {
      return LuaPushError(lua, "Lua redis() command arguments must be strings or integers");
    }
    // we can accept arg that can be converted to string
    argv.emplace_back(lua_tostring(v.lua_state(), v.stack_index()));
    LOG(INFO) << "push arg: " << argv.back();
  }

  // TODO improve argument check message in lua
  auto resp = std::make_shared<std::string>();
  bool need_sort = false;
  g_pika_server->lua_client_->ExecRedisCmdInLua(argv, resp, need_sort);
  if (raise_error && resp->front() != '-') raise_error = false;
  auto res = RedisProtocolToLuaType(lua, resp->data());

  /* Sort the output array if needed, assuming it is a non-null multi bulk
   * reply as expected. */
  if (need_sort && (resp->at(0) == '*' && resp->at(1) != '-')) {
    LuaSortArray(lua, res.first);
  }

  if (raise_error) {
    // throw a lua exception
    sol::table tb = res.first;
    throw std::runtime_error(tb["err"].get<std::string>());
  }

  return res.first;
}

sol::object LuaRedisCallCommand(sol::this_state l, sol::variadic_args va) {
  sol::state_view lua(l);
  return LuaRedisGenericCommand(lua, true, va);
}
sol::object LuaRedisPCallCommand(sol::this_state l, sol::variadic_args va) {
  sol::state_view lua(l);
  return LuaRedisGenericCommand(lua, false, va);
}

void EvalCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    if (evalsha_) {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameEvalSha);
    } else {
      res_.SetRes(CmdRes::kWrongNum, kCmdNameEval);
    }
    return;
  }
  if (evalsha_) {
    if (argv_[1].size() != 40) {
      res_.AppendContent("-NOSCRIPT No matching script. Please use EVAL.");
      return;
    }
  }
  script_ = argv_[1];
  auto iter = argv_.begin();
  iter++;
  iter++;
  if (!pstd::string2int(iter->data(), iter->size(), &numkeys_)) {
    res_.SetRes(CmdRes::kInvalidInt);
    return;
  }
  if (numkeys_ > (argv_.size() - 3)) {
    res_.SetRes(CmdRes::kInvalidKeyNums);
    return;
  }
  iter++;
  keys_.assign(iter, iter + numkeys_);
  iter += numkeys_;
  args_.assign(iter, argv_.end());
}

// lua脚本返回值支持一个， 多个可以用table返回
void EvalCmd::LuaReplyToRedisReply(sol::state &lua, sol::object lua_ret) {
  sol::type t = lua_ret.get_type();
  std::string lua_str;
  sol::protected_function_result lua_num;
  long long num;
  std::string reply;
  sol::table tb;
  sol::object val;

  switch (t) {
    case sol::type::string:
      lua_str = lua_ret.as<std::string>();
      RedisAppendLen(reply, lua_str.size(), "$");
      RedisAppendContent(reply, lua_str);
      reply_lst_.emplace_back(std::move(reply));
      break;
    case sol::type::boolean:
      if (lua_ret.as<bool>()) {
        RedisAppendLen(reply, 1, ":");
      } else {
        RedisAppendContent(reply, "$-1");
      }
      reply_lst_.emplace_back(std::move(reply));
      break;
    case sol::type::number:
      // use tonumber to do string -> double conversion
      lua_num = lua["tonumber"](lua_ret);
      if (lua_num.get_type() == sol::type::lua_nil) {
        num = 0;
      } else {
        num = lua_num.get<double>();
      }
      RedisAppendLen(reply, num, ":");
      reply_lst_.emplace_back(std::move(reply));
      break;
    case sol::type::table:
      /* We need to check if it is an array, an error, or a status reply.
       * Error are returned as a single element table with 'err' field.
       * Status replies are returned as single element table with 'ok' field */
      tb = lua_ret.as<sol::table>();
      val = tb["err"];
      t = val.get_type();
      if (t == sol::type::string) {
        reply = "-";
        reply.append(val.as<std::string>());
        std::replace(reply.begin(), reply.end(), '\r', ' ');
        std::replace(reply.begin(), reply.end(), '\n', ' ');
        reply.append("\r\n");
        reply_lst_.emplace_back(std::move(reply));
        return;
      }

      val = tb["ok"];
      t = val.get_type();
      if (t == sol::type::string) {
        reply = "+";
        reply.append(val.as<std::string>());
        std::replace(reply.begin(), reply.end(), '\r', ' ');
        std::replace(reply.begin(), reply.end(), '\n', ' ');
        reply.append("\r\n");
        reply_lst_.emplace_back(std::move(reply));
      } else {
        reply_lst_.emplace_back("");
        auto multibulk_len_iter = --reply_lst_.end();
        int j = 1, mbulklen = 0;

        while (1) {
          val = tb[j++];
          t = val.get_type();
          if (t == sol::type::nil) {
            break;
          }
          LuaReplyToRedisReply(lua, val);
          mbulklen++;
        }

        RedisAppendLen(*multibulk_len_iter, mbulklen, "*");
      }
      break;
    default:
      // fall down to default is nil
      reply_lst_.emplace_back("$-1\r\n");
  }
}

void EvalCmd::Do(std::shared_ptr<Slot> slot) {
  std::string funcname(42, '\0');
  long long numkeys;
  bool delhook = false;

  /* We want the same PRNG sequence at every call so that our PRNG is
   * not affected by external state. */
  redisSrand48(0);

  /* We set this flag to zero to remember that so far no random command
   * was called. This way we can allow the user to call commands like
   * SRANDMEMBER or RANDOMKEY from Lua scripts as far as no write command
   * is called (otherwise the replication and AOF would end with non
   * deterministic sequences).
   *
   * Thanks to this flag we'll raise an error every time a write command
   * is called after a random command was used. */
  g_pika_server->lua_random_dirty_ = false;
  g_pika_server->lua_write_dirty_ = false;

  /* We obtain the script SHA1, then check if this function is already
   * defined into the Lua state */
  funcname[0] = 'f';
  funcname[1] = '_';
  if (!evalsha_) {
    /* Hash the code if this is an EVAL call */
    sha1hex(funcname.data() + 2, script_.data(), script_.size());
  } else {
    /* We already have the SHA if it is a EVALSHA */
    int j;
    char *sha = argv_[1].data();

    for (j = 0; j < 40; j++) funcname[j + 2] = tolower(sha[j]);
  }

  sol::state &lua = (*g_pika_server->lua_);
  std::lock_guard lua_lk(g_pika_server->lua_mutex_);

  /* Try to lookup the Lua function */
  sol::protected_function f(lua[funcname], lua["__redis__error_handler"]);
  if (!f.valid()) {
    if (evalsha_) {
      res_.SetRes(CmdRes::kNoScript);
      return;
    }
    CreateLuaFuncRes res = LuaCreateFunction(lua, funcname, script_);
    if (res.status_ != CreateLuaFuncRes::CreateLuaFuncStatus::ok) {
      res_.SetRes(CmdRes::kErrOther, res.err_);
      return;
    }
    // f = lua[funcname];
    f = sol::protected_function(lua[funcname], lua["__redis__error_handler"]);
    assert(f.valid());
  }

  /* Populate the argv and keys table accordingly to the arguments that
   * EVAL received. */
  LuaSetGlobalArray(lua, "KEYS", keys_);
  LuaSetGlobalArray(lua, "ARGV", args_);

  /* Select the right DB in the context of the Lua client */
  // selectDb(server.lua_client, c->db->id);
  g_pika_server->lua_client_->SetCurrentDB(db_name_);

  g_pika_server->lua_time_start_ = std::chrono::system_clock::now();
  g_pika_server->lua_kill_ = false;
  g_pika_server->lua_timedout_ = false;

  /* Set an hook in order to be able to stop the script execution if it
   * is running for too much time.
   * We set the hook only if the time limit is enabled as the hook will
   * make the Lua script execution slower. */
  // 从节点不需要设置hook， 因为从节点接受主节点的命令， 而主节点接受的是客户端的命令
  if (g_pika_server->lua_time_limit_.count() > 0 && g_pika_server->master_ip() == "") {
    lua_sethook(lua, LuaMaskCountHook, LUA_MASKCOUNT, 100000);
    delhook = true;
  }

  // grab the db lock exclusive
  std::lock_guard rwl(g_pika_server->dbs_rw_);
  // set the bool indicates that we are running lua script, do not try to grab the db lock again
  g_pika_server->lua_calling_ = true;

  /* At this point whatever this script was never seen before or if it was
   * already defined, we can call it. We have zero arguments and expect
   * a single return value. */
  sol::protected_function_result res = f();

  g_pika_server->lua_calling_ = false;

  /* Perform some cleanup that we need to do both on error and success. */
  if (delhook) lua_sethook(lua, LuaMaskCountHook, 0, 0); /* Disable hook */
  // g_pika_server->lua_caller = nullptr;
  // TODO 对于slave来说这里的conn是空指针
  std::shared_ptr<PikaClientConn> conn = std::dynamic_pointer_cast<PikaClientConn>(GetConn());
  if (conn.get() != nullptr) {
    // set the client db to lua client db in case of using select in lua
    conn->SetCurrentDB(g_pika_server->lua_client_->CurrentDB());
  }
  lua.step_gc(1);

  if (!res.valid()) {
    res_.SetRes(CmdRes::kErrOther,
                fmt::format("Error running script (call to {}): {}", funcname, static_cast<sol::error>(res).what()));
  } else {
    /* On success convert the Lua return value into Redis protocol, and
     * send it to * the client. */
    sol::object first_ret = res.get<sol::object>();
    LuaReplyToRedisReply(lua, first_ret);
    // concat all reply to a single string
    res_.AppendStringRaw(std::accumulate(reply_lst_.begin(), reply_lst_.end(), std::string("")));
  }

  // if write happens, we set eval command flags to do binlog
  // if (g_pika_server->lua_write_dirty_) {
  //   flag_ |= kCmdFlagsWrite;
  // }
}

void ScriptCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameScript);
    return;
  }
}

// ScriptCmd不会拿db锁， 因此我们要拿lua锁
void ScriptCmd::Do(std::shared_ptr<Slot> slot) {
  if (argv_.size() == 2 && argv_[1] == "flush") {
    // 访问lua
    g_pika_server->ScriptingReset();
    res_.SetRes(CmdRes::kOk);
    // TODO make it replicate
    // flag_ |= kCmdFlagsWrite;
  } else if (argv_.size() >= 2 && argv_[1] == "exists") {
    // exists 命令只需要访问list
    // TODO lock granularity?
    std::lock_guard lua_lk(g_pika_server->lua_mutex_);
    auto& lua_scripts = g_pika_server->lua_scripts_;
    res_.AppendArrayLen(argv_.size() - 2);
    for (int j = 2; j < argv_.size(); j++) {
      if (lua_scripts.find(argv_[j]) == lua_scripts.end()) {
        res_.AppendInteger(0);
      } else {
        res_.AppendInteger(1);
      }
    }

  } else if (argv_.size() == 3 && argv_[1] == "load") {
    // 访问lua, list
    std::lock_guard lua_lk(g_pika_server->lua_mutex_);
    auto& lua_scripts = g_pika_server->lua_scripts_;
    std::string funcname(42, '\0');
    funcname[0] = 'f';
    funcname[1] = '_';
    sha1hex(funcname.data() + 2, argv_[2].data(), argv_[2].size());
    if (lua_scripts.find(funcname) == lua_scripts.end()) {
      CreateLuaFuncRes res = LuaCreateFunction(*g_pika_server->lua_, funcname, argv_[2]);
      if (res.status_ != CreateLuaFuncRes::CreateLuaFuncStatus::ok) {
        res_.SetRes(CmdRes::kErrOther, res.err_);
        return;
      }
    }
    res_.AppendString(funcname.substr(2));
  } else if (argv_.size() == 2 && argv_[1] == "kill") {
    // 并发正确性验证， lua_calling_为true的时候， 一定在运行脚本
    if (!g_pika_server->lua_calling_) {
      res_.AppendContent("-NOTBUSY No scripts in execution right now.");
    } else if (g_pika_server->lua_write_dirty_) {
      res_.AppendContent("-UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in an hard way using the SHUTDOWN NOSAVE command.");
    } else {
      LOG(INFO) << "User requested Lua script termination via SCRIPT KILL.";
      g_pika_server->lua_kill_ = true;
      res_.SetRes(CmdRes::kOk);
    }
  } else {
    res_.SetRes(CmdRes::kErrOther, "Unknown SCRIPT subcommand or wrong # of args. Try SCRIPT HELP.");
  }
}