#ifndef PIKA_SCRIPT_H
#define PIKA_SCRIPT_H

#include <cstdint>
#include "script/sol/sol.hpp"

#include "include/pika_command.h"
#include "include/pika_slot.h"

void sha1hex(char *digest, char *script, std::size_t len);
int redis_math_random(lua_State *L);
int redis_math_randomseed(lua_State *L);

sol::table LuaPushError(sol::state_view &lua, const char *error);
sol::object LuaRedisCallCommand(sol::this_state lua, sol::variadic_args va);
sol::object LuaRedisPCallCommand(sol::this_state lua, sol::variadic_args va);
void LuaLogCommand(sol::this_state l, sol::variadic_args va);
std::string LuaRedisSha1hexCommand(sol::this_state l, sol::variadic_args va);
sol::table LuaRedisErrorReplyCommand(sol::this_state l, sol::variadic_args va);
sol::table LuaRedisStatusReplyCommand(sol::this_state l, sol::variadic_args va);

/*
 * eval
 */
class EvalCmd : public Cmd {
 public:
  EvalCmd(const std::string &name, int arity, uint16_t flag, bool evalsha)
      : Cmd(name, arity, flag), evalsha_(evalsha) {}
  std::vector<std::string> current_key() const override {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys) override{};
  void Merge() override{};
  Cmd *Clone() override { return new EvalCmd(*this); }

 private:
  bool evalsha_{false};
  std::string script_;
  std::vector<std::string> keys_;
  std::vector<std::string> args_;
  long long numkeys_;

  std::list<std::string> reply_lst_;
  void DoInitial() override;
  void LuaReplyToRedisReply(sol::state &lua, sol::object lua_ret);
};

/*
 * script
 */
class ScriptCmd : public Cmd {
 public:
  ScriptCmd(const std::string &name, int arity, uint16_t flag, bool evalsha)
      : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys &hint_keys) override{};
  void Merge() override{};
  Cmd *Clone() override { return new ScriptCmd(*this); }

 private:
  void DoInitial() override;
};

#endif