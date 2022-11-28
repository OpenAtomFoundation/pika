#include "slash/include/base_conf.h"
#include "slash/include/xdebug.h"

using namespace slash;

int main()
{
  BaseConf b("./conf/pika.conf");

  if (b.LoadConf() == 0) {
    log_info("LoadConf true");
  } else {
    log_info("LoodConf error");
  }

  b.SetConfInt("port", 99999);
  b.SetConfStr("pidfile", "./anan.pid");
  b.WriteBack();
  b.DumpConf();
  b.WriteSampleConf();

  return 0;
}
