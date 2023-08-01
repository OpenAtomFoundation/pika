#include "pstd/include/base_conf.h"
#include "pstd/include/xdebug.h"

using namespace pstd;

int32_t main() {
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
