1. 把pika可执行程序拷贝到./src目录里，更名为redis-server.
2. 把pika配置文件拷贝到./tests/assets/目录里，更名为default.conf.
3. 删除上次执行的db, log目录，在Pika根目录下执行 tclsh tests/test_helper.tcl --clients 1 --single unit/type/set 测试pika set.
