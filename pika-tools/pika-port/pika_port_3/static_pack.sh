g++ \
  -o static_pika_port \
  ./pika_port.o \
  ./binlog_receiver_thread.o \
  ./migrator_thread.o \
  ./slaveping_thread.o \
  ./binlog_transverter.o \
  ./main.o ./pika_binlog.o \
  ./trysync_thread.o \
  ./redis_sender.o \
  ./pika_sender.o \
  ./const.o \
  ./master_conn.o  \
  -L./ \
  -L../../third/rocksdb/ \
  -L../../third/slash/slash/lib \
  -L../../third/pink/pink/lib \
  -L../../third/blackwidow/lib \
  -L./deps_source -lrocksdb -lpthread -lrt -lpink -lslash -lblackwidow -lrocksdb -lz -lbz2 -lsnappy -lglog -lgflags -lunwind -static-libstdc++ -llz4 -lzstd
