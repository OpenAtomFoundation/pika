g++ \
  manifest_generator.o \
  pika_binlog.o  \
  ../third/slash/slash/lib/libslash.a -o static_manifest_generator -lpthread -static-libstdc++
