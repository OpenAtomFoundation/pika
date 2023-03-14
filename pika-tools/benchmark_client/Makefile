CXX = g++

LDFLAGS=-lpthread -lsnappy

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
	-Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
	-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

CXXFLAGS = $(WARNING_FLAGS) -std=c++11 -g

THIRD_PATH=../third

SLASH_PATH=$(THIRD_PATH)/slash
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash.a

HIREDIS_PATH=$(THIRD_PATH)/hiredis-vip
HIREDIS_LIBRARY=$(HIREDIS_PATH)/libhiredis_vip.a

INCLUDE_PATH = -I./                        \
               -I$(SLASH_PATH)             \
			   -I$(HIREDIS_PATH)           \

OBJECT : SLASH HIREDIS benchmark_client

SLASH :
	make -C $(SLASH_PATH)/slash

HIREDIS:
	make -C $(HIREDIS_PATH)

benchmark_client : benchmark_client.cc
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $^ -o $@  $(INCLUDE_PATH) $(SLASH_LIBRARY) $(HIREDIS_LIBRARY)

clean:
	rm -rf benchmark_client

distclean:
	make -C $(SLASH_PATH)/slash clean
	make -C $(HIREDIS_PATH) clean
	rm -rf benchmark_client
