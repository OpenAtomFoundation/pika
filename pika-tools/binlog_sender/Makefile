CXX = g++

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
	-Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
	-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

CXXFLAGS = $(WARNING_FLAGS) -std=c++11 -g

TARGET = binlog_sender 

SRC_PATH=.
THIRD_PATH=../third

SLASH_PATH=$(THIRD_PATH)/slash
SLASH=$(SLASH_PATH)/slash/lib/libslash.a

PINK_PATH=$(THIRD_PATH)/net
PINK=$(PINK_PATH)/net/lib/libnet.a


INCLUDE_PATH = -I./                             \
               -I$(SLASH_PATH)                  \
               -I$(PINK_PATH)                   \

LIB_PATH     = -L$(SLASH_PATH)/slash/lib        \
               -L$(PINK_PATH)/net/lib          \

LIBS         = -lnet                           \
               -lslash                          \
               -lpthread                        \

SOURCE := $(wildcard $(SRC_PATH)/*.cc)
OBJS := $(patsubst %.cc, %.o, $(SOURCE))

$(TARGET): $(OBJS) $(PINK) $(SLASH)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)


$(PINK) :
	make -C $(PINK_PATH)/net SLASH_PATH=../../slash

$(SLASH) :
	make -C $(SLASH_PATH)/slash


.PHONY: clean distclean

clean:
	rm -rf $(OBJS)
	rm -rf $(TARGET)

distclean:
	rm -rf $(OBJS)
	rm -rf $(TARGET)
	make -C $(PINK_PATH)/net clean
	make -C $(SLASH_PATH)/slash clean
