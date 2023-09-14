# gcc setting
CC=gcc
STD=-std=c99 -pedantic -DREDIS_STATIC=''
WARN=-Wall -W -Wno-missing-field-initializers -Wno-uninitialized
OPT=-O2
DEBUG=-g
CFLAGS=-fPIC -Wcpp
FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS)

# ar setting
AR=ar
ARFLAGS=rs

# build proj setting
LIB_SOURCES := $(wildcard *.c)
LIB_OBJECTS = $(LIB_SOURCES:.c=.o)
LIB_NAME=libredisdb
LIBRARY=${LIB_NAME}.a

# target
.PHONY: all clean

all: $(LIBRARY)

$(LIB_OBJECTS): $(LIB_SOURCES)
	$(CC) $(FINAL_CFLAGS) -c $(LIB_SOURCES)

$(LIBRARY): $(LIB_OBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $(LIB_OBJECTS)

clean:
	rm -f $(LIBRARY)
	rm -f *.o 
