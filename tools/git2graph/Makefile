CC = gcc
LD = gcc
BATS = bats

LIBS = libgit2 glib-2.0

CFLAGS = -Wall -Werror $(shell pkg-config --cflags $(LIBS))
LDFLAGS = $(shell pkg-config --libs $(LIBS))
BATS_FLAGS =

all: git2graph

git2graph: git2graph.o
	$(LD) -o $@ $^ $(CFLAGS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

test: all
	$(BATS) $(BATS_FLAGS) tests/

clean:
	rm -f *.o git2graph

.PHONY: all clean test
