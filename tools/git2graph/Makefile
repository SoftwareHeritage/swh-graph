CC = gcc
LD = gcc
LIBS = libgit2

CFLAGS = -Wall -Werror $(shell pkg-config --cflags $(LIBS))
LDFLAGS = $(shell pkg-config --libs $(LIBS))

all: git2graph

git2graph: git2graph.o
	$(LD) -o $@ $^ $(CFLAGS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f *.o git2graph
