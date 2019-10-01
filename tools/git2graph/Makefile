CC = gcc
LD = gcc
CFLAGS = -Wall -Werror $(shell pkg-config --cflags libgit2)
LDFLAGS = $(shell pkg-config --libs libgit2)

all: git2graph

git2graph: git2graph.o
	$(LD) -o $@ $^ $(CFLAGS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f *.o git2graph
