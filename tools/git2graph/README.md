git2graph
=========

`git2graph` crawls a Git repository and outputs it as a graph, i.e., as a pair
of textual files <nodes, edges>. The nodes file will contain a list of graph
nodes as Software Heritage (SWH) Persistent Identifiers (PIDs); the edges file
a list of graph edges as <from, to> PID pairs.


Micro benchmark
---------------

    $ time ./git2graph /srv/src/linux >(pigz -c > nodes.csv.gz) >(pigz -c > edges.csv.gz)
    ./git2graph /srv/src/linux >(pigz -c > nodes.csv.gz) >(pigz -c > edges.csv.gz  233,67s user 15,76s system 91% cpu 4:32,62 total
    
    $ zcat nodes.csv.gz | wc -l
    6503402
    
    $ zcat edges.csv.gz | wc -l
    305095437
