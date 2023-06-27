

### .cmph file
The older Java version used to serialize the MPH structure using Java serialize
which stores integers in big-endian order, for this reason, we moved to a new
format `.cmph` which stores data in a little-endian order and without the Java
serialization format. This allows this file to be read from C, Rust, or any 
other language without worrying about Java object deserialization.

Moreover, this allows the file to be mmapped on little-endian machines.

To convert from `.mph` to the new `.cmph` file with the swh-graph utility:
```shell
java -classpath ~/src/swh-graph/java/target/swh-graph-3.0.1.jar ~/src/swh-graph/java/src/main/java/org/softwareheritage/graph/utils/Mph2Cmph.java graph.mph graph.cmph
```
or just with `webgraph-big` you can use `jshell` to call the `dump` mehtod:
```shell
echo '((it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction)it.unimi.dsi.fastutil.io.BinIO.loadObject("test.mph")).dump("test.cmph");' | jshell -classpath /path/to/webgraph-big.jar
```

### .ef file
The older Java version used the `.offests` file to build at runtime the elias-fano
structure. The offsets are just a contiguous big-endian bitstream of the 
gaps between successive offsets written as elias-gamma-codes.
To avoid re-building this structure every time we added the `.ef` file which 
can be memory-mapped with little parsing at the cost of being endianess dependent.
The `.ef` file is in little-endian,

To generate the `.ef` file from either a `.offsets` file or a `.graph` file,
you can use the `webgraph-rs` bin utility:
```shell
$ cargo run --release --bin build_eliasfano -- $BASENAME
```
this will create a `$BASENAME.ef` file in the same directory. 