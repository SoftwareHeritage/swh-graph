## Rust version
The code needs stable rust to be >= 1.65 because we require the [GAT feature](https://blog.rust-lang.org/2022/10/28/gats-stabilization.html`).

## Testing
```rust
cargo test
```

## Distribution
To distribute executables, you can statically compile the code with:
```rust
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release --target x86_64-unknown-linux-musl
```
To add the target use:
```shell
rustup target add x86_64-unknown-linux-musl
```
The target-cpu will limit the compatible cpus but will enable more optimizations.
Some Interesting architecture are:
- `native` for the compiling CPU architecture, this is the best option for
   performance when you are compiling and running on the same machine.
- `x86-64-v3` for Intel Haswell and newer (2013), oldest architecture that
   supports AVX2 and BMI2 instructions.
- `x86-64-v2` for Intel Core 2 and newer (2006), oldest reasonable architecture
   to compile for.

## Performance consideration
At every random access, we need to query Elias-Fano to find the bit-offset at
which the codes of the given nodes start. This operation has to find the i-th
one in a given word of memory.

The [implementation in `sux-rs`](https://github.com/vigna/sux-rs/blob/25fbdf42024b6cbe98741bd0d8135f3188293677/src/utils.rs#L26)
can exploit the [pdep instruction](https://www.felixcloutier.com/x86/pdep) to speed up the operation.
So it's important to compile the code with the `pdep` feature enabled, and generally
targeting the intended CPU architecture by using the `RUSTFLAGS` environment variable as:
```rust
RUSTFLAGS="-C target-cpu=native" cargo run --release --bin bfs
```
this compiles targeting the compiling CPU architecture.

For this reason, this is enabled by default in the `.cargo/config.toml` file.
Be aware that a file compiled with `pdep` enabled will not run on a CPU that does not support it.
Generally, **running a binary compiled with a given set of CPU features on another one
will result in SIGILL (illegal instruction) error**.

## Loading old graphs

The original Java (and C++) implementation of webgraph used slightly different
data structures in the Rust implementation.
Therefore, you need to generate new files in order to load old graphs with the Rust
implementation; this takes a few hours for graphs representing full SWH exports.

1. Convert the GOV minimal-perfect-hash function from `.mph` to `.cmph`: `java -classpath ~/src/swh-graph/java/target/swh-graph-3.0.1.jar ~/src/swh-graph/java/src/main/java/org/softwareheritage/graph/utils/Mph2Cmph.java graph.mph graph.cmph`
2. Generate Elias-Fano-encoded offsets of the graph: `cargo run --release --bin build_eliasfano --  ~/graph/latest/compressed/graph`
3. Generate Elias-Fano-encoded offsets of the transposed graph: `cargo run --release --bin compress build-eliasfano --  ~/graph/latest/compressed/graph-transposed`
4. Generate Elias-Fano-encoded offsets of the labelled graph: `cargo run --release --bin compress build-labels-eliasfano --  ~/graph/latest/compressed/graph-labelled $((1+ $(cat /~/graph/latest/compressed/graph.nodes.count.txt)))`
5. Generate Elias-Fano-encoded offsets of the labelled transposed graph: `cargo run --release --bin build-labels-eliasfano --  ~/graph/latest/compressed/graph-transposed-labelled ~/graph/latest/compressed/graph $((1+ $(cat /~/graph/latest/compressed/graph.nodes.count.txt)))`
6. Generate `node2type.bin` (as `node2type.map` is specific to Java): `cargo run --release --bin node2type --  ~/graph/latest/compressed/graph`
