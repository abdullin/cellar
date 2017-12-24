# Cellar

Cellar is the append-only storage backend in Go designed for the analytical
workloads. It replaces [geyser-net](https://github.com/abdullin/geyser-net).

Core features:

- events are automatically split into the chunks;
- chunks are encrypted and compressed;
- designed for batching operations (high throughput);
- supports single writer and multiple concurrent readers;
- store secondary indexes, lookups in the metadata DB.

This storage takes ideas from the [Message Vault](https://github.com/abdullin/messageVault),
which was based on the ideas of Kafka and append-only storage in [Lokad.CQRS](https://github.com/abdullin/lokad-cqrs)

Analytical pipeline on top of this library was deployed at
HappyPancake to run real-time aggregation and long-term data analysis
on the largest social website in Sweden. You can read more about it in
[Real-time Analytics with Go and LMDB](https://abdullin.com/bitgn/real-time-analytics/).

# Design

Cellar stores data in a very simple manner:

- LMDB database is used for keeping metadata (including user-defined);
- a single pre-allocated file is used to buffer all writes;
- when buffer fills, it is compressed, encrypted and added to the chunk list.

You can have **only one writer at a time**. This writer has two operations:

- `Append` - adds new bytes to the buffer, but doesn't flush it.
- `Checkpoint` - performs all the flushing and saves the checkpoints.

The store is optimized for throughput. You can efficiently execute
thousands of appends followed by a single call to `Checkpoint`.

Whenever a buffer is about to overflow (exceed the predefined max
size), it will be "sealed" into an immutable chunk (compressed,
encrypted and added to the chunk table) and replaced by a new buffer.

See tests in `writer_test.go` for sample usage patters (for both
writing and reading).


At any point in time multiple readers could be created, they will be
working against the last available snapshot, optionally reading into
the buffer as well. Readers have only one operation available for
them - reading sequentially within a given interval (or scanning
through the entire storage).


# License

3-clause BSD license.
