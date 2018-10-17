# Cellar

[![Build Status](https://travis-ci.com/abdullin/cellar.svg?branch=master)](https://travis-ci.com/carapace/cellar)

Cellar is the append-only storage backend in Go designed for the analytical
workloads. It replaces [geyser-net](https://github.com/abdullin/geyser-net).

Core features:

- events are automatically split into the chunks;
- chunks are encrypted (LZ4) and compressed;
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

# Writing

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

# Reading

At any point in time **multiple readers could be created** via
`NewReader(folder, encryptionKey)`. You can optionally configure
reader after creation by setting `StartPos` or `EndPos` to constrain
reading to a part of the database.


Readers have following operations available:

- `Scan` - reads the database by executing the passed function against
  each record;
- `ReadDb` - executes LMDB transaction against the metadata database
  (used to read lookup tables or indexes stored by the
  custom writing logic);
- `ScanAsync` - launches reading in a goroutine and returns a buffered
  channel that will be filled up with records.

Unit tests in `writer_test.go` feature use of readers as well.

Note, that the reader tries to help you in achieving maximum
throughput. While reading events from the chunk, it will decrypt and
unpack the entire file in one go, allocating a memory buffer. All
individual event reads will be performed against this buffer.

# Example: Incremental Reporting

This library was used as a building block for capturing millions and
billions of events and then running reports on them. Consider a
following example of building an incremental reporting pipeline.

There is an external append-only storage with billions of events and a
few terabytes of data (events are compressed separately with an
equivalent of Snappy). It is located on a remote storage (cloud or a
NAS). It is required to run custom reports on this data, refreshing
them every hour.

Cellar storage could be used to serve as a local cache on a dedicated
reporting machine (e.g. you can find an instance with 32GB of RAM,
Intel Xeon and 500GB of NNVMe SSD under 100 EUR per month). Since
Cellar storage compresses events in chunks, high compression ratio
could be achieved. For instance, protobuf messages tend to get
compression of 2-10 in chunks.

A solution might include an equivalent of a cron job that will execute
following apps in sequence:

- import job - a golang console that reads the last retrieved offset
  from the cellar, requests any new data from the remote storage and
  stores it locally in raw format;
- compaction job - a golang console that incrementally pumps data from
  the "raw" cellar storage to another (using checkpoints to determine
  the location), while compacting and filtering events to keep only
  the ones needed for reporting;
- report jobs - apps that perform a full scan on the compacted data,
  building reports in memory and then dumping them into the TSV (or
  whatever is format is used by your data processing framework).

All these steps usually execute fast even on large datasets, since (1)
and (2) are incremental and operate only on the fresh data. (3) can
require full DB, however it works with the optimized and compacted
data, hence it will be fast as well. To get the most performance, you
might need to structure your messages for very fast reads without
unnecessary memory allocations or CPU work (e.g. using something like
FlatBuffers instead of JSON or ProtoBuf).

Note, that the compaction job is optional. However, on fairly large
datasets, it might make sense to optimize messages for very fast
reads, while discarding all the unnecessary information. Should the
job requirements change, you'll need to update the compaction logic,
discard the compacted store and re-process all the raw data from the
start.

# License

3-clause BSD license.
