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

# License

3-clause BSD license.
