
# wal-kv

A persistent key-value store in Rust, backed by a write-ahead log (WAL).

This project is a from-scratch implementation of a durable key-value store. It uses a write-ahead log to ensure that all operations are atomic and durable, basically this means that the database can recover its state perfectly after a crash.

## Features

-   **Durability:** Every write is flushed to disk using `fsync` before the in-memory state is updated, guaranteeing that no acknowledged write is ever lost.
-   **Crash Recovery:** On startup, the store replays the log to restore the in-memory `HashMap` to its last known state.
-   **Log Compaction:** A compaction mechanism is implemented to reclaim disk space by removing obsolete log entries. The process uses an atomic rename to ensure the database is never in a corrupt state.
-   **Memory Efficient:** The log is read using a streaming iterator, ensuring constant memory usage regardless of the log's size on disk.

## How to Run

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/RedlineDevs/wal-kv.git
    cd wal-kv
    ```

2.  **Run the demo:**
    The `main.rs` file contains a demonstration of the store's lifecycle: setting values, recovering from a "crash" (restart), and performing log compaction.
    ```sh
    cargo run
    ```

## Example Output

```sh
$ cargo run
Set 2 entries
Recovered 2 entries after restart
Log file size before compaction: 3631 bytes
Log file size after compaction: 36 bytes
Compaction test passed
```