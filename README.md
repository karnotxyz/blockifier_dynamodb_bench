# Blockifier DynamoDB Benchmark 🚀

An exploratory proof of concept for integrating Starknet's Blockifier with Amazon DynamoDB to enable horizontal scalability of sequencers.

⚠️ **IMPORTANT**: This is a proof of concept and is not intended for production use.

## Overview 🌟

This project originated from work at [Finternet](https://finternetlab.io/) where we were exploring the scalability limits of [UNITS Engine](https://github.com/karnotxyz/units_engine) instances. The goal was to test if a UNITS instance could scale horizontally and not be bound to one vetically scalable instance. This is particularly helpful in scenarios where a UNITS deployment spans multiple jurisdictions with transactions that are naturally partitioned - since users in one jurisdiction rarely interact with state from another jurisdiction, their transactions can be processed in parallel. Additionally, we wanted to explore how to handle compliance requirements (like GDPR's data localization) within a single UNITS chain using a scalable database solution (Cloud Spanner's [multi region configuration](https://cloud.google.com/spanner/docs/instance-configurations#regional-configurations) for example).

## What it Does 🔧

The project implements a DynamoDB backend for Blockifier's StateReader trait, allowing state storage and retrieval from DynamoDB instead of an on disk database. Key features include:

- DynamoDB implementation of the StateReader trait
- Transfer transaction generator (inspired by [Starkware Sequencer](https://github.com/starkware-libs/sequencer))
- Batched transaction execution with atomic state updates
- Optimistic concurrency control for parallel execution

### Transaction Processing Flow 🔄

1. Transfer generator creates and submits transactions
2. Transactions are batched and executed
3. State diffs are committed to DynamoDB
4. Read tracking ensures atomic updates through DynamoDB transactions

### DynamoDB Tables 📊

The following tables are used in the implementation:

- `storage`: Contract storage states
- `nonce`: Account nonces
- `class_hash`: Contract class hashes
- `compiled_class`: Compiled contract classes
- `counter_table`: Atomic counter to sync across multiple instance of the bench
- `tx_log`: Transaction execution logs

## Getting Started 🏁

### Prerequisites 📋

1. AWS account with DynamoDB access
2. Rust toolchain

### Setup ⚙️

1. Create a `.env` file with your AWS credentials:

```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_region
```

2. Initialize DynamoDB tables:

```bash
cargo run -- migrate
```

### Running Benchmarks 📈

To run a single transfer generator:

```bash
cargo run -- bench --n-accounts 1 --n-txs 1000 --chunk-size 95
```

Parameters:

- `n-accounts`: Number of accounts to use in benchmark
- `n-txs`: Total number of transactions to execute
- `chunk-size`: Number of transactions to process in one batch

⚠️ **Note**: DynamoDB has a limit of 100 items per transaction. If your transactions touch many state items, you'll need to adjust the chunk size accordingly.

### Parallel Execution ⚡

You can run multiple transfer generators in parallel to achieve higher TPS:

Watch a demo of parallel execution [here](https://www.youtube.com/watch?v=Pd8Kvqm4Y_s).

### Monitoring 📊

Track live TPS on DynamoDB:

```bash
cargo run -- track-tps
```

### Cleanup 🧹

To remove all DynamoDB tables and data:

```bash
cargo run -- cleanup
```

## Implementation Details 🔍

The project uses optimistic concurrency control to enable parallel execution. During transaction execution, all state reads are tracked. When committing state diffs to DynamoDB, the transaction includes conditions ensuring the read values haven't changed. If any value has changed, the DynamoDB transaction fails, preventing state conflicts.

This approach allows multiple transfer generators to run in parallel while maintaining state consistency, effectively enabling horizontal scaling of the sequencer through multiple instances.

Currently, we use the existing hashing techniques that already exist in Starknet. Research can be done on optimizing DynamoDB partition key hashing strategies to ensure efficient sharding across nodes. This can help avoid hot partitions and achieve better throughput distribution when scaling horizontally.
