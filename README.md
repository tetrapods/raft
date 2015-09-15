raft
====

A java implementation of the [raft distributed consensus algorithm] (https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)

We're using this in production, after a decent amount of systems testing but our unit testing is poor.

You have to provide your own RPC implementation (A class that implements io.tetrapod.raft.RaftRPC) 

It supports log compaction and we have some support for non-blocking snapshotting using copy-on-writes, but this is not tested or complete.

Online cluster configuration changes are not yet supported

We target JDK 7+ and have no dependencies other than logback

Included StorageStateMachine implementation has key/value storage, atomic counters, and distributed locks.

We'd LOVE some more eyeballs on this project, help with unit testing, and documentation.
