# channels-lite

Django Channels layer backed by SQLite database.

## TODO

### High Priority (Robustness)

- [ ] Replace bare `except:` clauses with specific exception handling (`ChannelEmpty`, `OperationalError`, etc.)
- [ ] Replace print statements with proper logging (use `logging` module)
- [ ] Add retry logic for database OperationalError (lock timeouts)
- [ ] Implement receive cancellation handling to prevent message loss
- [ ] Add buffer size limits to prevent unbounded memory growth
- [ ] Implement periodic VACUUM for database maintenance (reclaim space from deleted messages)

### Documentation Needed

- [ ] Document SQLite WAL mode requirement for better concurrency
  - Users must configure: `PRAGMA journal_mode=WAL`
  - Recommended settings: `PRAGMA synchronous=NORMAL`, `PRAGMA busy_timeout=5000`
- [ ] Document Django database configuration requirements
- [ ] Add usage examples and setup guide
- [ ] Document limitations vs Redis layer

### Future Enhancements

- [ ] Create advanced layer using `aiosqlite` + `aiosqlitepool` for better async support
  - Direct connection pooling
  - Better concurrent write handling
  - More control over SQLite pragmas
- [ ] Consider adding encryption support for sensitive messages
- [ ] Add health check endpoint for monitoring
- [ ] Add metrics/instrumentation for observability

### Performance Optimizations

- [ ] Profile and optimize hot paths (send/receive loops)
- [ ] Consider batch operations for group_send
- [ ] Evaluate index optimization for Event and GroupMembership tables
- [ ] Benchmark against Redis layer for common workloads
