# Redis.jl

[![Build Status](https://github.com/JuliaServices/Redis.jl/workflows/CI/badge.svg)](https://github.com/JuliaServices/Redis.jl/actions?query=workflow%3ACI+branch%3Amain)

A Redis client for Julia, built on [Reseau.jl](https://github.com/JuliaServices/Reseau.jl).

Commands are pipelined over a single connection: each command is written under a
lock and a future is queued for its reply, so concurrent callers share one socket
without blocking each other.

```julia
using Redis

client = Redis.connect("127.0.0.1", 6379)

Redis.set(client, "key", "value")
Redis.get(client, "key")            # "value"
Redis.del(client, "key")

# expiry
Redis.set(client, "session", "abc"; ex=60)     # seconds
Redis.set(client, "otp", "123456"; px=30_000)  # milliseconds

# only set if absent
Redis.set(client, "lock", "owner"; nx=true)

# iterate keys matching a glob
for k in Redis.Scan(client, "session:*")
    @show k
end

# pipeline a batch
batch = Redis.Batch()
push!(batch, Redis.Commands.set("a", "1"))
push!(batch, Redis.Commands.get("a"))
Redis.execute(client, batch)

close(client)
```

Sorted sets, streams and consumer groups, sets, hashes, lists, geo commands, and
transactions are supported; see `src/commands.jl` for the full surface.

## Testing

Tests run against a real Redis in a throwaway container via
[Harbor.jl](https://github.com/JuliaServices/Harbor.jl), so Docker is required.

```bash
julia --project -e 'using Pkg; Pkg.test()'
```
