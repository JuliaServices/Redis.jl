using Test, Redis, Harbor

"""
    docker_available() -> Bool

Whether a Docker daemon that can run Linux containers is reachable.  The OS check
matters: a Windows runner has a responsive daemon in Windows-container mode, so a
`docker pull redis` fails partway through the run rather than skipping cleanly.
"""
function docker_available()
    Sys.which("docker") === nothing && return false
    try
        return strip(read(`docker info --format "{{.OSType}}"`, String)) == "linux"
    catch
        return false
    end
end

const DOCKER = docker_available()
DOCKER || @warn "docker unavailable — tests needing a Redis server will be skipped"

mutable struct FailingAfterWriteIO <: IO
    open::Bool
    wrote::Base.RefValue{Bool}
end

Base.isopen(io::FailingAfterWriteIO) = io.open
Base.close(io::FailingAfterWriteIO) = (io.open = false; nothing)
Base.flush(::FailingAfterWriteIO) = nothing

function Base.write(io::FailingAfterWriteIO, bytes::Union{String,SubString{String}})
    io.wrote[] = true
    return sizeof(bytes)
end

function Base.eof(io::FailingAfterWriteIO)
    while io.open && !io.wrote[]
        sleep(0.001)
    end
    throw(ErrorException("tls peek failed (code=1): unexpected EOF"))
end

@testset "Redis" begin
    @testset "readresponse!" begin
        mutable struct ChunkedIO <: IO
            chunks::Vector{Vector{UInt8}}
            nextchunk::Int
        end

        ChunkedIO(chunks::Vector{Vector{UInt8}}) = ChunkedIO(chunks, 1)

        Base.eof(io::ChunkedIO) = io.nextchunk > length(io.chunks)

        function Base.readbytes!(io::ChunkedIO, dst::AbstractVector{UInt8}, nb::Integer=length(dst))
            eof(io) && return 0
            chunk = io.chunks[io.nextchunk]
            nread = min(Int(nb), length(chunk))
            copyto!(dst, 1, chunk, 1, nread)
            if nread < length(chunk)
                io.chunks[io.nextchunk] = chunk[nread+1:end]
            else
                io.nextchunk += 1
            end
            return nread
        end

        simple_string = UInt8[0x2B, 0x4F, 0x4B, 0x0D, 0x0A]
        Redis.readresponse!(IOBuffer(simple_string)) do x
            @test x == "OK"
        end
        error_message = UInt8[0x2D, 0x45, 0x72, 0x72, 0x6F, 0x72, 0x20, 0x6D, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x0D, 0x0A]
        Redis.readresponse!(IOBuffer(error_message)) do x
            @test x isa Redis.RedisError
            @test x.msg == "Error message"
        end
        integer_response = UInt8[0x3A, 0x31, 0x30, 0x30, 0x30, 0x0D, 0x0A]
        Redis.readresponse!(IOBuffer(integer_response)) do x
            @test x == 1000
        end
        bulk_string = UInt8[0x24, 0x36, 0x0D, 0x0A, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, 0x0D, 0x0A]
        Redis.readresponse!(IOBuffer(bulk_string)) do x
            @test x == "foobar"
        end
        null_bulk_string = UInt8[0x24, 0x2D, 0x31, 0x0D, 0x0A]
        Redis.readresponse!(IOBuffer(null_bulk_string)) do x
            @test x === nothing
        end
        array_response = UInt8[
            0x2A, 0x32, 0x0D, 0x0A,  # *2\r\n
            0x24, 0x33, 0x0D, 0x0A,  # $3\r\n
            0x66, 0x6F, 0x6F, 0x0D, 0x0A,  # foo\r\n
            0x24, 0x33, 0x0D, 0x0A,  # $3\r\n
            0x62, 0x61, 0x72, 0x0D, 0x0A   # bar\r\n
        ]
        Redis.readresponse!(IOBuffer(array_response)) do x
            @test x == ["foo", "bar"]
        end
        mixed_array = UInt8[
            0x2A, 0x33, 0x0D, 0x0A,  # *3\r\n
            0x3A, 0x31, 0x0D, 0x0A,  # :1\r\n
            0x3A, 0x32, 0x0D, 0x0A,  # :2\r\n
            0x3A, 0x33, 0x0D, 0x0A   # :3\r\n
        ]
        Redis.readresponse!(IOBuffer(mixed_array)) do x
            @test x == [1, 2, 3]
        end
        nested_array = UInt8[
            0x2A, 0x32, 0x0D, 0x0A,  # *2\r\n
            0x2A, 0x33, 0x0D, 0x0A,  # *3\r\n
            0x3A, 0x31, 0x0D, 0x0A,  # :1\r\n
            0x3A, 0x32, 0x0D, 0x0A,  # :2\r\n
            0x3A, 0x33, 0x0D, 0x0A,  # :3\r\n
            0x2A, 0x32, 0x0D, 0x0A,  # *2\r\n
            0x2B, 0x46, 0x6F, 0x6F, 0x0D, 0x0A,  # +Foo\r\n
            0x2D, 0x42, 0x61, 0x72, 0x0D, 0x0A   # -Bar\r\n
        ]
        Redis.readresponse!(IOBuffer(nested_array)) do x
            @test x == [[1, 2, 3], ["Foo", Redis.RedisError("Bar")]]
        end
        nested_array2 = UInt8[
            0x2A, 0x32, 0x0D, 0x0A,  # *2\r\n
            0x2A, 0x33, 0x0D, 0x0A,  # *3\r\n
            0x3A, 0x31, 0x0D, 0x0A,  # :1\r\n
            0x3A, 0x32, 0x0D, 0x0A,  # :2\r\n
            0x3A, 0x33, 0x0D, 0x0A,  # :3\r\n
            0x2A, 0x32, 0x0D, 0x0A,  # *2\r\n
            0x2B, 0x46, 0x6F, 0x6F, 0x0D, 0x0A,  # +Foo\r\n
            0x2B, 0x42, 0x61, 0x72, 0x0D, 0x0A   # +Bar\r\n
        ]
        Redis.readresponse!(IOBuffer(nested_array2)) do x
            @test x == [[1, 2, 3], ["Foo", "Bar"]]
        end
        null_array = UInt8[0x2A, 0x2D, 0x31, 0x0D, 0x0A]
        Redis.readresponse!(IOBuffer(null_array)) do x
            @test x === nothing
        end
        empty_array = UInt8[0x2A, 0x30, 0x0D, 0x0A]
        Redis.readresponse!(IOBuffer(empty_array)) do x
            @test x == []
        end

        chunked_bulk = [
            UInt8[0x24, 0x36, 0x0D],
            UInt8[0x0A, 0x66],
            UInt8[0x6F, 0x6F, 0x62],
            UInt8[0x61, 0x72, 0x0D, 0x0A],
        ]
        Redis.readresponse!(ChunkedIO(chunked_bulk)) do x
            @test x == "foobar"
        end

        @testset "bulk response framing" begin
            bulk_frame(value) = "\$$(sizeof(value))\r\n$value\r\n"
            function read_replies(io, count; bufsize=4096)
                buf = Vector{UInt8}(undef, bufsize)
                pos, len = 1, 0
                values = Any[]
                for _ in 1:count
                    pos, len = Redis.readresponse!(x -> push!(values, x), io, buf, pos, len, false)
                end
                return values
            end

            for value in (0, -1, typemin(Int), typemax(Int))
                for bufsize in (1, 2, 7, 4096)
                    @test read_replies(IOBuffer(":$value\r\n+OK\r\n"), 2; bufsize) == [value, "OK"]
                end
            end

            binary = String(UInt8[0x00, 0xff, 0x0d, 0x0a, 0x80])
            for value in ("", "\r", "\n", "a\r\nb", "flight ✈️", binary,
                          String(collect(UInt8(0):UInt8(255))), repeat("a\r\n", 4096))
                for bufsize in (1, 2, 7, 4096)
                    wire = bulk_frame(value) * ":42\r\n"
                    @test read_replies(IOBuffer(wire), 2; bufsize) == [value, 42]
                end
            end

            wire = collect(codeunits("*3\r\n" * bulk_frame(binary) * "\$-1\r\n*1\r\n" * bulk_frame("") * ":42\r\n"))
            expected = Any[Any[binary, nothing, [""]], 42]
            # Split at every byte, including the lengths, binary data, and CRLFs.
            for split_at in 1:length(wire)-1
                chunks = [wire[1:split_at], wire[split_at+1:end]]
                @test read_replies(ChunkedIO(chunks), 2; bufsize=7) == expected
            end
            @test read_replies(ChunkedIO([[b] for b in wire]), 2; bufsize=1) == expected

            overflow = string(big(typemax(Int)) + 1)
            parse_error = isdefined(Redis.Parsers, :Error) ? Redis.Parsers.Error : OverflowError
            for invalid in ("\$-2\r\n", "*-2\r\n", "\$+1\r\na\r\n", "*+1\r\n:1\r\n",
                            "\$ 1\r\na\r\n", "\$1 \r\na\r\n", "\$\r\n", "*\r\n",
                            "\$$overflow\r\n", "*$overflow\r\n", "\$1\rXa\r\n",
                            "\$1\r\naXX", "\$0\r\nXX", "\$1\r\nab\r\n", "\$3\r\nab\r\n",
                            "*1\r\n\$-2\r\n", "+O\nK\r\n", "+OK\rX", "-ERR\rX", ":1\rX")
                for io in (IOBuffer(invalid), ChunkedIO([[b] for b in codeunits(invalid)]))
                    delivered = Ref(false)
                    @test_throws Union{Redis.RedisError,parse_error} Redis.readresponse!(
                        _ -> (delivered[] = true), io, Vector{UInt8}(undef, 7), 1, 0, false)
                    @test !delivered[]
                end
            end

            for truncated in ("\$1\r\na", "\$1\r\na\r", "\$2\r\na", "\$0\r\n\r",
                              "\$$(typemax(Int))\r\na", "\$-1\r", "*-1\r", "*1\r\n",
                              "+OK\r", "-ERR\r", ":1\r")
                delivered = Ref(false)
                @test_throws EOFError Redis.readresponse!(
                    _ -> (delivered[] = true), IOBuffer(truncated), Vector{UInt8}(undef, 7), 1, 0, false)
                @test !delivered[]
            end
            @test read_replies(IOBuffer("\$-1\r\n*-1\r\n\$0\r\n\r\n*0\r\n"), 4; bufsize=1) ==
                  Any[nothing, nothing, "", []]
        end

        @testset "malformed replies stay cheap" begin
            parse(wire) = Redis.readresponse!(_ -> nothing, IOBuffer(wire), Vector{UInt8}(undef, 64), 1, 0, false)
            # a huge array count followed by EOF allocates nothing up front
            @test_throws EOFError parse("*10\r\n:1\r\n")
            allocated = @allocated try parse("*10000000\r\n:1\r\n") catch end
            @test allocated < 1_000_000
            # nesting is bounded below what a reader task's stack can hold
            nested(depth) = repeat("*1\r\n", depth) * ":1\r\n"
            @test fetch(Threads.@spawn parse(nested(Redis.MAX_NESTING))) isa Tuple
            @test_throws Redis.RedisError parse(nested(Redis.MAX_NESTING + 1))
            # an unknown reply type is an error, and nothing is written to stdout
            printed = mktemp() do path, io
                redirect_stdout(io) do
                    @test_throws Redis.RedisError parse("_\r\n")
                end
                close(io)
                read(path, String)
            end
            @test isempty(printed)
        end
    end

    @testset "command encoding" begin
        # a request is "*n" and then exactly n "$len" bulk strings
        function wellformed(cmd)
            b = codeunits(cmd.cmd)
            m = match(r"^\*(\d+)\r\n", cmd.cmd)
            m === nothing && return false
            i = sizeof(m.match) + 1
            for _ in 1:parse(Int, m[1])
                j = findnext(==(UInt8('\r')), b, i)
                (j === nothing || b[i] != UInt8('$')) && return false
                i = j + 2 + parse(Int, String(b[(i + 1):(j - 1)]))
                (i + 1 <= length(b) && b[i] == UInt8('\r') && b[i + 1] == UInt8('\n')) || return false
                i += 2
            end
            return i == length(b) + 1
        end
        C = Redis.Commands
        k = "ключ✈" # multi-byte, so character and byte lengths differ
        cmds = [C.auth(k), C.select(3), C.scan("0"), C.scan("0"; match=k),
            C.zscan(k, "0"), C.zscan(k, "0"; match=k), C.sscan(k, "0"), C.sscan(k, "0"; match=k),
            C.hscan(k, 0), C.hscan(k, "0"; match=k), C.hscan(k, 0; match=""),
            C.set(k, k), C.set(k, k; nx=true, ex=5), C.set(k, k; xx=true, px=5), C.mset(k => k, "a" => "b"),
            C.append(k, k), C.incrby(k, 3), C.get(k), C.mget(k, k), C.del(k),
            C.zrange(k, "0", "1"), C.zrange(k, "0", "1", true), C.zrange(k, "0", "1", false, false), C.zrange(k, "0", "1", true, false),
            C.zadd(k, "1", k), C.zcard(k), C.zrem(k, k), C.zrevrangebyscore(k, "1", "0"),
            C.zrevrangebyscore(k, "1", "0"; limit_start=1, limit_count=2), C.zremrangebyrank(k, 0, -1),
            C.zrevrange(k, 0, -1), C.zrevrange(k, 0, -1; withscores=true),
            C.xadd(k, "*", k, k), C.xadd(k, "*", k, k, 10), C.xdel(k, "1-0"), C.xrange(k, "-", "+"), C.xtrim(k, 10), C.xtrim(k, 10, true),
            C.xgroup_create(k, k), C.xgroup_create(k, k, "0"; mkstream=true), C.xgroup_destroy(k, k), C.xgroup_setid(k, k, "0"),
            C.xgroup_delconsumer(k, k, k), C.xreadgroup(k, k, k => ">"), C.xreadgroup(k, k, k => ">", "s" => "0"; count=5, block=10, noack=true),
            C.xack(k, k, "1-0", "2-0"), C.xpending(k, k), C.xpending(k, k; count=10, idle=5, consumer=k),
            C.xclaim(k, k, k, 100, "1-0"), C.xclaim(k, k, k, 100, "1-0"; idle=1, time=2, retrycount=3, force=true, justid=true),
            C.xinfo_stream(k), C.xinfo_groups(k), C.xinfo_consumers(k, k),
            C.sadd(k, k), C.sismember(k, k), C.srem(k, k), C.scard(k), C.smembers(k), C.multi(), C.exec(), C.discard(),
            C.expire(k, 5), C.publish(k, k), C.hdel(k, k), C.hget(k, k), C.hlen(k), C.hset(k, k, k),
            C.rpush(k, k), C.lindex(k, 0), C.lpush(k, k), C.ltrim(k, 0, -1), C.lrange(k, 0, -1), C.lrem(k, 1, k),
            C.geoadd(k, "1", "2", k), C.geodist(k, k, k, "km"), C.geohash(k, k, k), C.geopos(k, k), C.geosearch(k, "1", "2", "3", "km"),
            C.georadius(k, "1", "2", "3", "km"),
            C.georadius(k, "1", "2", "3", "km"; asc=false, withcoord=true, withdist=true, withhash=true, count=3, store=k, storedist=k)]
        @test filter(!wellformed, cmds) == []
    end

    @testset "connection failures" begin
        socket = FailingAfterWriteIO(true, Ref(false))
        redis = Redis.Client(socket, false, ReentrantLock(), Redis.Future[])
        Redis.start_response_reader!(redis)
        err = try
            Redis.execute(redis, Redis.Commands.get("missing"))
            nothing
        catch e
            e
        end
        @test err isa Redis.RedisConnectionError
        @test !isopen(redis)
    end

    @testset "Basic connections" begin
      if !DOCKER
        @test_skip "server-backed tests (docker unavailable)"
      else
        Harbor.with_container("redis"; wait_strategy=(pattern="Ready to accept connections tcp",), ports=Dict(6379 => 6379), command=["redis-server"]) do _
            redis = Redis.connect("127.0.0.1", 6379)
            Redis.set(redis, "key2", "value2")
            @test Redis.get(redis, "key2") == "value2"
            # SET NX/XX and GEODIST reply nil when their condition is not met
            @test Redis.set(redis, "nx-key", "first"; nx=true) == "OK"
            @test Redis.set(redis, "nx-key", "second"; nx=true) === nothing
            @test Redis.set(redis, "xx-missing-key", "value"; xx=true) === nothing
            @test Redis.get(redis, "nx-key") == "first"
            @test Redis.geoadd(redis, "geo-key", "13.361389", "38.115556", "Palermo") == 1
            @test Redis.geodist(redis, "geo-key", "Palermo", "Nowhere") === nothing
            Redis.del(redis, "nx-key")
            Redis.del(redis, "geo-key")
            binary_value = "flight\r\ncontinuation\0" * String(UInt8[0xff])
            @test Redis.set(redis, "binary-key", binary_value) == "OK"
            @test Redis.get(redis, "binary-key") == binary_value
            @test Redis.mget(redis, "binary-key", "missing-binary-key") == [binary_value, nothing]
            @test Redis.execute(redis, Redis.Commands.get("binary-key"), Redis.Commands.get("key2")) ==
                  [binary_value, "value2"]
            close(redis.socket)
            @test Redis.set(redis, "reconnect-key", "reconnect-value") == "OK"
            @test Redis.get(redis, "reconnect-key") == "reconnect-value"
            reconnecting = Redis.Client(
                Redis.connectsocket("127.0.0.1", 6379),
                false,
                ReentrantLock(),
                Redis.Future[],
                Redis.ConnectionOptions("127.0.0.1", 6379, nothing, 0, false),
                false,
            )
            stale = Redis.Future{Union{Any,Exception}}()
            push!(reconnecting.responses, stale)
            close(reconnecting.socket)
            @lock reconnecting.lock Redis._ensure_connected_locked!(reconnecting)
            @test wait(stale) isa Redis.RedisConnectionError
            @test isopen(reconnecting)
            close(reconnecting)
            @test Redis.mget(redis, "missing-key-1", "missing-key-2") == [nothing, nothing]
            Redis.rpush(redis, "listkey", "v1")
            Redis.rpush(redis, "listkey", "v2")
            @test Redis.ltrim(redis, "listkey", -1, -1) == "OK"
            @test Redis.lrange(redis, "listkey", 0, -1) == ["v2"]
            Redis.rpush(redis, "unicode-listkey", "warning ⚠️")
            Redis.lpush(redis, "unicode-listkey", "plane ✈️")
            @test Redis.lrange(redis, "unicode-listkey", 0, -1) == ["plane ✈️", "warning ⚠️"]
            @test Redis.lrem(redis, "unicode-listkey", 1, "warning ⚠️") == 1
            @test Redis.lrange(redis, "unicode-listkey", 0, -1) == ["plane ✈️"]
            Redis.hset(redis, "unicode-hash", "message", "warning ⚠️")
            @test Redis.hget(redis, "unicode-hash", "message") == "warning ⚠️"
            unicode_value = "Hotel ✅ — Atlanta 🏨"
            Redis.rpush(redis, "unicode:list", unicode_value)
            @test Redis.ltrim(redis, "unicode:list", -1, -1) == "OK"
            @test Redis.lrange(redis, "unicode:list", 0, -1) == [unicode_value]
            @test Redis.hset(redis, "unicode:hash", "summary", unicode_value) == 1
            @test Redis.hget(redis, "unicode:hash", "summary") == unicode_value
            @test isempty(collect(Redis.Scan(redis, "scan:missing:*")))
            Redis.set(redis, "scan:key:1", "v1")
            Redis.set(redis, "scan:key:2", "v2")
            @test Set(collect(Redis.Scan(redis, "scan:key:*"))) == Set(["scan:key:1", "scan:key:2"])
            @test isempty(collect(Redis.Zscan(redis, "scan:missing")))
            @test isempty(collect(Redis.Sscan(redis, "scan:missing")))
            @test isempty(collect(Redis.Hscan(redis, "scan:missing")))
            Redis.zadd(redis, "scan:z", "1", "m1")
            Redis.sadd(redis, "scan:s", "s1")
            Redis.hset(redis, "scan:h", "f1", "v1")
            @test collect(Redis.Zscan(redis, "scan:z", "m*")) == ["m1", "1"]
            @test collect(Redis.Sscan(redis, "scan:s", "s*")) == ["s1"]
            @test collect(Redis.Hscan(redis, "scan:h", "f*")) == ["f1", "v1"]
            @test Redis.zrange(redis, "scan:z", "0", "5") == ["m1"]
            # every reply above went to its own command
            @test Redis.get(redis, "scan:key:1") == "v1"
            # cleanup
            Redis.del(redis, "key2")
            Redis.del(redis, "binary-key")
            Redis.del(redis, "reconnect-key")
            Redis.del(redis, "listkey")
            Redis.del(redis, "unicode-listkey")
            Redis.del(redis, "unicode-hash")
            Redis.del(redis, "unicode:list")
            Redis.del(redis, "unicode:hash")
            Redis.del(redis, "scan:key:1")
            Redis.del(redis, "scan:key:2")
            Redis.del(redis, "scan:z")
            Redis.del(redis, "scan:s")
            Redis.del(redis, "scan:h")
            # batch execution
            batch = Redis.Batch()
            push!(batch, Redis.Commands.set("batchkey1", "v1"))
            push!(batch, Redis.Commands.set("batchkey2", "v2"))
            push!(batch, Redis.Commands.get("batchkey1"))
            push!(batch, Redis.Commands.get("batchkey2"))
            results = Redis.execute(redis, batch)
            @test results[1] == "OK"
            @test results[2] == "OK"
            @test results[3] == "v1"
            @test results[4] == "v2"
            @test typeof(results) == Vector{String}
            # Cleanup
            Redis.del(redis, "batchkey1")
            Redis.del(redis, "batchkey2")
            @test begin
                close(redis)
                true
            end
            @test_throws Redis.RedisConnectionError Redis.get(redis, "key2")

            # a message whose handler throws stays pending; once idle it is
            # claimed and retried, and new messages keep flowing meanwhile
            redis = Redis.connect("127.0.0.1", 6379)
            config = Redis.ConsumerGroupConfig(; block_time=100, claim_min_idle_time=50)
            group = Redis.ConsumerGroup(redis, "retry-group", "retry-stream", "consumer-1"; config)
            delivered = Channel{String}(Inf)
            failed = Ref(false)
            consumer = Redis.startConsumer!(group; pending_check_interval=0) do event
                put!(delivered, event.fields["n"])
                failed[] || (failed[] = true; error("transient failure"))
                return true
            end
            Redis.xadd(redis, "retry-stream", "*", "n", "1")
            seen = String[]
            deadline = time() + 15
            while time() < deadline && length(seen) < 3
                if isready(delivered)
                    push!(seen, take!(delivered))
                    # added after the failure, so it is only seen if reading continues
                    length(seen) == 1 && Redis.xadd(redis, "retry-stream", "*", "n", "2")
                else
                    sleep(0.05)
                end
            end
            Redis.stop!(consumer)
            @test seen == ["1", "2", "1"] || seen == ["1", "1", "2"]
            @test consumer.stats.last_error === nothing
            sleep(0.3)  # let the consumer's blocking read finish before closing
            Redis.del(redis, "retry-stream")
            close(redis)
        end
        Harbor.with_container("redis"; wait_strategy=(pattern="Ready to accept connections tcp",), ports=Dict(6379 => 6379), command=["redis-server", "--requirepass", "yourpassword"]) do _
            redis = Redis.connect("127.0.0.1", 6379; password="yourpassword")
            Redis.set(redis, "key1", "value1")
            @test Redis.get(redis, "key1") == "value1"
            # cleanup
            Redis.del(redis, "key1")
            @test begin
                close(redis)
                true
            end
        end
      end
    end
end
