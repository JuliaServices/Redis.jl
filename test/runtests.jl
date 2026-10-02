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
            for invalid in ("\$-2\r\n", "*-2\r\n", "\$+1\r\na\r\n", "*+1\r\n:1\r\n",
                            "\$ 1\r\na\r\n", "\$1 \r\na\r\n", "\$\r\n", "*\r\n",
                            "\$$overflow\r\n", "*$overflow\r\n", "\$1\rXa\r\n",
                            "\$1\r\naXX", "\$0\r\nXX", "\$1\r\nab\r\n", "\$3\r\nab\r\n",
                            "*1\r\n\$-2\r\n", "+O\nK\r\n", "+OK\rX", "-ERR\rX", ":1\rX")
                for io in (IOBuffer(invalid), ChunkedIO([[b] for b in codeunits(invalid)]))
                    delivered = Ref(false)
                    @test_throws Union{Redis.RedisError,Redis.Parsers.Error} Redis.readresponse!(
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
