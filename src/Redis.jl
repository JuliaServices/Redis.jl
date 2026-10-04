module Redis

using Reseau, Logging, Base64, StringViews, Parsers, Dates

include("commands.jl")
using .Commands
import .Commands: get

const ReseauConn = Union{Reseau.TCP.Conn, Reseau.TLS.Conn}

function mcp_session_store end

mutable struct Future{T}
    cond::Threads.Condition
    ready::Bool
    result::Any
end

Future{T}() where {T} = Future{T}(Threads.Condition(), false, nothing)

function Base.wait(f::Future)
    lock(f.cond)
    try
        while !f.ready
            wait(f.cond)
        end
        return f.result
    finally
        unlock(f.cond)
    end
end

function Base.notify(f::Future, value)
    lock(f.cond)
    try
        f.ready && return nothing
        f.result = value
        f.ready = true
        notify(f.cond)
    finally
        unlock(f.cond)
    end
    return nothing
end

struct RedisError <: Exception
    msg::String
end

Base.showerror(io::IO, e::RedisError) = print(io, e.msg)

struct RedisConnectionError <: Exception
    msg::String
    cause::Union{Nothing,Exception}
end

RedisConnectionError(msg::AbstractString) = RedisConnectionError(String(msg), nothing)

function Base.showerror(io::IO, e::RedisConnectionError)
    print(io, e.msg)
    if e.cause !== nothing
        print(io, ": ")
        showerror(io, e.cause)
    end
    return
end

struct ConnectionOptions
    host::String
    port::Int
    password::Union{String,Nothing}
    db::Int
    tls::Bool
end

mutable struct Client{S <: IO}
    socket::S
    debug::Bool
    lock::ReentrantLock # held for single command submit + response, or entire batch
    responses::Vector{Future} # holds futures for responses
    options::Union{Nothing,ConnectionOptions}
    closed::Bool
end

Client(socket::S, debug::Bool, lock::ReentrantLock, responses::Vector{Future}) where {S<:IO} =
    Client{S}(socket, debug, lock, responses, nothing, false)

@inline _response_future(::Type{T}) where {T} = Future{Union{T, Exception}}()

@inline function _future_notify!(fut::Future, value)::Nothing
    notify(fut, value)
    return nothing
end

@inline function _future_wait(fut::Future)
    result = wait(fut)
    result isa Exception && throw(result)
    return result
end

@inline _coerce_result(::Type{Any}, result) = result
@inline _coerce_result(::Type{T}, result) where {T} = result::T
@inline function _coerce_result(::Type{Vector{T}}, result::AbstractVector) where {T}
    return T[_coerce_result(T, x) for x in result]
end

@inline function _future_wait_typed(fut::Future{Union{Exception, T}}) where {T}
    return _coerce_result(T, _future_wait(fut))::T
end

@inline function _future_wait_typed(fut::Future{T}) where {T}
    return _coerce_result(T, _future_wait(fut))::T
end

function connectsocket(host::AbstractString, port::Integer; tls::Bool=false)
    address = string(host, ":", Int(port))
    return if tls
        Reseau.TLS.connect(address; server_name=String(host))
    else
        Reseau.TCP.connect(address)
    end
end

function _close_quietly(socket)
    try
        close(socket)
    catch
    end
    return
end

function _execute_direct(socket::IO, command::Command{T}, debug::Bool=false) where {T}
    debug && @info "Redis startup command: $(command.cmd)"
    writemessage(socket, command.cmd)
    local result
    readresponse!(socket, Vector{UInt8}(undef, 4096), 1, 0, debug) do value
        result = value
    end
    result isa Exception && throw(result)
    return _coerce_result(T, result)
end

function _open_socket(options::ConnectionOptions, debug::Bool=false)
    socket = connectsocket(options.host, options.port; tls=options.tls)
    try
        if options.password !== nothing
            resp = _execute_direct(socket, auth(options.password), debug)
            debug && @info "AUTH sent: $resp"
        end
        if options.db > 0
            resp = _execute_direct(socket, select(options.db), debug)
            debug && @info "SELECT sent: $resp"
        end
        return socket
    catch
        _close_quietly(socket)
        rethrow()
    end
end

function connect(host::String, port::Integer=6379; password::Union{String,Nothing}=nothing, db::Integer=0, tls::Bool=false, debug::Bool=false, socketdebug::Bool=false)
    _ = socketdebug
    options = ConnectionOptions(host, Int(port), password, Int(db), tls)
    socket = _open_socket(options, debug)
    client = Client(socket, debug, ReentrantLock(), Future[], options, false)
    start_response_reader!(client)
    return client
end

function _fail_pending_locked!(client::Client, error::Exception)
    while !isempty(client.responses)
        _future_notify!(popfirst!(client.responses), error)
    end
    return
end

function _mark_closed_locked!(client::Client, socket, error::Exception)
    client.socket === socket || return
    client.closed = true
    _close_quietly(socket)
    _fail_pending_locked!(client, RedisConnectionError("redis connection closed", error))
    return
end

function _ensure_connected_locked!(client::Client)
    !client.closed && isopen(client.socket) && return
    options = client.options
    options === nothing && throw(RedisConnectionError("redis connection is closed and cannot be reconnected"))
    client.closed = true
    _fail_pending_locked!(client, RedisConnectionError("redis connection closed before reconnect"))
    _close_quietly(client.socket)
    socket = _open_socket(options, client.debug)
    client.socket = socket
    client.closed = false
    start_response_reader!(client)
    return
end

function Base.close(client::Client)
    @lock client.lock begin
        client.closed = true
        client.options = nothing
        _close_quietly(client.socket)
        _fail_pending_locked!(client, RedisConnectionError("redis connection closed"))
    end
    return
end

Base.isopen(client::Client) = !client.closed && isopen(client.socket)

function writemessage(socket::IO, message)
    write(socket, message)
    flush(socket)
end

# execution flow is:
# take client lock
# write command to socket
# add a future to the client.responses FIFO queue (vector)
# release lock
# wait for the future to be completed
# this blocks and wait will throw if the future is notified with an error
# concurrency works because we hold the lock while writing our message and immediately queuing our future
function execute(client::Client, command::Command{T}) where {T}
    fut = _response_future(T)
    @lock client.lock begin
        _ensure_connected_locked!(client)
        try
            writemessage(client.socket, command.cmd)
        catch e
            _mark_closed_locked!(client, client.socket, e)
            rethrow()
        end
        push!(client.responses, fut)
    end
    return _future_wait_typed(fut)
end

mutable struct Batch
    const lock::ReentrantLock
    const commands::IOBuffer
    const futures::Vector{Future}
    nbuffered::Int
    last_flush::DateTime
end

Batch() = Batch(ReentrantLock(), IOBuffer(), Future[], 0, Dates.now())

function Base.push!(batch::Batch, cmd::Command{T}) where {T}
    @lock batch.lock begin
        write(batch.commands, cmd.cmd)
        push!(batch.futures, _response_future(T))
        batch.nbuffered += 1
    end
    return batch
end

Base.length(batch::Batch) = @lock batch.lock batch.nbuffered

# batch execution works similarly to single command execution:
# first we get the batch commands as a single Vector{UInt8}
# also get all the futures for each command
# then get client lock, write commands, and append futures to the client.responses vector
# the error handling uses a CompositeException because we want to ensure *all* responses
# are read, then we can throw any/all after all responses have been read
function execute(client::Client, batch::Batch)
    local cmds::Vector{UInt8}
    local futs::Vector{Future}
    @lock batch.lock begin
        batch.last_flush = Dates.now()
        cmds = take!(batch.commands)
        futs = splice!(batch.futures, 1:batch.nbuffered)
        batch.nbuffered = 0
    end
    @lock client.lock begin
        _ensure_connected_locked!(client)
        try
            writemessage(client.socket, cmds)
        catch e
            _mark_closed_locked!(client, client.socket, e)
            rethrow()
        end
        append!(client.responses, futs)
    end
    err = CompositeException()
    results = map(futs) do fut
        try
            return _future_wait_typed(fut)
        catch e
            push!(err, e)
            return nothing
        end
    end
    if !isempty(err)
        throw(err)
    end
    return results
end

# poor man's batch execution given vararg commands
function execute(client::Client, cmds::Command...)
    commands = IOBuffer()
    for cmd in cmds
        write(commands, cmd.cmd)
    end
    cmdbytes = take!(commands)
    local futs
    @lock client.lock begin
        _ensure_connected_locked!(client)
        try
            writemessage(client.socket, cmdbytes)
        catch e
            _mark_closed_locked!(client, client.socket, e)
            rethrow()
        end
        len = length(client.responses)
        for cmd in cmds
            push!(client.responses, _response_future(resulttype(cmd)))
        end
        # make a copy of our cmd futures to wait on
        futs = client.responses[len+1:end]
    end
    err = CompositeException()
    results = map(futs) do fut
        try
            return _future_wait_typed(fut)
        catch e
            push!(err, e)
            return nothing
        end
    end
    if !isempty(err)
        throw(err)
    end
    return results
end

function start_response_reader!(client::Client)
    socket = client.socket
    errormonitor(Threads.@spawn begin
        buf = Vector{UInt8}(undef, 4096)
        pos = 1
        len = 0
        # this loop is meant to block on the eof call until there are bytes to read
        # we assume that once there are bytes to read, a future has been queued to notify
        while true
            if pos >= len
                try
                    eof(socket) && throw(EOFError())
                catch e
                    @lock client.lock _mark_closed_locked!(client, socket, e)
                    return
                end
            end
            local fut::Future
            @lock client.lock begin
                client.socket === socket || return
                if isempty(client.responses)
                    continue
                end
                fut = popfirst!(client.responses)
            end
            try
                pos, len = readresponse!(x -> _future_notify!(fut, x), socket, buf, pos, len, client.debug)
            catch e
                _future_notify!(fut, RedisConnectionError("redis response reader error", e))
                @lock client.lock _mark_closed_locked!(client, socket, e)
                return
            end
        end
    end)
    return
end

@inline function findnewline(socket, buf, start, pos, len)
    while true
        if pos > len
            start, pos, len = getmoredata!(socket, buf, start, pos, len)
        end
        buf[pos] == UInt8('\r') && return start, pos, len
        buf[pos] == UInt8('\n') && throw(RedisError("Unexpected newline in RESP line"))
        pos += 1
    end
    @assert false
end

# Consume a complete RESP terminator before delivering the response to its caller.
@inline function skipcrlf(socket, buf, pos, len)
    for expected in (UInt8('\r'), UInt8('\n'))
        if pos > len
            _, pos, len = getmoredata!(socket, buf, pos, pos, len)
        end
        buf[pos] == expected || throw(RedisError("Expected CRLF in RESP response"))
        pos += 1
    end
    return pos, len
end

function parselength(buf, start, stop)
    stop == start + 1 && buf[start] == UInt8('-') && buf[stop] == UInt8('1') && return -1
    start <= stop && all(i -> UInt8('0') <= buf[i] <= UInt8('9'), start:stop) ||
        throw(RedisError("Invalid RESP length"))
    return Parsers.parse(Int, @view buf[start:stop])
end

@inline function _read_some!(socket::ReseauConn, buf::AbstractVector{UInt8}, nb::Integer)::Int
    return readbytes!(socket, buf, nb; all=false)
end

@inline function _read_some!(socket::IO, buf::AbstractVector{UInt8}, nb::Integer)::Int
    return readbytes!(socket, buf, nb)
end

function getmoredata!(socket, buf, start, pos, len)
    eof(socket) && throw(EOFError())
    # reset start/pos/len, unless there are bytes between start and pos we need to shift down
    nrem = len - start + 1
    if nrem < 0
        # safety: start past end of buffer; treat as empty
        nrem = 0
    end
    if nrem > 0 && start > 1
        copyto!(buf, 1, buf, start, nrem)
        pos -= start - 1
        len -= start - 1
        start = 1
    elseif nrem == 0
        # no bytes to move, reset to start of buffer
        start = 1
        pos = 1
        len = 0
    end
    if pos > length(buf)
        resize!(buf, max(length(buf) * 2, pos))
    end
    free = length(buf) - pos + 1
    if free <= 0
        resize!(buf, max(length(buf) * 2, pos))
        free = length(buf) - pos + 1
    end
    nbytes = _read_some!(socket, @view(buf[pos:(pos + free - 1)]), free)
    nbytes == 0 && throw(EOFError())
    len += nbytes
    return start, pos, len
end

# read a single redis response from buf (filling buf from socket as needed)
# once a concrete value has been parsed from the response, apply f
@inline function readresponse!(f, socket::IO, buf=Vector{UInt8}(undef, 4096), pos=1, len=0, debug=true)
    # pos is the byte position in buf we read from next
    # len is the last byte position in buf of valid data to read
    GC.@preserve buf begin
        while true
            if pos > len
                _, pos, len = getmoredata!(socket, buf, pos, pos, len)
            end
            type = buf[pos]
            pos += 1
            debug && @info "RESP type: $(Char(type))"
            if type == UInt8('+')
                start = pos
                start, pos, len = findnewline(socket, buf, start, pos, len)
                value = unsafe_string(pointer(buf, start), pos - start)
                pos, len = skipcrlf(socket, buf, pos, len)
                f(value)
                return pos, len
            elseif type == UInt8('-')
                start = pos
                start, pos, len = findnewline(socket, buf, start, pos, len)
                value = RedisError(unsafe_string(pointer(buf, start), pos - start))
                pos, len = skipcrlf(socket, buf, pos, len)
                f(value)
                return pos, len
            elseif type == UInt8(':')
                start = pos
                start, pos, len = findnewline(socket, buf, start, pos, len)
                value = Parsers.parse(Int, @view buf[start:pos-1])
                pos, len = skipcrlf(socket, buf, pos, len)
                f(value)
                return pos, len
            elseif type == UInt8('$')
                start = pos
                start, pos, len = findnewline(socket, buf, start, pos, len)
                bulklen = parselength(buf, start, pos - 1)
                pos, len = skipcrlf(socket, buf, pos, len)
                debug && @info "Bulklen: $bulklen"
                if bulklen == -1
                    f(nothing)
                    return pos, len
                end
                start = pos
                while bulklen > len - start + 1
                    start, _, len = getmoredata!(socket, buf, start, len + 1, len)
                end
                value = unsafe_string(pointer(buf, start), bulklen)
                pos, len = skipcrlf(socket, buf, start + bulklen, len)
                f(value)
                return pos, len
            elseif type == UInt8('*')
                start = pos
                start, pos, len = findnewline(socket, buf, start, pos, len)
                nelem = parselength(buf, start, pos - 1)
                pos, len = skipcrlf(socket, buf, pos, len)
                debug && @info "Array count: $nelem"
                if nelem == -1
                    f(nothing)
                    return pos, len
                end
                plc = PosLenClosure(socket, buf, pos, len, debug)
                f(map(plc, 1:nelem))
                return plc.pos, plc.len
            else
                @error "dumping redis buffer" pos = pos len = len
                println(String(buf))
                close(socket)
                throw(RedisError("Unexpected RESP type: '$(Char(type))'"))
            end
        end
    end
    throw(EOFError())
end

mutable struct PosLenClosure{T<:IO}
    const socket::T
    const buf::Vector{UInt8}
    pos::Int
    len::Int
    const debug::Bool
end

function (f::PosLenClosure{T})(i) where {T}
    local x
    f.pos, f.len = readresponse!(f.socket, f.buf, f.pos, f.len, f.debug) do y
        x = y
    end
    return x
end

function Commands.set(client::Client, key::AbstractString, value::AbstractString; nx::Bool=false, xx::Bool=false, ex::Int=0, px::Int=0)
    command = Commands.set(key, value; nx=nx, xx=xx, ex=ex, px=px)
    client.debug && @info "SET Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.mset(client::Client, pairs::Pair{String,String}...)
    command = Commands.mset(pairs...)
    client.debug && @info "MSET Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.get(client::Client, key::AbstractString)
    command = Commands.get(key)
    client.debug && @info "GET Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.del(client::Client, key::AbstractString)
    command = Commands.del(key)
    client.debug && @info "DEL Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.append(client::Client, key::AbstractString, value::AbstractString)
    command = Commands.append(key, value)
    client.debug && @info "APPEND Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.incrby(client::Client, key::AbstractString, increment::Int)
    command = Commands.incrby(key, increment)
    client.debug && @info "INCRBY Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.mget(client::Client, keys::AbstractString...)
    command = Commands.mget(keys...)
    client.debug && @info "MGET Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.scan(client::Client, cursor::AbstractString="0"; match::AbstractString="")
    command = Commands.scan(cursor; match=match)
    client.debug && @info "SCAN Command: $(command.cmd)"
    return execute(client, command)
end

struct Scan
    client::Client
    match::String
end

Scan(client) = Scan(client, "")
Base.IteratorSize(::Type{Scan}) = Base.SizeUnknown()
Base.IteratorEltype(::Type{Scan}) = Base.HasEltype()
Base.eltype(::Type{Scan}) = String

function Base.iterate(ss::Scan, state=nothing)
    if state === nothing
        cursor = "0"
    else
        (cursor, keys), i = state
        i <= length(keys) && return keys[i], ((cursor, keys), i+1)
        cursor == "0" && return nothing
    end
    while true
        cursor, keys = scan(ss.client, cursor; match=ss.match)
        !isempty(keys) && return keys[1], ((cursor, keys), 2)
        cursor == "0" && return nothing
    end
end

function Commands.zrange(client::Client, key::AbstractString, min::AbstractString, max::AbstractString, bylex::Bool=false, useLegacyCommand::Bool=true)
    command = Commands.zrange(key, min, max, bylex, useLegacyCommand)
    client.debug && @info "ZRANGE Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.zadd(client::Client, key::AbstractString, score::AbstractString, member::AbstractString)
    command = Commands.zadd(key, score, member)
    client.debug && @info "ZADD Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.zcard(client::Client, key::AbstractString)
    command = Commands.zcard(key)
    client.debug && @info "ZCARD Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.zrem(client::Client, key::AbstractString, member::AbstractString)
    command = Commands.zrem(key, member)
    client.debug && @info "ZREM Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.zrevrangebyscore(client::Client, key::AbstractString, max::AbstractString, min::AbstractString; limit_start::Int=0, limit_count::Int=0)
    command = Commands.zrevrangebyscore(key, max, min; limit_start=limit_start, limit_count=limit_count)
    client.debug && @info "ZREVRANGEBYSCORE Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.zremrangebyrank(client::Client, key::AbstractString, start::Int, stop::Int)
    command = Commands.zremrangebyrank(key, start, stop)
    client.debug && @info "ZREMRANGEBYRANK Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.zrevrange(client::Client, key::AbstractString, start::Int, stop::Int; withscores::Bool=false)
    command = Commands.zrevrange(key, start, stop; withscores=withscores)
    client.debug && @info "ZREVRANGE Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.zscan(client::Client, key::AbstractString, cursor::AbstractString="0"; match::AbstractString="")
    command = Commands.zscan(key, cursor; match=match)
    client.debug && @info "ZSCAN Command: $(command.cmd)"
    return execute(client, command)
end

struct Zscan
    client::Client
    key::String
    match::String
end

Zscan(client, key) = Zscan(client, key, "")

function Base.iterate(ss::Zscan, state=nothing)
    if state === nothing
        cursor, members = zscan(ss.client, ss.key; match=ss.match)
        i = 1
    else
        (cursor, members), i = state
        if i > length(members)
            cursor == "0" && return nothing
            cursor, members = zscan(ss.client, ss.key, cursor; match=ss.match)
            i = 1
        end
    end
    return members[i], ((cursor, members), i+1)
end

function Commands.xadd(client::Client, key::AbstractString, id::AbstractString, field::AbstractString, value::AbstractString)
    command = Commands.xadd(key, id, field, value)
    client.debug && @info "XADD Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xadd(client::Client, key::AbstractString, id::AbstractString, field::AbstractString, value::AbstractString, maxlen::Int)
    command = Commands.xadd(key, id, field, value, maxlen)
    client.debug && @info "XADD Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xdel(client::Client, key::AbstractString, id::AbstractString)
    command = Commands.xdel(key, id)
    client.debug && @info "XDEL Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xrange(client::Client, key::AbstractString, start::AbstractString, stop::AbstractString)
    command = Commands.xrange(key, start, stop)
    client.debug && @info "XRANGE Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xtrim(client::Client, key::AbstractString, maxlen::Int, approximate::Bool=false)
    command = Commands.xtrim(key, maxlen, approximate)
    client.debug && @info "XTRIM Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.sadd(client::Client, key::AbstractString, member::AbstractString)
    command = Commands.sadd(key, member)
    client.debug && @info "SADD Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.sismember(client::Client, key::AbstractString, member::AbstractString)
    command = Commands.sismember(key, member)
    client.debug && @info "SISMEMBER Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.srem(client::Client, key::AbstractString, member::AbstractString)
    command = Commands.srem(key, member)
    client.debug && @info "SREM Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.scard(client::Client, key::AbstractString)
    command = Commands.scard(key)
    client.debug && @info "SCARD Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.sscan(client::Client, key::AbstractString, cursor::AbstractString="0"; match::AbstractString="")
    command = Commands.sscan(key, cursor; match=match)
    client.debug && @info "SSCAN Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.smembers(client::Client, key::AbstractString)
    command = Commands.smembers(key)
    client.debug && @info "SMEMBERS Command: $(command.cmd)"
    return execute(client, command)
end

struct Sscan
    client::Client
    key::String
    match::String
end

Sscan(client, key) = Sscan(client, key, "")

function Base.iterate(ss::Sscan, state=nothing)
    if state === nothing
        cursor, members = sscan(ss.client, ss.key; match=ss.match)
        i = 1
    else
        (cursor, members), i = state
        if i > length(members)
            cursor == "0" && return nothing
            cursor, members = sscan(ss.client, ss.key, cursor; match=ss.match)
            i = 1
        end
    end
    return members[i], ((cursor, members), i+1)
end

function Commands.multi(client::Client)
    command = Commands.multi()
    client.debug && @info "MULTI Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.exec(client::Client)
    command = Commands.exec()
    client.debug && @info "EXEC Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.discard(client::Client)
    command = Commands.discard()
    client.debug && @info "DISCARD Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.expire(client::Client, key::AbstractString, seconds::Int)
    command = Commands.expire(key, seconds)
    client.debug && @info "EXPIRE Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.publish(client::Client, channel::AbstractString, message::AbstractString)
    command = Commands.publish(channel, message)
    client.debug && @info "PUBLISH Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.hdel(client::Client, key::AbstractString, field::AbstractString)
    command = Commands.hdel(key, field)
    client.debug && @info "HDEL Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.hget(client::Client, key::AbstractString, field::AbstractString)
    command = Commands.hget(key, field)
    client.debug && @info "HGET Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.hlen(client::Client, key::AbstractString)
    command = Commands.hlen(key)
    client.debug && @info "HLEN Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.hset(client::Client, key::AbstractString, field::AbstractString, value::AbstractString)
    command = Commands.hset(key, field, value)
    client.debug && @info "HSET Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.hscan(client::Client, key::AbstractString, cursor::Int64; match::Union{Nothing,AbstractString}=nothing)
    command = Commands.hscan(key, cursor; match=match)
    client.debug && @info "HSCAN Command: $(command.cmd)"
    return execute(client, command)
end

struct Hscan
    client::Client
    key::String
    match::String
end

Hscan(client, key) = Hscan(client, key, "")

function Base.iterate(ss::Hscan, state=nothing)
    if state === nothing
        cursor, members = hscan(ss.client, ss.key, "0"; match=ss.match)
        i = 1
    else
        (cursor, members), i = state
        if i > length(members)
            cursor == "0" && return nothing
            cursor, members = hscan(ss.client, ss.key, cursor; match=ss.match)
            i = 1
        end
    end
    return members[i], ((cursor, members), i+1)
end

function Commands.rpush(client::Client, key::AbstractString, value::AbstractString)
    command = Commands.rpush(key, value)
    client.debug && @info "RPUSH Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.lindex(client::Client, key::AbstractString, index::Int64)
    command = Commands.lindex(key, index)
    client.debug && @info "LINDEX Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.lpush(client::Client, key::AbstractString, value::AbstractString)
    command = Commands.lpush(key, value)
    client.debug && @info "LPUSH Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.ltrim(client::Client, key::AbstractString, start::Int64, stop::Int64)
    command = Commands.ltrim(key, start, stop)
    client.debug && @info "LTRIM Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.lrange(client::Client, key::AbstractString, start::Int64, stop::Int64)
    command = Commands.lrange(key, start, stop)
    client.debug && @info "LRANGE Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.lrem(client::Client, key::AbstractString, count::Int64, value::AbstractString)
    command = Commands.lrem(key, count, value)
    client.debug && @info "LREM Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.geoadd(client::Client, key::AbstractString, longitude::AbstractString, latitude::AbstractString, member::AbstractString)
    command = Commands.geoadd(key, longitude, latitude, member)
    client.debug && @info "GEOADD Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.geodist(client::Client, key::AbstractString, member1::AbstractString, member2::AbstractString, unit::AbstractString="m")
    command = Commands.geodist(key, member1, member2, unit)
    client.debug && @info "GEODIST Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.geohash(client::Client, key::AbstractString, members::AbstractString...)
    command = Commands.geohash(key, members...)
    client.debug && @info "GEOHASH Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.geopos(client::Client, key::AbstractString, members::AbstractString...)
    command = Commands.geopos(key, members...)
    client.debug && @info "GEOPOS Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.geosearch(client::Client, key::AbstractString, longitude::AbstractString, latitude::AbstractString, radius::AbstractString, unit::AbstractString)
    command = Commands.geosearch(key, longitude, latitude, radius, unit)
    client.debug && @info "GEOSEARCH Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.georadius(client::Client, key::AbstractString, longitude::AbstractString, latitude::AbstractString, radius::AbstractString, unit::AbstractString;
    asc::Bool=true, withcoord::Bool=false, withdist::Bool=false, withhash::Bool=false, count::Union{Nothing, Int}=nothing, store::Union{Nothing, String}=nothing, storedist::Union{Nothing, String}=nothing)
    command = Commands.georadius(key, longitude, latitude, radius, unit; asc=asc, withcoord=withcoord, withdist=withdist, withhash=withhash, count=count, store=store, storedist=storedist)
    client.debug && @info "GEORADIUS Command: $(command.cmd)"
    return execute(client, command)
end

# xgroup_create, xgroup_destroy, xgroup_setid, xgroup_delconsumer, xreadgroup, xack, xpending, xclaim, xinfo_stream, xinfo_groups, xinfo_consumers
function Commands.xgroup_create(client::Client, stream::AbstractString, group::AbstractString, id::AbstractString="\$"; mkstream::Bool=false)
    command = Commands.xgroup_create(stream, group, id; mkstream=mkstream)
    client.debug && @info "XGROUP_CREATE Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xgroup_destroy(client::Client, stream::AbstractString, group::AbstractString)
    command = Commands.xgroup_destroy(stream, group)
    client.debug && @info "XGROUP_DESTROY Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xgroup_setid(client::Client, stream::AbstractString, group::AbstractString, id::AbstractString)
    command = Commands.xgroup_setid(stream, group, id)
    client.debug && @info "XGROUP_SETID Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xgroup_delconsumer(client::Client, stream::AbstractString, group::AbstractString, consumer::AbstractString)
    command = Commands.xgroup_delconsumer(stream, group, consumer)
    client.debug && @info "XGROUP_DELCONSUMER Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xreadgroup(client::Client, group::AbstractString, consumer::AbstractString, streams::Pair{String,String}...; count::Union{Int,Nothing}=nothing, block::Union{Int,Nothing}=nothing, noack::Bool=false)
    command = Commands.xreadgroup(group, consumer, streams...; count=count, block=block, noack=noack)
    client.debug && @info "XREADGROUP Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xack(client::Client, stream::AbstractString, group::AbstractString, ids::AbstractString...)
    command = Commands.xack(stream, group, ids...)
    client.debug && @info "XACK Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xpending(client::Client, stream::AbstractString, group::AbstractString; idle::Union{Int,Nothing}=nothing, start::AbstractString="-", end_::AbstractString="+", count::Union{Int,Nothing}=nothing, consumer::Union{String,Nothing}=nothing)
    command = Commands.xpending(stream, group; idle=idle, start=start, end_=end_, count=count, consumer=consumer)
    client.debug && @info "XPENDING Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xclaim(client::Client, stream::AbstractString, group::AbstractString, consumer::AbstractString, min_idle_time::Int, ids::AbstractString...; idle::Union{Int,Nothing}=nothing, time::Union{Int,Nothing}=nothing, retrycount::Union{Int,Nothing}=nothing, force::Bool=false, justid::Bool=false)
    command = Commands.xclaim(stream, group, consumer, min_idle_time, ids...; idle=idle, time=time, retrycount=retrycount, force=force, justid=justid)
    client.debug && @info "XCLAIM Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xinfo_stream(client::Client, stream::AbstractString)
    command = Commands.xinfo_stream(stream)
    client.debug && @info "XINFO_STREAM Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xinfo_groups(client::Client, stream::AbstractString)
    command = Commands.xinfo_groups(stream)
    client.debug && @info "XINFO_GROUPS Command: $(command.cmd)"
    return execute(client, command)
end

function Commands.xinfo_consumers(client::Client, stream::AbstractString, group::AbstractString)
    command = Commands.xinfo_consumers(stream, group)
    client.debug && @info "XINFO_CONSUMERS Command: $(command.cmd)"
    return execute(client, command)
end

include("consumergroup.jl")

end # module
