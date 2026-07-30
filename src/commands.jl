module Commands

export Command, resulttype, auth, select, set, mset, append, incrby, get, mget, del, scan, zrange, zadd, zcard, zrem, zrevrangebyscore, zremrangebyrank, zrevrange, zscan, xadd, xdel, xrange, xtrim, xgroup_create, xgroup_destroy, xgroup_setid, xgroup_delconsumer, xreadgroup, xack, xpending, xclaim, xinfo_stream, xinfo_groups, xinfo_consumers, sadd, sismember, srem, scard, sscan, smembers, multi, exec, discard, expire, publish, hdel, hget, hlen, hset, hscan, rpush, lindex, lpush, ltrim, lrange, lrem, geoadd, geodist, geohash, geopos, geosearch, georadius

# T is the return type of the command
struct Command{T}
    cmd::String
end

resulttype(cmd::Command{T}) where {T} = T

function auth(password::AbstractString)
    command = "*2\r\n\$4\r\nAUTH\r\n\$$(sizeof(password))\r\n$password\r\n"
    return Command{String}(command)
end

function select(db::Int)
    command = "*2\r\n\$6\r\nSELECT\r\n\$$(sizeof(string(db)))\r\n$(string(db))\r\n"
    return Command{String}(command)
end

function set(key::AbstractString, value::AbstractString; nx::Bool=false, xx::Bool=false, ex::Int=0, px::Int=0)
    narg = 3 + nx + xx + (ex > 0 ? 2 : 0) + (px > 0 ? 2 : 0)
    command = "*$narg\r\n\$3\r\nSET\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(value))\r\n$value\r\n"
    if nx
        command *= "\$2\r\nNX\r\n"
    end
    if xx
        command *= "\$2\r\nXX\r\n"
    end
    if ex > 0
        command *= "\$2\r\nEX\r\n\$$(sizeof(string(ex)))\r\n$(string(ex))\r\n"
    end
    if px > 0
        command *= "\$2\r\nPX\r\n\$$(sizeof(string(px)))\r\n$(string(px))\r\n"
    end
    return Command{String}(command)
end

function mset(pairs::Pair{String, String}...)
    n_args = 1 + 2 * length(pairs)
    command = "*$n_args\r\n\$4\r\nMSET\r\n"
    for (key, value) in pairs
        command *= "\$$(sizeof(key))\r\n$key\r\n"
        command *= "\$$(sizeof(value))\r\n$value\r\n"
    end
    return Command{String}(command)
end

function append(key::AbstractString, value::AbstractString)
    command = "*3\r\n\$6\r\nAPPEND\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(value))\r\n$value\r\n"
    return Command{Int}(command)
end

function incrby(key::AbstractString, increment::Int)
    incr = string(increment)
    command = "*3\r\n\$6\r\nINCRBY\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(incr))\r\n$incr\r\n"
    return Command{Int}(command)
end

function get(key::AbstractString)
    command = "*2\r\n\$3\r\nGET\r\n\$$(sizeof(key))\r\n$key\r\n"
    return Command{Union{String,Nothing}}(command)
end

function mget(keys::AbstractString...)
    narg = 1 + length(keys)
    command = "*$narg\r\n\$4\r\nMGET\r\n"
    for key in keys
        command *= "\$$(sizeof(key))\r\n$key\r\n"
    end
    return Command{Vector{Union{String,Nothing}}}(command)
end

function del(key::AbstractString)
    command = "*2\r\n\$3\r\nDEL\r\n\$$(sizeof(key))\r\n$key\r\n"
    return Command{Int}(command)
end

function scan(cursor::AbstractString="0"; match::AbstractString="")
    cnt = 2 + (match !== "" ? 2 : 0)
    command = "*$cnt\r\n\$4\r\nSCAN\r\n\$$(sizeof(cursor))\r\n$cursor\r\n"
    if !isempty(match)
        command *= "\$5\r\nMATCH\r\n\$$(sizeof(match))\r\n$match\r\n"
    end
    return Command{Any}(command)
end

function zrange(key::AbstractString, min::AbstractString, max::AbstractString, bylex::Bool=false, useLegacyCommand::Bool=true)
    if useLegacyCommand
        if bylex
            command = "*4\r\n\$11\r\nZRANGEBYLEX\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(min))\r\n$min\r\n\$$(sizeof(max))\r\n$max\r\n"
        else
            command = "*4\r\n\$14\r\nZRANGEBYSCORE\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(min))\r\n$min\r\n\$$(sizeof(max))\r\n$max\r\n"
        end
    else
        if bylex
            command = "*5\r\n\$6\r\nZRANGE\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(min))\r\n$min\r\n\$$(sizeof(max))\r\n$max\r\n\$5\r\nBYLEX\r\n"
        else
            command = "*4\r\n\$6\r\nZRANGE\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(min))\r\n$min\r\n\$$(sizeof(max))\r\n$max\r\n"
        end
    end
    return Command{Vector{String}}(command)
end

function zadd(key::AbstractString, score::AbstractString, member::AbstractString)
    command = "*4\r\n\$4\r\nZADD\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(score))\r\n$score\r\n\$$(sizeof(member))\r\n$member\r\n"
    return Command{Int}(command)
end

function zcard(key::AbstractString)
    command = "*2\r\n\$5\r\nZCARD\r\n\$$(sizeof(key))\r\n$key\r\n"
    return Command{Int}(command)
end

function zrem(key::AbstractString, member::AbstractString)
    command = "*3\r\n\$4\r\nZREM\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(member))\r\n$member\r\n"
    return Command{Int}(command)
end

function zrevrangebyscore(key::AbstractString, max::AbstractString, min::AbstractString; limit_start::Int=0, limit_count::Int=0)
    narg = 4 + (limit_count > 0 ? 3 : 0)
    command = "*$narg\r\n\$16\r\nZREVRANGEBYSCORE\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(max))\r\n$max\r\n\$$(sizeof(min))\r\n$min\r\n"
    if limit_count > 0
        command *= "\$5\r\nLIMIT\r\n\$$(length(string(limit_start)))\r\n$limit_start\r\n\$$(length(string(limit_count)))\r\n$limit_count\r\n"
    end
    return Command{Vector{String}}(command)
end

function zremrangebyrank(key::AbstractString, start::Int, stop::Int)
    command = "*4\r\n\$15\r\nZREMRANGEBYRANK\r\n\$$(sizeof(key))\r\n$key\r\n\$$(length(string(start)))\r\n$start\r\n\$$(length(string(stop)))\r\n$stop\r\n"
    return Command{Int}(command)
end

function zrevrange(key::AbstractString, start::Int, stop::Int; withscores::Bool=false)
    nargs = 4 + withscores
    command = "*$nargs\r\n\$9\r\nZREVRANGE\r\n\$$(sizeof(key))\r\n$key\r\n\$$(length(string(start)))\r\n$start\r\n\$$(length(string(stop)))\r\n$stop\r\n"
    if withscores
        command *= "\$10\r\nWITHSCORES\r\n"
    end
    return Command{Vector{String}}(command)
end

function zscan(key::AbstractString, cursor::AbstractString="0"; match::AbstractString="")
    command = "*3\r\n\$5\r\nZSCAN\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(cursor))\r\n$cursor\r\n"
    if !isempty(match)
        command *= "\$5\r\nMATCH\r\n\$$(sizeof(match))\r\n$match\r\n"
    end
    return Command{Any}(command)
end

function xadd(key::AbstractString, id::AbstractString, field::AbstractString, value::AbstractString)
    command = "*5\r\n\$4\r\nXADD\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(id))\r\n$id\r\n\$$(sizeof(field))\r\n$field\r\n\$$(sizeof(value))\r\n$value\r\n"
    return Command{String}(command)
end

function xadd(key::AbstractString, id::AbstractString, field::AbstractString, value::AbstractString, maxlen::Int)
    command = "*8\r\n\$4\r\nXADD\r\n\$$(sizeof(key))\r\n$key\r\n\$6\r\nMAXLEN\r\n\$1\r\n~\r\n\$$(length(string(maxlen)))\r\n$maxlen\r\n\$$(sizeof(id))\r\n$id\r\n\$$(sizeof(field))\r\n$field\r\n\$$(sizeof(value))\r\n$value\r\n"
    return Command{String}(command)
end

function xdel(key::AbstractString, id::AbstractString)
    command = "*3\r\n\$4\r\nXDEL\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(id))\r\n$id\r\n"
    return Command{Int}(command)
end

function xrange(key::AbstractString, start::AbstractString, stop::AbstractString)
    command = "*4\r\n\$6\r\nXRANGE\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(start))\r\n$start\r\n\$$(sizeof(stop))\r\n$stop\r\n"
    return Command{Any}(command)
end

function xtrim(key::AbstractString, maxlen::Int, approximate::Bool=false)
    if approximate
        command = "*5\r\n\$5\r\nXTRIM\r\n\$$(sizeof(key))\r\n$key\r\n\$6\r\nMAXLEN\r\n\$1\r\n~\r\n\$$(length(string(maxlen)))\r\n$maxlen\r\n"
    else
        command = "*4\r\n\$5\r\nXTRIM\r\n\$$(sizeof(key))\r\n$key\r\n\$6\r\nMAXLEN\r\n\$$(length(string(maxlen)))\r\n$maxlen\r\n"
    end
    return Command{Int}(command)
end

# Stream Consumer Group Commands

function xgroup_create(stream::AbstractString, group::AbstractString, id::AbstractString="\$"; mkstream::Bool=false)
    nargs = 5 + (mkstream ? 1 : 0)
    command = "*$nargs\r\n\$6\r\nXGROUP\r\n\$6\r\nCREATE\r\n\$$(sizeof(stream))\r\n$stream\r\n\$$(sizeof(group))\r\n$group\r\n\$$(sizeof(id))\r\n$id\r\n"
    if mkstream
        command *= "\$8\r\nMKSTREAM\r\n"
    end
    return Command{String}(command)
end

function xgroup_destroy(stream::AbstractString, group::AbstractString)
    command = "*4\r\n\$6\r\nXGROUP\r\n\$7\r\nDESTROY\r\n\$$(sizeof(stream))\r\n$stream\r\n\$$(sizeof(group))\r\n$group\r\n"
    return Command{Int}(command)
end

function xgroup_setid(stream::AbstractString, group::AbstractString, id::AbstractString)
    command = "*5\r\n\$6\r\nXGROUP\r\n\$5\r\nSETID\r\n\$$(sizeof(stream))\r\n$stream\r\n\$$(sizeof(group))\r\n$group\r\n\$$(sizeof(id))\r\n$id\r\n"
    return Command{String}(command)
end

function xgroup_delconsumer(stream::AbstractString, group::AbstractString, consumer::AbstractString)
    command = "*5\r\n\$6\r\nXGROUP\r\n\$11\r\nDELCONSUMER\r\n\$$(sizeof(stream))\r\n$stream\r\n\$$(sizeof(group))\r\n$group\r\n\$$(sizeof(consumer))\r\n$consumer\r\n"
    return Command{Int}(command)
end

function xreadgroup(group::AbstractString, consumer::AbstractString, streams::Pair{String,String}...; count::Union{Int,Nothing}=nothing, block::Union{Int,Nothing}=nothing, noack::Bool=false)
    nargs = 5 + (count !== nothing ? 2 : 0) + (block !== nothing ? 2 : 0) + (noack ? 1 : 0) + length(streams) * 2
    command = "*$nargs\r\n\$10\r\nXREADGROUP\r\n\$5\r\nGROUP\r\n\$$(sizeof(group))\r\n$group\r\n\$$(sizeof(consumer))\r\n$consumer\r\n"

    if count !== nothing
        command *= "\$5\r\nCOUNT\r\n\$$(length(string(count)))\r\n$count\r\n"
    end
    if block !== nothing
        command *= "\$5\r\nBLOCK\r\n\$$(length(string(block)))\r\n$block\r\n"
    end
    if noack
        command *= "\$5\r\nNOACK\r\n"
    end

    command *= "\$7\r\nSTREAMS\r\n"
    for (stream, id) in streams
        command *= "\$$(sizeof(stream))\r\n$stream\r\n"
    end
    for (stream, id) in streams
        command *= "\$$(sizeof(id))\r\n$id\r\n"
    end

    return Command{Any}(command)
end

function xack(stream::AbstractString, group::AbstractString, ids::AbstractString...)
    nargs = 3 + length(ids)
    command = "*$nargs\r\n\$4\r\nXACK\r\n\$$(sizeof(stream))\r\n$stream\r\n\$$(sizeof(group))\r\n$group\r\n"
    for id in ids
        command *= "\$$(sizeof(id))\r\n$id\r\n"
    end
    return Command{Int}(command)
end

function xpending(stream::AbstractString, group::AbstractString; idle::Union{Int,Nothing}=nothing, start::AbstractString="-", end_::AbstractString="+", count::Union{Int,Nothing}=nothing, consumer::Union{String,Nothing}=nothing)
    if count === nothing
        # Summary form
        command = "*3\r\n\$8\r\nXPENDING\r\n\$$(sizeof(stream))\r\n$stream\r\n\$$(sizeof(group))\r\n$group\r\n"
        return Command{Any}(command)
    else
        # Extended form
        nargs = 6 + (idle !== nothing ? 2 : 0) + (consumer !== nothing ? 1 : 0)
        command = "*$nargs\r\n\$8\r\nXPENDING\r\n\$$(sizeof(stream))\r\n$stream\r\n\$$(sizeof(group))\r\n$group\r\n"

        if idle !== nothing
            command *= "\$4\r\nIDLE\r\n\$$(length(string(idle)))\r\n$idle\r\n"
        end

        command *= "\$$(sizeof(start))\r\n$start\r\n\$$(sizeof(end_))\r\n$end_\r\n\$$(length(string(count)))\r\n$count\r\n"

        if consumer !== nothing
            command *= "\$$(sizeof(consumer))\r\n$consumer\r\n"
        end

        return Command{Any}(command)
    end
end

function xclaim(stream::AbstractString, group::AbstractString, consumer::AbstractString, min_idle_time::Int, ids::AbstractString...; idle::Union{Int,Nothing}=nothing, time::Union{Int,Nothing}=nothing, retrycount::Union{Int,Nothing}=nothing, force::Bool=false, justid::Bool=false)
    nargs = 5 + length(ids) + (idle !== nothing ? 2 : 0) + (time !== nothing ? 2 : 0) + (retrycount !== nothing ? 2 : 0) + (force ? 1 : 0) + (justid ? 1 : 0)
    command = "*$nargs\r\n\$6\r\nXCLAIM\r\n\$$(sizeof(stream))\r\n$stream\r\n\$$(sizeof(group))\r\n$group\r\n\$$(sizeof(consumer))\r\n$consumer\r\n\$$(length(string(min_idle_time)))\r\n$min_idle_time\r\n"

    for id in ids
        command *= "\$$(sizeof(id))\r\n$id\r\n"
    end

    if idle !== nothing
        command *= "\$4\r\nIDLE\r\n\$$(length(string(idle)))\r\n$idle\r\n"
    end
    if time !== nothing
        command *= "\$4\r\nTIME\r\n\$$(length(string(time)))\r\n$time\r\n"
    end
    if retrycount !== nothing
        command *= "\$10\r\nRETRYCOUNT\r\n\$$(length(string(retrycount)))\r\n$retrycount\r\n"
    end
    if force
        command *= "\$5\r\nFORCE\r\n"
    end
    if justid
        command *= "\$6\r\nJUSTID\r\n"
    end

    return Command{Any}(command)
end

function xinfo_stream(stream::AbstractString)
    command = "*3\r\n\$5\r\nXINFO\r\n\$6\r\nSTREAM\r\n\$$(sizeof(stream))\r\n$stream\r\n"
    return Command{Any}(command)
end

function xinfo_groups(stream::AbstractString)
    command = "*3\r\n\$5\r\nXINFO\r\n\$6\r\nGROUPS\r\n\$$(sizeof(stream))\r\n$stream\r\n"
    return Command{Any}(command)
end

function xinfo_consumers(stream::AbstractString, group::AbstractString)
    command = "*4\r\n\$5\r\nXINFO\r\n\$9\r\nCONSUMERS\r\n\$$(sizeof(stream))\r\n$stream\r\n\$$(sizeof(group))\r\n$group\r\n"
    return Command{Any}(command)
end

function sadd(key::AbstractString, member::AbstractString)
    command = "*3\r\n\$4\r\nSADD\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(member))\r\n$member\r\n"
    return Command{Int}(command)
end

function sismember(key::AbstractString, member::AbstractString)
    command = "*3\r\n\$9\r\nSISMEMBER\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(member))\r\n$member\r\n"
    return Command{Int}(command)
end

function srem(key::AbstractString, member::AbstractString)
    command = "*3\r\n\$4\r\nSREM\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(member))\r\n$member\r\n"
    return Command{Int}(command)
end

function scard(key::AbstractString)
    command = "*2\r\n\$5\r\nSCARD\r\n\$$(sizeof(key))\r\n$key\r\n"
    return Command{Int}(command)
end

function sscan(key::AbstractString, cursor::AbstractString="0"; match::AbstractString="")
    command = "*3\r\n\$5\r\nSSCAN\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(cursor))\r\n$cursor\r\n"
    if !isempty(match)
        command *= "\$5\r\nMATCH\r\n\$$(sizeof(match))\r\n$match\r\n"
    end
    return Command{Any}(command)
end

function smembers(key::AbstractString)
    command = "*2\r\n\$8\r\nSMEMBERS\r\n\$$(sizeof(key))\r\n$key\r\n"
    return Command{Vector{String}}(command)
end

function multi()
    command = "*1\r\n\$5\r\nMULTI\r\n"
    return Command{String}(command)
end

function exec()
    command = "*1\r\n\$4\r\nEXEC\r\n"
    return Command{Vector{Any}}(command)
end

function discard()
    command = "*1\r\n\$7\r\nDISCARD\r\n"
    return Command{String}(command)
end

function expire(key::AbstractString, seconds::Int)
    command = "*3\r\n\$6\r\nEXPIRE\r\n\$$(sizeof(key))\r\n$key\r\n\$$(length(string(seconds)))\r\n$seconds\r\n"
    return Command{Int}(command)
end

function publish(channel::AbstractString, message::AbstractString)
    command = "*3\r\n\$7\r\nPUBLISH\r\n\$$(sizeof(channel))\r\n$channel\r\n\$$(sizeof(message))\r\n$message\r\n"
    return Command{Int}(command)
end

function hdel(key::AbstractString, field::AbstractString)
    command = "*3\r\n\$4\r\nHDEL\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(field))\r\n$field\r\n"
    return Command{Int}(command)
end

function hget(key::AbstractString, field::AbstractString)
    command = "*3\r\n\$4\r\nHGET\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(field))\r\n$field\r\n"
    return Command{Union{String,Nothing}}(command)
end

function hlen(key::AbstractString)
    command = "*2\r\n\$4\r\nHLEN\r\n\$$(sizeof(key))\r\n$key\r\n"
    return Command{Int}(command)
end

function hset(key::AbstractString, field::AbstractString, value::AbstractString)
    command = "*4\r\n\$4\r\nHSET\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(field))\r\n$field\r\n\$$(sizeof(value))\r\n$value\r\n"
    return Command{Int}(command)
end

function hscan(key::AbstractString, cursor::Int64; match::Union{Nothing, AbstractString}=nothing)
    command = "*3\r\n\$5\r\nHSCAN\r\n\$$(sizeof(key))\r\n$key\r\n\$$(length(string(cursor)))\r\n$cursor\r\n"
    if match !== nothing
        command *= "\$5\r\nMATCH\r\n\$$(sizeof(match))\r\n$match\r\n"
    end
    return Command{Any}(command)
end

function rpush(key::AbstractString, value::AbstractString)
    command = "*3\r\n\$5\r\nRPUSH\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(value))\r\n$value\r\n"
    return Command{Int}(command)
end

function lindex(key::AbstractString, index::Int64)
    command = "*3\r\n\$6\r\nLINDEX\r\n\$$(sizeof(key))\r\n$key\r\n\$$(length(string(index)))\r\n$index\r\n"
    return Command{Union{String,Nothing}}(command)
end

function lpush(key::AbstractString, value::AbstractString)
    command = "*3\r\n\$5\r\nLPUSH\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(value))\r\n$value\r\n"
    return Command{Int}(command)
end

function ltrim(key::AbstractString, start::Int64, stop::Int64)
    command = "*4\r\n\$5\r\nLTRIM\r\n\$$(sizeof(key))\r\n$key\r\n\$$(length(string(start)))\r\n$start\r\n\$$(length(string(stop)))\r\n$stop\r\n"
    return Command{String}(command)
end

function lrange(key::AbstractString, start::Int64, stop::Int64)
    command = "*4\r\n\$6\r\nLRANGE\r\n\$$(sizeof(key))\r\n$key\r\n\$$(length(string(start)))\r\n$start\r\n\$$(length(string(stop)))\r\n$stop\r\n"
    return Command{Vector{String}}(command)
end

function lrem(key::AbstractString, count::Int64, value::AbstractString)
    command = "*4\r\n\$4\r\nLREM\r\n\$$(sizeof(key))\r\n$key\r\n\$$(length(string(count)))\r\n$count\r\n\$$(sizeof(value))\r\n$value\r\n"
    return Command{Int}(command)
end

function geoadd(key::AbstractString, longitude::AbstractString, latitude::AbstractString, member::AbstractString)
    command = "*5\r\n\$6\r\nGEOADD\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(longitude))\r\n$longitude\r\n\$$(sizeof(latitude))\r\n$latitude\r\n\$$(sizeof(member))\r\n$member\r\n"
    return Command{Int}(command)
end

function geodist(key::AbstractString, member1::AbstractString, member2::AbstractString, unit::AbstractString="m")
    command = "*5\r\n\$7\r\nGEODIST\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(member1))\r\n$member1\r\n\$$(sizeof(member2))\r\n$member2\r\n\$$(sizeof(unit))\r\n$unit\r\n"
    return Command{String}(command)
end

function geohash(key::AbstractString, members::AbstractString...)
    n_args = 2 + length(members)
    command = "*$n_args\r\n\$7\r\nGEOHASH\r\n\$$(sizeof(key))\r\n$key\r\n"
    for member in members
        command *= "\$$(sizeof(member))\r\n$member\r\n"
    end
    return Command{Any}(command)
end

function geopos(key::AbstractString, members::AbstractString...)
    n_args = 2 + length(members)
    command = "*$n_args\r\n\$6\r\nGEOPOS\r\n\$$(sizeof(key))\r\n$key\r\n"
    for member in members
        command *= "\$$(sizeof(member))\r\n$member\r\n"
    end
    return Command{Any}(command)
end

function geosearch(key::AbstractString, longitude::AbstractString, latitude::AbstractString, radius::AbstractString, unit::AbstractString)
    command = "*8\r\n\$9\r\nGEOSEARCH\r\n\$$(sizeof(key))\r\n$key\r\n\$10\r\nFROMLONLAT\r\n\$$(sizeof(longitude))\r\n$longitude\r\n\$$(sizeof(latitude))\r\n$latitude\r\n\$8\r\nBYRADIUS\r\n\$$(sizeof(radius))\r\n$radius\r\n\$$(sizeof(unit))\r\n$unit\r\n"
    return Command{Vector{String}}(command)
end

function georadius(key::AbstractString, longitude::AbstractString, latitude::AbstractString, radius::AbstractString, unit::AbstractString; asc::Bool=true, withcoord::Bool=false, withdist::Bool=false, withhash::Bool=false, count::Union{Nothing, Int}=nothing, store::Union{Nothing, String}=nothing, storedist::Union{Nothing, String}=nothing)
    n_args = 7
    if withcoord
        n_args += 1
    end
    if withdist
        n_args += 1
    end
    if withhash
        n_args += 1
    end
    if count !== nothing
        n_args += 2
    end
    if store !== nothing
        n_args += 2
    end
    if storedist !== nothing
        n_args += 2
    end
    command = "*$n_args\r\n\$9\r\nGEORADIUS\r\n\$$(sizeof(key))\r\n$key\r\n\$$(sizeof(longitude))\r\n$longitude\r\n\$$(sizeof(latitude))\r\n$latitude\r\n\$$(sizeof(radius))\r\n$radius\r\n\$$(sizeof(unit))\r\n$unit\r\n"
    if asc
        command *= "\$3\r\nASC\r\n"
    else
        command *= "\$4\r\nDESC\r\n"
    end
    if withcoord
        command *= "\$9\r\nWITHCOORD\r\n"
    end
    if withdist
        command *= "\$8\r\nWITHDIST\r\n"
    end
    if withhash
        command *= "\$8\r\nWITHHASH\r\n"
    end
    if count !== nothing
        command *= "\$5\r\nCOUNT\r\n\$$(length(string(count)))\r\n$count\r\n"
    end
    if store !== nothing
        command *= "\$5\r\nSTORE\r\n\$$(length(store))\r\n$store\r\n"
    end
    if storedist !== nothing
        command *= "\$9\r\nSTOREDIST\r\n\$$(length(storedist))\r\n$storedist\r\n"
    end
    return Command{Any}(command)
end

end # module Commands
