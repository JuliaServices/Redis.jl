module RedisModelContextProtocolExt

using JSON
using ModelContextProtocol
using Redis

const MCP = ModelContextProtocol

struct RedisMCPSessionStore{F} <: MCP.MCPSessionStore
    with_client::F
    key_prefix::String
    ttl_seconds::Int
end

function RedisMCPSessionStore(with_client::F; key_prefix::AbstractString="mcp:session:", ttl_seconds::Integer=60 * 60) where {F}
    ttl = Int(ttl_seconds)
    ttl > 0 || throw(ArgumentError("ttl_seconds must be positive"))
    return RedisMCPSessionStore{F}(with_client, String(key_prefix), ttl)
end

function Redis.mcp_session_store(with_client::Function; key_prefix::AbstractString="mcp:session:", ttl_seconds::Integer=60 * 60)
    return RedisMCPSessionStore(with_client; key_prefix, ttl_seconds)
end

function Redis.mcp_session_store(client::Redis.Client; key_prefix::AbstractString="mcp:session:", ttl_seconds::Integer=60 * 60)
    return RedisMCPSessionStore(f -> f(client); key_prefix, ttl_seconds)
end

session_key(store::RedisMCPSessionStore, session_id::AbstractString) = string(store.key_prefix, String(session_id))

function with_store_client(f::Function, store::RedisMCPSessionStore)
    return store.with_client(f)
end

function MCP.create_session!(store::RedisMCPSessionStore, session::MCP.MCPSession)
    return MCP.save_session!(store, session)
end

function MCP.find_session(store::RedisMCPSessionStore, session_id)
    session_id === nothing && return nothing
    payload = with_store_client(store) do redis
        return Redis.get(redis, session_key(store, String(session_id)))
    end
    payload === nothing && return nothing
    return MCP.session_from_dict(JSON.parse(String(payload)))
end

function MCP.save_session!(store::RedisMCPSessionStore, session::MCP.MCPSession)
    payload = JSON.json(MCP.session_to_dict(session))
    with_store_client(store) do redis
        Redis.set(redis, session_key(store, session.id), payload; ex=store.ttl_seconds)
        return nothing
    end
    return session
end

function MCP.delete_session!(store::RedisMCPSessionStore, session_id::AbstractString)
    with_store_client(store) do redis
        Redis.del(redis, session_key(store, session_id))
        return nothing
    end
    return nothing
end

function MCP.list_sessions(store::RedisMCPSessionStore)
    return with_store_client(store) do redis
        sessions = MCP.MCPSession[]
        for key in Redis.Scan(redis, string(store.key_prefix, "*"))
            payload = Redis.get(redis, key)
            payload === nothing && continue
            try
                push!(sessions, MCP.session_from_dict(JSON.parse(String(payload))))
            catch e
                @warn "Skipping invalid MCP session payload in Redis" key exception=(e, catch_backtrace())
            end
        end
        return sessions
    end
end

end
