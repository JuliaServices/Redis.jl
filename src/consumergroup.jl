# Redis Streams Consumer Group Support

"""
    StreamEvent

Represents a single event from a Redis stream.

# Fields
- `stream::String`: The stream name this event came from
- `id::String`: The unique ID of this event (timestamp-sequence format)
- `fields::Dict{String,Any}`: The key-value pairs of the event data
"""
struct StreamEvent
    stream::String
    id::String
    fields::Dict{String,Any}
end

"""
    ConsumerGroupConfig

Configuration for consumer group behavior.

# Fields
- `start_id::String = "0"`: Starting position for new groups ("0" from beginning, "\$" from end)
- `block_time::Int = 1000`: Milliseconds to block when waiting for new messages
- `batch_size::Int = 10`: Maximum number of messages to read in one operation
- `claim_min_idle_time::Int = 60000`: Milliseconds before a message can be claimed by another consumer
- `max_retries::Int = 3`: Maximum number of processing attempts before giving up
- `auto_ack::Bool = true`: Whether to automatically acknowledge successfully processed messages
"""
Base.@kwdef struct ConsumerGroupConfig
    start_id::String = "0"
    block_time::Int = 1000
    batch_size::Int = 10
    claim_min_idle_time::Int = 60000  # 1 minute
    max_retries::Int = 3
    auto_ack::Bool = true
end

"""
    ConsumerStats

Track consumer group statistics.
"""
mutable struct ConsumerStats
    @atomic messages_read::Int64
    @atomic messages_processed::Int64
    @atomic messages_failed::Int64
    @atomic messages_retried::Int64
    @atomic last_error::Union{Nothing,Exception}
    @atomic last_error_time::Union{Nothing,DateTime}
    ConsumerStats() = new(0,0,0,0,nothing,nothing)
end

"""
    ConsumerGroup

High-level abstraction for Redis streams consumer group operations.
Immutable configuration for a consumer group.

# Fields
- `client::Client`: Redis client connection
- `name::String`: Consumer group name
- `streams::Vector{String}`: Streams to consume from
- `consumer_name::String`: This consumer's unique name within the group
- `config::ConsumerGroupConfig`: Configuration parameters
- `stream_args::Vector{Pair{String,String}}`: Pre-computed stream arguments for xreadgroup
- `pending_buffer::Vector{Tuple{String,Any}}`: Reusable buffer for pending operations
"""
struct ConsumerGroup
    client::Client
    name::String
    streams::Vector{String}
    consumer_name::String
    config::ConsumerGroupConfig
    stream_args::Vector{Pair{String,String}}  # Pre-computed for read_events
    pending_buffer::Vector{Tuple{String,Any}}  # Reusable buffer for pending operations
    
    function ConsumerGroup(
        client::Client,
        name::String,
        streams::Union{String, Vector{String}},
        consumer_name::String=generate_consumer_name();
        config::ConsumerGroupConfig=ConsumerGroupConfig()
    )
        streams_vec = streams isa String ? [streams] : streams
        # Pre-compute stream arguments for read operations
        stream_args = [str => ">" for str in streams_vec]
        pending_buffer = Tuple{String,Any}[]
        group = new(client, name, streams_vec, consumer_name, config, stream_args, pending_buffer)
        @assert create!(group)
        return group
    end
end

"""
    StreamConsumer

Mutable wrapper for a ConsumerGroup with runtime state and statistics.
Represents an active consumer instance that can be started and stopped.

# Fields
- `group::ConsumerGroup`: The immutable consumer group configuration
- `state::Symbol`: Current state (:created, :running, :closed)
- `stats::ConsumerStats`: Runtime statistics
- `event_buffer::Vector{StreamEvent}`: Reusable buffer for reading events
"""
mutable struct StreamConsumer
    group::ConsumerGroup
    @atomic state::Symbol # :created, :running, :closed
    stats::ConsumerStats
    event_buffer::Vector{StreamEvent}  # Reusable buffer for events

    function StreamConsumer(group::ConsumerGroup)
        new(group, :created, ConsumerStats(), Vector{StreamEvent}())
    end
end

function generate_consumer_name()
    hostname = gethostname()
    pid = getpid()
    return "$(hostname)-$(pid)-$(Dates.now(UTC))"
end

"""
    create!(group::ConsumerGroup) -> Bool

Create the consumer group on Redis server. Safe to call multiple times.
Returns true if successful, false otherwise.
"""
function create!(group::ConsumerGroup)
    success = true
    for stream in group.streams
        try
            # Try to create the group
            xgroup_create(group.client, stream, group.name, group.config.start_id; mkstream=true)
            @info "Created consumer group $(group.name) for stream $(stream)"
        catch e
            if contains(string(e), "BUSYGROUP")
                # Group already exists, that's fine
                @debug "Consumer group $(group.name) already exists for stream $(stream)"
            else
                @error "Failed to create consumer group" group=group.name stream=stream exception=e
                success = false
            end
        end
    end
    return success
end

"""
    destroy!(group::ConsumerGroup) -> Bool

Destroy the consumer group on Redis server.
"""
function destroy!(group::ConsumerGroup)
    success = true
    for stream in group.streams
        try
            xgroup_destroy(group.client, stream, group.name)
            @info "Destroyed consumer group $(group.name) for stream $(stream)"
        catch e
            @error "Failed to destroy consumer group" group=group.name stream=stream exception=e
            success = false
        end
    end
    return success
end

"""
    info(group::ConsumerGroup) -> Vector{Tuple{String, Any}}

Get detailed information about the consumer group for each stream.
"""
function info(group::ConsumerGroup)
    info_list = []
    for stream in group.streams
        try
            result = xinfo_groups(group.client, stream)
            for group_info in result
                if isa(group_info, Vector) && Base.get(group_info, 2, "") == group.name
                    # Convert the flat array to a dict for easier access
                    info_dict = Dict{String,Any}()
                    for i in 1:2:length(group_info)
                        info_dict[string(group_info[i])] = group_info[i+1]
                    end
                    push!(info_list, (stream, info_dict))
                end
            end
        catch e
            @error "Failed to get group info" group=group.name stream=stream exception=e
        end
    end
    return info_list
end

"""
    read_events!(consumer::StreamConsumer; start_id=">", retry_on_error=true) -> Vector{StreamEvent}

Read events from streams using consumer group, reusing the consumer's event buffer.

# Arguments
- `start_id`: Position to read from (">" for new messages, "0" for pending)
- `retry_on_error`: Whether to retry on connection errors
"""
function read_events!(consumer::StreamConsumer; start_id=">", retry_on_error=true)
    group = consumer.group
    # Use pre-computed stream_args if start_id is ">" (the common case)
    stream_args = if start_id == ">"
        group.stream_args
    else
        # Create custom args for non-standard start_id
        [str => start_id for str in group.streams]
    end
    max_attempts = retry_on_error ? 3 : 1
    for attempt in 1:max_attempts
        try
            result = xreadgroup(group.client,
                group.name,
                group.consumer_name,
                stream_args...;
                count=group.config.batch_size, 
                block=group.config.block_time
            )
            parse_stream_results!(consumer.event_buffer, result)
            return consumer.event_buffer
        catch e
            if attempt < max_attempts
                @warn "Failed to read events, retrying..." attempt=attempt exception=e
                sleep(0.1 * attempt)  # Exponential backoff
            else
                @error "Failed to read events after $(max_attempts) attempts" exception=e
                empty!(consumer.event_buffer)
                return consumer.event_buffer
            end
        end
    end
    empty!(consumer.event_buffer)
    return consumer.event_buffer
end

"""
    parse_stream_results!(event_buffer::Vector{StreamEvent}, result) -> Vector{StreamEvent}

Parse Redis stream results into the provided buffer, reusing existing allocation.
Returns the same buffer for convenience.
"""
function parse_stream_results!(event_buffer::Vector{StreamEvent}, result)
    empty!(event_buffer)
    (result === nothing || !isa(result, Vector)) && return event_buffer
    for stream_data in result
        (!isa(stream_data, Vector) || length(stream_data) < 2) && continue
        stream_name = string(stream_data[1])
        stream_entries = stream_data[2]
        !isa(stream_entries, Vector) && continue
        for entry in stream_entries
            (!isa(entry, Vector) || length(entry) < 2) && continue
            event_id = string(entry[1])
            fields_array = entry[2]
            # Convert fields array to dictionary
            fields = parse_fields_array(fields_array)
            push!(event_buffer, StreamEvent(stream_name, event_id, fields))
        end
    end
    return event_buffer
end

function parse_fields_array(fields_array)
    fields = Dict{String,Any}()
    if isa(fields_array, Vector) && length(fields_array) % 2 == 0
        for i in 1:2:length(fields_array)
            fields[string(fields_array[i])] = fields_array[i+1]
        end
    end
    return fields
end

"""
    acknowledge!(group::ConsumerGroup, events::Vector{StreamEvent}) -> Bool

Acknowledge processing of events. Must be called to remove events from pending list.
Optimized to avoid allocations by processing stream by stream.
"""
function acknowledge!(group::ConsumerGroup, events::Union{StreamEvent,Vector{StreamEvent}})
    events_vec = events isa StreamEvent ? [events] : events
    isempty(events_vec) && return true
    success = true
    # Process each stream separately to avoid Dict allocation
    for stream in group.streams
        # Use lazy filtering to get IDs for this stream
        stream_ids = collect(event.id for event in events_vec if event.stream == stream)
        if !isempty(stream_ids)
            try
                xack(group.client, stream, group.name, stream_ids...)
                @debug "Acknowledged $(length(stream_ids)) messages from stream $(stream)"
            catch e
                @error "Failed to acknowledge events" stream=stream count=length(stream_ids) exception=e
                success = false
            end
        end
    end
    return success
end

"""
    PendingSummaryIterator

Iterator for pending summary information that avoids allocating arrays.
"""
struct PendingSummaryIterator
    group::ConsumerGroup
end

function Base.iterate(iter::PendingSummaryIterator, state=1)
    state > length(iter.group.streams) && return nothing
    stream = iter.group.streams[state]
    try
        result = xpending(iter.group.client, stream, iter.group.name)
        return ((stream, result), state + 1)
    catch e
        @error "Failed to get pending summary" stream=stream exception=e
        return ((stream, nothing), state + 1)
    end
end

Base.length(iter::PendingSummaryIterator) = length(iter.group.streams)

"""
    pending_summary(group::ConsumerGroup) -> PendingSummaryIterator

Get an iterator over pending summaries for each stream.
Use as: for (stream, summary) in pending_summary(group) ... end
"""
pending_summary(group::ConsumerGroup) = PendingSummaryIterator(group)

"""
    PendingDetailsIterator

Iterator for detailed pending information that avoids allocating arrays.
"""
struct PendingDetailsIterator
    group::ConsumerGroup
    count::Int
end

function Base.iterate(iter::PendingDetailsIterator, state=1)
    state > length(iter.group.streams) && return nothing
    stream = iter.group.streams[state]
    try
        result = xpending(iter.group.client, stream, iter.group.name; 
                         start="-", end_="+", count=iter.count, consumer=iter.group.consumer_name)
        return ((stream, result), state + 1)
    catch e
        @error "Failed to get pending details" stream=stream exception=e
        return ((stream, []), state + 1)
    end
end

Base.length(iter::PendingDetailsIterator) = length(iter.group.streams)

"""
    pending_details(group::ConsumerGroup; count=100) -> PendingDetailsIterator
    
Get an iterator over detailed pending information for each stream.
Use as: for (stream, details) in pending_details(group) ... end
"""
pending_details(group::ConsumerGroup; count=100) = PendingDetailsIterator(group, count)

"""
    claim_idle_messages!(group::ConsumerGroup) -> Vector{StreamEvent}

Claim messages that have been idle longer than the configured threshold.
"""
function claim_idle_messages!(group::ConsumerGroup)
    claimed_events = StreamEvent[]
    for (stream, details) in pending_details(group; count=100)
        isempty(details) && continue
        idle_ids = String[]
        for detail in details
            (!isa(detail, Vector) || length(detail) < 3) && continue
            idle_time = detail[2]  # Milliseconds idle
            idle_time >= group.config.claim_min_idle_time && push!(idle_ids, string(detail[1]))  # Message ID
        end
        if !isempty(idle_ids)
            try
                result = xclaim(
                    group.client, stream, group.name, group.consumer_name,
                    string(group.config.claim_min_idle_time), idle_ids...
                )
                events = parse_claim_results(stream, result)
                append!(claimed_events, events)
                @info "Claimed $(length(events)) idle messages from stream $(stream)"
            catch e
                @error "Failed to claim idle messages" stream=stream count=length(idle_ids) exception=e
            end
        end
    end
    return claimed_events
end

"""
    parse_claim_results(stream::String, result) -> Vector{StreamEvent}

Parse the results from XCLAIM command using shared parsing logic.
"""
function parse_claim_results(stream::String, result)
    events = StreamEvent[]
    !isa(result, Vector) && return events
    for entry in result
        if isa(entry, Vector) && length(entry) >= 2
            event_id = string(entry[1])
            fields_array = entry[2]
            fields = parse_fields_array(fields_array)
            push!(events, StreamEvent(stream, event_id, fields))
        end
    end
    return events
end

"""
    startConsumer!(group::ConsumerGroup, event_handler::Function; 
           error_handler=nothing, before_start=nothing) -> StreamConsumer

Start a consumer for the group in a background task, returning a StreamConsumer instance.

# Arguments
- `event_handler`: Function called for each event. Should return `true` to ACK, `false` to skip ACK, or throw to retry
- `error_handler`: Optional function called on errors with signature `(error, event)`
- `before_start`: Optional function called before starting consumption

# Event Handler
The event handler receives a `StreamEvent` and should:
- Return `true` (or nothing) to acknowledge the message
- Return `false` to skip acknowledgment (message remains pending)
- Throw an exception to trigger retry logic

# Example
```julia
consumer = startConsumer!(group, event -> begin
    println("Processing: ", event.fields)
    # Process the event...
    return true  # Acknowledge
end)
```
"""
function startConsumer!(group::ConsumerGroup, event_handler::Function; isdone=nothing, error_handler=nothing, pending_check_interval::Int=30)
    consumer = StreamConsumer(group)
    @assert consumer.state == :created "StreamConsumer can't start: state = $(consumer.state)"
    @atomic consumer.state = :running
    errormonitor(Threads.@spawn begin
        @info "Starting consumer" group=group.name consumer=group.consumer_name
        last_pending_check = time()
        retry_counts = Dict{String,Int}()  # Track retries per message
        try
            while consumer.state == :running && (isdone === nothing || !isdone())
                try
                    # Check for idle messages periodically
                    if time() - last_pending_check > pending_check_interval
                        claimed = claim_idle_messages!(group)
                        !isempty(claimed) && (@info "Processing $(length(claimed)) claimed messages"; process_events!(consumer, claimed, event_handler, error_handler, retry_counts))
                        last_pending_check = time()
                    end
                    # Read new messages
                    events = read_events!(consumer)
                    !isempty(events) && (
                        @atomic consumer.stats.messages_read += length(events);
                        process_events!(consumer, events, event_handler, error_handler, retry_counts);
                        empty!(consumer.event_buffer)  # Clear the buffer after processing
                    )
                catch read_error
                    if consumer.state == :running
                        @error "Error in consumer loop" exception=(read_error, catch_backtrace())
                        @atomic consumer.stats.last_error = read_error
                        @atomic consumer.stats.last_error_time = now()
                        sleep(1)  # Brief pause before retrying
                    end
                end
            end
        catch task_error
            @error "Consumer task crashed" exception=task_error
        finally
            @atomic consumer.state = :closed
            @info "Consumer stopped" group=group.name consumer=group.consumer_name
        end
    end)
    return consumer
end

startConsumer!(f::Function, group::ConsumerGroup; kw...) = startConsumer!(group, f; kw...)

"""
    process_events!(consumer::StreamConsumer, events::Vector{StreamEvent}, retry_counts::Dict)

Process a batch of events with error handling and retry logic.
"""
function process_events!(consumer::StreamConsumer, events::Vector{StreamEvent}, event_handler, error_handler, retry_counts::Dict)
    group = consumer.group
    to_ack = StreamEvent[]
    for event in events
        message_key = "$(event.stream):$(event.id)"
        retries = Base.get(retry_counts, message_key, 0)
        try
            # Call user's event handler
            result = event_handler(event)
            # Process based on handler result
            if result !== false
                # Success - acknowledge the message
                push!(to_ack, event)
                delete!(retry_counts, message_key)
                @atomic consumer.stats.messages_processed += 1
            else
                # Handler explicitly returned false - don't ACK
                @debug "Skipping acknowledgment for message" id=event.id
            end
        catch handler_error
            @atomic consumer.stats.messages_failed += 1
            # Check retry limit
            if retries >= group.config.max_retries
                @error "Message exceeded retry limit, acknowledging to remove from pending" id=event.id retries=retries
                push!(to_ack, event)  # ACK to remove from pending
                delete!(retry_counts, message_key)
                # Call error handler for final failure
                if error_handler !== nothing
                    try
                        error_handler(handler_error, event)
                    catch
                        # Ignore error handler failures
                    end
                end
            else
                # Increment retry count
                retry_counts[message_key] = retries + 1
                @atomic consumer.stats.messages_retried += 1
                @warn "Error processing message, will retry" id=event.id retries=retries+1 exception=handler_error
                # Call error handler
                if error_handler !== nothing
                    try
                        error_handler(handler_error, event)
                    catch error_handler_error
                        @error "Error in error handler" exception=error_handler_error
                    end
                end
            end
        end
    end
    # Acknowledge successfully processed messages
    !isempty(to_ack) && group.config.auto_ack && acknowledge!(group, to_ack)
end

"""
    stop!(consumer::StreamConsumer; timeout=10.0) -> Bool

Stop the consumer background task gracefully.

# Arguments
- `timeout`: Maximum seconds to wait for the consumer to stop
"""
function stop!(consumer::StreamConsumer)
    state = @atomicswap consumer.state = :closed
    state != :closed && @info "Stopping consumer" group=consumer.group.name consumer=consumer.group.consumer_name
    return
end

"""
    isrunning(consumer::StreamConsumer) -> Bool

Check if the consumer is currently running.
"""
isrunning(consumer::StreamConsumer) = consumer.state == :running

"""
    stats(consumer::StreamConsumer) -> ConsumerStats

Get the current consumer statistics.
"""
stats(consumer::StreamConsumer) = consumer.stats

"""
    reset_stats!(consumer::StreamConsumer)

Reset the consumer statistics.
"""
function reset_stats!(consumer::StreamConsumer)
    s = consumer.stats
    @atomic s.messages_read = 0
    @atomic s.messages_processed = 0
    @atomic s.messages_failed = 0
    @atomic s.messages_retried = 0
    @atomic s.last_error = nothing
    @atomic s.last_error_time = nothing
    return
end
