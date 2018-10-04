-module(wolfmq_queues_catalog).

%% API
-export([init/0]).
-export([insert/2, delete/1]).
-export([is_existing/1]).
-export([get_meta/1]).

%% API
init() ->
    _ = ets:new(?MODULE, [public, named_table, {read_concurrency, true}]),
    ok.

insert(ExternalQueueId, {InternalQueueId, WorkerPid}) ->
    Meta = #{
        internal_id => InternalQueueId,
        worker_pid  => WorkerPid
    },
    true = ets:insert(?MODULE, {ExternalQueueId, Meta}),
    ok.

delete(ExternalQueueId) ->
    true = ets:delete(?MODULE, ExternalQueueId),
    ok.

is_existing(ExternalQueueId) ->
    try get_meta(ExternalQueueId) of
        #{worker_pid := WorkerPid} ->
            is_process_alive(WorkerPid)
    catch
        error:badarg -> false;
        C:R -> erlang:C(R)
    end.

get_meta(ExternalQueueId) ->
    ets:lookup_element(?MODULE, ExternalQueueId, 2).
