-module(view_query_perf).

-export([main/1]).

% TODOs
%
% 1) All the following constants should be configurable, via cli parameters and/or
%    some configuration file (json, erlang terms).
% 2) Ability to query N views/design documents.

-define(NUM_WORKERS, 100).
-define(QUERIES_PER_WORKER, 100).

-define(HOST, "localhost").
-define(PORT, 9500).
-define(BUCKET_NAME, "default").
-define(DDOC_ID, "_design/test").
-define(VIEW_NAME, "view1").
-define(QUERY_PARAMS, [{limit, 10}, {stale, update_after}]).

-record(stats, {
    errors = 0,
    resp_times = []
}).

-record(final_stats, {
    avg_resp_time,
    std_dev_resp_time,
    max_resp_time,
    min_resp_time,
    errors = 0
}).


main(_ArgsList) ->
    ok = inets:start(),
    Url = query_url(?HOST, ?PORT, ?BUCKET_NAME, ?DDOC_ID, ?VIEW_NAME, ?QUERY_PARAMS),
    io:format("Spawning ~p workers, each will perform ~p view queries~n",
              [?NUM_WORKERS, ?QUERIES_PER_WORKER]),
    io:format("View query URL is:  ~s~n", [Url]),
    WorkerPids = lists:map(
        fun(_) ->
            spawn_monitor(fun() ->
                worker_loop(Url, #stats{}, ?QUERIES_PER_WORKER)
            end)
        end,
        lists:seq(1, ?NUM_WORKERS)),
    io:format("~nWaiting for workers to finish...~n~n", []),
    StatsList = lists:foldl(
        fun({Pid, Ref}, Acc) ->
            receive
            {'DOWN', Ref, process, Pid, {ok, Stats}} ->
                [Stats | Acc];
            {'DOWN', Ref, process, Pid, Reason} ->
                io:format(standard_error,
                          "[ERROR] Worker ~p died with reason: ~p~n~n",
                          [Pid, Reason]),
                Acc
            end
        end,
        [], WorkerPids),
    FinalStats = compute_stats(StatsList),
    io:format("~n", []),
    io:format("All workers finished. Final statistics are:~n~n", []),
    io:format("    Average response time:   ~pms~n", [FinalStats#final_stats.avg_resp_time]),
    io:format("    Highest response time:   ~pms~n", [FinalStats#final_stats.max_resp_time]),
    io:format("    Lowest response time:    ~pms~n", [FinalStats#final_stats.min_resp_time]),
    io:format("    Response time std dev:   ~pms~n", [FinalStats#final_stats.std_dev_resp_time]),
    io:format("    # of errors:             ~p~n", [FinalStats#final_stats.errors]),
    io:format("~nBye!~n", []),
    ok.


worker_loop(_ViewUrl, Stats, 0) ->
    exit({ok, Stats});
worker_loop(ViewUrl, Stats, NumQueries) when NumQueries > 0 ->
    T0 = os:timestamp(),
    Resp = httpc:request(get,
                         {ViewUrl, [{"Accept", "application/json"}]},
                         [{connect_timeout, infinity}, {timeout, infinity}],
                         [{sync, true}]),
    T1 = os:timestamp(),
    Time = timer:now_diff(T1, T0) / 1000,
    Stats2 = Stats#stats{
        resp_times = [Time | Stats#stats.resp_times]
    },
    Stats3 = case Resp of
    {ok, {{_, 200, _}, _Headers, _Body}} ->
        % TODO: JSON decode the body and checks if it has an "errors" field
        Stats2;
    _Error ->
        Stats2#stats{
            errors = Stats2#stats.errors + 1
        }
    end,
    worker_loop(ViewUrl, Stats3, NumQueries - 1).


compute_stats(StatsList) ->
    Errors = lists:foldl(
        fun(#stats{errors = E}, Acc) -> E + Acc end, 0, StatsList),
    Max = lists:foldl(
        fun(#stats{resp_times = Times}, Acc) ->
            erlang:max(Acc, lists:max(Times))
        end,
        -999999, StatsList),
    Min = lists:foldl(
        fun(#stats{resp_times = Times}, Acc) ->
            erlang:min(Acc, lists:min(Times))
        end,
        infinity, StatsList),
    {Count, Sum} = lists:foldl(
        fun(#stats{resp_times = Times}, {C, S}) ->
            {C + length(Times), S + lists:sum(Times)}
        end,
        {0, 0}, StatsList),
    Avg = Sum / Count,
    DevSum = lists:foldl(
        fun(#stats{resp_times = Times}, Acc) ->
            lists:foldl(
                fun(X, Acc2) ->
                    Acc2 + math:pow(X - Avg, 2)
                end,
                Acc, Times)
        end,
        0, StatsList),
    Dev = math:sqrt(DevSum / Count),
    #final_stats{
        max_resp_time = Max,
        min_resp_time = Min,
        avg_resp_time = Avg,
        std_dev_resp_time = Dev,
        errors = Errors
    }.


query_url(Host, Port, BucketName, DDocId, ViewName, QueryParams) ->
    Base = "http://" ++ Host ++ ":" ++ integer_to_list(Port) ++ "/" ++
        BucketName ++ "/" ++ DDocId ++ "/_view/" ++ ViewName,
    {Qs, _} = lists:foldl(
        fun({K, V}, {Acc, Sep}) ->
            {Acc ++ [Sep, to_list(K), $=, to_list(V)], "&"}
        end,
        {[], ""},
        QueryParams),
    case Qs of
    [] ->
        Base;
    _ ->
        Base ++ binary_to_list(iolist_to_binary([$?, Qs]))
    end.


to_list(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
to_list(Bin) when is_binary(Bin) ->
    binary_to_list(Bin);
to_list(List) when is_list(List) ->
    List;
to_list(Int) when is_integer(Int) ->
    integer_to_list(Int).
