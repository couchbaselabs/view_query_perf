-module(view_query_perf).

-export([main/1]).

% TODOs
%
% 1) All the following constants should be configurable, via cli parameters and/or
%    some configuration file (json, erlang terms).
% 2) Ability to query N views/design documents.

-define(NUM_WORKERS, 50).
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


usage() ->
    io:format("Usage:~n~n", []),
    io:format("    ~s [options]~n~n", [escript:script_name()]),
    io:format("Available options are:~n~n", []),
    io:format("    --host Host              "
              "Host to connect to, defaults to ~s.~n",
              [?HOST]),
    io:format("    --port Port              "
              "Port to connect to, defaults to ~p.~n",
              [?PORT]),
    io:format("    --queries N              "
              "Number of consecutive queries each worker process does. Defaults to ~p.~n",
              [?QUERIES_PER_WORKER]),
    io:format("    --workers N              "
              "Number of worker processes to use. Defaults to ~p.~n",
              [?NUM_WORKERS]),
    io:format("    --output-times FileName  "
              "Output response times to a file.~n"
              "                             "
              "Useful to use with the ministat program (FreeBSD, Minix3).~n"
              "                             "
              "Mac OS X version of ministat at: git://github.com/codahale/ministat.git~n",
              []),
    io:format("~n", []),
    halt(1).


main(ArgsList) ->
    ok = inets:start(),
    Options = parse_options(ArgsList),
    Host = proplists:get_value(host, Options, ?HOST),
    Port = proplists:get_value(port, Options, ?PORT),
    NumWorkers = proplists:get_value(workers, Options, ?NUM_WORKERS),
    QueriesPerWorker = proplists:get_value(queries, Options, ?QUERIES_PER_WORKER),
    Url = query_url(Host, Port, ?BUCKET_NAME, ?DDOC_ID, ?VIEW_NAME, ?QUERY_PARAMS),
    io:format("Spawning ~p workers, each will perform ~p view queries~n",
              [NumWorkers, QueriesPerWorker]),
    io:format("View query URL is:  ~s~n", [Url]),
    WorkerPids = lists:map(
        fun(_) ->
            spawn_monitor(fun() ->
                worker_loop(Url, #stats{}, QueriesPerWorker)
            end)
        end,
        lists:seq(1, NumWorkers)),
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
    io:format("~n", []),
    case proplists:get_value(times_file, Options) of
    undefined ->
        ok;
    FileName ->
        io:format("Saving query response times to file ~s~n", [FileName]),
        case output_times(StatsList, FileName) of
        ok ->
            ok;
        {error, Reason} ->
            io:format(standard_error, "Error writing to file ~s: ~s~n",
                      [FileName, file:format_error(Reason)])
        end
    end,
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
    Error ->
        io:format(standard_error, "View query response error: ~p~n", [Error]),
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


output_times(StatsList, FileName) ->
    case file:open(FileName, [write, raw, binary]) of
    {ok, Fd} ->
        lists:foreach(
            fun(#stats{resp_times = Times}) ->
                lists:foreach(
                    fun(T) ->
                        ok = file:write(Fd, io_lib:format("~p~n", [T]))
                    end,
                    Times)
            end,
            StatsList),
        ok = file:close(Fd);
    Error ->
        Error
    end.


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


parse_options(ArgsList) ->
    parse_options(ArgsList, []).

parse_options([], Acc) ->
    Acc;
parse_options(["-h" | _Rest], _Acc) ->
    usage();
parse_options(["--help" | _Rest], _Acc) ->
    usage();
parse_options(["--host" | Rest], Acc) ->
    case Rest of
    [] ->
        io:format(standard_error, "Missing argument for option --host~n", []),
        halt(1);
    [Host | Rest2] ->
        parse_options(Rest2, [{host, Host} | Acc])
    end;
parse_options(["--port" = Opt | Rest], Acc) ->
    {Port, Rest2} = parse_int_param(Opt, Rest),
    parse_options(Rest2, [{port, Port} | Acc]);
parse_options(["--workers" = Opt | Rest], Acc) ->
    {Workers, Rest2} = parse_int_param(Opt, Rest),
    parse_options(Rest2, [{workers, Workers} | Acc]);
parse_options(["--queries" = Opt | Rest], Acc) ->
    {Queries, Rest2} = parse_int_param(Opt, Rest),
    parse_options(Rest2, [{queries, Queries} | Acc]);
parse_options(["--output-times" | Rest], Acc) ->
    case Rest of
    [] ->
        io:format(standard_error, "Missing argument for option --output-times~n", []),
        halt(1);
    [FileName | Rest2] ->
        parse_options(Rest2, [{times_file, FileName} | Acc])
    end;
parse_options([Opt | _Rest], _Acc) ->
    io:format(standard_error, "Unrecognized argument/option: ~s~n", [Opt]),
    halt(1).


parse_int_param(Opt, []) ->
    io:format(standard_error, "Error: integer value missing for the option ~s.~n", [Opt]),
    halt(1);
parse_int_param(_Opt, [Int | RestArgs]) ->
    {list_to_integer(Int), RestArgs}.
