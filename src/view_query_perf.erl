-module(view_query_perf).

-export([main/1]).

% TODOs
%
% 1) All the following constants should be configurable, via cli parameters and/or
%    some configuration file (json, erlang terms).
% 2) Ability to query N views/design documents.

-define(NUM_WORKERS, 50).
-define(QUERIES_PER_WORKER, 100).
-define(QUERY_URL,
        "http://localhost:9500/default/_design/test/_view/view1?limit=10&stale=update_after").

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
    io:format("    --query-url Url          "
              "URL to query, defaults to ~s.~n",
              [?QUERY_URL]),
    io:format("    --query-body String      "
              "Body of the view query request (a POST request).~n"
              "                             "
              "Defaults to an empty string.~n",
              []),
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
    NumWorkers = proplists:get_value(workers, Options, ?NUM_WORKERS),
    QueriesPerWorker = proplists:get_value(queries, Options, ?QUERIES_PER_WORKER),
    Url = proplists:get_value(query_url, Options, ?QUERY_URL),
    Body = proplists:get_value(query_body, Options, []),
    io:format("Spawning ~p workers, each will perform ~p view queries~n",
              [NumWorkers, QueriesPerWorker]),
    io:format("View query URL is:  ~s~n", [Url]),
    WorkerPids = lists:map(
        fun(_) ->
            spawn_monitor(fun() ->
                worker_loop(Url, Body, #stats{}, QueriesPerWorker)
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


worker_loop(_ViewUrl, _ViewBody, Stats, 0) ->
    exit({ok, Stats});
worker_loop(ViewUrl, ViewBody, Stats, NumQueries) when NumQueries > 0 ->
    T0 = os:timestamp(),
    Resp = case ViewBody of
    [] ->
        httpc:request(get,
                      {ViewUrl, [{"Accept", "application/json"}]},
                      [{connect_timeout, infinity}, {timeout, infinity}],
                      [{sync, true}]);
    _ ->
        httpc:request(post,
                      {ViewUrl, [{"Accept", "application/json"}], "application/json", ViewBody},
                      [{connect_timeout, infinity}, {timeout, infinity}],
                      [{sync, true}])
    end,
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
    worker_loop(ViewUrl, ViewBody, Stats3, NumQueries - 1).


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


parse_options(ArgsList) ->
    parse_options(ArgsList, []).

parse_options([], Acc) ->
    Acc;
parse_options(["-h" | _Rest], _Acc) ->
    usage();
parse_options(["--help" | _Rest], _Acc) ->
    usage();
parse_options(["--query-url" | Rest], Acc) ->
    case Rest of
    [] ->
        io:format(standard_error, "Missing argument for option --query-url~n", []),
        halt(1);
    [Url | Rest2] ->
        parse_options(Rest2, [{query_url, Url} | Acc])
    end;
parse_options(["--query-body" | Rest], Acc) ->
    case Rest of
    [] ->
        io:format(standard_error, "Missing argument for option --query-body~n", []),
        halt(1);
    [Body | Rest2] ->
        parse_options(Rest2, [{query_body, Body} | Acc])
    end;
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
