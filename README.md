# A tool to measure Couchbase Server view query performance

## Compile && run

<pre>
$ make
$ ./run
</pre>


## Configuration

At the moment all parameters (number of workers, queries per worker, server, port, etc)
are defined as constants in the source file src/view_query_perf.erl. You'll have to edit
it and re-run make.


## TODOs

. Allow parameters to be passed through command line flags and/or configuration files;
. Allow workers to query multiple views and design documents;
. Check the correctness of the view query results (at the moment it just checks for HTTP
  status code errors);
. etc.