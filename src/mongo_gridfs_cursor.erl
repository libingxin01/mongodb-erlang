-module(mongo_gridfs_cursor).

-export([new/6]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {write_mode, read_mode, connection, database, bucket, mongo_cursor}).

new(WriteMode, ReadMode, Connection, Database, Bucket, MongoCursor) ->
	{ok, Pid} = gen_server:start_link(?MODULE, [WriteMode, ReadMode, Connection, Database, Bucket, MongoCursor], []),
	Pid.

init([WriteMode, ReadMode, Connection, Database, Bucket, MongoCursor]) ->
    {ok, #state{write_mode=WriteMode,
				read_mode=ReadMode,
				connection=Connection,
				database=Database,
				bucket=Bucket,
				mongo_cursor=MongoCursor}}.

handle_call(close, _From, State) ->
	{stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
	mongo_cursor:close(State#state.mongo_cursor),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
