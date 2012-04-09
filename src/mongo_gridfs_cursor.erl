-module(mongo_gridfs_cursor).

-export([new/6, close/1, next/1]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {write_mode, read_mode, connection, database, bucket, mongo_cursor}).

new(WriteMode, ReadMode, Connection, Database, Bucket, MongoCursor) ->
	{ok, Pid} = gen_server:start_link(?MODULE, [WriteMode, ReadMode, Connection, Database, Bucket, MongoCursor], []),
	Pid.

close(Pid) ->
	gen_server:call(Pid, close, infinity).

next(Pid) ->
	gen_server:call(Pid, next, infinity).


init([WriteMode, ReadMode, Connection, Database, Bucket, MongoCursor]) ->
    {ok, #state{write_mode=WriteMode,
				read_mode=ReadMode,
				connection=Connection,
				database=Database,
				bucket=Bucket,
				mongo_cursor=MongoCursor}}.

handle_call(close, _From, State) ->
	{stop, normal, ok, State};
handle_call(next, _From, State) ->
	case mongo_cursor:next(State#state.mongo_cursor) of
		{} ->
			{reply, {error, "No more files."}, State};
		{{'_id', Id}} ->
			Reply = mongo_gridfs_file:new(State#state.write_mode, 
										  State#state.read_mode, 
										  State#state.connection, 
										  State#state.database, 
										  State#state.bucket, 
										  Id),
			{reply, {ok, Reply}, State}
	end.
	


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
	mongo_cursor:close(State#state.mongo_cursor),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
