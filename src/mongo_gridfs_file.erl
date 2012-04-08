-module(mongo_gridfs_file).

-export([new/6, get_file_size/1]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {write_mode, read_mode, connection, database, bucket, id}).

new(WriteMode, ReadMode, Connection, Database, Bucket, Id) ->
	{ok, Pid} = gen_server:start_link(?MODULE, [WriteMode, ReadMode, Connection, Database, Bucket, Id], []),
	Pid.

-spec(get_file_size(pid()) -> {ok, Size::integer()}).
% Get the size of the file.
get_file_size(Pid) ->
	gen_server:call(Pid, file_size, infinity).

%@doc Initiates the server.
init([WriteMode, ReadMode, Connection, Database, Bucket, Id]) ->
    {ok, #state{write_mode=WriteMode,
				read_mode=ReadMode,
				connection=Connection,
				database=Database,
				bucket=Bucket,
				id=Id}}.

%@doc Responds to synchronous messages.
handle_call(file_size, _From, State) ->
    Reply = file_size(State),
    {reply, Reply, State}.

%@doc Handles asynchronous messages.
handle_cast(_Msg, State) ->
    {noreply, State}.

%@doc Handles out of band messages.
handle_info(_Info, State) ->
    {noreply, State}.

%@doc Cleans up process on completion.
terminate(_Reason, _State) ->
    ok.

%@doc Updates state on code changes.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

file_size(State) ->
	Coll = list_to_atom(atom_to_list(State#state.bucket) ++ ".files"),
	{ok, {{length,FileSize}}} = mongo:do(State#state.write_mode, State#state.read_mode, State#state.connection, 
							  State#state.database, 
							  fun() ->
									  mongo:find_one(Coll, {'_id', State#state.id}, {'_id', 0, length, 1})
							  end),
	{ok, FileSize}.
