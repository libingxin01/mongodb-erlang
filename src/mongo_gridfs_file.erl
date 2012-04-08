-module(mongo_gridfs_file).

-export([new/6, close/1, get_file_size/1, pread/3]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {write_mode, read_mode, connection, database, bucket, id}).

new(WriteMode, ReadMode, Connection, Database, Bucket, Id) ->
	{ok, Pid} = gen_server:start_link(?MODULE, [WriteMode, ReadMode, Connection, Database, Bucket, Id], []),
	Pid.

close(Pid) ->
	gen_server:call(Pid, close, infinity).
	
-spec(get_file_size(pid()) -> {ok, Size::integer()}).
% Get the size of the file.
get_file_size(Pid) ->
	gen_server:call(Pid, file_size, infinity).

pread(Pid, Offset, Length) ->
	gen_server:call(Pid, {pread, Offset, Length}, infinity).

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
    Reply = get_attribute(State, length),
    {reply, {ok, Reply}, State};
handle_call(close, _From, State) ->
	{stop, normal, ok, State};
handle_call({pread, Offset, _Length}, _From, State) ->
	ChunkSize = get_attribute(State, chunkSize),
	Length = get_attribute(State, length),
	NumChunks = (Length + ChunkSize - 1) div ChunkSize, 
	ChunkNum = Offset div ChunkSize,
	ChunkOffset = Offset rem ChunkSize,
	Reply = read(State, ChunkNum, ChunkOffset, Length, NumChunks, <<>>),
	{reply, {ok, Reply}, State}.

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

get_attribute(State, Attribute) ->
	Coll = list_to_atom(atom_to_list(State#state.bucket) ++ ".files"),
	{ok, {{Attribute,Value}}} = mongo:do(State#state.write_mode, State#state.read_mode, State#state.connection,
										 State#state.database,
										 fun() ->
												 mongo:find_one(Coll, {'_id', State#state.id}, {'_id', 0, Attribute, 1})
										 end),
	Value.

read(_State, ChunkNum, _Offset, _NumToRead, NumberOfChunks, Result) when ChunkNum > NumberOfChunks ->
	Result;
read(_State, _ChunkNum, _Offset, NumToRead, _NumberOfChunks, Result) when NumToRead =:= 0 ->
	Result;
read(State, ChunkNum, Offset, _NumToRead, NumberOfChunks, Result) ->
	Coll = list_to_atom(atom_to_list(State#state.bucket) ++ ".chunks"),
	{ok, {{data,{bin, bin, BinData}}}} = mongo:do(State#state.write_mode, State#state.read_mode, State#state.connection,
										 State#state.database,
										 fun() ->
												 mongo:find_one(Coll, {'files_id', State#state.id, n, ChunkNum}, {'_id', 0, data, 1})
										 end),
	BinData.
