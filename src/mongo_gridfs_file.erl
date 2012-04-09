-module(mongo_gridfs_file).

-export([new/6, close/1, get_file_size/1, pread/3, read/1, get_md5/1, get_id/1, get_file_name/1]).

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

get_md5(Pid) ->
	gen_server:call(Pid, md5, infinity).

get_id(Pid) ->
	gen_server:call(Pid, id, infinity).
	
get_file_name(Pid) ->
	gen_server:call(Pid, file_name, infinity).

pread(Pid, Offset, Length) ->
	gen_server:call(Pid, {pread, Offset, Length}, infinity).

read(Pid) ->
	gen_server:call(Pid, read, infinity).
	
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
    Length = get_attribute(State, length),
    {reply, {ok, Length}, State};
handle_call(md5, _From, State) ->
    Md5 = get_attribute(State, md5),
    {reply, {ok, Md5}, State};
handle_call(file_name, _From, State) ->
    FileName = get_attribute(State, filename),
    {reply, {ok, FileName}, State};
handle_call(id, _From, State) ->
    {reply, {ok, State#state.id}, State};
handle_call(close, _From, State) ->
	{stop, normal, ok, State};
handle_call(read, _From, State) ->
	ChunkSize = get_attribute(State, chunkSize),
	Length = get_attribute(State, length),
	NumChunks = (Length + ChunkSize - 1) div ChunkSize, 
	Reply = read(State, 0, 0, Length, NumChunks, <<>>),
	{reply, {ok, Reply}, State};
handle_call({pread, Offset, NumToRead}, _From, State) ->
	ChunkSize = get_attribute(State, chunkSize),
	Length = get_attribute(State, length),
	NumChunks = (Length + ChunkSize - 1) div ChunkSize, 
	ChunkNum = Offset div ChunkSize,
	ChunkOffset = Offset rem ChunkSize,
	Reply = read(State, ChunkNum, ChunkOffset, NumToRead, NumChunks, <<>>),
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

read(_State, ChunkNum, _Offset, _NumToRead, NumberOfChunks, Result) when ChunkNum >= NumberOfChunks ->
	Result;
read(_State, _ChunkNum, _Offset, NumToRead, _NumberOfChunks, Result) when NumToRead =< 0 ->
	Result;
read(State, ChunkNum, Offset, NumToRead, NumberOfChunks, Result) ->
	Coll = list_to_atom(atom_to_list(State#state.bucket) ++ ".chunks"),
	{ok, {{data,{bin, bin, BinData}}}} = mongo:do(State#state.write_mode, State#state.read_mode, State#state.connection,
										 State#state.database,
										 fun() ->
												 mongo:find_one(Coll, {'files_id', State#state.id, n, ChunkNum}, {'_id', 0, data, 1})
										 end),
	if
		Offset > 0 ->
			ListData1 = binary_to_list(BinData),
			{_, ListData2} = lists:split(Offset, ListData1),
			BinData2 = list_to_binary(ListData2);
		true ->
			BinData2 = BinData
	end,
	if
		size(BinData2) > NumToRead ->
			ListData3 = binary_to_list(BinData2),
			{ListData4, _} = lists:split(NumToRead, ListData3),
			BinData3 = list_to_binary(ListData4),
			<<Result/binary, BinData3/binary>>;
		true ->
			read(State, ChunkNum+1, 0, NumToRead-size(BinData2), NumberOfChunks, <<Result/binary, BinData2/binary>>)
	end.
