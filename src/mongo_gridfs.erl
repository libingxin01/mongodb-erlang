%@doc GridFS functions.
-module(mongo_gridfs).

-include_lib("kernel/include/file.hrl").

-export([delete/1, delete/2]).
-export([find_one/1, find_one/2]).
-export([find/1, find/2]).
-export([put/2, put/3]).
-export([copy/1, copy/2]).

-record(context, {
	write_mode,
	read_mode,
	dbconn :: mongo_connect:dbconnection() }).

-define(CHUNK_SIZE, 262144).

-spec(delete(bson:document()) -> ok).
%@doc Deletes files matching the selector from the fs.files and fs.chunks collections.
delete(Selector) ->
	delete(fs, Selector).

-spec(delete(atom(), bson:document()) -> ok).
%@doc Deletes files matching the selector from the specified bucket.
delete(Bucket, Selector) ->
	FilesColl = list_to_atom(atom_to_list(Bucket) ++ ".files"),
	ChunksColl = list_to_atom(atom_to_list(Bucket) ++ ".chunks"),
	Cursor = mongo:find(FilesColl, Selector, {'_id', 1}),
	Files = mongo_cursor:rest(Cursor),
	mongo_cursor:close(Cursor),
	Ids = [Id || {'_id', Id} <- Files],
	mongo:delete(ChunksColl, {files_id, {'$in', Ids}}),
	mongo:delete(FilesColl, {'_id', {'$in', Ids}}).

find_one(Selector) ->
	find_one(fs, Selector).

find_one(Bucket, Selector) ->
	FilesColl = list_to_atom(atom_to_list(Bucket) ++ ".files"),
	{{'_id', Id}} = mongo:find_one(FilesColl, Selector, {'_id', 1}),
	Context = get(mongo_action_context),
	WriteMode = Context#context.write_mode,
	ReadMode = Context#context.read_mode,
	{Database, Connection} = Context#context.dbconn,
	mongo_gridfs_file:new(WriteMode, ReadMode, Connection, Database, Bucket, Id).

find(Selector) ->
	find(fs, Selector).

find(Bucket, Selector) ->
	FilesColl = list_to_atom(atom_to_list(Bucket) ++ ".files"),
	MongoCursor = mongo:find(FilesColl, Selector, {'_id', 1}),
	Context = get(mongo_action_context),
	WriteMode = Context#context.write_mode,
	ReadMode = Context#context.read_mode,
	{Database, Connection} = Context#context.dbconn,
	mongo_gridfs_cursor:new(WriteMode, ReadMode, Connection, Database, Bucket, MongoCursor).

put(FileName, Data) ->
	put(fs, FileName, Data).

put(Bucket, FileName, Data) when is_list(FileName) ->
	put(Bucket, unicode:characters_to_binary(FileName), Data);
put(Bucket, FileName, Data) ->
	FilesColl = list_to_atom(atom_to_list(Bucket) ++ ".files"),
	ChunksColl = list_to_atom(atom_to_list(Bucket) ++ ".chunks"),
	ObjectId = mongodb_app:gen_objectid(),
	put(ChunksColl, ObjectId, 0, Data),
	Md5 = list_to_binary(bin_to_hexstr(crypto:md5(Data))),
	mongo:insert(FilesColl, {'_id', ObjectId, length, size(Data), chunkSize, ?CHUNK_SIZE, 
							 uploadDate, now(), md5, Md5, filename, FileName}).

copy(FileName) ->
	copy(fs, FileName).
copy(Bucket, FileName) when is_list(FileName) ->
	copy(Bucket, unicode:characters_to_binary(FileName));
copy(Bucket, FileName) ->
	FilesColl = list_to_atom(atom_to_list(Bucket) ++ ".files"),
	ChunksColl = list_to_atom(atom_to_list(Bucket) ++ ".chunks"),
	ObjectId = mongodb_app:gen_objectid(),
	{ok, FileInfo} = file:read_file_info(FileName), 
	FileSize = FileInfo#file_info.size,
	{ok, IoStream} = file:open(FileName, [read, binary]),
	Md5 = list_to_binary(bin_to_hexstr(copy(ChunksColl, ObjectId, FileSize, 0, IoStream, crypto:md5_init()))),
	file:close(IoStream),
	mongo:insert(FilesColl, {'_id', ObjectId, length, FileSize, chunkSize, ?CHUNK_SIZE, 
							 uploadDate, now(), md5, Md5, filename, FileName}).
	

put(Coll, ObjectId, N, Data) when size(Data) =< ?CHUNK_SIZE ->
	mongo:insert(Coll, {'files_id', ObjectId, data, {bin, bin, Data}, n, N});
put(Coll, ObjectId, N, Data) ->
	<<Data1:(?CHUNK_SIZE*8), Data2/binary>> = Data,
	mongo:insert(Coll, {'files_id', ObjectId, data, {bin, bin, <<Data1:(?CHUNK_SIZE*8)>>}, n, N}),
	put(Coll, ObjectId, N+1, Data2).

copy(_ChunksColl, _ObjectId, Size, N, _IoStream, Md5Context) when (N * ?CHUNK_SIZE) >= Size ->
	crypto:md5_final(Md5Context);
copy(ChunksColl, ObjectId, Size, N, IoStream, Md5Context) ->
	{ok, Data} = file:pread(IoStream, N * ?CHUNK_SIZE, ?CHUNK_SIZE),
	mongo:insert(ChunksColl, {'files_id', ObjectId, data, {bin, bin, Data}, n, N}),
	copy(ChunksColl, ObjectId, Size, N+1, IoStream, crypto:md5_update(Md5Context, Data)).

bin_to_hexstr(Bin) ->
	lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]).

