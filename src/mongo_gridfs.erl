%@doc GridFS functions.
-module(mongo_gridfs).

-export([delete/1, delete/2]).
-export([find_one/1, find_one/2]).
-export([find/1, find/2]).
-export([put/2, put/3]).

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

put(Coll, ObjectId, N, Data) when size(Data) =< ?CHUNK_SIZE ->
	mongo:insert(Coll, {'files_id', ObjectId, data, {bin, bin, Data}, n, N});
put(Coll, ObjectId, N, Data) ->
	<<Data1:(?CHUNK_SIZE*8), Data2/binary>> = Data,
	mongo:insert(Coll, {'files_id', ObjectId, data, {bin, bin, <<Data1:(?CHUNK_SIZE*8)>>}, n, N}),
	put(Coll, ObjectId, N+1, Data2).

bin_to_hexstr(Bin) ->
	lists:flatten([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(Bin)]).
