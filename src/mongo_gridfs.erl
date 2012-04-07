%@doc GridFS functions.
-module(mongo_gridfs).

-export([delete/1, delete/2]).
-export([find_one/1, find_one/2]).

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
	ok.
