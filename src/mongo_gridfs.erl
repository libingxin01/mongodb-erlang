%@doc GridFS functions.
-module(mongo_gridfs).

-export([delete/1, delete/2]).

delete(Selector) ->
	delete(fs, Selector).

delete(Coll, Selector) ->
	FilesColl = list_to_atom(atom_to_list(Coll) ++ ".files"),
	ChunksColl = list_to_atom(atom_to_list(Coll) ++ ".chunks"),
	Cursor = mongo:find(FilesColl, Selector, {'_id', 1}),
	Files = mongo_cursor:rest(Cursor),
	Ids = [Id || {'_id', Id} <- Files],
	mongo:delete(ChunksColl, {files_id, {'$in', Ids}}),
	mongo:delete(FilesColl, {'_id', {'$in', Ids}}).
