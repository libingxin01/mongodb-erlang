%@doc GridFS functions.
-module(mongo_gridfs).

-export([delete/2]).

delete(Coll, Selector) ->
	FilesColl = list_to_atom(atom_to_list(Coll) ++ ".fs.files"),
	ChunksColl = list_to_atom(atom_to_list(Coll) ++ ".fs.chunks"),
	Cursor = mongo:find(FilesColl, Selector, {'_id', 1}),
	Files = mongo_cursor:rest(Cursor),
	Ids = [Id || {'_id', Id} <- Files],
	mongo:delete(ChunksColl, {file_id, {'$in', Ids}}),
	mongo:delete(FilesColl, {file_id, {'$in', Ids}}).
