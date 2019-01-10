-module(elasticsearch_document).

-export([
    document/2,
    document/3,
    document/4,
    index/1,
    type/1,
    id/1,
    data/1
]).

-record(document, {
    index :: binary(),
    type :: binary(),
    id :: binary(),
    document :: map()
}).

-opaque document() :: #document{}.
-export_type([document/0]).

document(Id, Context) ->
    document(elasticsearch:index(Context), Id, Context).

document(Index, Id, Context) ->
    Document = elasticsearch_mapping:map_rsc(Id, Context),
    document(Index, <<"resource">>, Id, Document).

document(Index, Type, Id, Document) when is_map(Document) ->
    #document{
        index = z_convert:to_binary(Index),
        type = z_convert:to_binary(Type),
        id = z_convert:to_binary(Id),
        document = Document
    }.

index(#document{index = Index}) ->
    Index.

type(#document{type = Type}) ->
    Type.

id(#document{id = Id}) ->
    Id.

data(#document{document = Data}) ->
    Data.
