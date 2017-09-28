%% @doc Manages Elasticsearch indexes and index mappings
-module(elasticsearch_index).

-export([
    upgrade/4,
    ensure_index/1
]).

-include_lib("zotonic.hrl").
-include_lib("erlastic_search/include/erlastic_search.hrl").

%% @doc Upgrade mappings for an index
%%      This:
%%      1. creates the new index
%%      2. creates the new mappings in it
%%      3. reindex the new index with data from the old one
%%      4. updates the alias so it points to the new index.
-spec upgrade(binary(), [{Type :: binary(), Mappings :: map()}], pos_integer(), z:context()) -> ok.
upgrade(Index, TypeMappings, Version, _Context) ->
    VersionedIndex = versioned_index(Index, Version),
    case index_exists(VersionedIndex) of
        true ->
            %% Even if the index already exists, try to create new types in case
            %% any were added. Adding types (instead of updating them) does not
            %% require a reindex.
            maybe_create_types(VersionedIndex, TypeMappings);
        false ->
            {ok, _} = create_index(VersionedIndex),
            maybe_create_types(VersionedIndex, TypeMappings),
            maybe_reindex(Index, VersionedIndex),
            {ok, _Response2} = update_alias(Index, VersionedIndex),
            ok
    end.

%% @doc Create new type mappings if they don't exist already.
-spec maybe_create_types(binary(), [{Type :: binary(), Mappings :: map()}]) -> ok.
maybe_create_types(VersionedIndex, TypeMappings) ->
    lists:foreach(
        fun({Type, Mappings}) ->
            maybe_create_type(VersionedIndex, Type, Mappings)
        end,
        TypeMappings
    ).

%% @doc Create a new type mapping if it doesn't exist already.
-spec maybe_create_type(binary(), binary(), {Type :: binary(), Mappings :: map()}) -> ok.
maybe_create_type(Index, Type, Mappings) ->
    case type_exists(Index, Type) of
        true ->
            %% Don't try to change existing types.
            ok;
        false ->
            {ok, _} = erlastic_search:put_mapping(Index, Type, Mappings)
    end.

%% @doc Populate the new index with data from the previous index, but only if
%%      that previous index exists.
maybe_reindex(Alias, NewIndex) ->
    case index_exists(Alias) of
        false ->
            %% No alias yet, so no reindex necessary
            nop;
        true ->
            %% Previous version of (aliased) index exists, so reindex from that
            Body = #{
                <<"source">> => #{
                    <<"index">> => Alias
                },
                <<"dest">> => #{
                    <<"index">> => NewIndex
                },
                <<"conflicts">> => <<"proceed">>
            },
            
            %% This can take a while before completing
            elasticsearch:handle_response(
                erlastic_search:reindex(Body)
            )
    end.

%% @doc Point index alias (e.g. site_name) to versioned index (e.g. site_name_3)
-spec update_alias(binary(), integer()) -> tuple().
update_alias(Alias, Index) ->
    Actions = [
        #{<<"add">> => #{
            <<"index">> => Index,
            <<"alias">> => Alias
        }}
    |
        case index_exists(Alias) of
            true ->
                [#{<<"remove_index">> => #{
                    <<"index">> => Alias
                }}];
            false ->
                []
        end
    ],
    Body = #{<<"actions">> => Actions},
    erlastic_search:aliases(Body).

%% @doc Get name of index appended with version number
versioned_index(Index, Version) ->
    <<Index/binary, "_", Version/binary>>.
  
%% Create index only if it doesn't yet exist
-spec ensure_index(binary()) -> ok.
ensure_index(Index) ->
    case index_exists(Index) of
        true ->
            ok;
        false ->
            create_index(Index),
            ok
    end.

%% @doc Create Elasticsearch index
-spec create_index(string()) -> string().
create_index(Index) ->
    lager:info("mod_elasticsearch: creating index ~s", [Index]),
    Response = erlastic_search:create_index(elasticsearch:connection(), Index),
    elasticsearch:handle_response(Response).

%% @doc Check if index exists.
-spec index_exists(binary()) -> boolean().
index_exists(Index) ->
    url_exists(Index).

%% @doc Check if type exists.
-spec type_exists(binary(), binary()) -> boolean().
type_exists(Index, Type) ->
    url_exists(<<Index/binary, "/_mapping", Type/binary>>).

url_exists(Url) ->
    Connection = elasticsearch:connection(),
    Response = erls_resource:head(
        Connection,
        Url,
        [],
        [],
        Connection#erls_params.http_client_options
    ),
    case Response of
        {error, 404} ->
            false;
        ok ->
            true
    end.
