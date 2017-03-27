%% @doc Manages Elasticsearch indexes and index mappings
-module(elasticsearch_index).

-export([
    upgrade/5
    , ensure_index/1]).

-include_lib("zotonic.hrl").
-include_lib("erlastic_search/include/erlastic_search.hrl").

%% @doc Upgrade mappings for an index
%%      This:
%%      1. creates the new index
%%      2. creates the new mappings in it
%%      3. reindex the new index with data from the old one
%%      4. updates the alias so it points to the new index.
-spec upgrade(binary(), binary(), pos_integer(), any(), z:context()) -> ok.
upgrade(Index, Type, Version, Mappings, Context) ->
    VersionedIndex = versioned_index(Index, Version),
    ok = ensure_index(VersionedIndex),
    {ok, _} = elasticsearch:put_mapping(Type, Mappings, Context),
    
    Body = #{
        <<"source">> => #{
            <<"index">> => Index
        },
        <<"dest">> => #{
            <<"index">> => VersionedIndex
        }
    },
    
    %% This can take a while before completing
    {ok, _Response} = erlastic_search:reindex(Body),
    {ok, _Response2} = update_alias(Index, Version),
    ok.

%% @doc Point index alias (e.g. site_name) to versioned index (e.g. site_name_3)
-spec update_alias(binary(), integer()) -> tuple().
update_alias(Index, Version) ->
    VersionedIndex = versioned_index(Index, Version),
    Body = #{
        <<"actions">> => [
            #{<<"add">> => #{
                <<"index">> => VersionedIndex,
                <<"alias">> => Index
            }},
            #{<<"remove_index">> => #{
                <<"index">> => Index
            }}
        ]
    },
    erlastic_search:aliases(Body).

%% @doc Get name of index appended with version number
versioned_index(Index, Version) ->
    <<Index/binary, "_", (z_convert:to_binary(Version))/binary>>.
  
%% Create index only if it doesn't yet exist
-spec ensure_index(binary()) -> noop | ok.
ensure_index(Index) ->
    case index_exists(Index) of
        true ->
            ok;
        false ->
            create_index(Index),
            ok
    end.

%% Create Elasticsearch index
-spec create_index(string()) -> string().
create_index(Index) ->
    lager:info("mod_elasticsearch: creating index ~p", [z_convert:to_list(Index)]),
    Response = erlastic_search:create_index(elasticsearch:connection(), z_convert:to_binary(Index)),
    elasticsearch:handle_response(Response).

%% Check if index exists
-spec index_exists(binary()) -> boolean().
index_exists(Index) ->
    Connection = elasticsearch:connection(),
    Response = erls_resource:get(
        Connection,
        Index,
        [],
        [],
        Connection#erls_params.http_client_options
    ),
    case Response of
        {error,{404, _}} ->
            false;
        {ok, _} ->
            true
    end.
