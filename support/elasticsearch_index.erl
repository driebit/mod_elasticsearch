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
            nop;
        false ->
            {ok, _} = create_index(VersionedIndex),
            
            lists:foreach(
                fun({Type, Mappings}) ->
                    {ok, _} = erlastic_search:put_mapping(VersionedIndex, Type, Mappings)
                end,
                TypeMappings
            ),
            
            maybe_reindex(Index, VersionedIndex),
            {ok, _Response2} = update_alias(Index, VersionedIndex),
            ok
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
                }
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

%% Create Elasticsearch index
-spec create_index(string()) -> string().
create_index(Index) ->
    lager:info("mod_elasticsearch: creating index ~s", [Index]),
    Response = erlastic_search:create_index(elasticsearch:connection(), Index),
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
        {error, {404, _}} ->
            false;
        {ok, _} ->
            true
    end.
