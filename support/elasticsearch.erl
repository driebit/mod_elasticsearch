%% A wrapper around the Erlasticsearch library that integrates it with Zotonic
-module(elasticsearch).
-author("Driebit <tech@driebit.nl>").

-export([
    connection/0,
    index/1,
    create_index/1,
    ensure_index/1,
    index_exists/1,
    put_doc/2,
    put_doc/3,
    put_mapping/3,
    put_mapping/4,
    delete_doc/2,
    delete_doc/3
]).

-include("zotonic.hrl").
-include("deps/erlastic_search/include/erlastic_search.hrl").

%% Get Elasticsearch connection params from config
connection() ->
    EsHost = z_config:get(elasticsearch_host, <<"127.0.0.1">>),
    EsPort = z_config:get(elasticsearch_port, 9200),
    #erls_params{host=EsHost, port=EsPort, http_client_options=[]}.

%% Get Elasticsearch index name from config; default to site name
-spec index(#context{}) -> binary().
index(Context) ->
    SiteName = z_context:site(Context),
    z_convert:to_binary(m_config:get_value(?MODULE, elasticsearch_index, SiteName, Context)).

%% Create index only if it doesn't yet exist
-spec ensure_index(binary()) -> noop | ok.
ensure_index(Index) ->
    case index_exists(Index) of
        true ->
            noop;
        false ->
            create_index(Index),
            ok
    end.

%% Create Elasticsearch index
-spec create_index(string()) -> string().
create_index(Index) ->
    lager:info("mod_elasticsearch: creating index ~p", [z_convert:to_list(Index)]),
    Response = erlastic_search:create_index(connection(), z_convert:to_binary(Index)),
    handle_response(Response).

%% Check if index exists
-spec index_exists(binary()) -> boolean().
index_exists(Index) ->
    Connection = connection(),
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

%% Create Elasticsearch mapping
put_mapping(Type, Doc, Context) ->
    put_mapping(index(Context), Type, Doc, Context).

put_mapping(Index, Type, Doc, _Context) ->
    Response = erlastic_search:put_mapping(connection(), Index, Type, Doc),
    handle_response(Response).

%% Save a resource to Elasticsearch
put_doc(Id, Context) ->
    put_doc(Id, index(Context), Context).

put_doc(Id, Index, Context) ->
    % All resource properties
    Props = elasticsearch_mapping:map_rsc(Id, Context),
    Data = lists:flatten(z_notifier:foldl({elasticsearch_put, Id}, Props, Context)),
    Response = erlastic_search:index_doc_with_id(
        connection(), Index, "resource", z_convert:to_binary(Id), Data
    ),
    handle_response(Response).

delete_doc(Id, Context) ->
    delete_doc(Id, index(Context), Context).

delete_doc(Id, Index, Context) ->
    Type = lists:last(m_rsc:is_a(Id, Context)),
    Response = erlastic_search:delete_doc(connection(), Index, Type, Id),
    handle_response(Response).

%% Handle Elasticsearch response
handle_response(Response) ->
    case Response of
        {error, {StatusCode, Error}} ->
            lager:error(
                "Elasticsearch error: ~p: ~p with connection ~p",
                [StatusCode, Error, connection()]
            ),
            Response;
        {error, Reason} ->
            lager:error(
                "Elasticsearch error: ~p with connection ~p",
                [Reason, connection()]
            );
        {ok, _} ->
            Response
    end.

