%% A wrapper around the Erlasticsearch library that integrates it with Zotonic
-module(elasticsearch).
-author("Driebit <tech@driebit.nl>").

-export([
    index/1,
    create_index/1,
    put_doc/2,
    put_doc/3,
    put_mapping/3,
    put_mapping/4,
    delete_doc/2,
    delete_doc/3,
    map_rsc/2
]).

-include("zotonic.hrl").
-include("deps/erlastic_search/include/erlastic_search.hrl").

%% Get Elasticsearch connection params from config
elasticsearch_connection() ->
    EsHost = z_config:get(elasticsearch_host, <<"127.0.0.1">>),
    EsPort = z_config:get(elasticsearch_port, 9200),
    #erls_params{host=EsHost, port=EsPort, http_client_options=[]}.

%% Get Elasticsearch index name from config; default to site name
index(Context) ->
    SiteName = z_context:site(Context),
    z_convert:to_binary(m_config:get_value(?MODULE, elasticsearch_index, SiteName, Context)).

create_index(Index) ->
    Result = erlastic_search:create_index(z_convert:to_binary(Index)),
    ?DEBUG(Result).

put_mapping(Type, Doc, Context) ->
    put_mapping(index(Context), Type, Doc, Context).

put_mapping(Index, Type, Doc, _Context) ->
    erlastic_search:put_mapping(elasticsearch_connection(), Index, Type, Doc).

%% Save a resource to Elasticsearch
put_doc(Id, Context) ->
    put_doc(Id, index(Context), Context).

put_doc(Id, Index, Context) ->
    % All resource properties
    Data = map_rsc(Id, Context)
        ++ lists:flatten(z_notifier:foldl({elasticsearch_put, Id}, [], Context)),

    % Todo: check if resource is published?

    ?DEBUG(Data),

    erlastic_search:index_doc_with_id(elasticsearch_connection(), Index, "resource", z_convert:to_binary(Id), Data).

delete_doc(Id, Context) ->
    delete_doc(Id, index(Context), Context).

delete_doc(Id, Index, Context) ->
    Type = lists:last(m_rsc:is_a(Id, Context)),
    Result = elastic_search:delete_doc(elasticsearch_connection(), Index, Type, Id),
    ?DEBUG({"DELETE RESULT", Result}).

%% Map a Zotonic resource properties
map_rsc(Id, Context) ->
    [
        {category, m_rsc:is_a(Id, Context)},
        {location, [
            {lat, m_rsc:p(Id, location_lat, Context)},
            {lon, m_rsc:p(Id, location_lng, Context)}
        ]}
    ] ++
    lists:foldl(
        fun(Prop, Acc) ->
            case map_property(Prop) of
                undefined ->
                    Acc;
                {Key, Value} ->
                    [{Key, Value}|Acc]
            end
        end,
        [],
        m_rsc:get(Id, Context)
    ).

map_property({Key, Value}) ->
    % Exclude some properties that don't need to be added to index
    IgnoredProps = [
        id, pivot_geocode, managed_props, blocks, location_lng, location_lat,
        location_zoom_levell
    ],
    case lists:member(Key, IgnoredProps) of
        true ->
            undefined;
        false ->
            {Key, map_value(Value)}
    end.

% Make Zotonic resource value Elasticsearch-friendly
map_value({{Y,M,D},{H,I,S}} = DateTime) when
is_integer(Y), is_integer(M), is_integer(D), is_integer(H), is_integer(I), is_integer(S) ->
    z_convert:to_binary(z_convert:to_isotime(DateTime));
map_value({trans, Translations}) ->
    lists:map(
        % Strip HTML tags from translated props. This is only needed for body,
        % but won't hurt the other translated props.
        %
        fun({LangCode, Translation}) ->
            {LangCode, z_html:strip(Translation)}
        end,
        Translations
    );
map_value(Value) ->
    Value.
