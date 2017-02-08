%% @doc Zotonic search handler for Elasticsearch
-module(elasticsearch_search).

-export([
    search/2
]).

-include_lib("zotonic.hrl").

%% @doc Convert Zotonic search query to an Elasticsearch query
-spec search(#search_query{}, #context{}) -> #search_result{}.
search(#search_query{search = {Type, Query}, offsetlimit = {From, Size}}, Context) when Size > 9999 ->
    %% Size defaults to 10.000 max
    %% See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html
    search(#search_query{search = {Type, Query}, offsetlimit = {From, 9999}}, Context);
search(#search_query{search = {_, Query}, offsetlimit = {From, Size}}, Context) ->
    Query2 = with_query_id(Query, Context),
    ElasticQuery = [
        {from, From - 1},
        {size, Size},
        {'_source', false}, % Don't return _source as we only need the id
        {sort, lists:filtermap(fun(Q) -> map_sort(Q, Context) end, Query2)},
        {query, [
            {bool, [
                {must, lists:filtermap(fun(Q) -> map_query(Q, Context) end, Query2)},
                {filter, [
                    {bool, [
                        {should, lists:filtermap(fun(Q) -> map_should(Q, Context) end, Query2)},
                        {must_not, lists:filtermap(fun(Q) -> map_must_not(Q, Context) end, Query2)},
                        {must, lists:filtermap(fun(Q) -> map_must(Q, Context) end, Query2)}
                    ]}
                ]}
            ]}
        ]}
    ],

    %% Invisible by default, as Zotonic has minimum log level 'info'
    lager:debug("Elasticsearch query ~s", [jsx:encode(ElasticQuery)]),

    case erlastic_search:search(elasticsearch:connection(), elasticsearch:index(Context), ElasticQuery) of
        {ok, Json} ->
            Hits = proplists:get_value(<<"hits">>, Json),
            Total = proplists:get_value(<<"total">>, Hits),
            Results = proplists:get_value(<<"hits">>, Hits),
            Ids = [z_convert:to_integer(proplists:get_value(<<"_id">>, Result)) || Result <- Results],
            #search_result{result = Ids, total = Total};
        {error, Error} ->
            lager:error(
                "Elasticsearch query failed: ~p for query ~s (from Zotonic query ~p)",
                [Error, jsx:encode(ElasticQuery), Query]
            ),
            %% Return empty search result
            #search_result{}
    end.

with_query_id(Query, Context) ->
    case proplists:get_value(query_id, Query) of
        undefined ->
            Query;
        QueryId ->
            search_query:parse_query_text(
                z_html:unescape(
                    m_rsc:p(QueryId, 'query', Context)
                )
            ) ++ Query
    end.

map_sort({sort, Property}, Context) when is_atom(Property) or is_list(Property) ->
    map_sort({sort, z_convert:to_binary(Property)}, Context);
map_sort({sort, <<"seq">>}, _Context) ->
    %% Ignore sort by seq: edges are (by default) indexed in order of seq
    false;
map_sort({sort, <<"-", Property/binary>>}, Context) ->
    map_sort(Property, <<"desc">>, Context);
map_sort({sort, <<"+", Property/binary>>}, Context) ->
    map_sort(Property, <<"asc">>, Context);
map_sort({sort, Property}, Context) ->
    map_sort(Property, <<"asc">>, Context);
map_sort(_, _) ->
    false.

map_sort(Property, Order, _Context) ->
    {true, [{map_sort_property(Property), [{order, Order}]}]}.

%% @doc Map full text query
-spec map_query({atom(), any()}, #context{}) -> {true, list()} | false.
map_query({text, <<>>}, _Context) ->
    false;
map_query({text, Text}, Context) when is_binary(Text) ->
    DefaultFields = [
        <<"_all">>,
        <<"title*^2">>
    ],
    {true, [{multi_match, [
        {query, Text},
        {fields, z_notifier:foldr({elasticsearch_fields, Text}, DefaultFields, Context)}
    ]}]};
map_query({text, Text}, Context) ->
    map_query({text, z_convert:to_binary(Text)}, Context);
map_query(_, _) ->
    false.

%% @doc Map OR
-spec map_should({atom(), any()}, #context{}) -> {true, list()} | false.
map_should({cat, []}, _Context) ->
    false;
map_should({cat, Name}, Context) ->
    Cats = case {is_string_or_list(Name), is_binary(Name)} of
        {string, false} -> [iolist_to_binary(Name)];
        {_, true} -> [Name];
        {false, _} -> [Name];
        {list, _} ->
            [Hd|_] = Name,
            case is_string_or_list(Hd) of
                list -> Hd;
                string -> Name;
                false -> Name
            end;
        _ -> Name
    end,

    case filter_categories(Cats, Context) of
        [] ->
            false;
        Filtered ->
            {true, [{terms, [{category, Filtered}]}]}
    end;
map_should(_, _Context) ->
    false.

%% @doc Map NOT
-spec map_must_not({atom(), any()}, #context{}) -> {true, list()} | false.
map_must_not({cat_exclude, []}, _Context) ->
    false;
map_must_not({cat_exclude, [Hd|[]]}, Context) when is_list(Hd) ->
    map_must_not({cat_exclude, Hd}, Context);
map_must_not({cat_exclude, Cats}, Context) ->
    case filter_categories(Cats, Context) of
        [] ->
            false;
        Filtered ->
            {true, [{terms, [{category, Filtered}]}]}
    end;
map_must_not({id_exclude, Id}, _Context) ->
    {true, [{term, [{id, z_convert:to_integer(Id)}]}]};
map_must_not(_, _Context) ->
    false.

%% @doc Map AND
-spec map_must({atom(), any()}, #context{}) -> {true, list()} | false.
map_must({authoritative, Bool}, _Context) ->
    {true, [{term, [{is_authoritative, z_convert:to_bool(Bool)}]}]};
map_must({is_featured, Bool}, _Context) ->
    {true, [{term, [{is_featured, Bool}]}]};
map_must({is_published, Bool}, _Context) ->
    {true, [{term, [{is_published, Bool}]}]};
map_must({upcoming, true}, _Context) ->
    {true, [{range, [{date_start, [{<<"gt">>, <<"now">>}]}]}]};
map_must({ongoing, true}, _Context) ->
    {true, [{range, [
        {date_start, [{<<"lt">>, <<"now">>}]},
        {date_end, [{<<"gr">>, <<"now">>}]}
    ]}]};
map_must({finished, true}, _Context) ->
    {true, [{range, [{date_end, [{<<"lt">>, <<"now">>}]}]}]};
map_must({unfinished, true}, _Context) ->
    {true, [{range, [{date_end, [{<<"gt">>, <<"now">>}]}]}]};
map_must({date_start_before, Date}, _Context) ->
    {true, [{range, [
        {date_start, [{<<"lte">>, z_convert:to_datetime(Date)}]}
    ]}]};
map_must({date_start_after, Date}, _Context) ->
    {true, [{range, [
        {date_start, [{<<"gte">>, z_convert:to_datetime(Date)}]}
    ]}]};
map_must({date_end_before, Date}, _Context) ->
    {true, [{range, [
        {date_end, [{<<"lte">>, z_convert:to_datetime(Date)}]}
    ]}]};
map_must({date_end_after, Date}, _Context) ->
    {true, [{range, [
        {date_end, [{<<"gte">>, z_convert:to_datetime(Date)}]}
    ]}]};
map_must({date_start_year, Year}, _Context) ->
    {true, [{term, [{date_start, z_convert:to_binary(Year)}]}]};
map_must({date_end_year, Year}, _Context) ->
    {true, [{term, [{date_end, z_convert:to_binary(Year)}]}]};
map_must({publication_year, Year}, _Context) ->
    {true, [{term, [{publication_start, z_convert:to_binary(Year)}]}]};
map_must({content_group, []}, _Context) ->
    false;
map_must({content_group, undefined}, _Context) ->
    false;
map_must({content_group, Id}, Context) ->
    {true, [{term, [{content_group_id, m_rsc:rid(Id, Context)}]}]};
%% TODO Add support for other filter types
%% http://docs.zotonic.com/en/latest/developer-guide/search.html#filter
%% Use regular fields where Zotonic uses pivot_ fields
%% @see z_pivot_rsc:pivot_resource/2
map_must({filter, [Key, Value]}, Context) when is_list(Key) ->
    map_must({filter, [list_to_binary(Key), Value]}, Context);
map_must({filter, [<<"pivot_", _/binary>> = Pivot, Value]}, Context) ->
    map_must({filter, [map_pivot(Pivot), Value]}, Context);
map_must({filter, [<<"is_", _/binary>> = Key, Value]}, _Context) ->
    %% Map boolean fields (is_*)
    {true, [{term, [{Key, z_convert:to_bool(Value)}]}]};
map_must({filter, [Key, Value]}, _Context) ->
    %% Use a multi_match for wildcard fields, such as title_*, which we need
    %% for multilingual setups, that have title_nl, title_en etc.
    BinaryKey = z_convert:to_binary(Key),
    {true, [{multi_match, [
        {fields, [BinaryKey, <<BinaryKey/binary, "_*">>]},
        {query, z_convert:to_binary(Value)}
    ]}]};
map_must({hasobject, [Object, Predicate]}, Context) ->
    {true, [{nested, [
        {path, <<"outgoing_edges">>},
        {query, [
            {bool, [
                {filter, [
                    [{term, [{<<"outgoing_edges.object_id">>, m_rsc:rid(Object, Context)}]}],
                    [{term, [{<<"outgoing_edges.predicate_id">>, m_predicate:name_to_id_check(Predicate, Context)}]}]
                ]}
            ]}
        ]}
    ]}]};
map_must({hasobject, Object}, Context) when is_integer(Object); is_binary(Object) ->
    map_must({hasobject, maybe_split_list(Object)}, Context);
map_must({hasobject, Object}, Context) ->
    {true, [{nested, [
        {path, <<"outgoing_edges">>},
        {query, [
            {bool, [
                {filter, [
                    {term, [{<<"outgoing_edges.object_id">>, m_rsc:rid(Object, Context)}]}
                ]}
            ]}
        ]}
    ]}]};
map_must({hassubject, Object}, Context) when is_integer(Object); is_binary(Object) ->
    map_must({hassubject, maybe_split_list(Object)}, Context);
map_must({hassubject, [Subject, Predicate]}, Context) ->
    {true, [{nested, [
        {path, <<"incoming_edges">>},
        {query, [
            {bool, [
                {filter, [
                    [{term, [{<<"incoming_edges.subject_id">>, m_rsc:rid(Subject, Context)}]}],
                    [{term, [{<<"incoming_edges.predicate_id">>, m_predicate:name_to_id_check(Predicate, Context)}]}]
                ]}
            ]}
        ]}
    ]}]};
map_must({hassubject, Subject}, Context) ->
    {true, [{nested, [
        {path, <<"incoming_edges">>},
        {query, [
            {bool, [
                {filter, [
                    {term, [{<<"incoming_edges.subject_id">>, m_rsc:rid(Subject, Context)}]}
                ]}
            ]}
        ]}
    ]}]};
map_must(_, _) ->
    false.
%% TODO: hasanyobject unfinished_or_nodate publication_month


%% @doc Filter out empty category values (<<>>, undefined) and non-existing categories
-spec filter_categories(list(), #context{}) -> list(binary()).
filter_categories(Cats, Context) ->
    lists:filtermap(
        fun(Category) ->
            case m_category:name_to_id(Category, Context) of
                {ok, _Id} ->
                    {true, z_convert:to_binary(Category)};
                _ ->
                    false
            end
        end,
        Cats
    ).


%% @doc Map pivot column name to regular property name.
%% @see z_pivot_rsc:pivot_resource/2
%% While Zotonic (PostgreSQL) needs Pivot columns, we can do without in
%% Elasticsearch.
%% Built-in pivots:
map_pivot(<<"rsc.", Pivot/binary>>) -> map_pivot(Pivot);
map_pivot(<<"pivot_street">>) -> <<"address_street_1">>;
map_pivot(<<"pivot_city">>) -> <<"address_city">>;
map_pivot(<<"pivot_postcode">>) -> <<"address_postcode">>;
map_pivot(<<"pivot_state">>) -> <<"address_state">>;
map_pivot(<<"pivot_country">>) -> <<"address_country">>;
map_pivot(<<"pivot_first_name">>) -> <<"name_first">>;
map_pivot(<<"pivot_surname">>) -> <<"name_surname">>;
map_pivot(<<"pivot_gender">>) -> <<"gender">>;
map_pivot(<<"pivot_title">>) -> <<"title_*">>;
map_pivot(<<"pivot_location_lat">>) -> <<"location_lat">>;
map_pivot(<<"pivot_location_lng">>) -> <<"location_lng">>;
%% Fur custom pivots, assume a custom pivot's name corresponds to the property
%% it pivots:
map_pivot(<<"pivot_", Property/binary>>) -> Property;
map_pivot(NoPivot) -> NoPivot.

%% @doc Map Zotonic sort fields to Elasticsearch-compatible ones
-spec map_sort_property(binary()) -> binary().
%% Sort Sort on dates
map_sort_property(<<"rsc.", Pivot/binary>>) -> map_sort_property(Pivot);
map_sort_property(<<"pivot_date_", Property/binary>>) -> <<"date_", Property/binary>>;
%% For other fields, sort on keyword. Even in case mod_translation is enabled,
%% the keyword field is named title.keyword, not title_en.keyword etc.
map_sort_property(<<"pivot_surname">>) -> <<"name_surname.keyword">>;
map_sort_property(<<"pivot_first_name">>) -> <<"name_first.keyword">>;
map_sort_property(<<"pivot_", Property/binary>>) -> <<Property/binary, ".keyword">>;
map_sort_property(Sort) -> map_pivot(Sort).

is_string_or_list(StringOrList) when is_list(StringOrList) ->
    case z_string:is_string(StringOrList) of
        true -> string;
        false -> list
    end;
is_string_or_list(_) ->
    false.

%% Taken from search_query
maybe_split_list(Id) when is_integer(Id) ->
    [Id];
maybe_split_list(<<"[", Rest/binary>>) ->
    split_list(Rest);
maybe_split_list([$[|Rest]) ->
    split_list(z_convert:to_binary(Rest));
maybe_split_list(Other) ->
    [Other].

split_list(Bin) ->
    Bin1 = binary:replace(Bin, <<"]">>, <<>>, [global]),
    Parts = binary:split(Bin1, <<",">>, [global]),
    [ unquot(z_string:trim(P)) || P <- Parts ].

unquot(<<C, Rest/binary>>) when C =:= $'; C =:= $"; C =:= $` ->
    binary:replace(Rest, <<C>>, <<>>);
unquot([C|Rest]) when C =:= $'; C =:= $"; C =:= $` ->
    [ X || X <- Rest, X =/= C ];
unquot(B) ->
    B.