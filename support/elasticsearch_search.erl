%% @doc Zotonic search handler for Elasticsearch
-module(elasticsearch_search).

-export([
    search/2,
    search/3
]).

-include_lib("zotonic.hrl").
-include_lib("../include/elasticsearch.hrl").

%% @doc Convert Zotonic search query to an Elasticsearch query
-spec search(#search_query{}, z:context()) -> #search_result{} | undefined.
search(#search_query{search = {Type, Query}, offsetlimit = {From, Size}}, Context) when Size > 9999 ->
    %% Size defaults to 10.000 max
    %% See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html
    search(#search_query{search = {Type, Query}, offsetlimit = {From, 9999}}, Context);
%% @doc Free search query in any index (non-resources)
search(#search_query{search = {elastic, Query}, offsetlimit = Offset}, Context) ->
    ElasticQuery = build_query(Query, Offset, Context),
    do_search(ElasticQuery, Query, Offset, Context);

%% @doc Elasticsearch suggest completion query
search(#search_query{search = {elastic_suggest, Query}, offsetlimit = Offset}, Context) ->
    Text = z_string:trim(proplists:get_value(suggest, Query)),
    Words = filter_split:split(Text, <<" ">>, Context),
    Prefix = z_convert:to_binary(filter_last:last(Words, Context)),
    Field = z_convert:to_binary(proplists:get_value(field, Query, <<"suggest">>)),
    Size = proplists:get_value(size, Query, 6),
    ElasticQuery = #{
        <<"suggest">> => #{
            <<"suggest">> => #{
                <<"prefix">> => Prefix,
                <<"completion">> => #{
                    <<"field">> => Field,
                    <<"size">> => Size
                }
            }
        },
        <<"_source">> => source(Query, false)
    },
    do_search(ElasticQuery, Query, Offset, Context);

%% @doc Elasticsearch suggest "did you mean" query
search(#search_query{search = {elastic_didyoumean, Query}, offsetlimit = Offset}, Context) ->
    Text = z_string:trim(proplists:get_value(suggest, Query)),
    Words = filter_split:split(Text, <<" ">>, Context),
    Word = z_convert:to_binary(filter_last:last(Words, Context)),
    Field = z_convert:to_binary(proplists:get_value(field, Query, <<"pivot_title">>)),
    Size = proplists:get_value(size, Query, 1),
    ElasticQuery = #{
        <<"suggest">> => #{
            <<"suggest">> => #{
                <<"text">> => Word,
                <<"term">> => #{
                    <<"field">> => Field,
                    <<"size">> => Size,
                    <<"sort">> => <<"frequency">>
                }
            }
        }
    },
    do_search(ElasticQuery, Query, Offset, Context);

%% @doc Resource search query
search(#search_query{} = Search, Context) ->
    search(Search, #elasticsearch_options{}, Context).

-spec search(#search_query{}, #elasticsearch_options{}, z:context()) -> #search_result{} | undefined.
search(#search_query{search = {Type, Query}, offsetlimit = {From, Size}}, Options, Context) when Size > 9999 ->
    %% Size defaults to 10.000 max
    %% See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html
    search(#search_query{search = {Type, Query}, offsetlimit = {From, 9999}}, Options, Context);
search(#search_query{search = {query, Query}, offsetlimit = Offset}, Options, Context) ->
    ZotonicQuery = with_query_id(Query, Context),
    case is_elasticsearch(ZotonicQuery, Options) of
        true ->
            ElasticQuery = build_query(ZotonicQuery, Offset, Context),
            %% Optimize performance by not returning full source document
            NoSourceElasticQuery = ElasticQuery#{<<"_source">> => false},
            #search_result{result = Items} = SearchResult =
                do_search(NoSourceElasticQuery, ZotonicQuery, Offset, Context),
            Ids = [id_to_integer(Item) || Item <- Items],
            SearchResult#search_result{result = Ids};
        false ->
            undefined
    end.

%% @doc Should this query be executed against Elasticsearch?
is_elasticsearch(_, #elasticsearch_options{fallback = false}) ->
    %% No fallback: execute all queries against Elasticsearch
    true;
is_elasticsearch(ZotonicQuery, #elasticsearch_options{fallback = true}) ->
    %% With fallback to PostgreSQL: see if we can execute this query against
    %% PostgreSQL
    is_elasticsearch_props(ZotonicQuery).

%% @doc Must Elasticsearch be consulted for these properties?
%%      - text/prefix: all fulltext searches go through Elasticsearch
%%      - filter/query_context_filter: this can be a filter on a custom property
%%        for which no custom pivot is defined in PostgreSQL.
-spec is_elasticsearch_props(atom()) -> boolean().
is_elasticsearch_props([]) ->
    false;
is_elasticsearch_props([{text, _} | _]) ->
    true;
is_elasticsearch_props([{prefix, _} | _]) ->
    true;
is_elasticsearch_props([{filter, _} | _]) ->
    true;
is_elasticsearch_props([{query_context_filter, _} | _]) ->
    true;
is_elasticsearch_props([{score_function, _} | _]) ->
    true;
is_elasticsearch_props([_Prop | List]) ->
    is_elasticsearch_props(List).

id_to_integer(#{<<"_id">> := Id}) ->
    %% JSX 3.0+
    z_convert:to_integer(Id);
id_to_integer(Item) ->
    z_convert:to_integer(proplists:get_value(<<"_id">>, Item)).

%% @doc Build Elasticsearch query from Zotonic query
-spec build_query(binary(), {pos_integer(), pos_integer()}, z:context()) -> map().
build_query(Query, {From, Size}, Context) ->
    #{
        <<"from">> => From - 1,
        <<"size">> => Size,
        <<"sort">> => lists:flatten(lists:filtermap(fun(Q) -> map_sort(Q, Context) end, Query)),
        <<"query">> => #{
            <<"function_score">> => #{
                <<"query">> => #{
                    <<"bool">> => #{
                        <<"must">> => lists:filtermap(fun(Q) -> map_query(Q, Context) end, Query),
                        <<"filter">> => build_filter(Query, Context)
                    }
                },
                <<"functions">> => lists:filtermap(fun(Q) -> map_score_function(Q, Context) end, Query)
            }
        },
        <<"aggregations">> => lists:foldl(fun(Arg, Acc) -> map_aggregation(Arg, Acc, Context) end, #{}, Query)
    }.

%% @doc Build filter clause
build_filter(Query, Context) ->
    #{
        <<"bool">> => #{
            <<"must">> => lists:filtermap(fun(Q) -> map_must(Q, Context) end, Query),
            <<"must_not">> => lists:filtermap(fun(Q) -> map_must_not(Q, Context) end, Query)
        }
    }.

%% @doc Map custom score function (function_score).
-spec map_score_function({atom(), list() | map()}, z:context()) -> {true, _} | false.
map_score_function({score_function, Function}, Context) when is_list(Function) ->
    map_score_function({score_function, maps:from_list(Function)}, Context);
map_score_function({score_function, #{<<"filter">> := Filter} = Function}, Context) ->
    {true, Function#{<<"filter">> => build_filter(Filter, Context)}};
map_score_function({score_function, Function}, _Context) ->
    {true, Function};
map_score_function(_, _Context) ->
    false.

-spec do_search(map(), proplists:proplist(), {pos_integer(), pos_integer()}, z:context()) -> #search_result{}.
do_search(ElasticQuery, ZotonicQuery, {From, Size}, Context) ->
    Index = z_convert:to_binary(proplists:get_value(index, ZotonicQuery, elasticsearch:index(Context))),

    %% Invisible by default, as Zotonic has minimum log level 'info'
    lager:debug("Elasticsearch query on index ~s: ~s", [Index, jsx:encode(ElasticQuery)]),
    search_result(erlastic_search:search(Index, ElasticQuery), ElasticQuery, ZotonicQuery, {From, Size}).

%% @doc Process search result
-spec search_result(tuple(), map(), proplists:proplist(), tuple()) -> any().
search_result({error, Error}, ElasticQuery, ZotonicQuery, _Offset) ->
    lager:error(
        "Elasticsearch query failed: ~p for query ~s (from Zotonic query ~p)",
        [Error, jsx:encode(ElasticQuery), ZotonicQuery]
    ),

    %% Return empty search result
    #search_result{};
search_result({ok, Json}, _ElasticQuery, _ZotonicQuery, {From, Size}) when is_list(Json) ->
    Hits = proplists:get_value(<<"hits">>, Json),
    Total = proplists:get_value(<<"total">>, Hits),
    Results = proplists:get_value(<<"hits">>, Hits),
    Page = From div Size + 1,
    Pages = Total div Size,
    Aggregations = proplists:get_value(<<"aggregations">>, Json),

    #search_result{result = Results, total = Total, pagelen = Size, pages = Pages, page = Page, facets = Aggregations};
search_result({ok, #{<<"_shards">> := #{<<"failures">> := Failures}}}, ElasticQuery, ZotonicQuery, _Offset) ->
    lager:error(
        "Elasticsearch query failed: ~p for query ~s (from Zotonic query ~p)",
        [Failures, jsx:encode(ElasticQuery), ZotonicQuery]
    ),

    %% Return empty search result
    #search_result{};
search_result({ok, #{<<"suggest">> := Suggest}}, _ElasticQuery, _ZotonicQuery, {_From, _Size}) ->
    [#{<<"options">> := Options}] = maps:get(hd(maps:keys(Suggest)), Suggest),
    Options;
search_result({ok, #{<<"hits">> := Hits} = Json}, _ElasticQuery, _ZotonicQuery, {From, Size}) ->
    %% From jsx 3.0+ or JSX_FORCE_MAPS is set
    #{<<"total">> := Total, <<"hits">> := Results} = Hits,
    Page = From div Size + 1,
    Pages = Total div Size,
    Aggregations = maps:get(<<"aggregations">>, Json, []),
    #search_result{result = Results, total = Total, pagelen = Size, pages = Pages, page = Page, facets = Aggregations}.

%% @doc Add search arguments from query resource to original query
with_query_id(Query, Context) ->
    case proplists:get_value(query_id, Query) of
        undefined ->
            Query;
        QueryId ->
            zotonic_query_rsc:parse(QueryId, Context) ++ Query
    end.

source(Query, Default) ->
    case proplists:get_value(source, Query) of
        undefined ->
            Default;
        Fields ->
            [z_convert:to_binary(Field) || Field <- Fields]
    end.

%% @doc Map sort query arguments.
%%      Return false to ignore the query argument.
map_sort({sort, Property}, Context) when is_atom(Property) or is_list(Property) ->
    map_sort({sort, z_convert:to_binary(Property)}, Context);
map_sort({sort, Seq}, _Context) when Seq =:= <<"seq">>; Seq =:= <<"+seq">>; Seq =:= <<"-seq">>; Seq =:= <<>>  ->
    %% Ignore sort by seq: edges are (by default) indexed in order of seq
    false;
map_sort({sort, <<"-", Property/binary>>}, Context) ->
    map_sort(Property, <<"desc">>, Context);
map_sort({sort, <<"+", Property/binary>>}, Context) ->
    map_sort(Property, <<"asc">>, Context);
map_sort({match_objects, _Id}, _Context) ->
    %% Sort a match_objects }by date
    {true, [
        <<"_score">>, %% Primary sort on score
        #{<<"modified">> => <<"desc">>}

    ]};
map_sort({sort, Property}, Context) ->
    map_sort(Property, <<"asc">>, Context);
map_sort(_, _) ->
    false.

map_sort(Property, Order, _Context) ->
    {true, [{map_sort_property(Property), [{order, Order}]}]}.

%% @doc Map full text query
-spec map_query({atom(), any()}, z:context()) -> {true, list()} | false.
map_query({text, Text}, Context) when not is_binary(Text) ->
    map_query({text, z_convert:to_binary(Text)}, Context);
map_query({text, <<>>}, _Context) ->
    false;
map_query({text, <<"id:", _/binary>>}, _Context) ->
    %% Find by id: don't create a fulltext search clause
    false;
map_query({text, Text}, Context) ->
    DefaultFields = [
        <<"_all">>,
        <<"title*^2">>
    ],
    {true, #{<<"multi_match">> => #{
        <<"query">> => Text,
        <<"fields">> => z_notifier:foldr(#elasticsearch_fields{query = Text}, DefaultFields, Context)
    }}};
map_query({prefix, Prefix}, Context) when not is_binary(Prefix) ->
    map_query({prefix, z_convert:to_binary(Prefix)}, Context);
map_query({prefix, <<>>}, _Context) ->
    false;
map_query({prefix, Prefix}, Context) ->
    {true, #{<<"multi_match">> => #{
        <<"query">> => z_convert:to_binary(Prefix),
        <<"type">> => <<"phrase_prefix">>,
        <<"fields">> => z_notifier:foldr(
            #elasticsearch_fields{query = #{<<"prefix">> => Prefix}},
            [<<"title">>],
            Context
        )
    }}};
map_query({elastic_query, ElasticQuery}, _Context) ->
    case jsx:is_json(ElasticQuery) of
        true ->
            {true, jsx:decode(ElasticQuery)};
        false ->
            false
    end;
map_query({query_id, Id}, Context) ->
    ElasticQuery = z_html:unescape(m_rsc:p(Id, <<"elastic_query">>, Context)),
    map_query({elastic_query, ElasticQuery}, Context);
map_query({match_objects, ObjectIds}, Context) when is_list(ObjectIds) ->
    Clauses = map_edge(any, ObjectIds, <<"outgoing_edges">>, Context),
    {true, [{nested, [
        {path, <<"outgoing_edges">>},
        {query, [
            {bool, [
                {should, Clauses}
            ]}
        ]}
    ]}]};
map_query({match_objects, Id}, Context) ->
    %% Look up all outgoing edges of this resource
    Clauses = lists:map(
        fun({Predicate, OutgoingEdges}) ->
            ObjectIds = [proplists:get_value(object_id, Edge) || Edge <- OutgoingEdges],
            map_outgoing_edge(Predicate, ObjectIds, Context)
        end,
        m_edge:get_edges(Id, Context)
    ),

    {true, on_resource([{nested, [
        {path, <<"outgoing_edges">>},
        {query, [
            {bool, [
                {should, Clauses}
            ]}
        ]}
    ]}])};
map_query({query_context_filter, Filter}, Context) ->
    %% Query context filters
    map_filter(Filter, Context);
map_query(_, _) ->
    false.

%% @doc Map NOT
-spec map_must_not({atom(), any()}, z:context()) -> {true, list()} | false.
map_must_not({cat_exclude, []}, _Context) ->
    false;
map_must_not({cat_exclude, Name}, Context) ->
    Cats = parse_categories(Name),
    case filter_categories(Cats, Context) of
        [] ->
            false;
        Filtered ->
            {true, [{terms, [{category, Filtered}]}]}
    end;
map_must_not({filter, [Key, Operator, Value]}, Context) when is_list(Key), is_atom(Operator) ->
    map_must_not({filter, [list_to_binary(Key), Operator, Value]}, Context);
map_must_not({filter, [Key, Operator, Value]}, _Context) when Operator =:= '<>'; Operator =:= ne ->
    {true, [{term, [{Key, z_convert:to_binary(Value)}]}]};
map_must_not({filter, [Key, missing]}, _Context) ->
    {true, [{<<"exists">>, [{field, Key}]}]};
map_must_not({exclude_document, [Type, Id]}, _Context) ->
    {true, #{<<"bool">> => #{
        <<"must">> => [
            #{<<"term">> => #{<<"_type">> => Type}},
            #{<<"term">> => #{<<"_id">> => Id}}
        ]}
    }};
map_must_not({id_exclude, Id}, _Context) when Id =/= undefined ->
    {true, [{term, [{id, z_convert:to_integer(Id)}]}]};
map_must_not(_, _Context) ->
    false.

%% @doc Map AND
-spec map_must({atom(), any()}, z:context()) -> {true, list()} | false.
map_must({cat, []}, _Context) ->
    false;
map_must({cat, Name}, Context) ->
    Cats = parse_categories(Name),
    case filter_categories(Cats, Context) of
        [] ->
            false;
        Filtered ->
            {true, #{
                <<"bool">> => #{
                    <<"should">> => [
                        %% either it's a non-resource (some other document)
                        #{<<"bool">> => #{
                            <<"must_not">> => [
                                #{<<"term">> => #{
                                    <<"_type">> => <<"resource">>}
                                }
                            ]
                        }},
                        %% or (if a resource) it must match the categories
                        #{<<"terms">> => #{
                            <<"category">> => Filtered
                        }}
                    ]
                }}
            }
    end;
map_must({authoritative, Bool}, _Context) ->
    {true, on_resource(#{<<"term">> => #{<<"is_authoritative">> => z_convert:to_bool(Bool)}})};
map_must({is_featured, Bool}, _Context) ->
    {true, [{term, [{is_featured, Bool}]}]};
map_must({is_published, Bool}, _Context) ->
    {true, on_resource(#{<<"term">> => #{<<"is_published">> => Bool}})};
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
        {date_start, [{<<"lt">>, z_convert:to_datetime(Date)}]}
    ]}]};
map_must({date_start_after, Date}, _Context) ->
    {true, [{range, [
        {date_start, [{<<"gt">>, z_convert:to_datetime(Date)}]}
    ]}]};
map_must({date_end_before, Date}, _Context) ->
    {true, [{range, [
        {date_end, [{<<"lt">>, z_convert:to_datetime(Date)}]}
    ]}]};
map_must({date_end_after, Date}, _Context) ->
    {true, [{range, [
        {date_end, [{<<"gt">>, z_convert:to_datetime(Date)}]}
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
map_must({filter, Filters}, Context) ->
    map_filter(Filters, Context);
map_must({hasobject, [Object, Predicate]}, Context) ->
    {true, #{<<"nested">> =>
        #{
            <<"path">> => <<"outgoing_edges">>,
            <<"query">> => map_outgoing_edge(Predicate, [Object], Context)
        }
    }};
map_must({hasobject, [Object]}, Context) ->
    map_must({hasobject, Object}, Context);
map_must({hasobject, Object}, Context) ->
    map_must({hasobject, [Object, any]}, Context);
%% @doc hassubject: all resources that have an incoming edge from Subject.
map_must({hassubject, [Subject]}, Context) ->
    map_must({hassubject, Subject}, Context);
map_must({hassubject, [Subject, Predicate]}, Context) ->
    {true, #{<<"nested">> =>
        #{
            <<"path">> => <<"incoming_edges">>,
            <<"query">> => map_incoming_edge(Predicate, [Subject], Context)
        }
    }};
map_must({hassubject, Subject}, Context) ->
    map_must({hassubject, [Subject, any]}, Context);
map_must({hasanyobject, ObjectPredicates}, Context) ->
    Expanded = search_query:expand_object_predicates(ObjectPredicates, Context),
    OutgoingEdges = [map_outgoing_edge(Predicate, [Object], Context) || {Object, Predicate} <- Expanded],
    {true, on_resource(#{<<"nested">> => #{
        <<"path">> => <<"outgoing_edges">>,
        <<"ignore_unmapped">> => true,
        <<"query">> => #{
            <<"bool">> => #{
                <<"should">> => OutgoingEdges
            }
        }}})};
map_must({rsc_id, Id}, _Context) ->
    {true, #{<<"match">> => #{<<"_id">> => Id}}};
map_must({text, "id:" ++ _ = Val}, Context) ->
    map_must({text, list_to_binary(Val)}, Context);
map_must({text, <<"id:", Id/binary>>}, _Context) ->
    %% Find by id when text argument equals "id:123"
    {true, #{<<"match">> => #{
        <<"_id">> => z_string:trim(Id)
    }}};
map_must(_, _) ->
    false.
%% TODO: unfinished_or_nodate publication_month

%% @doc Map aggregations (facets).
map_aggregation({aggregation, [Name, Type, Values]}, Map, Context) ->
    map_aggregation({agg, [Name, Type, Values]}, Map, Context);
map_aggregation({agg, [Name, Type, Values]}, Map, _Context) ->
    Map#{
        z_convert:to_binary(Name) => #{
            z_convert:to_binary(Type) => maps:from_list(Values)
        }
    };
map_aggregation({agg, [Name, Values]}, Map, _Context) ->
    %% Nested aggregation
    Map#{
        z_convert:to_binary(Name) => maps:from_list(Values)
    };
map_aggregation(_, Map, _) ->
    Map.

%% @doc Filter out empty category values (<<>>, undefined) and non-existing categories
-spec filter_categories(list(), z:context()) -> list(binary()).
filter_categories(Cats, Context) ->
    lists:filtermap(
        fun(Category) ->
            CategoryName = case Category of
                {CatId} ->
                    m_rsc:p_no_acl(CatId, name, Context);
                CatId when is_integer(CatId) ->
                    m_rsc:p_no_acl(CatId, name, Context);
                CatName ->
                    case m_category:name_to_id(CatName, Context) of
                        {ok, _Id} -> CatName;
                        _ -> undefined
                    end
            end,
            case CategoryName of
                undefined -> false;
                _ -> {true, z_convert:to_binary(CategoryName)}
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
map_sort_property(<<"pivot_title">>) -> <<"pivot_title">>;
map_sort_property(<<"pivot_", Property/binary>>) -> <<Property/binary, ".keyword">>;
map_sort_property(Sort) -> map_pivot(Sort).

map_incoming_edge(Predicate, Subjects, Context) ->
    map_edge(Predicate, Subjects, <<"incoming_edges">>, Context).

map_outgoing_edge(Predicate, Objects, Context) ->
    map_edge(Predicate, Objects, <<"outgoing_edges">>, Context).

%% @doc Map an incoming/outgoing edge predicate/object(s) combination.
%%      Filter out 'any' objects and predicates.
-spec map_edge(m_rsc:resource() | any, [m_rsc:resource() | any], Path :: binary(), z:context()) -> map().
%% @doc Map outgoing edges and filter out any predicate and object parts.
map_edge(any, [], _Path, _Context) ->
    #{};
map_edge(any, [any], _Path, _Context) ->
    #{};
map_edge(Predicate, Ids, Path, Context) when Ids =:= []; Ids =:= [any] ->
    #{<<"bool">> => #{
        <<"must">> => [
            map_edge_predicate(Predicate, Path, Context)
        ]
    }};
map_edge(any, Ids, Path, Context) ->
    #{<<"bool">> => #{
        <<"must">> => [
            map_edge_ids(Path, Ids, Context)
        ]
    }};
map_edge(Predicate, Ids, Path, Context) ->
    #{<<"bool">> => #{
        <<"must">> => [
            map_edge_ids(Path, Ids, Context),
            map_edge_predicate(Predicate, Path, Context)
        ]
    }}.

map_edge_ids(<<"incoming_edges">>, Ids, Context) ->
    #{<<"terms">> =>
        #{<<"incoming_edges.subject_id">> => map_resources(Ids, Context)}
    };
map_edge_ids(<<"outgoing_edges">>, Ids, Context) ->
    #{<<"terms">> =>
        #{<<"outgoing_edges.object_id">> => map_resources(Ids, Context)}
    }.

map_edge_predicate(Predicate, Path, Context) ->
    {ok, Id} = m_predicate:name_to_id(Predicate, Context),
    #{<<"term">> =>
        #{<<Path/binary, ".predicate_id">> => Id}
    }.

%% @doc Map a filter search argument.
-spec map_filter(list() | binary(), z:context()) -> {true, map()} | false.
%% http://docs.zotonic.com/ en/latest/developer-guide/search.html#filter
%% Use regular fields where Zotonic uses pivot_ fields
map_filter([[Key | _] | _] = Filters, Context) when is_list(Key); is_binary(Key); is_atom(Key) ->
    %% Multiple filters: OR
    OrFilters = lists:filtermap(fun(Filter) -> map_filter(Filter, Context) end, Filters),
    AllFilters = case lists:filtermap(fun(Filter) -> map_must_not({filter, Filter}, Context) end, Filters) of
        [] ->
            OrFilters;
        OrNotFilters ->
            OrFilters ++ [
                #{<<"bool">> => #{
                    <<"must_not">> => OrNotFilters
                }}
            ]
    end,
    {true, #{<<"bool">> => #{
        <<"should">> => AllFilters
    }}};
map_filter([Key, Value], Context) when is_list(Key) ->
    map_filter([list_to_binary(Key), Value], Context);
map_filter([<<"pivot_", _/binary>> = Pivot, Value], Context) ->
    map_filter([map_pivot(Pivot), Value], Context);
map_filter([<<"is_", _/binary>> = Key, Value], _Context) ->
    {true, #{<<"term">> => #{Key => z_convert:to_bool(Value)}}};
map_filter([Key, exists], _Context) ->
    {true, #{<<"exists">> => #{<<"field">> => Key}}};
map_filter([Key, Value], _Context) when Value =/= missing ->
    {true, #{<<"term">> => #{Key => z_convert:to_binary(Value)}}};
map_filter([Key, Value, Options], Context) when is_map(Options) ->
    map_filter([Key, <<"eq">>, Value, Options], Context);
map_filter([Key, Operator, Value], Context) ->
    map_filter([Key, Operator, Value, #{}], Context);
map_filter([Key, Operator, Value, Options], Context) when is_list(Key); not is_binary(Operator) ->
    map_filter([z_convert:to_binary(Key), z_convert:to_binary(Operator), Value, Options], Context);
map_filter([Key, Operator, Value, Options], Context) when is_list(Options) ->
    map_filter([Key, Operator, Value, maps:from_list(Options)], Context);
map_filter([Key, <<">">>, Value, Options], Context) ->
    map_filter([Key, <<"gt">>, Value, Options], Context);
map_filter([Key, Operator, Value, Options], _Context)
    when Operator =:= <<"gte">>; Operator =:= <<"gt">>; Operator =:= <<"lte">>; Operator =:= <<"lt">>
->
    %% Example: {filter, [<<"dcterms:date">>, <<"gte">>, 2016, [{<<"format">>, <<"yyyy">>}]]}
    {true, #{<<"range">> => #{
        Key => Options#{Operator => Value}
    }}};
map_filter([Key, Operator, Value, #{<<"path">> := Path}], Context) ->
    {true, Query} = map_filter([Key, Operator, Value], Context),
    Nested = #{
        <<"nested">> => #{
            <<"path">> => Path,
            <<"ignore_unmapped">> => true,
            <<"query">> => Query
        }
    },
    {true, Nested};
map_filter([Key, Operator, Value, Options], _Context) when Operator =:= <<"=">>; Operator =:= <<"eq">> ->
    {true, #{<<"term">> => #{
        Key => Options#{<<"value">> => z_convert:to_binary(Value)}
    }}};
map_filter(_Filter, _Context) ->
    %% Fall through for '<>'/'ne' (handled in map_must_not) and undefined
    %% filters (nothing to be done)
    false.

%% @doc Map edge subjects/objects, filtering out resources that do not exist.
map_resources(Ids, Context) ->
    Ids2 = [m_rsc:rid(O, Context) || O <- Ids],
    lists:filter(fun(Id) -> Id =/= undefined end, Ids2).

is_string_or_list(StringOrList) when is_list(StringOrList) ->
    case z_string:is_string(StringOrList) of
        true -> string;
        false -> list
    end;
is_string_or_list(_) ->
    false.

%% @doc Parse cat, cat_exclude etc. argument, which can be a single or multiple
%%      categories.
-spec parse_categories(string() | list() | binary()) -> list().
parse_categories(Name) ->
    case {is_string_or_list(Name), is_binary(Name)} of
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
    end.

%% @doc Apply a search argument only on documents of type resource.
%%      Other document types are excluded through a boolean OR.
-spec on_resource(map()) -> map().
on_resource(Argument) ->
    #{
        <<"bool">> => #{
            <<"should">> => [
                %% either it's a non-resource (some other document)
                #{<<"bool">> => #{
                    <<"must_not">> => [
                        #{<<"term">> => #{
                            <<"_type">> => <<"resource">>}
                        }
                    ]
                }},
                Argument
            ]
        }
    }.
