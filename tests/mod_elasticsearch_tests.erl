-module(mod_elasticsearch_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("zotonic.hrl").
-include_lib("../include/elasticsearch.hrl").

build_query_test() ->
    Query = elasticsearch_search:build_query([], {1,100}, context()),
    ?assertEqual(Query, #{<<"aggregations">> => #{},
                          <<"from">> => 0,
                          <<"query">> =>
                              #{<<"function_score">> =>
                                    #{<<"functions">> => [],
                                      <<"query">> =>
                                          #{<<"bool">> =>
                                                #{<<"filter">> =>
                                                      #{<<"bool">> =>
                                                            #{<<"must">> => [],
                                                              <<"must_not">> => []
                                                             }
                                                       },
                                                  <<"must">> => []
                                                 }
                                           }
                                     }
                               },
                          <<"size">> => 100,
                          <<"sort">> => []
                         }
                ).

build_query_sort_random_test() ->
    Seed = 10,
    Query = elasticsearch_search:build_query([{sort,<<"random">>}, {seed, Seed}], {1,100}, context()),
    ?assertEqual(Query, #{<<"aggregations">> => #{},
                          <<"from">> => 0,
                          <<"query">> =>
                              #{<<"function_score">> =>
                                    #{<<"boost_mode">> => <<"sum">>,
                                      <<"functions">> => [#{<<"random_score">> => #{<<"seed">> => Seed}}],
                                      <<"query">> =>
                                          #{<<"bool">> =>
                                                #{<<"filter">> =>
                                                      #{<<"bool">> =>
                                                            #{<<"must">> => [],
                                                              <<"must_not">> => []
                                                             }
                                                       },
                                                  <<"must">> => []
                                                 }
                                           }
                                     }
                               },
                          <<"size">> => 100,
                          <<"sort">> => []
                         }
                ).

map_test() ->
    {ok, Id} = m_rsc:insert(
        [
            {category, article},
            {title, <<"Just a title">>},
            {empty_string, <<"">>},
            {translated, {trans, [
                {nl, <<"Apekool">>},
                {en, <<"Hogwash">>}
            ]}}
        ],
        z_acl:sudo(context())
    ),
    Mapped = elasticsearch_mapping:map_rsc(Id, context()),
    ?assertEqual([text, article], maps:get(category, Mapped)),
    ?assertEqual(m_rsc:rid(article, context()), maps:get(category_id, Mapped)),
    ?assertEqual(<<"Just a title">>, maps:get(title, Mapped)),
    ?assertEqual(null, maps:get(empty_string, Mapped)),
    ?assertEqual(<<"Apekool">>, maps:get(<<"translated_nl">>, Mapped)),
    ?assertEqual(<<"Hogwash">>, maps:get(<<"translated_en">>, Mapped)).

put_doc_test() ->
    start_module(context()),
    {ok, Id} = m_rsc:insert(
        [
            {category, keyword},
            {title, <<"Some keyword">>}
        ],
        z_acl:sudo(context())
    ),

    meck:new(erlastic_search),
    meck:expect(
        erlastic_search,
        index_doc_with_id,
        fun(_Connection, <<"testsandboxdb">>, <<"resource">>, DocId, Data) ->
            ?assertEqual(z_convert:to_binary(Id), DocId),
            ?assert(maps:is_key(suggest, Data)),
            {ok, <<>>}
        end
    ),
    elasticsearch:put_doc(Id, context()),
    meck:unload(erlastic_search).

context() ->
    z_context:new(testsandboxdb).

-spec start_module(z:context()) -> ok.
start_module(Context) ->
    ok = z_module_manager:activate_await(mod_elasticsearch, Context).
