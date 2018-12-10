-module(mod_elasticsearch_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("zotonic.hrl").
-include_lib("../include/elasticsearch.hrl").

map_test() ->
    {ok, Id} = m_rsc:insert(
        [
            {category, text},
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

-spec start_module(z:context()) -> any().
start_module(Context) ->
    ok = z_module_manager:activate_await(mod_elasticsearch, Context),
    %% Call init/1 synchronously to wait for initialization to finish.
    %% See also https://github.com/zotonic/zotonic/issues/1916.
    mod_elasticsearch:init([{context, context()}]),
    ok.
