-module(mod_elasticsearch_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("zotonic.hrl").
-include_lib("../include/elasticsearch.hrl").

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
    ?assertEqual(<<"Just a title">>, maps:get(title, Mapped)),
    ?assertEqual(null, maps:get(empty_string, Mapped)),
    ?assertEqual(<<"Apekool">>, maps:get(<<"translated_nl">>, Mapped)),
    ?assertEqual(<<"Hogwash">>, maps:get(<<"translated_en">>, Mapped)),
    ?assertEqual([<<"text">>, <<"article">>], maps:get(<<"category">>, Mapped)),
    ?assertEqual(m_rsc:rid(article, context()), maps:get(<<"category_id">>, Mapped)),
    ?assertEqual(<<"article">>, maps:get(<<"category_exact">>, Mapped)).

context() ->
    z_context:new(testsandboxdb).
