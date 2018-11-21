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

context() ->
    z_context:new(testsandboxdb).
