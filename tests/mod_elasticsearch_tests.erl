-module(mod_elasticsearch_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("zotonic.hrl").
-include_lib("../include/elasticsearch.hrl").

map_test() ->
    {ok, Id} = m_rsc:insert(
        [
            {category, text},
            {title, <<"Just a title">>},
            {empty_string, <<"">>}
        ],
        z_acl:sudo(context())
    ),
    Mapped = elasticsearch_mapping:map_rsc(Id, context()),
    ?assertEqual(<<"Just a title">>, proplists:get_value(<<"title">>, Mapped)),
    ?assertEqual(null, proplists:get_value(<<"empty_string">>, Mapped)).

context() ->
    z_context:new(testsandboxdb).
