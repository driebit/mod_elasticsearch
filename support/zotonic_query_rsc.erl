%% @doc Support for Zotonic query resources.
-module(zotonic_query_rsc).

-export([
    parse/2
]).

-include_lib("zotonic.hrl").

%% @doc Parse a Zotonic query resource text.
parse(QueryId, Context) ->
    Parts = search_query:parse_query_text(
        z_html:unescape(
            m_rsc:p(QueryId, 'query', Context)
        )
    ),
    
    [{Key, maybe_split_list(Value)} || {Key, Value} <- Parts].

maybe_split_list(Id) when is_integer(Id) ->
    [Id];
maybe_split_list(<<"[", Rest/binary>>) ->
    split_list(Rest);
maybe_split_list([$[ | Rest]) ->
    split_list(z_convert:to_binary(Rest));
maybe_split_list(<<"true">>) ->
    true;
maybe_split_list(Other) ->
    Other.

split_list(Bin) ->
    Bin1 = binary:replace(Bin, <<"]">>, <<>>, [global]),
    Parts = binary:split(Bin1, <<",">>, [global]),
    [unquot(z_string:trim(P)) || P <- Parts].

unquot(<<C, Rest/binary>>) when C =:= $'; C =:= $"; C =:= $` ->
    binary:replace(Rest, <<C>>, <<>>);
unquot([C | Rest]) when C =:= $'; C =:= $"; C =:= $` ->
    [X || X <- Rest, X =/= C];
unquot(B) ->
    B.

