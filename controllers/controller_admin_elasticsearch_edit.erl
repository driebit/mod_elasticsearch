-module(controller_admin_elasticsearch_edit).
-author("Driebit <tech@driebit.nl>").

%% API
-export([
    event/2
]).

-include("zotonic.hrl").

%% Previewing the results of a elastic query in the admin edit
event(#postback{message={elastic_query_preview, Opts}}, Context) ->
    DivId = proplists:get_value(div_id, Opts),
    Index = proplists:get_value(index, Opts),
    Id = proplists:get_value(rsc_id, Opts),
    QueryType = list_to_atom(proplists:get_value(query_type, Opts)),

    Q = z_convert:to_binary(z_context:get_q("triggervalue", Context)),

    case jsx:is_json(Q) of
        false ->
            z_render:growl_error("There is an error in your query", Context);
        true ->
            S = z_search:search({QueryType, [{elastic_query, Q}, {index, Index}]}, Context),
            {Html, Context1} = z_template:render_to_iolist({cat, "_admin_query_preview.tpl"}, [{result,S}, {id, Id}], Context),
            z_render:update(DivId, Html, Context1)
    end.
