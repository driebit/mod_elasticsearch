-module(mod_elasticsearch).
-author("Driebit <tech@driebit.nl>").

-mod_title("Elasticsearch").
-mod_description("Elasticsearch integration for Zotonic").
-mod_prio(500).
-mod_schema(1).
-behaviour(gen_server).

-export([
    pid_observe_rsc_update_done/3,
    pid_observe_rsc_pivot_done/3,
    observe_search_query/2,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    start_link/1,
    manage_schema/2,
    observe_elasticsearch_put/3,
    pool/0
]).

-include("zotonic.hrl").
-include("include/elasticsearch.hrl").

-record(state, {context}).

start_link(Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, Args, []).

pid_observe_rsc_update_done(Pid, Msg, _Context) ->
    gen_server:cast(Pid, Msg).

pid_observe_rsc_pivot_done(Pid, Msg, _Context) ->
    gen_server:cast(Pid, Msg).

observe_search_query(#search_query{} = Search, Context) ->
    search(Search, Context).

init(Args) ->
    start(),
    {context, Context} = proplists:lookup(context, Args),
    {ok, #state{context = z_context:new(Context)}}.

manage_schema(_Version, Context) ->
    %% Set default config
    Index = elasticsearch:index(Context),
    default_config(index, Index, Context),

    Datamodel = #datamodel{
        categories = [
            {elastic_query, query, [
                {title, {trans, [
                    {nl, "Elasticsearch zoekopdracht"},
                    {en, "Elasticsearch query"}
                ]}}
            ]}
        ]
    },
    z_datamodel:manage(?MODULE, Datamodel, Context),

    %% When starting mod_elasticsearch for the first time, this function runs
    %% before init/1, so make sure dependencies are started.
    start(),
    prepare_index(Context).

handle_call({#search_query{} = Search, Context}, _From, State) ->
    {reply, search(Search, Context), State};
handle_call(Message, _From, State) ->
    {stop, {unknown_call, Message}, State}.

handle_cast(#rsc_update_done{id = Id, action = delete}, State = #state{context = Context}) ->
    elasticsearch:delete_doc(Id, Context),
    {noreply, State};
handle_cast(#rsc_update_done{id = Id}, State = #state{context = Context}) ->
    elasticsearch:put_doc(Id, Context),
    {noreply, State};
handle_cast(#rsc_pivot_done{id = Id}, State = #state{context = Context}) ->
    elasticsearch:put_doc(Id, Context),
    {noreply, State};
handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% @doc When the resource is a keyword, add its title to the suggest completion field.
-spec observe_elasticsearch_put(#elasticsearch_put{}, map(), z:context()) -> map().
observe_elasticsearch_put(#elasticsearch_put{type = <<"resource">>, id = Id}, Data, Context) ->
    case m_rsc:is_a(Id, keyword, Context) of
        true ->
            case elasticsearch_mapping:default_translation(m_rsc:p(Id, title, Context), Context) of
                <<>> ->
                    Data;
                Title ->
                    %% Assume good keywords are linked more often than erratic ones.
                    Weight = length(m_edge:subjects(z_convert:to_integer(Id), Context)),
                    Data#{
                        suggest => #{
                            input => Title,
                            weight => Weight
                        }
                    }
            end;
        false ->
            Data
    end;
observe_elasticsearch_put(_, Data, _) ->
    Data.

%% @doc Only handle 'elastic' and 'query' search types
search(#search_query{search = {elastic, _Query}} = Search, Context) ->
    elasticsearch_search:search(Search, Context);
search(#search_query{search = {elastic_suggest, _Query}} = Search, Context) ->
    elasticsearch_search:search(Search, Context);
search(#search_query{search = {elastic_didyoumean, _Query}} = Search, Context) ->
    elasticsearch_search:search(Search, Context);
search(#search_query{search = {query, _Query}} = Search, Context) ->
    Options = #elasticsearch_options{fallback = true},
    elasticsearch_search:search(Search, Options, Context);
search(_Search, _Context) ->
    undefined.

default_config(Key, Value, Context) ->
    case m_config:get(?MODULE, Key, Context) of
        undefined ->
            m_config:set_value(?MODULE, Key, Value, Context);
        _ ->
            nop
    end.

-spec prepare_index(z:context()) -> ok.
prepare_index(Context) ->
    {Hash, Mapping} = elasticsearch_mapping:default_mapping(resource, Context),
    Index = elasticsearch:index(Context),
    ok = elasticsearch_index:upgrade(Index, [{<<"resource">>, Mapping}], Hash, Context).

pool() ->
    elastic_pool.

max_connections() ->
    1000.

%% @doc Start the erlastic_search app and its dependencies.
start() ->
    application:ensure_all_started(erlastic_search),
    hackney_pool:start_pool(pool(), [{max_connections, max_connections()}]).
