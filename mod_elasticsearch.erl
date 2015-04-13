-module(mod_elasticsearch).
-author("Driebit <tech@driebit.nl>").

-mod_title("Elasticsearch module").
-mod_description("Elasticsearch integration for Zotonic").
-mod_prio(500).

-behaviour(gen_server).

-export([
    pid_observe_rsc_pivot_done/3,
    pid_observe_rsc_delete/3,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    start_link/1
]).

-include("zotonic.hrl").

-record(state, {context}).

start_link(Args) when is_list(Args) ->
    gen_server:start_link(?MODULE, Args, []).

pid_observe_rsc_pivot_done(Pid, Msg, _Context) ->
    gen_server:cast(Pid, Msg).

pid_observe_rsc_delete(Pid, Msg, _Context) ->
    gen_server:cast(Pid, Msg).

%% gen_server callbacks
init(Args) ->
    hackney:start(),
    {context, Context} = proplists:lookup(context, Args),

    %% Create default index if it doens't yet exist
    Index = elasticsearch:index(Context),
    case elasticsearch:index_exists(Index) of
        true -> noop;
        false ->
            lager:info("mod_elasticsearch: creating index ~p", [z_convert:to_list(Index)]),
            elasticsearch:create_index(Index)
    end,

    %% Create default mapping
    DefaultMapping = elasticsearch_mapping:default_mapping(resource, Context),
    {ok, _} = elasticsearch:put_mapping("resource", DefaultMapping, Context),

    {ok, #state{context=z_context:new(Context)}}.

handle_call(Message, _From, State) ->
    {stop, {unknown_call, Message}, State}.

handle_cast({rsc_pivot_done, Id, _IsA}, State=#state{context=Context}) ->
    elasticsearch:put_doc(Id, Context),
    {noreply, State};

handle_cast({rsc_delete, Id}, State=#state{context=Context}) ->
    elasticsearch:delete_doc(Id, Context),
    {noreply, State};

handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
