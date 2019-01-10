-module(elasticsearch_bulk_indexer).

-behaviour(gen_server).

-export([
    start_link/0,
    queue/2,
    flush/1
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    queue = [] :: list(),
    batch_size = 100 :: pos_integer(),
    batch_time = 10 :: pos_integer() %% in seconds
}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, #{}, []).

-spec queue(pid(), elasticsearch_document:document()) -> ok.
queue(Pid, Document) ->
    gen_server:cast(Pid, {queue, Document}).

-spec flush(any()) -> {ok, map()} | {error, any()}.
flush(Pid) ->
    gen_server:call(Pid, flush).

init(Args) when is_map(Args) ->
    {ok, #state{batch_size = maps:get(batch_size, Args, 100)}}.

to_tuple(Document) ->
    {
        elasticsearch_document:index(Document),
        elasticsearch_document:type(Document),
        elasticsearch_document:id(Document),
        elasticsearch_document:data(Document)
    }.

handle_call(flush, _From, State) ->
    Result = do_flush(State#state.queue),
    {reply, Result, State#state{queue = []}}.

handle_cast({queue, Document}, State) ->
    {noreply, maybe_flush(State#state{queue = [Document | State#state.queue]})};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_flush(#state{queue = Queue, batch_size = BatchSize} = State) when length(Queue) >= BatchSize ->
    do_flush(Queue),
    State#state{queue = []};
maybe_flush(State) ->
    State.

do_flush(Queue) ->
    Tuples = [to_tuple(Document) || Document <- Queue],
    erlastic_search:bulk_index_docs(elasticsearch:connection(), Tuples).
