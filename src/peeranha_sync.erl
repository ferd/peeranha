-module(peeranha_sync).
-behaviour(gen_server).
-export([boot/1, retire/1, write/3, delete/2, trigger/1,
         acquire/1, release/2, diff/2, access/4]).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-define(name(Name), {via, gproc, {n, l, {sync,Name}}}).

-record(diff, {local_ref :: reference(),
               remote :: term(),
               remote_ref :: reference(),
               from :: gen_server:from(),
               pid :: pid()}).

-record(state, {name :: term(),
                canonical = undefined :: merklet:tree(),
                copies = [] :: [{reference(), merklet:tree()}], % remote ref
                diff = [#diff{}]
               }).


%%%%%%%%%%%%%%
%%% PUBLIC %%%
%%%%%%%%%%%%%%
boot(Name) ->
    supervisor:start_child(peeranha, [Name]).

start_link(Name) ->
    gen_server:start_link(?name(Name), ?MODULE, Name, []).

retire(Name) ->
    gen_server:call(?name(Name), retire).

write(Name, Key, Val) ->
    gen_server:call(?name(Name), {write, Key, Val}).

delete(Name, Key) ->
    gen_server:call(?name(Name), {delete, Key}).

trigger(Name) ->
    gen_server:call(?name(Name), trigger).

acquire(Name) -> % {ok, Ref}
    gen_server:call(?name(Name), {acquire, self()}).

release(Name, Ref) ->
    gen_server:cast(?name(Name), {release, Ref}).

diff(Name, Remote) ->
    %% Protocol messages: {at, Path}, {keys, Path}, {child_at, Path}
    %%
    %% There must be one sender and one receiver.
    %% The local call made from the node whose tree is being diffed
    %% should make use of an accessor function that will send these
    %% messages and ask the remote node for its data.
    %% The remote one will receive the messages and need to forward them
    %% through its own accessor function.
    %%
    %% TreeLocal                                TreeRemote
    %%    |                                        |
    %%  diff(Local, RPCAccessor)                   |
    %%              ^                              |
    %%              '------------------------< LocalAccessor
    %%
    %% A local diff is thus expected to be more costly (and sequential)
    %% than the remote one given tree traversal shouldn't be too slow in
    %% order to return a minimal amount of data.
    gen_server:call(?name(Name), {diff, Remote}).

access(Name, RefLocal, Op, Path) ->
    case gen_server:call(?name(Name), {access, RefLocal, Op, Path}) of
        {error, badref} -> error(badref);
        {ok, Data} -> Data
    end.

%%%%%%%%%%%%%%%%%%
%%% GEN_SERVER %%%
%%%%%%%%%%%%%%%%%%
init(Name) ->
    Tree = load(Name),
    %% TODO: trap exits of child differs
    {ok, #state{name=Name, canonical=Tree}}.

%% Key/Val management for updates
handle_call(retire, _From, State=#state{}) ->
    {stop, {shutdown, retire}, ok, State};
handle_call({write, Key, Val}, _From, State=#state{canonical=Tree}) ->
    {reply, ok, State#state{canonical=merklet:insert({Key, term_to_binary(Val)}, Tree)}};
handle_call({delete, Key}, _From, State=#state{canonical=Tree}) ->
    {reply, ok, State#state{canonical=merklet:delete(Key, Tree)}};
handle_call(trigger, _From, State=#state{name=Name}) ->
    Tree = load(Name),
    {reply, ok, State#state{canonical=Tree}};

%% Diff management
handle_call({acquire, Pid}, _From, State=#state{canonical=Tree, copies=Cp}) ->
    %% Make a copy of the tree so that it's stable on behalf of a remote node.
    %% We use a monitor to see if the caller dies so we GC the tree. This is
    %% not the most reliable system (say if a process holds a connection on
    %% behalf of a remote one, but this should be fixed in time if the problem
    %% ever arises.
    Ref = erlang:monitor(process, Pid),
    {reply, {ok, Ref}, State#state{copies=[{Ref, Tree} | Cp]}};
handle_call({release, Ref}, _From, State=#state{copies=Cp}) ->
    %% Get rid of the tree reference
    erlang:demonitor(Ref, [flush]),
    {reply, ok, State#state{copies=lists:keydelete(Ref, 1, Cp)}};
handle_call({diff, Remote}, From, State=#state{canonical=Tree,
                                               diff=Diffs}) ->
    %% Actual diffing. For this one we must make a local copy for he current
    %% operation, then make one of the remote tree.
    {_Pid, Ref} = spawn_monitor(fun() -> diff(From, Tree, Remote) end),
    {noreply, State#state{diff=[{Ref, From}|Diffs]}};
handle_call({access, RefLocal, Op, Path}, _From, State=#state{copies=Cp}) ->
    case lists:keyfind(RefLocal, 1, Cp) of
        false ->
            {reply, {error, badref}, State};
        {_, Tree} ->
            F = merklet:access_serialize(Tree),
            {reply, {ok, F(Op, Path)}, State}
    end;

handle_call(Call, _From, State=#state{}) ->
    error_logger:warning_report(unexpected_msg, {?MODULE, call, Call}),
    {noreply, State}.

handle_cast({'DOWN', Ref, process, _, normal}, State=#state{diff=DiffList}) ->
    %% Process ended normally, assume the answer was sent.
    {noreply, State=#state{diff=lists:keydelete(Ref, 1, DiffList)}};
handle_cast({'DOWN', Ref, process, Pid, Error}, State=#state{diff=DiffList}) ->
    %% Process ended abnormally. Assume answer wasn't sent.
    case lists:keyfind(Ref, 1, DiffList) of
        false ->
            error_logger:warning_report(unexpected_monitor, {?MODULE, Pid, Error}),
            {noreply, State};
        {Ref, From} ->
            gen_server:reply(From, {error, Error}),
            {noreply, State#state{diff=lists:keydelete(Ref, 1, DiffList)}}
    end;
handle_cast(Cast, State=#state{}) ->
    error_logger:warning_report(unexpected_msg, {?MODULE, cast, Cast}),
    {noreply, State}.

handle_info(Info, State=#state{}) ->
    error_logger:warning_report(unexpected_msg, {?MODULE, info, Info}),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate({shutdown, trigger}, _State) ->
    {shutdown, trigger}.

%%%%%%%%%%%%%%%
%%% PRIVATE %%%
%%%%%%%%%%%%%%%
load(Name) ->
    Keys = interclock:keys(Name),
    lists:foldl(
        fun(Key, Tree) ->
            Val = interclock:read(Name, Key),
            merklet:insert({Key, term_to_binary(Val)}, Tree)
        end,
        undefined,
        Keys
    ).

diff(From, Tree, Remote) ->
    {ok, RefRemote} = peeranha_peer:acquire(Remote),
    AccessFun = fun(Op, Path) ->
        peeranha_peer:access(Remote, RefRemote, Op, Path)
    end,
    Diff = merklet:dist_diff(Tree, merklet:access_unserialize(AccessFun)),
    gen_server:reply(From, {ok, Diff}).

