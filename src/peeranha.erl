%%% TODO: - Deleted entries GC in merkle tree
%%%       - allow to retire into another node
-module(peeranha).
-export([boot/2, retire/1, fork/3, sync/2, pull/2, pull/4]).
-export([read/2, peek/2, write/3, delete/2]).
-export([sync/4, fork/1]).

%%%%%%%%%%%%%%%%%%%%%
%%% DB MANAGEMENT %%%
%%%%%%%%%%%%%%%%%%%%%
boot(Name, Opts) ->
    case interclock:boot(Name, Opts) of
        ok -> % make an inventory of the data, sync
            peeranha_sync:boot(Name),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.


fork(Remote, Name, Dir) ->
    {UUID, NewId} = peeranha_peer:fork(Remote),
    peeranha:boot(Name, [{uuid, UUID}, {name, Name}, {dir, Dir},
                         {type, normal}, {id, NewId}]).


%% Push/pull mechanism
sync(Local, Peer) ->
    {ok, Diff} = peeranha_sync:diff(Local, Peer),
    Ops = [case interclock:peek(Local, Key) of
            {error, undefined} ->
                %% Fetch the value from a peer node // Pull Anti-Entropy
                {ok, Event, Vals} = peeranha_peer:peek(Peer, Key),
                Res = interclock:sync(Local, Key, Event, Vals),
                %% Update our sync tree
                TreeVal = peeranha:peek(Local, Key),
                peeranha_sync:write(Local, Key, TreeVal),
                %% Build the diff list
                {Res, Key};
            {ok, Event, Vals} ->
                %% We have a value, but maybe our peer doesn't // Push-Pull Anti-Entropy
                peeranha_peer:sync(Peer, Key, Event, Vals), % can optimize based on result
                %% Read back and merge locally
                {ok, MergedEvent, MergedVals} = peeranha_peer:peek(Peer, Key),
                Res = interclock:sync(Local, Key, MergedEvent, MergedVals),
                %% Update the sync tree
                TreeVal = peeranha:peek(Local, Key),
                peeranha_sync:write(Local, Key, TreeVal),
                to_diff(Local, Key, Res)
           end || Key <- Diff],
     lists:foldr(fun({conflict,Key}, {Merge,Conflict}) -> {Merge, [Key|Conflict]}
                 ;  ({_, Key}, {Merge,Conflict}) -> {[Key|Merge], Conflict} end,
                 {[],[]},
                 Ops).

pull(Local, Remote) ->
    Pre = fun(_Winner, _Key, _LocalVals, _RemoteVals) -> ok end,
    Post = fun(_Key, _Vals) -> ok end,
    pull(Local, Remote, Pre, Post).

-spec pull(Local, Peer, Pre, Post) -> Result when
    Local :: term(),
    Peer :: term(),
    Pre :: fun((Winner, Key, LocalVal, PeerVal) -> ok | skip),
    Post :: fun((Key, Val) -> ok),
    Winner :: peer | local | conflict,
    Key :: binary(),
    LocalVal :: Val,
    PeerVal :: Val,
    Val :: undefined | {ok, term()} | {conflict, [term()]} | {conflict, deleted, [term()]},
    Result :: {Merged::[Key], Conflicted::[Key], Skipped::[Key]}.
pull(Local, Peer, Pre, Post) ->
    {ok, Diff} = peeranha_sync:diff(Local, Peer),
    Ops = [case interclock:peek(Local, Key) of
            {error, undefined} -> % peer owns it
                {ok, PeerEvent, PeerVals} = peeranha_peer:peek(Peer, Key),
                pull_merge(Local, Key, Pre, Post, undefined, PeerEvent, PeerVals);
            {ok, _LocalEvent, LocalVals} -> % we have a copy
                case peeranha_peer:peek(Peer, Key) of
                    {error, undefined} -> % we own it
                        {skip, Key};
                    {ok, PeerEvent, PeerVals} ->
                        pull_merge(Local, Key, Pre, Post, LocalVals, PeerEvent, PeerVals)
                end
           end || Key <- Diff],
     lists:foldr(fun({conflict,Key}, {Merge,Conflict,Skip}) -> {Merge, [Key|Conflict], Skip}
                 ;  ({skip, Key}, {Merge,Conflict,Skip}) -> {Merge, Conflict, [Key|Skip]}
                 ;  ({_, Key}, {Merge,Conflict,Skip}) -> {[Key|Merge], Conflict, Skip} end,
                 {[],[],[]},
                 Ops).

retire(Name) ->
    peeranha_sync:retire(Name),
    interclock:retire(Name).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% KEY/VAL OPERATIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
read(Name, Key) ->
    interclock:read(Name, Key).

peek(Name, Key) ->
    interclock:peek(Name, Key).

write(Name, Key, Val) ->
    interclock:write(Name, Key, Val),
    TreeVal = interclock:peek(Name, Key),
    peeranha_sync:write(Name, Key, TreeVal).

delete(Name, Key) ->
    interclock:delete(Name, Key),
    TreeVal = interclock:peek(Name, Key),
    peeranha_sync:write(Name, Key, TreeVal).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% INTERNAL CALLBACKS FROM REMOTE NODES %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
sync(Name, Key, Event, Val) ->
    Res = interclock:sync(Name, Key, Event, Val),
    %% Update our sync tree
    TreeVal = peeranha:peek(Name, Key),
    peeranha_sync:write(Name, Key, TreeVal),
    Res.

fork(Name) ->
    interclock:fork(Name).

%%%%%%%%%%%%%%%
%%% PRIVATE %%%
%%%%%%%%%%%%%%%

pull_merge(Local, Key, Pre, Post, LocalVals, PeerEvent, PeerVals) ->
    Win = interclock:simulate_sync(Local, Key, PeerEvent, PeerVals),
    case Pre(Win, Key, vals_to_hook(LocalVals), vals_to_hook(PeerVals)) of
        skip ->
            {skip, Key};
        ok ->
            Win = interclock:sync(Local, Key, PeerEvent, PeerVals),
            %% Update the sync tree
            TreeVal = {ok,_,FinalVal} = peeranha:peek(Local, Key),
            peeranha_sync:write(Local, Key, TreeVal),
            Post(Key, vals_to_hook(FinalVal)),
            to_diff(Local, Key, Win)
    end.

vals_to_hook(undefined) -> undefined;
vals_to_hook([Val]) -> {ok, Val};
vals_to_hook(Vals=[_|_]) -> {conflict, Vals};
vals_to_hook({deleted, _, []}) -> undefined;
vals_to_hook({deleted, _, Vals}) -> {conflict, deleted, Vals}.

to_diff(Name, Key, Res) ->
    case peeranha:read(Name, Key) of
        {conflict, _} ->
            {conflict, Key};
        {conflict, deleted, _} ->
            {conflict, Key};
        _ ->
            {Res, Key}
    end.

