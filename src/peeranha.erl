-module(peeranha).
-export([boot/2, retire/1, fork/3, sync/2]).
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


sync(Local, Remote) ->
    {ok, Diff} = peeranha_sync:diff(Local, Remote),
    Ops = [case interclock:peek(Local, Key) of
            {error, undefined} ->
                %% Fetch the value from a peer node // Pull Anti-Entropy
                {ok, Event, Vals} = peeranha_peer:peek(Remote, Key),
                Res = interclock:sync(Local, Key, Event, Vals),
                %% Update our sync tree
                TreeVal = peeranha:peek(Local, Key),
                peeranha_sync:write(Local, Key, TreeVal),
                %% Build the diff list
                {Res, Key};
            _ ->
                %% We have a value, but maybe our peer doesn't // Push-Pull
                {ok, Event, Vals} = interclock:peek(Local, Key),
                peeranha_peer:sync(Remote, Key, Event, Vals), % can optimize based on result
                %% Read back and merge locally
                {ok, MergedEvent, MergedVals} = peeranha_peer:peek(Remote, Key),
                Res = interclock:sync(Local, Key, MergedEvent, MergedVals),
                %% Update the sync tree
                TreeVal = peeranha:peek(Local, Key),
                peeranha_sync:write(Local, Key, TreeVal),
                %% Build the diff list
                case peeranha:read(Local, Key) of
                    {conflict, _} ->
                        {conflict, Key};
                    {conflict, deleted, _} ->
                        {conflict, Key};
                    _ ->
                        {Res, Key}
                end
           end || Key <- Diff],
     lists:foldr(fun({conflict,Key}, {Merge,Conflict}) -> {Merge, [Key|Conflict]}
                 ;  ({_, Key}, {Merge,Conflict}) -> {[Key|Merge], Conflict} end,
                 {[],[]},
                 Ops).

%% TODO: allow to retire into another node
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

