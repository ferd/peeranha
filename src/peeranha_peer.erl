-module(peeranha_peer).
-export([acquire/1, release/2, peek/2, sync/4, fork/1]).
-export([access/4]).

%% Lock the remote tree to prepare for a diffing
acquire({Mod, Addr}) ->
    Mod:acquire(Addr).

%% Diffing over
release({Mod, Addr}, Ref) ->
    Mod:release(Addr, Ref).

%% Look at a specific value
peek({Mod, Addr}, Key) ->
    Mod:peek(Addr, Key).

%% Force a merge on the value by sending the local one
sync({Mod, Addr}, Key, Event, Val) ->
    Mod:sync(Addr, Key, Event, Val).

%% Fork a DB to be copied from the remote version
fork({Mod, Addr}) ->
    Mod:fork(Addr).

%% Accessor function required by the diffing algorithm
access({Mod, Addr}, Ref, Op, Path) ->
    Mod:access(Addr, Ref, Op, Path).
