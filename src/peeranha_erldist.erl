-module(peeranha_erldist).
-export([acquire/1, release/2, peek/2, sync/4, fork/1]).
-export([access/4]).

acquire({Node, Name}) ->
    rpc:call(Node, peeranha_sync, acquire, [Name]).

release({Node, Name}, Ref) ->
    rpc:call(Node, peeranha_sync, release, [Name, Ref]).

peek({Node, Name}, Key) ->
    rpc:call(Node, peeranha, peek, [Name, Key]).

sync({Node, Name}, Key, Event, Vals) ->
    rpc:call(Node, peeranha, sync, [Name, Key, Event, Vals]).

fork({Node, Name}) ->
    rpc:call(Node, peeranha, fork, [Name]).

access({Node, Name}, Ref, Op, Path) ->
    rpc:call(Node, peeranha_sync, access, [Name, Ref, Op, Path]).
