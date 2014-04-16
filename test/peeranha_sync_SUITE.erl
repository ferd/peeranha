-module(peeranha_sync_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [build, diff1, diff2, diff3,
     pull1, pull2].

init_per_testcase(build, Config) ->
    {ok, Started} = application:ensure_all_started(peeranha),
    FakeUUID = term_to_binary({self(), os:timestamp()}),
    Name = "build",
    Path = filename:join(?config(priv_dir, Config), Name),
    [{started, Started},
     {uuid, FakeUUID},
     {name, Name},
     {dir, Path} | Config];
init_per_testcase(Case, Config) ->
    {ok, Started} = application:ensure_all_started(peeranha),
    FakeUUID = term_to_binary({self(), os:timestamp()}),
    Name = unicode:characters_to_list(atom_to_binary(Case, utf8)),
    ForkName = Name ++ "_fork",
    Path = filename:join(?config(priv_dir, Config), Name),
    PathAlt = filename:join(?config(priv_dir, Config), ForkName),
    ok = peeranha:boot(Name, [{uuid, FakeUUID}, {name, Name}, {dir, Path}, {type, root}]),
    {FakeUUID, NewId} = interclock:fork(Name), % to replace by proper replication mechanism
    ok = peeranha:boot(ForkName, [{type, normal}, {dir, PathAlt},
                                  {uuid, FakeUUID}, {id, NewId}]),
    [{started, Started},
     {uuid, FakeUUID},
     {name, Name},
     {name_fork, ForkName}| Config].

end_per_testcase(_, Config) ->
    [application:stop(Start) || Start <- ?config(started, Config)].

%%%===================================================================
%%% Test cases
%%%===================================================================

build(Config) ->
    UUID = ?config(uuid, Config),
    Name = ?config(name, Config),
    Path = ?config(dir, Config),
    ok = peeranha:boot(Name, [{uuid, UUID}, {name, Name}, {dir, Path},
                              {type, root}]),
    peeranha:write(Name, <<"key">>, val),
    {ok, val} = peeranha:read(Name, <<"key">>).

diff1(Config) ->
    %% Create a bunch of entries. One is the same on both
    %% nodes (albeit with a different clock), two are
    %% different (one on each) and one conflicts.
    Name = ?config(name, Config),
    NameFork = ?config(name_fork, Config),
    peeranha:write(Name, <<"key1">>, val),
    peeranha:write(Name, <<"key2">>, val),
    peeranha:write(Name, <<"key4">>, val),
    peeranha:write(NameFork, <<"key1">>, val),
    peeranha:write(NameFork, <<"key3">>, val),
    peeranha:write(NameFork, <<"key4">>, otherval),
    %% Syncing takes place
    Sync = peeranha:sync(Name, {peeranha_erldist, {node(), NameFork}}),
    {[<<"key1">>,<<"key2">>,<<"key3">>],[<<"key4">>]} = Sync,
    {ok, val} = peeranha:read(Name, <<"key1">>),
    {ok, val} = peeranha:read(Name, <<"key2">>),
    {ok, val} = peeranha:read(Name, <<"key3">>),
    {conflict, [otherval,val]} = peeranha:read(Name, <<"key4">>),
    %% Nothing needs to be re-sync'd.
    {[],[]} = peeranha:sync(Name, {peeranha_erldist, {node(), NameFork}}),
    {ok, val} = peeranha:read(Name, <<"key1">>),
    {ok, val} = peeranha:read(Name, <<"key2">>),
    {ok, val} = peeranha:read(Name, <<"key3">>),
    {conflict, [otherval,val]} = peeranha:read(Name, <<"key4">>),
    %% Crush the conflict
    peeranha:write(Name, <<"key4">>, crushed),
    {[<<"key4">>],[]} = peeranha:sync(Name, {peeranha_erldist, {node(), NameFork}}),
    {ok, crushed} = peeranha:read(Name, <<"key4">>).

diff2(Config) ->
    %% Create a bunch of entries. One is the same on both
    %% nodes (albeit with a different clock), one conflicts.
    Name = ?config(name, Config),
    NameFork = ?config(name_fork, Config),
    peeranha:write(Name, <<"key1">>, val),
    peeranha:write(Name, <<"key2">>, val),
    peeranha:write(NameFork, <<"key1">>, val),
    peeranha:write(NameFork, <<"key2">>, otherval),
    %% Syncing takes place
    Sync = peeranha:sync(Name, {peeranha_erldist, {node(), NameFork}}),
    {[<<"key1">>],[<<"key2">>]} = Sync,
    {ok, val} = peeranha:read(Name, <<"key1">>),
    {conflict, [otherval,val]} = peeranha:read(Name, <<"key2">>),
    %% Now we can modify key1 and expect the merge to take place.
    %% Because both values are the same before the change,
    %% We don't want a conflict after syncing.
    peeranha:write(Name, <<"key1">>, newval),
    %% conflicts aren't mentioned again
    {[<<"key1">>],[]} = peeranha:sync(Name, {peeranha_erldist, {node(), NameFork}}),
    {ok, newval} = peeranha:read(Name, <<"key1">>),
    {ok, _, [newval]} = peeranha_peer:peek({peeranha_erldist, {node(), NameFork}}, <<"key1">>),
    peeranha:delete(Name, <<"key2">>),
    {[<<"key2">>],[]} = peeranha:sync(Name, {peeranha_erldist, {node(), NameFork}}).

diff3(Config) ->
    %% Deletes syncing. Two present keys, while one is deleted
    %% and one is modified being merged result in a conflict
    %% no matter the order of operations if they weren't sync'd first
    Name = ?config(name, Config),
    NameFork = ?config(name_fork, Config),
    peeranha:write(Name, <<"key1">>, val),
    peeranha:write(Name, <<"key2">>, val),
    peeranha:write(Name, <<"key3">>, val),
    peeranha:delete(Name, <<"key3">>),
    peeranha:write(NameFork, <<"key1">>, val),
    peeranha:write(NameFork, <<"key2">>, val),
    peeranha:write(NameFork, <<"key3">>, val),
    peeranha:delete(NameFork, <<"key2">>),
    peeranha:delete(NameFork, <<"key3">>),
    {[<<"key1">>,<<"key3">>],[<<"key2">>]} = peeranha:sync(Name, {peeranha_erldist, {node(), NameFork}}),
    {[],[]} = peeranha:sync(Name, {peeranha_erldist, {node(), NameFork}}),
    {conflict, deleted, [val]} = peeranha:read(Name, <<"key2">>),
    {error, undefined} = peeranha:read(Name, <<"key3">>),
    peeranha:delete(Name, <<"key1">>),
    peeranha:delete(Name, <<"key2">>),
    {[<<"key1">>, <<"key2">>], []} = peeranha:sync(Name, {peeranha_erldist, {node(), NameFork}}),
    {error, undefined} = peeranha:read(Name, <<"key1">>),
    {error, undefined} = peeranha:read(Name, <<"key2">>),
    {error, undefined} = peeranha:read(Name, <<"key3">>).

pull1(Config) ->
    %% Create a bunch of entries. One is the same on both
    %% nodes (albeit with a different clock), two are
    %% different (one on each) and one conflicts.
    Name = ?config(name, Config),
    NameFork = ?config(name_fork, Config),
    peeranha:write(Name, <<"key1">>, val),
    peeranha:write(Name, <<"key2">>, val),
    peeranha:write(Name, <<"key4">>, val),
    peeranha:write(NameFork, <<"key1">>, val),
    peeranha:write(NameFork, <<"key3">>, val),
    peeranha:write(NameFork, <<"key4">>, otherval),
    %% Pulling takes place
    Pre = fun(Win, Key, LocVal, PeerVal) ->
            ct:pal("Pre: ~p", [{Win,Key,LocVal,PeerVal}])
    end,
    Post = fun(Key, Final) ->
            ct:pal("Post: ~p", [{Key,Final}])
    end,
    Pull = peeranha:pull(Name, {peeranha_erldist, {node(), NameFork}}, Pre, Post),
    {[<<"key1">>,<<"key3">>],[<<"key4">>],[<<"key2">>]} = Pull,
    {ok, val} = peeranha:read(Name, <<"key1">>),
    {ok, val} = peeranha:read(Name, <<"key2">>),
    {ok, val} = peeranha:read(Name, <<"key3">>),
    {conflict, Conflict} = peeranha:read(Name, <<"key4">>),
    ?assert(lists:member(val, Conflict)),
    ?assert(lists:member(otherval, Conflict)),
    %% Nothing needs to be pulled back, but we still end up re-comparing
    %% all keys because the other end has not sync'd back. Because this is
    %% a pull mechanism instead of a push/pull like sync/2, this will happen
    %% until the other node has pulled from this one.
    {[<<"key1">>],[<<"key4">>],[<<"key2">>]} = peeranha:pull(Name, {peeranha_erldist, {node(), NameFork}},Pre,Post),
    {ok, val} = peeranha:read(Name, <<"key1">>),
    {ok, val} = peeranha:read(Name, <<"key2">>),
    {ok, val} = peeranha:read(Name, <<"key3">>),
    {conflict, Conflict} = peeranha:read(Name, <<"key4">>),
    ?assert(lists:member(val, Conflict)),
    ?assert(lists:member(otherval, Conflict)),
    %% Crush the conflict
    peeranha:write(Name, <<"key4">>, crushed),
    %% this one pull is also done without hooks -- should have no impact
    {[<<"key1">>,<<"key4">>],[],[<<"key2">>]} = peeranha:pull(Name, {peeranha_erldist, {node(), NameFork}}),
    {ok, crushed} = peeranha:read(Name, <<"key4">>),
    %% Pull from the remote node and the conflicts should go away
    {[<<"key1">>,<<"key2">>,<<"key4">>],[],[]} = peeranha:pull(NameFork, {peeranha_erldist, {node(), Name}},Pre,Post),
    {[],[],[]} = peeranha:pull(Name, {peeranha_erldist, {node(), NameFork}},Pre,Post).

pull2(Config) ->
    %% Create a bunch of entries. Two are the same on both
    %% nodes (albeit with a different clock), one is
    %% different (on the peer) and one conflicts.
    Name = ?config(name, Config),
    NameFork = ?config(name_fork, Config),
    peeranha:write(Name, <<"key1">>, val),
    peeranha:write(Name, <<"key2">>, val),
    peeranha:write(Name, <<"key4">>, val),
    peeranha:write(NameFork, <<"key1">>, val),
    peeranha:write(NameFork, <<"key2">>, val),
    peeranha:write(NameFork, <<"key3">>, val),
    peeranha:write(NameFork, <<"key4">>, otherval),
    peeranha:write(NameFork, <<"key5">>, otherval),
    %% Pulling takes place
    Pre = fun(conflict, _Key, _LocVal, _PeerVal) -> skip
          ;  (peer, _Key, _LocVal, {ok,otherval}) -> skip
          ;  (peer, _Key, _LocVal, _PeerVal) -> ok
          ;  (local, <<"key1">>, _LocVal, _PeerVal) -> skip
          ;  (local, <<"key2">>, _LocVal, _PeerVal) -> ok
    end,
    Post = fun(Key, Final) ->
            ct:pal("Post: ~p", [{Key,Final}])
    end,
    Pull = peeranha:pull(Name, {peeranha_erldist, {node(), NameFork}}, Pre, Post),
    {[<<"key2">>,<<"key3">>],[],[<<"key1">>,<<"key4">>,<<"key5">>]} = Pull,
    {ok, val} = peeranha:read(Name, <<"key1">>),
    {ok, val} = peeranha:read(Name, <<"key2">>),
    {ok, val} = peeranha:read(Name, <<"key3">>),
    {ok, val} = peeranha:read(Name, <<"key4">>),
    {error, undefined} = peeranha:read(Name, <<"key5">>),
    {_,[<<"key4">>],_} = peeranha:pull(Name, {peeranha_erldist, {node(), NameFork}}),
    {conflict, Conflict} = peeranha:read(Name, <<"key4">>),
    ?assert(lists:member(val, Conflict)),
    ?assert(lists:member(otherval, Conflict)),
    Pre2 = fun(local, <<"key4">>, {conflict,[_,_]}, {ok,otherval}) -> skip
           ;  (_, Key, _, _) when Key =/= <<"key4">> -> skip
           ;  (A, B, C, D) -> error({A,B,C,D})
    end,
    peeranha:pull(Name, {peeranha_erldist, {node(), NameFork}}, Pre2, Post),
    peeranha:delete(NameFork, <<"key4">>),
    Pre3 = fun(conflict, <<"key4">>, {conflict,[_,_]}, undefined) -> ok
           ;  (_, Key, _, _) when Key =/= <<"key4">> -> skip
           ;  (A, B, C, D) -> error({A,B,C,D})
    end,
    {_,[<<"key4">>],_} = peeranha:pull(Name, {peeranha_erldist, {node(), NameFork}}, Pre3, Post),
    Pre4 = fun(local, <<"key4">>, {conflict,deleted,[_,_]}, undefined) -> ok
           ;  (_, Key, _, _) when Key =/= <<"key4">> -> skip
           ;  (A, B, C, D) -> error({A,B,C,D})
    end,
    %% Even though the local value is higher, this remains a conflict
    {_,[<<"key4">>],_} = peeranha:pull(Name, {peeranha_erldist, {node(), NameFork}}, Pre4, Post),
    %% Remote pull
    Pre5 = fun(peer, <<"key4">>, undefined, {conflict,deleted,[_,_]}) -> ok
           ;  (_, Key, _, _) when Key =/= <<"key4">> -> skip
           ;  (A, B, C, D) -> error({A,B,C,D})
    end,
    {_,[<<"key4">>],_} = peeranha:pull(NameFork, {peeranha_erldist, {node(), Name}}, Pre5, Post).

