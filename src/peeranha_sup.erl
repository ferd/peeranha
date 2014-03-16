-module(peeranha_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, peeranha}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
          [{peeranha_sync,
            {peeranha_sync, start_link, []},
            transient, 5000, worker, [peeranha_sync]}
          ]}}.

