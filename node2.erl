-module(node2).
-export([start/1, start/2]).
-define(Stabilize, 1000).
-define(Timeout, 10000).

start(Id) ->
	start(Id, nil).

start(Id, Peer) ->
	timer:start(),
	spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
	Predecessor = nil,
	{ok, Successor} = connect(Id, Peer),
	schedule_stabilize(),
	node(Id, Predecessor, Successor, storage:create()).

connect(Id, nil) -> % We are alone
	{ok, {Id, self()}};
connect(_, Peer) ->
	Qref = make_ref(),
	Peer ! {key, Qref, self()},
	receive
		{Qref, Skey} ->
			{ok, {Skey, Peer}}

		after ?Timeout ->
			io:format("Time out: no response~n",[])
	end.



% Generates a random number between 1 and 1000000
generate() -> random:uniform(1000000000).

between(Key, From, To) ->
	if
		From<To ->
			(Key>From) and (Key=<To);
		From>To ->
			(Key>From) or (Key=<To);
		true ->
			true
	end.

node(Id, Predecessor, Successor, Store) ->
	receive
		% A peer needs to know our key
		{key, Qref, Peer} ->
			Peer ! {Qref, Id},
			node(Id, Predecessor, Successor, Store);

		% A new node tells "I'm your predecessor", we do not trust
		{notify, New} ->
			{Nkey, _} = New,
			io:format("Node ~p: Node ~p tells me i'm your predecessor.~n", [Id, Nkey]),
			{Pred, St} = notify(New, Id, Predecessor, Store),
			node(Id, Pred, Successor, St);

		% A node(a predecessor) wants to know our predecessor
		{request, Peer} ->
			request(Peer, Predecessor),
			node(Id, Predecessor, Successor, Store);

		% Our successor informs us about its predecessor
		{status, Pred} ->
			Succ = stabilize(Pred, Id, Successor),
			node(Id, Predecessor, Succ, Store);

		stabilize ->
			stabilize(Successor),
			node(Id, Predecessor, Successor, Store);

		{add, Key, Value, Qref, Client} ->
			Added = add(Key, Value, Qref, Client,
			Id, Predecessor, Successor, Store),
			node(Id, Predecessor, Successor, Added);

		{lookup, Key, Qref, Client} ->
			lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
			node(Id, Predecessor, Successor, Store);

		{handover, Elements} ->
			Merged = storage:merge(Store, Elements),
			node(Id, Predecessor, Successor, Merged);

		probe ->
			create_probe(Id, Successor),
			node(Id, Predecessor, Successor, Store);

		{probe, Id, Nodes, T} ->
			remove_probe(T, Nodes),
			node(Id, Predecessor, Successor, Store);

		{probe, Ref, Nodes, T} ->
			forward_probe(Ref, T, Nodes, Id, Successor),
			node(Id, Predecessor, Successor, Store)
	end.
%%TODO: Check
add(Key, Value, Qref, Client,
				 Id, {Pkey, _}, {_, Spid}, Store) ->
	case between(Key, Pkey, Id) of
		true ->
			Client ! {Qref, ok},
			storage:add(Key, Value, Store);
		
		false ->
			Spid ! {add, Key, Value, Qref, Client},
			Store
	end.

%%TODO: Check
lookup(Key, Qref, Client, Id, {Pkey, _}, Successor, Store) ->
	case between(Key, Pkey, Id) of
		true ->
			Result = storage:lookup(Key, Store),
			Client ! {Qref, Result};
		false ->
			{_, Spid} = Successor,
			Spid ! {lookup, Key, Qref, Client}
	end.

stabilize(Pred, Id, Successor) ->
	{Skey, Spid} = Successor,
	
	case Pred of
		nil ->
			Spid ! {notify, {Id, self()}},
	    	Successor;
		
		{Id, _} ->
			Successor;
		
		{Skey, _} ->
			Spid ! {notify, {Id, self()}},
			Successor;

		{Xkey, Xpid} ->
			case between(Xkey, Id, Skey) of
				true ->
					Xpid ! {request, self()}, %Run stabilization again.
		    		Pred; %We adopt out successor's predecessor as our successor.

				false -> %we should be inbetween this nodes
					Spid ! {notify, {Id, self()}},
					Successor
			end

	end.

schedule_stabilize() ->
	timer:send_interval(?Stabilize, self(), stabilize).

stabilize({_, Spid}) ->
	Spid ! {request, self()}.

request(Peer, Predecessor) ->
	case Predecessor of
		nil ->
			Peer ! {status, nil};
		{Pkey, Ppid} ->
			Peer ! {status, {Pkey, Ppid}}
	end.

%%%%TODO: RE CONTRA CHECK
notify({Nkey, Npid}, Id, Predecessor, Store) ->
	case Predecessor of
		nil ->
			Keep = handover(Id, Store, Nkey, Npid),
			{{Nkey, Npid}, Keep};

	{Pkey, _} ->
		case between(Nkey, Pkey, Id) of
			true ->
				Keep = handover(Id, Store, Nkey, Npid),
				{{Nkey, Npid}, Keep};
			false ->
				{Predecessor, Store}
		end
	end.

%storage:split(1, 5, [{1,a},{2,a},{3,a},{4,a},{5,a},{6,a},{7,a},{8,a},{9,a},{10,a}]).
%{[{10,a},{9,a},{8,a},{7,a},{6,a},{1,a}],
% [{5,a},{4,a},{3,a},{2,a}]}
%23> storage:split(5, 1, [{1,a},{2,a},{3,a},{4,a},{5,a},{6,a},{7,a},{8,a},{9,a},{10,a}]).
%{[{5,a},{4,a},{3,a},{2,a}],
% [{10,a},{9,a},{8,a},{7,a},{6,a},{1,a}]}

handover(Id, Store, Nkey, Npid) ->
	{Keep, Rest} = storage:split(Id, Nkey, Store), %%%%%%%%%TODO: Check % Creo que esta bien
	Npid ! {handover, Rest},
	Keep.

create_probe(Id, {_, Spid}) ->
	Spid ! {probe, Id, [Id], erlang:system_time(micro_seconds)}.

remove_probe(T, Nodes) ->
	Duration = erlang:system_time(micro_seconds) - T,
	Printer = fun(E) -> io:format("~p ", [E]) end,
	lists:foreach(Printer, Nodes),
	io:format("~n ", []),
	io:format("~n Time = ~p~n", [Duration]).

forward_probe(Ref, T, Nodes, Id, {_, Spid}) ->
	Spid ! {probe, Ref, Nodes ++ [Id], T}.
