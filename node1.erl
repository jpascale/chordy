-module(node1).
-export([start/1, start/2, between/3]).
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
	node(Id, Predecessor, Successor).

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

node(Id, Predecessor, Successor) ->
	receive
		% A peer needs to know our key
		{key, Qref, Peer} ->
			Peer ! {Qref, Id},
			node(Id, Predecessor, Successor);

		% A new node tells "I'm your predecessor", we do not trust
		{notify, New} ->
			{Nkey, _} = New,
			io:format("Node ~p: Node ~p tells me i'm your predecessor", [Id, Nkey]),
			Pred = notify(New, Id, Predecessor),
			node(Id, Pred, Successor);

		% A node(a predecessor) wants to know our predecessor
		{request, Peer} ->
			request(Peer, Predecessor),
			node(Id, Predecessor, Successor);

		% Our successor informs us about its predecessor
		{status, Pred} ->
			Succ = stabilize(Pred, Id, Successor),
			node(Id, Predecessor, Succ);

		stabilize ->
			stabilize(Successor),
			node(Id, Predecessor, Successor);

		probe ->
			create_probe(Id, Successor),
			node(Id, Predecessor, Successor);

		{probe, Id, Nodes, T} ->
			remove_probe(T, Nodes),
			node(Id, Predecessor, Successor);

		{probe, Ref, Nodes, T} ->
			forward_probe(Ref, T, Nodes, Id, Successor),
			node(Id, Predecessor, Successor)
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

notify({Nkey, Npid}, Id, Predecessor) ->
	case Predecessor of
		nil ->
			{Nkey, Npid};

	{Pkey, _} ->
		case between(Nkey, Pkey, Id) of
			true ->
				{Nkey, Npid};
			false ->
				Predecessor
		end
	end.


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