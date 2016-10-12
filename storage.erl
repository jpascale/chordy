%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%% storage
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(storage).
-export([create/0, add/3, lookup/2, split/3, merge/2]).
% Create a new store
create() -> [].

% Add a key value pair, return the updated
% store
add(Key, Value, Store) ->
	[{Key, Value} | Store].

% Return a tuple {Key, Value} or the atom false
lookup(Key, Store) ->
	case lists:keyfind(Key, 1, Store) of
		{Key, Value} ->	     
			Value;
		false ->
			false
	end.

% Return a tuple {Updated, Rest} where the
% updated store only contains the key value pairs requested and the rest
% are found in a list of key-value pairs
split(From, To, Store) ->
	lists:foldl(fun({Key, Value}, {Split1, Split2}) ->
						case node1:between(Key, To, From) of
							true ->
								{[{Key, Value} | Split1], Split2};
							false ->
								{Split1, [{Key, Value} | Split2]}
						end

					end, {[],[]}, Store).

% add a list of key-value pairs to a store
merge(Entries, Store) ->
	lists:append(Entries, Store).