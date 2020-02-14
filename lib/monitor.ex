
# Ioan-Daniel Savu (is319) 
# coursework, raft consensus, v1

defmodule Monitor do

def notify(s, message) do send s.config.monitorP, message end

def debug(s, string) do 
 if s.config.debug_level == 0 do IO.puts "server #{s.id} #{string}" end
end # debug

def debug(s, level, string) do 
 if level >= s.config.debug_level do IO.puts "server #{s.id} #{string}" end
end # debug

def pad(key), do: String.pad_trailing("#{key}", 10)

def state(s, level, string) do 
 if level >= s.config.debug_level do 
   state_out = for {key, value} <- s, into: "" do "\n  #{pad(key)}\t #{inspect value}" end
   IO.puts "\nserver #{s.id} #{s.role}: #{inspect s.selfP} #{string} state = #{state_out}"
 end # if
end # state

def halt(string) do
  IO.puts "monitor: #{string}"
  System.stop
end # halt

def halt(s, string) do
  IO.puts "server #{s.id} #{string}"
  System.stop
end # halt

def letter(s, letter) do 
  if s.config.debug_level == 3, do: IO.write(letter)
end # letter

def start(config) do
  state = %{
    config:             config,
    clock:              0,
    requests:           Map.new,
    updates:            Map.new,
    moves:              Map.new,
    match_indexes:      []
    # rest omitted
  }
  Process.send_after(self(), { :PRINT }, state.config.print_after)
  Monitor.next(state)
end # start

def clock(state, v), do: Map.put(state, :clock, v)

def requests(state, i, v), do: 
    Map.put(state, :requests, Map.put(state.requests, i, v))

def updates(state, i, v), do: 
    Map.put(state, :updates,  Map.put(state.updates, i, v))

def moves(state, v), do: Map.put(state, :moves, v)

def next(state) do
  receive do
  { :DB_move, db, seqnum, command} ->
    { :move, amount, from, to } = command

    done = Map.get(state.updates, db, 0)

    if seqnum != done + 1, do: 
       Monitor.halt "  ** error db #{db}: seq #{seqnum} expecting #{done+1}"

    moves =
      case Map.get(state.moves, seqnum) do
      nil ->
        # IO.puts "db #{db} seq #{seqnum} = #{done+1}"
        Map.put state.moves, seqnum, %{ amount: amount, from: from, to: to }

      t -> # already logged - check command
        if amount != t.amount or from != t.from or to != t.to, do:
	  Monitor.halt " ** error db #{db}.#{done} [#{amount},#{from},#{to}] " <>
            "= log #{done}/#{map_size(state.moves)} [#{t.amount},#{t.from},#{t.to}]"
        state.moves
      end # case

    state = Monitor.moves(state, moves)
    state = Monitor.updates(state, db, seqnum)
    Monitor.next(state)

  { :CLIENT_REQUEST, server_num } ->  # client requests seen by leaders
    state = Monitor.requests(state, server_num, Map.get(state.requests, server_num, 0) + 1)
    Monitor.next(state)

  { :MATCH_INDEXES, match_indexes } ->  # client requests seen by leaders
    state = Map.put(state, :match_indexes, match_indexes)
    Monitor.next(state)

  { :DB_UPDATE, server_id, seqnum, command } ->  # client requests seen by leaders
    state = Monitor.updates(state, server_id, Map.get(state.updates, server_id, 0) + 1)
    Monitor.next(state)

  { :PRINT } ->
    clock  = state.clock + state.config.print_after
    state  = Monitor.clock(state, clock)
    sorted = state.updates  |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock}      db updates done = #{inspect sorted}"
    sorted = state.requests |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock} client requests seen = #{inspect sorted}"
    IO.puts "time = #{clock}        match_indexes = #{inspect state.match_indexes}"

    if state.config.debug_level >= 0 do  # always
      min_done   = state.updates  |> Map.values |> Enum.min(fn -> 0 end)
      n_requests = state.requests |> Map.values |> Enum.sum
      IO.puts "time = #{clock}           total seen = #{n_requests} max lag = #{n_requests-min_done}"
    end

    IO.puts ""
    Process.send_after(self(), { :PRINT }, state.config.print_after)
    Monitor.next(state)

  # ** ADD ADDITIONAL MONITORING MESSAGES HERE

  unexpected ->
    Monitor.halt "monitor: unexpected message #{inspect unexpected}"
  end # receive
end # next

end # Monitor

