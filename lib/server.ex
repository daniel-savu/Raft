
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consenus, v1

defmodule Server do

# using: s for 'server/state', m for 'message'

def start(config, server_id, databaseP) do
    receive do
    { :BIND, servers } ->
        s = State.initialise(config, server_id, servers, databaseP)
        Follower.next(s)
    end # receive
end # start

def next(s) do
    # IO.puts "#{s.id} #{inspect s.next_election_time} #{:os.system_time(:millisecond)}"

end # next

end # Server

