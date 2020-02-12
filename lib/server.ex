
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consenus, v1

defmodule Server do

# using: s for 'server/state', m for 'message'

def start(config, server_id, databaseP) do
    receive do
    { :BIND, servers } ->
        s = State.initialise(config, server_id, servers, databaseP)
        next(s)
    end # receive
end # start

def next(s) do
    # IO.puts "#{s.id} #{inspect s.next_election_time} #{:os.system_time(:millisecond)}"
    s = check_elapsed_election_time(s)
    s = check_rpc_timeouts(s)
    if s.role == LEADER do
        send_dummy_heartbeats(s)
        random_failure()
    end

    receive do
        {:HEARTBEAT, term, leader_pid} ->
            # IO.puts "HB #{s.id} #{s.curr_term} #{term}"
            s = handle_heartbeat(s, term, leader_pid)
            next(s)

        {:VOTE_REQ, term, candidate_pid, id} ->
            # IO.puts "received candidate: #{inspect candidate_pid}"
            s = handle_vote_request(s, term, candidate_pid, id)
            # IO.puts inspect s
            next(s)

        {:VOTE_REPLY, term, voted_for, voter_pid, id} ->
            # IO.puts inspect binding()
            # IO.puts "#{inspect id} voted for #{inspect voted_for} #{term == s.curr_term && s.role == CANDIDATE}"
            s = handle_vote_reply(s, term, voted_for, voter_pid, id)
            
            next(s)
        after s.refresh_rate -> next(s)
    end
end # next

def stepdown(s, term) do
    IO.puts "stepdown #{s.id}"
    s = State.curr_term(s, term)
    s = State.role(s, FOLLOWER)
    s = State.voted_for(s, nil)
    update_election_timeout(s)
end

def send_append_entries(s, server) do
    # IO.puts inspect s.selfP
    send server, {:HEARTBEAT, s.curr_term, self()}
end

def check_elapsed_election_time(s) do
    if s.next_election_time <= :os.system_time(:millisecond) do
        if Enum.member?([FOLLOWER, CANDIDATE], s.role) do
            IO.puts "#{inspect s.id} timeout term #{s.curr_term}"
            s = update_election_timeout(s)
            s = State.curr_term(s, s.curr_term + 1)
            s = State.role(s, CANDIDATE)
            s = State.voted_for(s, s.id)
            s = State.votes_received(s, [s.id])
            next_append_entries_timeout = Enum.reduce(s.servers, s.next_append_entries_timeout, fn x, acc ->
                Map.put(acc, x, :os.system_time(:millisecond))
            end)
            State.next_append_entries_timeout(s, next_append_entries_timeout)
        else
            s
        end
    else
        s
    end
end

def send_dummy_heartbeats(s) do
    for server <- s.servers do
        send_append_entries(s, server) 
    end
end

def handle_heartbeat(s, term, leader_pid) do
    s = State.leader(s, leader_pid)
    if term > s.curr_term do
        stepdown(s, term)
    else
        update_election_timeout(s)
    end
end

# function used to test the resilience of the leader election implementation
def random_failure() do
    coin = :rand.uniform(1000000)
    if(coin > 999998) do
        Process.exit(self(), :kill)
    end
end

def check_rpc_timeouts(s) do
    next_append_entries_timeout = Enum.reduce(s.servers, s.next_append_entries_timeout, fn server, acc ->
        if Map.get(s.next_append_entries_timeout, server) <= :os.system_time(:millisecond) && 
            Map.get(s.next_append_entries_timeout, server) >= 0 && server != self() do
            # send vote request
            send server, {:VOTE_REQ, s.curr_term, self(), s.id}
            Map.put(acc, server, :os.system_time(:millisecond) + s.config.append_entries_timeout)
        else
            acc
        end
    end)
    State.next_append_entries_timeout(s, next_append_entries_timeout)
end

def handle_vote_request(s, term, candidate_pid, id) do
    s = check_stepdown(s, term)
    # IO.puts "#{inspect s.id} #{term == s.curr_term && Enum.member?([candidate_pid, nil], s.voted_for)} #{inspect s.voted_for}"
    s =
    if term == s.curr_term && Enum.member?([id, nil], s.voted_for) do
        # IO.puts "#{inspect s.id} voted for #{inspect id}, term #{inspect term} #{inspect s.voted_for}"

        s = State.voted_for(s, id)
        send candidate_pid, {:VOTE_REPLY, term, s.voted_for, self(), s.id}
        update_election_timeout(s)
    else
        s
    end
end

def update_election_timeout(s) do
    new_election_timeout = :os.system_time(:millisecond) + 
        s.config.election_timeout + 
        :rand.uniform(s.config.election_timeout)
    State.next_election_time(s, new_election_timeout)
end

def handle_vote_reply(s, term, voted_for, voter_pid, id) do
    s = check_stepdown(s, term)
    if term == s.curr_term && s.role == CANDIDATE do
        next_append_entries_timeout = Map.put(s.next_append_entries_timeout, voter_pid, -1)
        s = State.next_append_entries_timeout(s, next_append_entries_timeout)
        s =
        if voted_for == s.id do
            State.votes_received(s, [id | s.votes_received])
        else
            s
        end
        IO.puts "#{s.id}: #{inspect s.votes_received}"
        s =
        if length(s.votes_received) >= s.majority do
            new_leader(s)
        else
            s
        end
        s
    else
        s
    end
end

def check_stepdown(s, term) do
    if term > s.curr_term do
        stepdown(s, term)
    else
        s
    end
end

def new_leader(s) do
    IO.puts "found a leader #{inspect s.id} term #{s.curr_term}"
    s = State.role(s, LEADER)
    s = State.leader(s, s.selfP)
    for server <- s.servers do
        send_append_entries(s, server)
    end
    next_append_entries_timeout = Enum.reduce(
        s.servers, 
        s.next_append_entries_timeout, 
        fn x, acc -> Map.put(acc, x, -1) end
    )
    State.next_append_entries_timeout(s, next_append_entries_timeout)
end

end # Server

