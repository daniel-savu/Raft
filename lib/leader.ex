
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consenus, v1

defmodule Leader do

    def start(s) do
        IO.puts "found a leader #{inspect s.id} term #{s.curr_term}"
        s = State.role(s, LEADER)
        s = State.leader(s, s.selfP)
        send_dummy_heartbeats(s)
        Leader.next(s)
    end

    def next(s) do
        send_dummy_heartbeats(s)
        # random_failure(s)

        receive do
            {:VOTE_REQ, term, candidate_pid, id} ->
                if term > s.curr_term do
                    Follower.stepdown(s, term)
                end
            
            after s.refresh_rate -> next(s)
        end
    end


    def send_append_entries(s, server) do
        # IO.puts inspect s.selfP
        send server, {:HEARTBEAT, s.curr_term, self()}
    end

    def send_dummy_heartbeats(s) do
        for server <- s.servers do
            send_append_entries(s, server) 
        end
    end

    # function used to test the resilience of the leader election implementation
    def random_failure(s) do
        coin = :rand.uniform(1000)
        if(coin > 992) do
            IO.puts "killed leader #{s.id}"
            Process.exit(self(), :kill)
        end
    end

end

