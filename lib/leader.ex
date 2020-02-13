
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consenus, v1

defmodule Leader do

    def start(s) do
        IO.puts "found a leader #{inspect s.id} term #{s.curr_term}"
        s = State.role(s, LEADER)
        s = State.leader(s, s.selfP)
        new_next_index = Enum.reduce(
            s.servers, 
            s.next_index, 
            fn x, acc -> Map.put(acc, x, length(s.log) + 1) end
        )
        s = State.next_index(s, new_next_index)

        new_match_index = Enum.reduce(
            s.servers, 
            s.match_index, 
            fn x, acc -> Map.put(acc, x, 0) end
        )
        s = State.match_index(s, new_match_index)

        broadcast_heartbeats(s)
        Leader.next(s)
    end

    def next(s) do
        broadcast_heartbeats(s)
        # random_failure(s)

        receive do
            {:VOTE_REQ, term, candidate_pid, id} ->
                if term > s.curr_term do
                    # IO.puts "will step down as leader"
                    Follower.stepdown(s, term)
                end
            
            after s.refresh_rate -> next(s)
        end
    end

    def broadcast_heartbeats(s) do
        for server <- s.servers do
            if server != self() do
                send_append_entry(s, server)
            end
        end
    end

    def send_append_entry(s, server) do
        prev_log_index = min(Map.get(s.next_index, server), length(s.log) - 1) - 1
        prev_log_term = get_log_term(s, prev_log_index)
        
        entries = Enum.slice(s.log, prev_log_index, length(s.log) - prev_log_index)
        send server, {:APPEND_REQ, s.curr_term, self(), prev_log_index, prev_log_term, entries, s.commit_index}
    end

    # function used to test the resilience of the leader election implementation
    def random_failure(s) do
        coin = :rand.uniform(1000)
        if(coin > 992) do
            IO.puts "killed leader #{s.id}"
            Process.exit(self(), :kill)
        end
    end

    def get_log_term(s, index) do
        if index >= length(s.log) || index < 0 do
            0
        else
            IO.puts inspect s.log
            {_, last_log_term} = Enum.at(s.log, index)
            last_log_term
        end
    end
    
    def get_log_index(s) do
        if s.log == [] do
            0
        else
            last_log_index = length(s.log) - 1
            last_log_index
        end
    end

end

