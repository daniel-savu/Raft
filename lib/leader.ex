
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consenus, v1

defmodule Leader do

    def start(s) do
        s = State.role(s, LEADER)
        broadcast_heartbeats(s)
        s = Follower.reset_election_timeout(s)
        IO.puts "found a leader #{inspect s.id} term #{s.curr_term}"

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

        Leader.next(s)
    end

    def next(s) do
        broadcast_heartbeats(s)
        s = Follower.reset_election_timeout(s)
        s = check_new_commit_index(s)
        s = Follower.commit_to_state_machine_if_needed(s)

        # random_failure(s)
        # random_sleep(s)

        receive do
            {:CLIENT_REQUEST, m} ->
                IO.puts "client req"
                clientP = m.clientP
                uid = m.uid
                cmd = m.cmd
                Monitor.notify s, { :CLIENT_REQUEST, clientP }

                # need to store client/uid/cmd pair so as to know which client to reply to
                # when entry is committed
                s = State.log(s, s.log ++ [{cmd, s.curr_term}])
                s = State.match_index(s, self(), Map.get(s.match_index, self()) + 1)
                # IO.puts "log: #{inspect s.log}"
                broadcast_heartbeats(s)
                next(s)

            {:VOTE_REQ, term, candidate_pid, id, last_log_term, last_log_index} ->
                if term > s.curr_term do
                    # IO.puts "will step down as leader"
                    s = Follower.stepdown(s, term)
                    s = Follower.vote_req_logic(s, term, candidate_pid, id, last_log_term, last_log_index)
                    Follower.next(s)
                end

            {:APPEND_REPLY, pid, term, success, index, timenow} ->
                # IO.puts "#{inspect pid} received #{inspect {:APPEND_REPLY, pid, term, success, index, timenow}}"
                s =
                    if term > s.curr_term do
                        s = Follower.stepdown(s, term)
                        Follower.start(s)
                    else
                        if term == s.curr_term do
                            s =
                                if success do
                                    # IO.puts "updating index #{index}"
                                    s = State.next_index(s, pid, index + 1)
                                    State.match_index(s, pid, index + 1)
                                else
                                    State.next_index(s, pid, max(1, Map.get(s.next_index, pid) - 1))
                                end
                            if Map.get(s.next_index, pid) < length(s.log) do
                                broadcast_heartbeats(s)
                            end
                            s
                        else
                            s
                        end
                    end
                # IO.puts "append_reply "
                next(s)

            {:APPEND_REQ, term, leader_pid, prev_log_index, prev_log_term, entries, leader_commit_index} ->
                if term > s.curr_term do
                    s = Follower.handle_append_request(s, term, leader_pid, prev_log_index, prev_log_term, entries, leader_commit_index)
                    Follower.start(s)
                else
                    next(s)
                end
            
            after s.refresh_rate -> 
                if s.role == LEADER do
                    next(s)
                end
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
        prev_log_index = min(Map.get(s.next_index, server), length(s.log))
        prev_log_index = max(prev_log_index, 0)
        prev_log_term = get_log_term(s, prev_log_index)

        # IO.puts "prevlogindex #{prev_log_index}"
        # IO.puts "#{inspect server} nextindex #{inspect s.next_index} #{Map.get(s.next_index, server)} #{length(s.log) - 1}; prevlogindex #{prev_log_index}"
        
        entries = Enum.slice(s.log, prev_log_index, length(s.log))
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

    def random_sleep(s) do
        coin = :rand.uniform(1000)
        if(coin > 993) do
            IO.puts "2s sleep leader #{s.id}"
            Process.sleep(2000)
        end
    end

    def get_log_term(s, index) do
        if index >= length(s.log) || index < 0 do
            0
        else
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

    def check_new_commit_index(s) do
        match_indexes = Enum.map(s.match_index, fn {_, i} -> i end)
        n = find_majority_el(s, Enum.max(match_indexes), match_indexes)
        term = get_log_term(s, n - 1)
        s =
            if term == s.curr_term do
                State.commit_index(s, n)
            else
                s
            end
        # IO.puts "match_index. leader: #{s.id} commit: #{s.commit_index}"
        # IO.inspect match_indexes
        s
    end

    def find_majority_el(s, n, list) do
        c = count_greater_than(list, n)
        if c >= s.majority do
            n
        else
            find_majority_el(s, n - 1, list)
        end
    end

    def count_greater_than([], _) do
        0
    end

    def count_greater_than([h|list], n) do
        if h >= n do
            1 + count_greater_than(list, n)
        else
            count_greater_than(list, n)
        end
    end

end

