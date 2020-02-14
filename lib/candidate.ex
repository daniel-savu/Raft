
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consenus, v1

defmodule Candidate do
    def start(s) do
        s = State.role(s, CANDIDATE)
        s = Follower.reset_election_timeout(s)
        s = State.curr_term(s, s.curr_term + 1)
        s = State.voted_for(s, s.id)
        s = State.votes_received(s, [s.id])
        last_log_term = Leader.get_log_term(s, length(s.log) - 1)
        last_log_index = Leader.get_log_index(s)
        IO.puts "candidate #{s.id}"
        for server <- s.servers do
            if server != self() do
                send server, {:VOTE_REQ, s.curr_term, self(), s.id, last_log_term, last_log_index}
            end
        end
        Candidate.next(s)
    end

    def next(s) do
        Follower.check_elapsed_election_time(s)
        s = Follower.commit_to_state_machine_if_needed(s)

        receive do
            # {:CLIENT_REQUEST, m} ->
            #     IO.puts "client req"
            #     next(s)
            {:VOTE_REPLY, term, voted_for, voter_pid, id} ->
                if term > s.curr_term do
                    # IO.puts "will stepdown from candidate #{s.id}"
                    s = Follower.stepdown(s, term)
                    Follower.start(s)
                end
                if term == s.curr_term do
                    s =
                        if voted_for == s.id do
                            State.votes_received(s, [id | s.votes_received])
                        else
                            s
                        end
                    IO.puts "#{s.id}: #{inspect s.votes_received} #{s.role} #{s.curr_term}"
                    if length(s.votes_received) >= s.majority do
                        Leader.start(s)
                    else
                        Candidate.next(s)
                    end
                end

            {:VOTE_REQ, term, candidate_pid, id, last_log_term, last_log_index} ->
                IO.puts "#{s.id}: received candidate: #{inspect id} #{s.role}"
                if term > s.curr_term do
                    s = Follower.stepdown(s, term)
                    Follower.vote_req_logic(s, term, candidate_pid, id, last_log_term, last_log_index)
                else 
                    if term == s.curr_term do
                        send candidate_pid, {:VOTE_REPLY, term, s.voted_for, self(), s.id}
                        Candidate.next(s)
                    end
                end

            {:APPEND_REQ, term, leader_pid, prev_log_index, prev_log_term, entries, leader_commit_index} ->
                # IO.puts "HB #{s.id} #{s.curr_term} #{term}"
                s = Follower.handle_append_request(s, term, leader_pid, prev_log_index, prev_log_term, entries, leader_commit_index)
                Follower.start(s)

            after s.refresh_rate -> 
                if s.role == CANDIDATE do
                    next(s)
                end
        end
    end

end

