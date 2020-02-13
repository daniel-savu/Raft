
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consenus, v1

defmodule Candidate do
    def start(s) do
        s = Follower.reset_election_timeout(s)
        s = State.curr_term(s, s.curr_term + 1)
        s = State.role(s, CANDIDATE)
        s = State.voted_for(s, s.id)
        s = State.votes_received(s, [s.id])
        for server <- s.servers do
            if server != self() do
                send server, {:VOTE_REQ, s.curr_term, self(), s.id}
            end
        end
        Candidate.next(s)
    end

    def next(s) do
        Follower.check_elapsed_election_time(s)

        receive do
            {:VOTE_REPLY, term, voted_for, voter_pid, id} ->
                if term > s.curr_term do
                    s = Follower.stepdown(s, term)
                    Follower.next(s)
                else
                    s =
                        if voted_for == s.id do
                            State.votes_received(s, [id | s.votes_received])
                        else
                            s
                        end
                    IO.puts "#{s.id}: #{inspect s.votes_received}"
                    if length(s.votes_received) >= s.majority do
                        Leader.start(s)
                    else
                        Candidate.next(s)
                    end
                end

            {:VOTE_REQ, term, candidate_pid, id} ->
                IO.puts "#{s.id}: received candidate: #{inspect id}"
                if term > s.curr_term do
                    Follower.vote_req_logic(s, term, candidate_pid, id)
                else 
                    if term == s.curr_term do
                        send candidate_pid, {:VOTE_REPLY, term, s.voted_for, self(), s.id}
                        Candidate.next(s)
                    end
                end

            {:HEARTBEAT, term, leader_pid} ->
                # IO.puts "HB #{s.id} #{s.curr_term} #{term}"
                s = Follower.handle_heartbeat(s, term, leader_pid)
                Follower.next(s)

            after s.refresh_rate -> next(s)
        end
    end

end

