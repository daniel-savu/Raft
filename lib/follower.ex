
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consenus, v1

defmodule Follower do


    def next(s) do
        Follower.check_elapsed_election_time(s)

        receive do
            {:VOTE_REQ, term, candidate_pid, id} ->
                vote_req_logic(s, term, candidate_pid, id)

            {:HEARTBEAT, term, leader_pid} ->
                # IO.puts "HB #{s.id} #{s.curr_term} #{term}"
                s = handle_heartbeat(s, term, leader_pid)
                next(s)
            
            after s.refresh_rate -> next(s)
        end
    end

    def vote_req_logic(s, term, candidate_pid, id) do
        s = check_stepdown(s, term)
        IO.puts "#{s.id} follower: received candidate: #{inspect id}"
        if term == s.curr_term && Enum.member?([id, nil], s.voted_for) do
            s = State.voted_for(s, id)
            IO.puts "#{inspect s.id} voted for #{inspect id}, term #{inspect term}"
            send candidate_pid, {:VOTE_REPLY, term, s.voted_for, self(), s.id}
            s = reset_election_timeout(s)
            Follower.next(s)
        end
    end

    def check_elapsed_election_time(s) do
        if s.next_election_time <= :os.system_time(:millisecond) do
            IO.puts "#{inspect s.id} election timeout elapsed term #{s.curr_term}"
            Candidate.start(s)
        end
    end

    def reset_election_timeout(s) do
        new_election_timeout = :os.system_time(:millisecond) + 
            s.config.election_timeout + 
            :rand.uniform(s.config.election_timeout)
        State.next_election_time(s, new_election_timeout)
    end

    def check_stepdown(s, term) do
        if term > s.curr_term do
            stepdown(s, term)
        else
            s
        end
    end

    def stepdown(s, term) do
        IO.puts "stepdown #{s.id}"
        s = State.curr_term(s, term)
        s = State.role(s, FOLLOWER)
        s = State.voted_for(s, nil)
        s = reset_election_timeout(s)
        s
    end

    def handle_heartbeat(s, term, leader_pid) do
        s = State.leader(s, leader_pid)
        if term > s.curr_term do
            stepdown(s, term)
        else
            reset_election_timeout(s)
        end
    end
end

