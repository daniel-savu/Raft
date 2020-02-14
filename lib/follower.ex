
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consenus, v1

defmodule Follower do
    def start(s) do
        s = State.role(s, FOLLOWER)
        next(s)
    end

    def next(s) do
        Follower.check_elapsed_election_time(s)
        s = Follower.commit_to_state_machine_if_needed(s)
        receive do
            {:VOTE_REQ, term, candidate_pid, id, last_log_term, last_log_index} ->
                s = vote_req_logic(s, term, candidate_pid, id, last_log_term, last_log_index)
                next(s)

            {:APPEND_REQ, term, leader_pid, prev_log_index, prev_log_term, entries, leader_commit_index} ->
                # IO.puts "HB #{s.id} #{s.curr_term} #{term}"
                # s = handle_heartbeat(s, term, leader_pid)
                s = handle_append_request(s, term, leader_pid, prev_log_index, prev_log_term, entries, leader_commit_index)
                next(s)
            
            after s.refresh_rate -> 
                if s.role == FOLLOWER do
                    next(s)
                end
        end
    end

    def vote_req_logic(s, term, candidate_pid, id, last_log_term, last_log_index) do
        s = check_stepdown(s, term)
        IO.puts "#{s.id} follower: received candidate: #{inspect id} #{s.role}"
        log_term = Leader.get_log_term(s, length(s.log) - 1)
        s =
            if term == s.curr_term && Enum.member?([id, nil], s.voted_for) && 
            (last_log_term > log_term || 
            (last_log_term == log_term && last_log_index >= length(s.log) - 1))
            do
                s = State.voted_for(s, id)
                # IO.puts "#{inspect s.id} voted for #{inspect id}, term #{inspect term}"
                send candidate_pid, {:VOTE_REPLY, term, s.voted_for, self(), s.id}
                s = reset_election_timeout(s)
            else
                s
            end
        s
    end

    def handle_append_request(s, term, leader_pid, prev_log_index, prev_log_term, entries, leader_commit_index) do
        s = check_stepdown(s, term)
        s = 
            if term < s.curr_term do
                send leader_pid, {:APPEND_REPLY, s.curr_term, false, 0}
                s
            else
                s = State.leader(s, leader_pid)
                # s = reset_election_timeout(s)
                # IO.puts "LENGTHS #{prev_log_index} #{length(s.log) - 1}"
                success = 
                    if prev_log_index == 0 do
                        true
                    else 
                        if prev_log_index < length(s.log) - 1 do
                            term = Leader.get_log_term(s, prev_log_index - 1)
                            term == prev_log_term
                        else
                            false
                        end
                    end
                result =
                    if success do
                        # IO.puts "success"
                        {store_entries(s, prev_log_index, entries, leader_commit_index), s}
                        # {0, s}
                    else
                        {{0, s}}
                    end

                index = elem(elem(result,0),0)
                s = elem(elem(result,0),1)
                # IO.puts "#{s.id} #{inspect s.log}"
                # IO.puts "#{s.id} sucess #{inspect success} index: #{inspect index}"
                timenow = :os.system_time(:millisecond)
                # IO.puts "#{s.id} #{inspect {:APPEND_REPLY, self(), s.curr_term, success, index, timenow}}"

                send leader_pid, {:APPEND_REPLY, self(), s.curr_term, success, index, timenow}
                handle_heartbeat(s, term, leader_pid)
            end
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

    def check_elapsed_election_time(s) do
        if s.next_election_time <= :os.system_time(:millisecond) do
            IO.puts "#{inspect s.id} election timeout elapsed term #{s.curr_term}"
            Candidate.start(s)
        end
    end

    def reset_election_timeout(s) do
        # IO.puts "resetting timeout #{s.id}"
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

    def store_entries(s, prev_log_index, entries, leader_commit_index) do
        log_to_keep = Enum.slice(s.log, 0, prev_log_index)
        rest_of_log = Enum.slice(s.log, prev_log_index, length(entries))
        new_log = store_entries(entries, log_to_keep, rest_of_log)
        # IO.puts "#{s.id} entries:#{inspect entries} #{prev_log_index}"
        # IO.puts "#{s.id} log to keep:#{inspect log_to_keep}; rest_of_log: #{inspect rest_of_log}; prevlog: #{prev_log_index}"
        # IO.puts "#{s.id} new log:#{inspect new_log}"
        s = State.log(s, new_log)
        commit_index = 
            if leader_commit_index > s.commit_index do
                min(leader_commit_index, length(new_log) - 1)
            else
                s.commit_index
            end
        s = State.commit_index(s, commit_index)
        {length(new_log) - 1, s}
    end
    
    def store_entries(entries, log_acc, []) do
        log_acc ++ entries
    end
    
    def store_entries([entries_iterator|entries], log_acc, [log_iterator|rest_of_log]) do
        {_, log_term} = log_iterator
        {_, entry_term} = entries_iterator
        # if log_term != entry_term do
        store_entries(entries, log_acc ++ [entries_iterator], rest_of_log)
    end

    def commit_to_state_machine_if_needed(s) do
        if s.commit_index > s.last_applied do
            {cmd, _} = Enum.at(s.log, s.last_applied)
            s = State.last_applied(s, s.last_applied + 1)
            IO.puts "#{inspect cmd}"
            send s.databaseP, {:EXECUTE, cmd}
            s
        else
            s
        end
    end
end

