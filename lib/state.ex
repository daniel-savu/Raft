
# Ioan-Daniel Savu (is319) 
# coursework, raft consensus, v1

defmodule State do

# *** short-names: s for 'server', m for 'message'

def initialise(config, server_id, servers, databaseP) do
  %{
    config:       config,     # system configuration parameters (from DAC)
    selfP:        self(),     # server's process id
    id:	          server_id,  # server's id (simple int)
    servers:      servers,    # list of process id's of servers
    databaseP:    databaseP,  # process id of local database
    majority:     div(length(servers), 2) + 1,
    votes:        0,          # count of votes incl. self

    # -- various process id's - omitted

    # -- raft persistent data
    # -- update on stable storage before replying to requests
    curr_term:	  0,
    voted_for:	  nil,
    log:          [],  

    # -- raft non-persistent data
    role:	  FOLLOWER,
    commit_index: 0,
    last_applied: 0,
    next_index:   Map.new,   
    match_index:  Map.new,   
 
    # add additional state variables of interest
    leader: nil,
    next_election_time: :os.system_time(:millisecond) + config.election_timeout + :rand.uniform(config.election_timeout),
    votes_received: [],
    refresh_rate: round(config.election_timeout / 10),
  }
end # initialise

# setters
def votes(s, v),          do: Map.put(s, :votes, v)

# setters for raft state
def role(s, v),           do: Map.put(s, :role, v)
def curr_term(s, v),      do: Map.put(s, :curr_term, v)
def voted_for(s, v),      do: Map.put(s, :voted_for, v)
def commit_index(s, v),   do: Map.put(s, :commit_index, v)
def last_applied(s, v),   do: Map.put(s, :last_applied, v)
def next_index(s, v),     do: Map.put(s, :next_index, v)      # sets new next_index map
def next_index(s, i, v),  do: Map.put(s, :next_index,
                                  Map.put(s.next_index, i, v))
def match_index(s, v),    do: Map.put(s, :match_index, v)     # sets new  match_index map
def match_index(s, i, v), do: Map.put(s, :match_index,
                                  Map.put(s.match_index, i, v))

# add additional setters 
def leader(s, v),           do: Map.put(s, :leader, v)
def next_election_time(s, v),           do: Map.put(s, :next_election_time, v)
def votes_received(s, v),           do: Map.put(s, :votes_received, v)
def log(s, v), do: Map.put(s, :log, v)

end # State
