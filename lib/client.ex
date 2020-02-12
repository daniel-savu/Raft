
# distributed algorithms, n.dulay, 4 feb 2020
# coursework, raft consensus, v1

defmodule Client do

def start(config, client_id, servers) do
  c = %{
    config:     config,
    id:         client_id,
    servers:    servers,
    leaderP:    Enum.random(servers),  # randomly pick a server
    cmd_seqnum: 0,
  }
  Monitor.debug(c, "Client #{client_id} at #{DAC.node_ip_addr}")
  Process.send_after(self(), { :CLIENT_STOP }, c.config.client_stop)
  Client.next(c)
end # start

def stop(c) do
   Monitor.halt "Client #{c.id} going to sleep, sent = #{c.cmd_seqnum}"
end # stop

def next(c) do
  receive do
  { :CLIENT_STOP } ->
    Client.stop(c)

  after c.config.client_sleep ->
    account1  = Enum.random 1 .. c.config.n_accounts
    account2  = Enum.random 1 .. c.config.n_accounts
    amount    = Enum.random 1 .. c.config.max_amount

    cmd  = { :move, amount, account1, account2 }
    c    = Map.put(c, :cmd_seqnum, c.cmd_seqnum + 1)
    uid  = { c.id, c.cmd_seqnum }              # unique id for cmd

    client_request = 
    client_request = { :CLIENT_REQUEST, %{clientP: self(), uid: uid, cmd: cmd } }

    {c, _client_result} = Client.send_request(c, client_request)  # result not used

    Client.next(c)
  end # receive
end # next

def send_request(c, client_request) do
  send c.leaderP, client_request

  receive do
  { :CLIENT_REPLY, result } ->
    c  = Map.put(c, :leaderP, result.leaderP)  # update leaderP
    if c.cmd_seqnum == c.config.client_requests, do: Client.stop(c)
    {c, result}

  { :CLIENT_STOP } -> Client.stop(c)

  after c.config.client_timeout ->
    # leader probably crashed, retry command with different server
    non_leader = for server <- c.servers, server != c.leaderP do server end
    c = Map.put(c, :leaderP, Enum.random(non_leader))
    {_c, _result} = Client.send_request(c, client_request)
  end # receive
end # send_command

end # Client

