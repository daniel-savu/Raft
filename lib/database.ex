
# distributed algorithms, n.dulay 4 feb 2020
# coursework, raft consensus, v1

defmodule Database do

def start(config, server_id) do
  db = %{config: config, server_id: server_id, seqnum: 0, balances: Map.new}
  Database.next(db)
end # start

# setters
def seqnum(db, v), do: Map.put(db, :seqnum, v)
def balances(db, i, v), do:
    Map.put(db, :balances, Map.put(db.balances, i, v))

def next(db) do
  receive do
  { :EXECUTE, command } ->  # should send a result back, but assuming always okay
    { :move, amount, account1, account2 } = command

    balance1 = Map.get db.balances, account1, 0
    balance2 = Map.get db.balances, account2, 0
    db = Database.balances(db, account1, balance1 + amount)
    db = Database.balances(db, account2, balance2 - amount)

    db = Database.seqnum(db, db.seqnum + 1)

    Monitor.notify db, { :DB_UPDATE, db.server_id, db.seqnum, command }

    Database.next(db)
  unexpected ->
    Monitor.halt(db, "Database: unexpected message #{inspect unexpected}")
  end # receive
end # next

end # Database
