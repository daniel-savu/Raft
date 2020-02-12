
# distributed algorithms, n.dulay, 4 feb 2020
# Makefile, raft consensus, v1

SERVERS  = 5
CLIENTS  = 5
START    = Raft.start
MAX_TIME = 10000

HOST	:= 127.0.0.1

# --------------------------------------------------------------------

TIME    := $(shell date +%H:%M:%S)
SECS    := $(shell date +%S)
COOKIE  := $(shell echo $$PPID)

NODE_SUFFIX := ${SECS}_${LOGNAME}@${HOST}

ELIXIR  := elixir --no-halt --cookie ${COOKIE} --name
MIX 	:= -S mix run -e ${START} ${MAX_TIME} ${NODE_SUFFIX} ${SERVERS} ${CLIENTS}

# --------------------------------------------------------------------

run run_multi: compile
	@ ${ELIXIR} server1_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@ ${ELIXIR} server2_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@ ${ELIXIR} server3_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@ ${ELIXIR} server4_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@ ${ELIXIR} server5_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@ ${ELIXIR} client1_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@ ${ELIXIR} client2_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@ ${ELIXIR} client3_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@ ${ELIXIR} client4_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@ ${ELIXIR} client5_${NODE_SUFFIX} ${MIX} multi_node_wait &
	@sleep 2
	@ ${ELIXIR} raft_${NODE_SUFFIX} ${MIX} multi_node_start

compile:
	mix compile

clean:
	mix clean
	@rm -f erl_crash.dump

# -- 'make ps' will list Elixir nodes running locally
ps:
	@echo ------------------------------------------------------------
	epmd -names


