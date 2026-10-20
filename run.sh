#!/usr/bin/env bash
# Compiles the project, starts 3 DataNodes + the NameNode on localhost,
# then opens the interactive Client. Everything stops when you exit the Client.
# Commands:  append <file> <text>   |   read <file>   |   shutdown
set -e
cd "$(dirname "$0")"
mkdir -p bin run
javac -d bin src/*.java
cd run
pids=()
for p in 65530 65531 65532; do
  java -cp ../bin DataNode "$p" > "DNode_$p.log" 2>&1 &
  pids+=($!)
done
java -cp ../bin NameNode > namenode.log 2>&1 &
pids+=($!)
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT
sleep 2
echo "DFS running on localhost (NameNode :5558, DataNodes :65530-65532). Logs in ./run"
java -cp ../bin Client
