#!/bin/bash

processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

i=0
base_port=5000
relay_port=6000

java -Xmx128m -cp test.jar StartRelay $((relay_port)) &
echo "launched relay on port $((relay_port))"
sleep 0.5

java -Xmx128m -cp test.jar StartPeer $((base_port + $i)) $((relay_port)) 5001 & #$((base_port + $RANDOM % processes)) &
echo "launched peer on port $((base_port + $i))"
sleep 0.5
i=$(($i + 1))

while [ $i -lt $processes ]; do
  java -Xmx128m -cp test.jar StartPeer $((base_port + $i)) $((relay_port)) 5000 & #$((base_port + $RANDOM % processes)) &
  echo "launched peer on port $((base_port + $i))"
  sleep 0.5
  i=$(($i + 1))
done

sleep 2

echo "------------- Press enter to kill servers. --------------------"
read -p ""

kill $(ps aux | grep 'test.jar' | awk '{print $2}')

echo "All processes done!"
