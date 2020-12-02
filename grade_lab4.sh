#!/bin/bash

./test-lab4-fxmark ./yfs1 | tee /dev/tty | grep -q 'Pass'
if [ $? -ne 0 ]; then
	echo "Failed test-lab4-fxmark"
else
	echo "Passed test-lab4-fxmark"
fi

./start.sh

works=$(./fxmark/bin/fxmark --type=YFS --root=./yfs1 --ncore=1 --duration=1 | tee /dev/tty | sed -n '2p' | awk '{print $3}')
if [ $(echo "$works >= 900" | bc) -eq 1 ]; then
	echo "Passed fxmark performance"
else
	echo "Failed fxmark performance"
fi

./stop.sh
