#!/bin/bash
uname -a
rm test.pid;
for i in `seq 1 100`;do
   sleep 200 &
   pid=$!;
   echo $pid >> test.pid;
done