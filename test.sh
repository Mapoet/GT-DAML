#!/bin/bash
# uname -a
# rm test.pid;
# for i in `seq 1 100`;do
#    sleep 200 &
#    pid=$!;
#    echo $pid >> test.pid;
# done

python3 ./dl.py 2009-01-01 2009-12-31      
