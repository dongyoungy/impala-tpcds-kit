#!/bin/bash
 
let "count=0"
for node in cp-1 cp-2 cp-3 cp-4 cp-5 cp-6 cp-7 cp-8 cp-9 cp-10 cp-11 cp-12 cp-13 cp-14 cp-15 cp-16 cp-17 cp-18
do
    (ssh -o "StrictHostKeyChecking no" -p 22 $node "sudo sync && echo 3 | sudo tee /proc/sys/vm/drop_caches") &
    let "count += 1"
done
wait
sleep 30
