#!/bin/bash
cur=`cd $(dirname $0); pwd`
logs=$cur/../logs
type=merger
mkdir -p $logs
nohup sh $cur/wharf $type> $logs/localhost-$(whoami)-${type}.out 2>&1 &
sleep 1
tail $logs/localhost-$(whoami)-${type}.out
