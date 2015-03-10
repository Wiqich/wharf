#!/bin/bash
cur=`cd $(dirname $0); pwd`
logs=$cur/../logs
type=worker
mkdir -p $logs
nohup sh $cur/wharf $type> $logs/localhost-$(whoami)-${type}.log 2>&1 &
sleep 2
tail $logs/localhost-$(whoami)-${type}.log
