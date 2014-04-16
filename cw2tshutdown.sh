#!/bin/bash
#- stop trade server
../node_modules/forever/bin/forever stop tradeserver.js
#- stop manager
../node_modules/forever/bin/forever stop manager.js
#- stop price server
# ???
#stop redis server
cd
sudo /etc/init.d/redis_6379 stop
