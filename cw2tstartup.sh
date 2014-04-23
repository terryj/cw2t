#!/bin/bash
# ensure root
cd
# start redis server
sudo /etc/init.d/redis_6379 start
#- stop trade server
cd cw2t
../node_modules/forever/bin/forever stop tradeserver.js
#- stop manager (optional)
../node_modules/forever/bin/forever stop manager.js
#- stop price server
# ???
#- node startofday
# terry's new command goes here !!!
#- start trade server
../node_modules/forever/bin/forever start tradeserver.js
#- start manager
../node_modules/forever/bin/forever start manager.js
#- start price server
# command goes here when I have worked out what it might be!
