#!/bin/bash

# ensure root
cd

#- stop trade server
cd cw2t
../node_modules/forever/bin/forever stop tradeserver.js

#- stop manager (optional)
../node_modules/forever/bin/forever stop manager.js

#- stop price server
# command goes here when I have worked out what it might be!

#stop redis server
sudo /etc/init.d/redis_6379 stop
