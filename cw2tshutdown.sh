#!/bin/bash
cd
sudo /node_modules/forever/bin/forever stop tradeserver.js
sudo /node_modules/forever/bin/forever stop manager.js
sudo /etc/init.d/redis_6379 stop
