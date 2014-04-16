#!/bin/bash
../node_modules/forever/bin/forever stop tradeserver.js
../node_modules/forever/bin/forever stop manager.js
../etc/init.d/redis_6379 stop
