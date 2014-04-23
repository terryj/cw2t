../node_modules/forever/bin/forever start tradeserver.js
../node_modules/forever/bin/forever start manager.js
cd jETIS4
java -classpath bin:/home/ec2-user/jETIS4/lib/log4j.jar://home/ec2-user/jETIS4/lib/jETIS.jar:/home/ec2-user/jETIS4/lib/trove-2.0.3.jar:/home/ec2-user/jETIS4/lib/jLZO.jar:/home/ec2-user/jETIS4/lib/commons-codec-1.3.jar:/home/ec2-user/jETIS4/lib/jedis-2.1.0.jar:/home/ec2-user/jETIS4/lib/commons-pool-1.6.jar Cw2tClient
