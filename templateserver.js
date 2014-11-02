/****************
* templateserver.js
* Template server
* Cantwaittotrade Limited
* Terry Johnston
* November 2014
****************/

// node libraries
var http = require('http');
var fs = require("fs");

// external libraries
var sockjs = require('sockjs');
var node_static = require('node-static');
var redis = require('redis');

// cw2t libraries
var common = require('./common.js');

// publish & subscribe channels
var clientserverchannel = 1;
var userserverchannel = 2;
var tradeserverchannel = 3;
var ifaserverchannel = 4;
var webserverchannel = 5;
var tradechannel = 6;
var priceserverchannel = 7;
var pricehistorychannel = 8;
var pricechannel = 9;

// redis
var redishost = "127.0.0.1";
var redisport = 6379;

// globals
var connections = {}; // added to when a client logs on
var static_directory = new node_static.Server(__dirname); // static files server
var cw2tport = 8083; // listen port
var servertype = "mm";
var mmid = 1; // could have multiple mm's

// set-up redis client
db = redis.createClient(redisport, redishost);
db.on("connect", function(err) {
  if (err) {
    console.log(err);
    return;
  }
  console.log("Connected to Redis at " + redishost + " port " + redisport);
  initialise();
});

db.on("error", function(err) {
  console.log("Db Error:" + err);
});

function initialise() {
  pubsub();
  listen();
}

// set-up pubsub connection
function pubsub() {
  dbsub = redis.createClient(redisport, redishost);

  dbsub.on("subscribe", function(channel, count) {
    console.log("subscribed to:" + channel + ", num. channels:" + count);
  });

  dbsub.on("unsubscribe", function(channel, count) {
    console.log("unsubscribed from:" + channel + ", num. channels:" + count);
  });

  // messages any subscribed channels arrive here
  dbsub.on("message", function(channel, message) {
    console.log("msg rec'd, channel " + channel);
    console.log(message);

    try {
      var obj = JSON.parse(message);

      if ("price" in obj) {
        priceReceived(obj.price);
      } else if ("trade" in obj) {
        tradeReceived(obj.trade);
      }
    } catch (e) {
      console.log(e);
      console.log(message);
    }
  });

  // subscribe to any required channels here
  dbsub.subscribe(tradechannel);
}

// sockjs server
var sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.3.min.js"};
var sockjs_svr = sockjs.createServer(sockjs_opts);

// options for https server
var options = {
  key: fs.readFileSync('key.pem'),
  cert: fs.readFileSync('cert.pem')
};

// http server
function listen() {
  var server = http.createServer();

  server.addListener('request', function(req, res) {
    static_directory.serve(req, res);
  });
  server.addListener('upgrade', function(req, res){
    res.end();
  });

  sockjs_svr.installHandlers(server, {prefix:'/echo'});

  server.listen(cw2tport, function() {
    console.log('Listening on port ' + cw2tport);
  });

  sockjs_svr.on('connection', function(conn) {
    console.log("new connection");

    // add connection to our list
    connections[mmid] = conn;

    // web page data arrives here
    conn.on('data', function(msg) {
      console.log(msg);

      try {
        var obj = JSON.parse(msg);

        if ("order" in obj) {
          orderReceived(obj.order);
        } else if ("mmparams" in obj) {
          mmparamsReceived(obj.mmparams);
        } else {
          console.log("unknown msg received:" + msg);
        }
      } catch (e) {
        console.log(e);
        return;
      }
    });

    conn.on('close', function() {
      console.log("connection has closed");
    });
  });
}

// things start here
function start(clientid, conn) {
  console.log("start");
}

// order received from client
function orderReceived(order) {
  console.log("order received");
  console.log(order);
}

// published price received
function priceReceived(price) {
  console.log("priceReceived");
  console.log(price);
}

// published trade received
function tradeReceived(trade) {
  console.log("tradeReceived");
  console.log(trade);
}

// form parameters received
function mmparamsReceived(mmparams) {
  console.log("mmparamsReceived");
  console.log(mmparams);

  // redis update
  //db.set/hset...

  // build a reply
  var mmparamsreply = {};
  mmparamsreply.updated = true;

  // send it
  connections[mmid].write("{\"mmparamsreply\":" + JSON.stringify(mmparamsreply) + "}");
}