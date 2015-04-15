/****************
* backoffice.js
* Back-office server
* Cantwaittotrade Limited
* Terry Johnston
* November 2014
****************/

// node libraries

// external libraries
var redis = require('redis');

// cw2t libraries
var common = require('./commonfo.js');

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
var backofficechannel = 10;

// redis
var redishost;
var redisport;
var redisauth;
var redispassword;
var redislocal = true; // local or external server

if (redislocal) {
  // local
  redishost = "127.0.0.1";
  redisport = 6379;
  redisauth = false;
} else {
  // redistogo
  redishost = "cod.redistogo.com";
  redisport = 9282;
  redisauth = true;
  redispassword = "4dfeb4b84dbb9ce73f4dd0102cc7707a";
}

// globals

// redis scripts

// set-up a redis client
db = redis.createClient(redisport, redishost);
if (redisauth) {
  db.auth(redispassword, function(err) {
    if (err) {
      console.log(err);
      return;
    }
    console.log("Redis authenticated at " + redishost + " port " + redisport);
    initialise();
  });
} else {
  db.on("connect", function(err) {
    if (err) {
      console.log(err);
      return;
    }
    console.log("Connected to Redis at " + redishost + " port " + redisport);
    initialise();
  });
}

db.on("error", function(err) {
  console.log(err);
});

function initialise() {
  //initDb();
  common.registerCommonScripts();
  registerScripts();
  pubsub();
}

// pubsub connections
function pubsub() {
  dbsub = redis.createClient(redisport, redishost);

  dbsub.on("subscribe", function(channel, count) {
    console.log("subscribed to:" + channel + ", num. channels:" + count);
  });

  dbsub.on("unsubscribe", function(channel, count) {
    console.log("unsubscribed from:" + channel + ", num. channels:" + count);
  });

  dbsub.on("message", function(channel, message) {
    try {
      console.log("channel:" + channel + " " + message);
      var obj = JSON.parse(message);

      if ("price" in obj) {
        priceReceived(obj.price);
      }
    } catch(e) {
      console.log(e);
      return;
    }
  });

  // listen for back-office messages
  dbsub.subscribe(backofficechannel);
}

function priceReceived(price) {
  console.log("priceReceived");
  console.log(price);

  db.eval(scriptcheckaccount, 1, price.symbol, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function registerScripts() {
  //
  // check account for each client with a position in this symbol
  // params: symbol
  //
  scriptcheckaccount = common.scriptgetaccount + '\
  local clients = redis.call("smembers", "position:" + KEYS[1] + ":clients") \
  local ret \
  for index = 1, #clients do \
    ret = scriptgetaccount(clients[index]) \
  end \
  return ret \
  ';
}