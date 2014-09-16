/****************
* pricehistoryserver.js
* Price history server
* Cantwaittotrade Limited
* Terry Johnston
* August 2014
****************/

// external libraries
var redis = require('redis');

// cw2t libraries
var common = require('./common.js');

// globals
var clientserverchannel = 1;
var userserverchannel = 2;
var tradeserverchannel = 3;
var ifaserverchannel = 4;
var webserverchannel = 5;
var tradechannel = 6;
var pricechannel = 7;
var pricehistorychannel = 8;

// redis
var redishost;
var redisport;
var redisauth;
var redispassword;
var redislocal = true; // local or external server
var holidays = {};

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
  initDb();
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
      var obj = JSON.parse(message);

      if ("pricehistoryrequest" in obj) {
        pricehistoryRequest(obj.pricehistoryrequest);
      } else {
        console.log("unknown msg received:" + message);
      }
    } catch (e) {
      console.log(e);
      return;
    }
  });

  dbsub.subscribe(pricehistorychannel);
}

function initDb() {
  registerScripts();
}

// receives a pricehistoryrequest object
function pricehistoryRequest(phr) {
  //console.log(phr);
  var pricehist = {};
  pricehist.clientid = phr.clientid;

  // get price history
  db.eval(scriptgetpricehist, 3, phr.symbol, phr.startperiod, phr.endperiod, function(err, ret) {
    if (err) throw err;
    //console.log(ret);
    console.log("got pricehist");
    console.log(ret);
    //pricehist.prices = ret;
    //console.log(ret.length);
    db.publish(webserverchannel, "{\"pricehistory\":{\"clientid\":" + phr.clientid + ",\"prices\":" + ret + "}}");
  });
}

function registerScripts() {
  // get price history between two datetimes for a symbol
  // params: symbol, startperiod, endperiod
  scriptgetpricehist = '\
    local fields = {"bid", "ask", "timestamp", "id"} \
    local tblpricehist = {} \
    local vals \
    local pricehist = redis.call("zrangebyscore", "pricehistory:" .. KEYS[1], KEYS[2], KEYS[3]) \
    for index = 1, #pricehist do \
      vals = redis.call("hmget", "pricehistory:" .. pricehist[index], unpack(fields)) \
      table.insert(tblpricehist, {bid = vals[1], ask = vals[2], timestamp = vals[3], id = vals[4]}) \
    end \
    return cjson.encode(tblpricehist) \
  ';
}