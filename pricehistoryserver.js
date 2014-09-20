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
  console.log(phr);

  phr.startperiod = 1411110000245;
  phr.endperiod = 1411111004245;
  var interval = 10;

  // get price history
  db.eval(scriptgetpricehistinterval, 6, phr.symbol, phr.startperiod, phr.endperiod, phr.clientid, webserverchannel, interval, function(err, ret) {
    if (err) throw err;
    console.log("got pricehist:" + phr.symbol);
  });
}

function registerScripts() {
  // get price tick history between two datetimes for a symbol
  // params: symbol, startperiod, endperiod, clientid, channel
  // i.e. "LLOY.D", 1411110004245, 1411111004245, 1, 5
  scriptgetpricehisttick = '\
    local fields = {"bid", "ask", "timestamp", "id"} \
    local tblpricehist = {} \
    local vals \
    local pricehist = redis.call("zrangebyscore", "pricehistory:" .. KEYS[1], KEYS[2], KEYS[3]) \
    for index = 1, #pricehist do \
      vals = redis.call("hmget", "pricehistory:" .. pricehist[index], unpack(fields)) \
      table.insert(tblpricehist, {bid=vals[1], ask=vals[2], timestamp=vals[3], id=vals[4]}) \
    end \
    --[[ publish to requested channel ]] \
    local pricehist = "{" .. cjson.encode("pricehistory") .. ":{" .. cjson.encode("symbol") .. ":" .. cjson.encode(KEYS[1]) .. "," .. cjson.encode("clientid") .. ":" .. KEYS[4] .. "," .. cjson.encode("prices") .. ":" .. cjson.encode(tblpricehist) .. "}}" \
    redis.call("publish", KEYS[5], pricehist) \
  ';

  // get interval price history between two datetimes for a symbol
  // params: symbol, startperiod, endperiod, clientid, channel, interval
  // i.e. "LLOY.D", 1411110004245, 1411111004245, 10, 1, 5
  scriptgetpricehistinterval = '\
    local fields = {"bid", "ask", "timestamp", "id"} \
    local tblpricehist = {} \
    local vals \
    local mid \
    local interval = KEYS[6] * 1000 \
    local nexttimeinterval = KEYS[2] + interval \
    local pricehist = redis.call("zrangebyscore", "pricehistory:" .. KEYS[1], KEYS[2], KEYS[3]) \
    for index = 1, #pricehist do \
      vals = redis.call("hmget", "pricehistory:" .. pricehist[index], unpack(fields)) \
      mid = (tonumber(vals[1]) + tonumber(vals[2])) / 2 \
      if tonumber(vals[3]) > nexttimeinterval then \
        table.insert(tblpricehist, {mid=mid, timestamp=nexttimeinterval}) \
        nexttimeinterval = nexttimeinterval + interval \
      end \
    end \
    local pricehist = "{" .. cjson.encode("pricehistory") .. ":{" .. cjson.encode("symbol") .. ":" .. cjson.encode(KEYS[1]) .. "," .. cjson.encode("clientid") .. ":" .. KEYS[4] .. "," .. cjson.encode("prices") .. ":" .. cjson.encode(tblpricehist) .. "}}" \
    redis.call("publish", KEYS[5], pricehist) \
  ';
}