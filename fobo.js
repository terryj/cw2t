/****************
* fobo.js
* Front-office to Back-office link
* Cantwaittotrade Limited
* Terry Johnston
* October 2013
****************/

// node libraries
var http = require('http');

// external libraries
var redis = require('redis');

// globals
var options = {
  host: 'ec2-54-235-66-162.compute-1.amazonaws.com',
  //port: 80,
  //path: '',///focalls',
  //method: 'GET'
};
var bointerval;

// scripts
var scriptfoboorder;
var scriptfobotrade;
var scriptfobook;
var scriptfoboerror;

// redis
var redishost = "127.0.0.1";
var redisport = 6379;

// set-up a redis client
db = redis.createClient(redisport, redishost);
db.on("connect", function(err) {
  if (err) {
    console.log(err);
    return;
  }

  console.log("Connected to Redis at " + redishost + " port " + redisport);

  registerScripts();
  startBOInterval();
});

db.on("error", function(err) {
  console.log(err);
});

// run every time interval to download transactions to the back-ofice
function startBOInterval() {
  bointerval = setInterval(bodump, 5000); // todo: make 10
  console.log("Interval timer started");
}

function bodump() {
  console.log("dumping to bo");
  
  // get all the orders waiting to be sent to the back-office
  db.smembers("orders", function(err, orderids) {
    if (err) {
      console.log("Error in bodump: " + err);
      return;
    }

    // send each one
    orderids.forEach(function(orderid, i) {
      db.hgetall("order:" + orderid, function(err, order) {
        if (err) {
          console.log(err);
          return;
        }

        if (order == null) {
          console.log("Order #" + orderid + " not found");
          return;
        }

        sendOrder(order);
      });
    });
  });

  // & all the trades
  db.smembers("trades", function(err, tradeids) {
    if (err) {
      console.log("Error in bodump: " + err);
      return;
    }

    // send each one
    tradeids.forEach(function(tradeid, i) {
      db.hgetall("trade:" + tradeid, function(err, trade) {
        if (err) {
          console.log(err);
          return;
        }

        if (trade == null) {
          console.log("Trade #" + tradeid + " not found");
          return;
        }

        sendTrade(trade);
      });
    });
  });
}

//
// function called on response to the http request
//
callback = function(res) {
  var str = '';

  res.on('data', function(chunk) {
    str += chunk;
  });

  res.on('end', function() {
    var reply;

    console.log(str);

    if (str.charAt(0) == "{") {
      reply = JSON.parse(str);
      if (reply.result) {
        db.eval(scriptfobook, 1, reply.msgid, function(err, ret) {
          if (err) throw err;
        });
      } else {
        db.eval(scriptfoboerror, 2, reply.msgid, reply.error, function(err, ret) {
          if (err) throw err;
        });
      }
    }
  });
}

function sendOrder(order) {
  // call script to get msgid
  db.eval(scriptfoboorder, 2, order.orderid, order.symbol, function(err, ret) {
    if (err) throw err;

    options.path = "/focalls/orderadd.aspx?"
            + "msgid=" + ret[0] + "&"
            + "orgid=" + order.orgid + "&"
            + "clientid=" + order.clientid + "&"
            + "isin=" + ret[1] + "&"
            + "side=" + order.side + "&"
            + "quantity=" + order.quantity + "&"
            + "price=" + order.price + "&"
            + "ordertype=" + order.ordertype + "&"
            + "remquantity=" + order.remquantity + "&"
            + "status=" + order.status + "&"
            + "reason=" + order.reason + "&"
            + "markettype=" + order.markettype + "&"
            + "futsettdate=" + order.futsettdate + "&"
            + "partfill=" + order.partfill + "&"
            + "quoteid=" + order.quoteid + "&"
            + "currency=" + order.currency + "&"
            + "timestamp=" + order.timestamp + "&"
            + "margin=" + order.margin + "&"
            + "timeinforce=" + order.timeinforce + "&"
            + "expiredate=" + order.expiredate + "&"
            + "expiretime=" + order.expiretime + "&"
            + "settlcurrency=" + order.settlcurrency + "&"
            + "text=" + order.text + "&"
            + "orderid=" + order.orderid + "&"
            + "execid=" + order.execid + "&"
            + "externalorderid=" + order.externalorderid + "&"
            + "currencyindtoorg=" + order.currencyindtoorg + "&"
            + "currencyratetoorg=" + order.currencyratetoorg;

    console.log(options.path);

    // make the request
    http.request(options, function(res) {
      callback(res);
    }).on('error', function(e) {
      console.log(e);
    }).end();
  });
}

function sendTrade(trade) {
  // call script to get msgid
  db.eval(scriptfobotrade, 2, trade.tradeid, trade.symbol, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    options.path = "/focalls/tradeadd.aspx?"
            + "msgid=" + ret[0] + "&"
            + "orgid=" + order.orgid + "&"
            + "clientid=" + order.clientid + "&"
            + "isin=" + ret[1] + "&"
            + "side=" + order.side + "&"
            + "quantity=" + order.quantity + "&"
            + "price=" + order.price + "&"
            + "ordertype=" + order.ordertype + "&"
            + "remquantity=" + order.remquantity + "&"
            + "status=" + order.status + "&"
            + "reason=" + order.reason + "&"
            + "markettype=" + order.markettype + "&"
            + "futsettdate=" + order.futsettdate + "&"
            + "partfill=" + order.partfill + "&"
            + "quoteid=" + order.quoteid + "&"
            + "currency=" + order.currency + "&"
            + "timestamp=" + order.timestamp + "&"
            + "margin=" + order.margin + "&"
            + "timeinforce=" + order.timeinforce + "&"
            + "expiredate=" + order.expiredate + "&"
            + "expiretime=" + order.expiretime + "&"
            + "settlcurrency=" + order.settlcurrency + "&"
            + "text=" + order.text + "&"
            + "orderid=" + order.orderid + "&"
            + "execid=" + order.execid + "&"
            + "externalorderid=" + order.externalorderid + "&"
            + "currencyindtoorg=" + order.currencyindtoorg + "&"
            + "currencyratetoorg=" + order.currencyratetoorg;

    console.log(options.path);

    // make the request
    http.request(options, function(res) {
      callback(res);
    }).on('error', function(e) {
      console.log(e);
    }).end();
  });
}

function registerScripts() {
  scriptfoboorder = '\
  local fobomsgid = redis.call("incr", "fobomsgid") \
  redis.call("hmset", "fobo:" .. fobomsgid, "msgtype", 1, "orderid", KEYS[1], "status", 0) \
  local isin = redis.call("hget", "symbol:" .. KEYS[2], "isin") \
  return {fobomsgid, isin} \
  ';

  scriptfobotrade = '\
  local fobomsgid = redis.call("incr", "fobomsgid") \
  redis.call("hmset", "fobo:" .. fobomsgid, "msgtype", 2, "tradeid", KEYS[1], "status", 0) \
  local isin = redis.call("hget", "symbol:" .. KEYS[2], "isin") \
  return {fobomsgid, isin} \
  ';

  // update message status & remove order/trade from their set
  scriptfobook = '\
  redis.call("hmset", "fobo:" .. KEYS[1], "status", 1) \
  local msgtype = redis.call("hget", "fobo:" .. KEYS[1], "msgtype") \
  if msgtype == 1 then \
    local orderid = redis.call("hget", "fobo:" .. KEYS[1], "orderid") \
    redis.call("sdel", "orders", orderid) \
  else \
    local tradeid = redis.call("hget", "fobo:" .. KEYS[1], "tradeid") \
    redis.call("sdel", "trades", tradeid) \
  end \
  return \
  ';

  // update message error, add message to errors set & remove order/trade from their set
  scriptfoboerror = '\
  redis.call("hmset", "fobo:" .. KEYS[1], "error", KEYS[2]) \
  redis.call("sadd", "errors", KEYS[1]) \
  local msgtype = redis.call("hget", "fobo:" .. KEYS[1], "msgtype") \
  if msgtype == 1 then \
    local orderid = redis.call("hget", "fobo:" .. KEYS[1], "orderid") \
    redis.call("srem", "orders", orderid) \
  else \
    local tradeid = redis.call("hget", "fobo:" .. KEYS[1], "tradeid") \
    redis.call("srem", "trades", tradeid) \
  end \
  return \
  ';
}
