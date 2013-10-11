/****************
* market.js
* Front-office information server
* Cantwaittotrade Limited
* Terry Johnston
* October 2013
****************/

// node libraries
var http = require('http');
var net = require('net');

// external libraries
var sockjs = require('sockjs');
var node_static = require('node-static');
var redis = require('redis');

// globals
var connections = {}; // added to if & when a client logs on
var static_directory = new node_static.Server(__dirname); // static files server
var cw2tport = 8080; // client listen port
var outofhours = false; // in or out of market hours - todo: replace with markettype?
var ordertypes = {};
var orgid = "1"; // todo: via logon

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
  listen();
}

// pubsub connections
function pubsub() {
  dbsub = redis.createClient(redisport, redishost);
  dbpub = redis.createClient(redisport, redishost);

  dbsub.on("subscribe", function(channel, count) {
    console.log("subscribed to:" + channel + ", num. channels:" + count);
  });

  dbsub.on("unsubscribe", function(channel, count) {
    console.log("unsubscribed from:" + channel + ", num. channels:" + count);
  });

  dbsub.on("message", function(channel, message) {
    var topickey;

    console.log("channel " + channel + ": " + message);
    db.smembers("proquote:" + channel, function(err, replies) {
      if (err) {
        console.log(err);
        return;
      }

      topickey = "topic:" + channel;

      // get the symbol from the proquote topic
      db.hgetall(topickey, function(err, inst) {
        if (err) {
          console.log(err);
          return;
        }

        if (inst == null) {
          console.log("Topic:" + channel + " not found");
          return;
        }

        // just send the message as is
        replies.forEach(function(orgclientkey, i) {
          if (orgclientkey in connections) {
            connections[orgclientkey].write(message);
          }
        });
      });
    });
  });
}

// sockjs server
var sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.3.min.js"};
var sockjs_svr = sockjs.createServer(sockjs_opts);

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

  server.listen(cw2tport, '0.0.0.0');
  console.log('Listening on port ' + cw2tport);

  sockjs_svr.on('connection', function(conn) {
    // this will be overwritten if & when a client logs on
    var clientid = "0";
    var orgclientkey;

    console.log('new connection');

    // data callback
    // todo: multiple messages in one data event
    conn.on('data', function(msg) {
      var obj;
      console.log('recd:' + msg);

      if (msg.substr(2, 5) == "index") {
        obj = JSON.parse(msg);
        sendIndex(orgclientkey, obj.index, conn);
      } else if (msg.substr(2, 6) == "signin") {
        obj = JSON.parse(msg);
        signIn(obj.signin);
      } else if (msg.substr(0, 4) == "ping") {
        conn.write("pong");
      } else {
        console.log("unknown msg received:" + msg);
      }
    });

    // close connection callback
    conn.on('close', function() {
      // todo: check for existence
      if (clientid != "0") {
        if (orgclientkey in connections) {
          console.log("client " + orgclientkey + " logged off");
          // remove from list
          delete connections[orgclientkey];

          // remove from database
          db.srem("connections", orgclientkey);
        }

        unsubscribeTopics(orgclientkey);
        clientid = "0";
      }
    });

    // client sign on
    function signIn(signin) {
      var reply = {};
      reply.success = false;

      db.get(signin.email, function(err, orgclientid) {
        if (err) {
          console.log("Error in signIn:" + err);
          return;
        }

        if (!orgclientid) {
          console.log("Email not found:" + signin.email);
          reply.reason = "Email or password incorrect. Please try again.";
          replySignIn(reply, conn);
          return;
        }

        // validate email/password
        db.hgetall("client:" + orgclientid, function(err, client) {
          if (err) {
            console.log("Error in signIn:" + err);
            return;
          }

          if (!client || signin.email != client.username || signin.password != client.password) {
            reply.reason = "Email or password incorrect. Please try again.";
            replySignIn(reply, conn);
            return;
          }

          // validated, so set client id & add client to list of connections
          clientid = client.clientid;
          orgclientkey = orgclientid;
          console.log("adding " + orgclientkey + " to connections");
          connections[orgclientkey] = conn;

          // keep a record
          db.sadd("connections", orgclientkey);

          // send a successful logon reply
          reply.success = true;
          reply.email = signin.email;
          replySignIn(reply, conn);

          console.log("client " + orgclientkey + " logged on");

          // send the data
          start(orgclientkey, conn);
        });
      });
    }
  });
}

function sendInstruments(conn) {
  // get sorted subset of instruments
  db.eval(scriptgetinst, 0, function(err, ret) {
    conn.write("{\"instruments\":" + ret + "}");
  });
}

function sendIndex(orgclientkey, index, conn) {
  db.smembers("index:" + index, function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    replies.forEach(function(symbol, j) {
      db.hgetall("symbol:" + symbol, function(err, inst) {
        if (err) {
          console.log(err);
          return;
        }

        if (inst == null) {
          console.log("Symbol " + symbol + " not found");
          return;
        }

        orderBookOut(orgclientkey, symbol, conn);
      });
    });
  });
}

function sendOrderBook(symbol, level1arr, send, conn) {
  var level1 = {};
  var count;

  level1.symbol = symbol;
  level1.bestbid = 0;
  level1.bestoffer = 0;

  // we go through the entire order book to determine the best bid & offer
  // todo: may be a better way
  db.zrange(symbol, 0, -1, "WITHSCORES", function(err, orders) {
    if (err) {
      console.log(err);
      return;
    }

    // we are assuming there is always at least 1 order
    count = orders.length;

    orders.forEach(function (reply, i) {
      if (i % 2 != 0) {
        if (i == 1) { // first score
          if (reply < 0) {
            level1.bestbid = -parseFloat(reply);
          } else if (reply > 0) {
            level1.bestoffer = parseFloat(reply);
          }
        } else if (level1.bestoffer == 0) {
          if (reply > 0) {
            level1.bestoffer = parseFloat(reply);
          }
        }
      }
 
      // level 1 prices for a single instrument
      count--;
      if (count <= 0) {
        // add level1 object to our array
        level1arr.push(level1);

        if (send) {
          conn.write("{\"orderbooks\":" + JSON.stringify(level1arr) +"}");
        }
      }
    });
  });
}

//
// send all the orderbooks for a single client
//
function sendOrderBooksClient(orgclientkey, conn) {
  // get all the instruments in the order book for this client
  db.smembers(orgclientkey + ":orderbooks", function(err, instruments) {
    if (err) {
      console.log("error in sendOrderBooksClient:" + err);
      return;
    }

    // send the order book for each instrument
    instruments.forEach(function (symbol, i) {
      orderBookOut(orgclientkey, symbol, conn);
    });
  });
}

function publishMessage(message) {
  // todo: alter to just cater for interested parties
  for (var c in connections) {
    if (connections.hasOwnProperty(c)) {
      connections[c].write(message);
    }
  }
}

function start(orgclientkey, conn) {
  sendIndex(orgclientkey, "UKX", conn);
  sendReadyToTrade(conn);
}

function sendReadyToTrade(conn) {
  conn.write("{\"status\":\"readytotrade\"}");
}

function replySignIn(reply, conn) {
    conn.write("{\"signinreply\":" + JSON.stringify(reply) + "}");
}

function initDb() {
  // clear any connected clients
  db.smembers("connections", function(err, connections) {
    if (err) {
      console.log(err);
      return;
    }

    connections.forEach(function(connection, i) {
      db.srem("connections", connection);
    });
  });

  registerScripts();
}

function orderBookOut(orgclientkey, symbol, conn) {
  if (outofhours) {
    broadcastLevelTwo(symbol, conn);
  } else {
    subscribeAndSend(orgclientkey, symbol, conn);
  }
}

function subscribeAndSend(orgclientkey, symbol, conn) {
  // get the proquote topic
  db.hgetall("symbol:" + symbol, function(err, inst) {
    if (err) {
      console.log(err);
      return;
    }

    if (inst == null) {
      console.log("symbol:" + symbol + " not found");
      return;
    }

    // get the client to check market extension - defaults to 'LD' for LSE delayed
    db.hgetall("client:" + orgclientkey, function(err, client) {
      if (err) {
        console.log(err);
        return;
      }

      if (!client) {
        console.log("client:" + orgclientkey + " not found");
        return;
      }

      // may need to adjust the topic to delayed
      if (client.marketext == "LD") {
        inst.topic += "D";
      }

      // are we subscribed to this topic?
      db.sismember("proquote", inst.topic, function(err, instsubscribed) {
        if (err) {
          console.log(err);
          return;
        }

        // if not, add it
        if (!instsubscribed) {
          dbsub.subscribe(inst.topic);

          // let proquote know
          dbpub.publish("cw2t", "subscribe:" + inst.topic);

          // add topic to the set
          db.sadd("proquote", inst.topic);
        }

        // is this client subscribed?
        db.sismember("proquote:" + inst.topic, orgclientkey, function(err, clientfound) {
          if (err) {
            console.log(err);
            return;
          }

          if (!clientfound) {
            console.log("subscribing " + orgclientkey + " to " + inst.topic)
            // add client to the set for this instrument
            db.sadd("proquote:" + inst.topic, orgclientkey);
          }
        });
      });

      // send the orderbook, with the current stored prices
      sendCurrentOrderBook(orgclientkey, symbol, inst.topic, conn);
    });
  });
}

function sendCurrentOrderBook(orgclientkey, symbol, topic, conn) {
  var orderbook = {pricelevels : []};
  var pricelevel1 = {};
  var pricelevel2 = {};
  var pricelevel3 = {};
  var pricelevel4 = {};
  var pricelevel5 = {};
  var pricelevel6 = {};

  db.hgetall("topic:" + topic, function(err, topicrec) {
    if (err) {
      console.log(err);
      return;
    }

    if (!topicrec) {
      return;
    }

    // 3 levels
    pricelevel1.bid = topicrec.bid1;
    pricelevel1.level = 1;
    orderbook.pricelevels.push(pricelevel1);
    pricelevel2.offer = topicrec.offer1;
    pricelevel2.level = 1;
    orderbook.pricelevels.push(pricelevel2);
    pricelevel3.bid = topicrec.bid2;
    pricelevel3.level = 2;
    orderbook.pricelevels.push(pricelevel3);
    pricelevel4.offer = topicrec.offer2;
    pricelevel4.level = 2;
    orderbook.pricelevels.push(pricelevel4);
    pricelevel5.bid = topicrec.bid3;
    pricelevel5.level = 3;
    orderbook.pricelevels.push(pricelevel5);
    pricelevel6.offer = topicrec.offer3;
    pricelevel6.level = 3;
    orderbook.pricelevels.push(pricelevel6);

    orderbook.symbol = symbol;

    if (conn != null) {
      conn.write("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
    }

    /*var interval = setInterval(function() {
      orderbook.pricelevels = [];

      // 3 levels
      pricelevel1.bid = parseFloat(pricelevel1.bid) + 0.01;//parseFloat(topicrec.bid1)+diff;
      pricelevel1.level = 1;
      orderbook.pricelevels.push(pricelevel1);
      pricelevel2.offer = parseFloat(pricelevel2.offer) + 0.01;//parseFloat(topicrec.offer1)+diff;
      pricelevel2.level = 1;
      orderbook.pricelevels.push(pricelevel2);
      pricelevel3.bid = parseFloat(pricelevel3.bid) + 0.01;//parseFloat(topicrec.bid2)+diff;
      pricelevel3.level = 2;
      orderbook.pricelevels.push(pricelevel3);
      pricelevel4.offer = parseFloat(pricelevel4.offer) + 0.01;//parseFloat(topicrec.offer2)+diff;
      pricelevel4.level = 2;
      orderbook.pricelevels.push(pricelevel4);
      pricelevel5.bid = parseFloat(pricelevel5.bid) + 0.01;//parseFloat(topicrec.bid3)+diff;
      pricelevel5.level = 3;
      orderbook.pricelevels.push(pricelevel5);
      pricelevel6.offer = parseFloat(pricelevel6.offer) + 0.01;//parseFloat(topicrec.offer3)+diff;
      pricelevel6.level = 3;
      orderbook.pricelevels.push(pricelevel6);

      conn.write("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
    }, 9000);*/
  });
}

function unsubscribeTopic(orgclientkey, symbol, marketext) {
  // get the proquote topic
  db.hgetall("symbol:" + symbol, function(err, inst) {
    if (err) {
      console.log(err);
      return;
    }

    if (inst == null) {
      console.log("symbol:" + symbol + " not found");
      return;
    }

    // adjust the topic for the client market extension (i.e. delayed/live)
    if (marketext == "LD") {
      inst.topic += "D";
    }

    // remove client from set for this topic
    db.srem("proquote:" + inst.topic, orgclientkey);

    // unsubscribe from this topic if no clients are looking at it
    db.scard("proquote:" + inst.topic, function(err, numelements) {
      if (numelements == 0) {
        dbsub.unsubscribe(inst.topic);
        db.srem("proquote", inst.topic);
        dbpub.publish("cw2t", "unsubscribe:" + inst.topic);
      }
    });
  });
}

function unsubscribeTopics(orgclientkey) {
  // get the market extension for this client, as topic may need to be adjusted for live/delayed
  db.hget("client:" + orgclientkey, "marketext", function(err, marketext) {
    if (err) {
      console.log("Error in unsubscribeTopics:" + err);
      return;
    }

    // get all the orderbooks
    db.smembers(orgclientkey + ":orderbooks", function(err, orderbooks) {
      if (err) {
        console.log("Error in unsubscribeTopics:" + err);
        return;
      }

      // & unsubscibe
      orderbooks.forEach(function(symbol, i) {
        unsubscribeTopic(orgclientkey, symbol, marketext);
      });
    });
  });
}

function registerScripts() {
  //
  // parameter: symbol
  // returns: isin & proquote symbol
  //
  getproquotesymbol = '\
  local getproquotesymbol = function(symbol) \
    local symbolkey = "symbol:" .. symbol \
    local isin = redis.call("hget", symbolkey, "isin") \
    local proquotesymbol = redis.call("hget", symbolkey, "proquotesymbol") \
    local market = redis.call("hget", symbolkey, "market") \
    return {isin, proquotesymbol, market} \
  end \
  ';

  scriptgetinst = '\
  local instruments = redis.call("sort", "instruments", "ALPHA") \
  local fields = {"instrumenttype", "description", "currency"} \
  local vals \
  local inst = {} \
  for index = 1, #instruments do \
    vals = redis.call("hmget", "symbol:" .. instruments[index], unpack(fields)) \
    if vals[1] == "DE" then \
      table.insert(inst, {symbol = instruments[index], description = vals[2], currency = vals[3]}) \
    end \
  end \
  return cjson.encode(inst) \
  ';
}