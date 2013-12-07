/****************
* manager.js
* Front-office management server
* Cantwaittotrade Limited
* Terry Johnston
* November 2013
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
var cw2tport = 8081; // client listen port
var outofhours = false; // in or out of market hours - todo: replace with markettype?
var ordertypes = {};
var brokerid = "1"; // todo: via logon
var defaultnosettdays = 3;
var operatortype = 2;
var tradeserverchannel = 3;
var userserverchannel = 2;

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
var scriptgetclients;
var scriptnewclient;
var scriptgetbrokers;
var scriptgetifas;
var scriptgetinstrumenttypes;
var scriptgetcashtranstypes;
var scriptgetcurrencies;
var scriptcashtrans;
var scriptupdateclient;
var scriptgetinst;
var scriptgetclienttypes;
var scriptinstupdate;
var scripthedgeupdate;
var scriptgethedgebooks;
var scriptcost;
var scriptgetcosts;
var scriptifa;
var scriptsubscribeinstrument;
var scriptunsubscribeinstrument;
var scriptsubscribeuser;
var scriptunsubscribeuser;
var scriptnewprice;

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

  dbsub.on("subscribe", function(channel, count) {
    console.log("subscribed to:" + channel + ", num. channels:" + count);
  });

  dbsub.on("unsubscribe", function(channel, count) {
    console.log("unsubscribed from:" + channel + ", num. channels:" + count);
  });

  dbsub.on("message", function(channel, message) {
    console.log("channel:" + channel + ", " + message);

    if (message.substr(0, 8) == "quoteack") {
      sendQuoteack(message.substr(9));
    } else if (message.substr(0, 5) == "quote") {
      sendQuote(message.substr(6));
    } else if (message.substr(0, 5) == "order") {
      getSendOrder(message.substr(6));
    } else if (message.substr(0, 5) == "trade") {
      getSendTrade(message.substr(6));
    } else {
      newPrice(channel, message);
    }
  });

  // listen for trading messages
  dbsub.subscribe(userserverchannel);
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
    var userid = "0";

    console.log('new connection');

    // data callback
    // todo: multiple messages in one data event
    conn.on('data', function(msg) {
      var obj;
      console.log('recd:' + msg);

      // todo: no orgclientkey

      if (msg.substr(2, 18) == "ordercancelrequest") {
        // todo: test
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 16) == "orderbookrequest") {
        obj = JSON.parse(msg);
        orderBookRequest(userid, obj.orderbookrequest, conn);
      } else if (msg.substr(2, 22) == "orderbookremoverequest") {
        obj = JSON.parse(msg);
        orderBookRemoveRequest(userid, obj.orderbookremoverequest, conn);
      } else if (msg.substr(2, 5) == "order") {
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 12) == "quoterequest") {
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 8) == "register") {
        obj = JSON.parse(msg);
        registerClient(obj.register, conn);
      } else if (msg.substr(2, 6) == "signin") {
        obj = JSON.parse(msg);
        signIn(obj.signin);
      } else if (msg.substr(2, 22) == "positionhistoryrequest") {
        obj = JSON.parse(msg);
        positionHistory(clientid, obj.positionhistoryrequest, conn);
      } else if (msg.substr(2, 5) == "index") {
        obj = JSON.parse(msg);
        sendIndex(orgclientkey, obj.index, conn);        
      } else if (msg.substr(2, 9) == "newclient") {
        obj = JSON.parse(msg);
        newClient(obj.newclient, conn);
      } else if (msg.substr(2, 9) == "cashtrans") {
        obj = JSON.parse(msg);
        cashTrans(obj.cashtrans, userid, conn);
      } else if (msg.substr(2, 10) == "instupdate") {
        obj = JSON.parse(msg);
        instUpdate(obj.instupdate, userid, conn);
      } else if (msg.substr(2, 15) == "hedgebookupdate") {
        obj = JSON.parse(msg);
        hedgebookUpdate(obj.hedgebookupdate, userid, conn);
      } else if (msg.substr(2, 4) == "cost") {
        obj = JSON.parse(msg);
        costUpdate(obj.cost, conn);
      } else if (msg.substr(2, 3) == "ifa") {
        obj = JSON.parse(msg);
        newIfa(obj.ifa, conn);
      } else if (msg.substr(0, 4) == "ping") {
        conn.write("pong");
      } else {
        console.log("unknown msg received:" + msg);
      }
    });

    // close connection callback
    conn.on('close', function() {
      tidy(userid);

      // todo: check for existence
      userid = "0";
    });

    // user sign in
    function signIn(signin) {
      var reply = {};
      reply.success = false;

      db.get("user:" + signin.email, function(err, emailuserid) {
        if (err) {
          console.log("Error in signIn:" + err);
          return;
        }

        if (!emailuserid) {
          console.log("Email not found:" + signin.email);
          reply.reason = "Email or password incorrect. Please try again.";
          replySignIn(reply, conn);
          return;
        }

        // validate email/password
        db.hgetall("user:" + emailuserid, function(err, user) {
          if (err) {
            console.log("Error in signIn:" + err);
            return;
          }

          if (!user || signin.email != user.email || signin.password != user.password) {
            reply.reason = "Email or password incorrect. Please try again.";
            replySignIn(reply, conn);
            return;
          }

          // validated, so set user id
          userid = user.userid;
          connections[userid] = conn;

          // send a successful logon reply
          reply.success = true;
          reply.email = signin.email;
          replySignIn(reply, conn);

          console.log("user:" + userid + " logged on");

          // send the data
          start(userid, user.brokerid, conn);
        });
      });
    }
  });
}

function tidy(userid) {
  if (userid != "0") {
    if (userid in connections) {
      console.log("user:" + userid + " logged off");

      // remove from list
      delete connections[userid];

      // remove from database
      db.srem("connections:users", userid);
    }

    db.eval(scriptunsubscribeuser, 1, userid, function(err, ret) {
      if (err) throw err;

      // the script tells us if we need to unsubscribe
      for (var i = 0; i < ret.length; i++) {
        dbsub.unsubscribe(ret[i]);
      }
    });
  }
}

function newPrice(topic, msg) {
  var jsonmsg;
  /*db.eval(scriptnewprice, 1, topic, function(err, ret) {
    if (err) throw err;

    console.log(ret);

    //conn.write("{\"hedgebooks\":" + ret + "}");
  });*/

  db.smembers("topic:" + topic + ":symbols", function(err, symbols) {
    if (err) throw err;

    // get the symbols covered by this topic - equity price covers cfd, spb...
    symbols.forEach(function(symbol, i) {
      console.log(symbol);

      // build the message according to the symbol
      jsonmsg = "{\"orderbook\":{\"symbol\":\"" + symbol + "\"," + msg + "}}";
      console.log(jsonmsg);

      // get the users watching this symbol
      db.smembers("topic:" + topic + ":symbol:" + symbol + ":users", function(err, users) {
        if (err) throw err;

        // send the message to each user
        users.forEach(function(user, i) {
          console.log(user);

          if (user in connections) {
            connections[user].write(jsonmsg);
          }
        });
      });
    });
  });
}

function orderBookRequest(userid, symbol, conn) {
  db.eval(scriptsubscribeinstrument, 2, symbol, userid, function(err, ret) {
    if (err) throw err;

    console.log(ret);

    // the script tells us if we need to subscribe to a topic
    if (ret[0]) {
      dbsub.subscribe(ret[1]);
    }

    // send the orderbook, with the current stored prices
    sendCurrentOrderBook(symbol, ret[1], conn);
  });
/*
  // add the user to the orderbook set for this instrument
  db.sadd("orderbook:users:" + symbol, userid);

  // & add the instrument to the watchlist for this user
  db.sadd("user:" + userid + ":orderbooks", symbol);

  orderBookOut(userid, symbol, conn);*/
}

function orderBookOut(userid, symbol, conn) {
  if (outofhours) {
    // todo:
    //broadcastLevelTwo(symbol, conn);
  } else {
    subscribeAndSend(userid, symbol, conn);
  }
}

function subscribeAndSend(userid, symbol, conn) {
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

    // get the user to check market extension - defaults to 'LD' for LSE delayed
    db.hgetall("user:" + userid, function(err, user) {
      if (err) {
        console.log(err);
        return;
      }

      if (!user) {
        console.log("user:" + userid + " not found");
        return;
      }

      // may need to adjust the topic to delayed
      inst.topic += user.marketext;

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
          db.publish("proquote", "subscribe:" + inst.topic);

          // add topic to the set
          db.sadd("proquote", inst.topic);
        }

        // is this user subscribed?
        /*db.sismember("proquote:users:" + inst.topic, userid, function(err, userfound) {
          if (err) {
            console.log(err);
            return;
          }

          if (!userfound) {
            console.log("subscribing user:" + userid + " to " + inst.topic)
            // add client to the set for this instrument
            db.sadd("proquote:users:" + inst.topic, userid);
          }
        });*/
      });

      // send the orderbook, with the current stored prices
      sendCurrentOrderBook(symbol, inst.topic, conn);
    });
  });
}

function sendCurrentOrderBook(symbol, topic, conn) {
  var orderbook = {prices : []};
  var level1 = {};
  var level2 = {};
  var level3 = {};
  var level4 = {};
  var level5 = {};
  var level6 = {};

  db.hgetall("topic:" + topic, function(err, topicrec) {
    if (err) {
      console.log(err);
      return;
    }

    if (!topicrec) {
      console.log("topic:" + topic + " not found");

      // send zeros
      topicrec = {};
      topicrec.bid1 = 0;
      topicrec.offer1 = 0;
      topicrec.bid2 = 0;
      topicrec.offer2 = 0;
      topicrec.bid3 = 0;
      topicrec.offer3 = 0;
    }

    // 3 levels
    level1.bid = topicrec.bid1;
    level1.level = 1;
    orderbook.prices.push(level1);
    level2.offer = topicrec.offer1;
    level2.level = 1;
    orderbook.prices.push(level2);
    level3.bid = topicrec.bid2;
    level3.level = 2;
    orderbook.prices.push(level3);
    level4.offer = topicrec.offer2;
    level4.level = 2;
    orderbook.prices.push(level4);
    level5.bid = topicrec.bid3;
    level5.level = 3;
    orderbook.prices.push(level5);
    level6.offer = topicrec.offer3;
    level6.level = 3;
    orderbook.prices.push(level6);

    orderbook.symbol = symbol;

    if (conn != null) {
      conn.write("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
    }
  });
}

function orderBookRemoveRequest(userid, symbol, conn) {
  db.eval(scriptunsubscribeinstrument, 2, symbol, userid, function(err, ret) {
    if (err) throw err;

    console.log(ret);

    // the script will tell us if we need to unsubscribe from the topic
    if (ret[0]) {
      dbsub.unsubscribe(ret[1]);
    }
  });
}

function newClient(client, conn) {
  console.log("new client");
  console.log(client);

  // maybe a new client or an updated client
  if (client.clientid == "") {
    db.eval(scriptnewclient, 10, client.brokerid, client.name, client.email, client.mobile, client.address, client.ifaid, client.type, client.insttypes, client.hedge, client.brokerclientcode, function(err, ret) {
      if (err) throw err;

      if (ret[0] != 0) {
        console.log("Error in scriptnewclient:" + getReasonDesc(ret[0]));
        return;
      }

      getSendClient(ret[1], conn);
    });
  } else {
    db.eval(scriptupdateclient, 11, client.clientid, client.brokerid, client.name, client.email, client.mobile, client.address, client.ifaid, client.type, client.insttypes, client.hedge, client.brokerclientcode, function(err, ret) {
      if (err) throw err;

      if (ret != 0) {
        console.log("Error in scriptupdateclient:" + getReasonDesc(ret));
        return;
      }

      getSendClient(client.clientid, conn);
    });
  }
}

function newIfa(ifa, conn) {
  console.log("new ifa");
  console.log(ifa);

  db.eval(scriptifa, 5, ifa.ifaid, ifa.name, ifa.email, ifa.address, ifa.mobile, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      console.log("Error in scriptifa:" + getReasonDesc(ret[0]));
      return;
    }

    getSendIfa(ret[1], conn);
  });
}

function getSendIfa(ifaid, conn) {
    db.hgetall("ifa:" + ifaid, function(err, ifa) {
      if (err) throw err;

      conn.write("{\"ifa\":" + JSON.stringify(ifa) + "}");
    });
}

function cashTrans(cashtrans, userid, conn) {
  console.log("cashtrans");

  cashtrans.timestamp = getUTCTimeStamp();

  db.eval(scriptcashtrans, 8, cashtrans.clientid, cashtrans.currency, cashtrans.transtype, cashtrans.amount, cashtrans.desc, cashtrans.timestamp, operatortype, userid, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      console.log("Error in scriptcashtrans:" + getReasonDesc(ret[0]));
      return;
    }

    getSendCashtrans(ret[1], conn);
  });
}

function instUpdate(inst, userid, conn) {
  console.log("instUpdate");
  console.log(inst);

  db.eval(scriptinstupdate, 3, inst.symbol, inst.marginpercent, inst.hedge, function(err, ret) {
    if (err) throw err;

    getSendInst(inst.symbol, conn);
  });
}

function hedgebookUpdate(hedgebook, userid, conn) {
  console.log("hedgebookUpdate");
  console.log(hedgebook);

  db.eval(scripthedgeupdate, 3, hedgebook.insttype, hedgebook.currency, hedgebook.hedgebookid, function(err, ret) {
    if (err) throw err;

    getSendHedgebook(hedgebook, conn);
  });
}

function getSendHedgebook(hedgebook, conn) {
    var hedgebookkey = hedgebook.insttype + ":" + hedgebook.currency;

    db.get("hedgebook:" + hedgebookkey, function(err, hedgebookid) {
      var hedgebk = {};
      hedgebk.hedgebookkey = hedgebookkey;
      hedgebk.hedgebookid = hedgebookid;
      conn.write("{\"hedgebook\":" + JSON.stringify(hedgebk) + "}");
    })
}

function costUpdate(cost, conn) {
  console.log("costUpdate");
  console.log(cost);

  db.eval(scriptcost, 11, cost.insttype, cost.currency, cost.side, cost.commissionpercent, cost.commissionmin, cost.ptmlevylimit, cost.ptmlevy, cost.stampdutylimit, cost.stampdutypercent, cost.contractcharge, cost.finance, function(err, ret) {
    if (err) throw err;

    getSendCost(cost.insttype, cost.currency, cost.side, conn);
  });
}

function getSendCost(insttype, currency, side, conn) {
    var costkey = insttype + ":" + currency + ":" + side;

    db.hgetall("cost:" + costkey, function(err, cost) {
      conn.write("{\"cost\":" + JSON.stringify(cost) + "}");
    })
}

function getSendClient(clientid, conn) {
  db.hgetall("client:" + clientid, function(err, client) {
    if (err) {
      console.log(err);
      return;
    }

    // send anyway, even if no position, as may need to clear f/e - todo: review
    if (client == null) {
      console.log("Client not found, id:" + clientid);
      return;
    }

    // add instrument types this client can trade
    db.smembers(clientid + ":instrumenttypes", function(err, insttypes) {
      if (err) {
        console.log("Error in getSendClient:" + err);
        return;
      }

      client.insttypes = insttypes;

      conn.write("{\"client\":" + JSON.stringify(client) + "}");
    });
  });
}

function getSendCashtrans(cashtransid, conn) {
  db.hgetall("cashtrans:" + cashtransid, function(err, cashtrans) {
    if (err) {
      console.log(err);
      return;
    }

    // send anyway, even if no position, as may need to clear f/e - todo: review
    if (cashtrans == null) {
      console.log("Cash transaction not found:" + cashtransid);
      return;
    }

    conn.write("{\"cashtrans\":" + JSON.stringify(cashtrans) + "}");
  });
}

function getSendInst(symbol, conn) {
  db.hgetall("symbol:" + symbol, function(err, inst) {
    if (err) {
      console.log(err);
      return;
    }

    if (inst == null) {
      console.log("Instrument not found");
      return;
    }

    conn.write("{\"instrument\":" + JSON.stringify(inst) + "}");
  });
}

// roll date forwards by T+n number of days
function getSettDate(nosettdays) {
  var today = new Date();

  for (i = 0; i < nosettdays; i++) {
    today.setDate(today.getDate() + 1);

    // ignore weekends
    if (today.getDay() == 6) {
      today.setDate(today.getDate() + 2);
    } else if (today.getDay() == 0) {
      today.setDate(today.getDate() + 1);
    }

    // todo: add holidays
  }
  console.log(today);

  return today;
}

/*
// Convert dd-mmm-yyyy to FIX date format 'yyyymmdd'
*/
function getFixDate(date) {
  var month;

  var day = date.substr(0,2);
  var monthstr = date.substr(3,3);
  var year = date.substr(7,4);

  if (monthstr == "Jan") {
    month = "1";
  } else if (monthstr == "Feb") {
    month = "2";
  } else if (monthstr == "Mar") {
    month = "3";
  } else if (monthstr == "Apr") {
    month = "4";
  } else if (monthstr == "May") {
    month = "5";
  } else if (monthstr == "Jun") {
    month = "6";
  } else if (monthstr == "Jul") {
    month = "7";
  } else if (monthstr == "Aug") {
    month = "8";
  } else if (monthstr == "Sep") {
    month = "9";
  } else if (monthstr == "Oct") {
    month = "10";
  } else if (monthstr == "Nov") {
    month = "11";
  } else if (monthstr == "Dec") {
    month = "12";
  }

  if (month.length == 1) {
    month = "0" + month;
  }

  return year + month + day;
}

/*function rejectQuoteRequest(quoterequest, reason, conn) {
  var quote = {};

  // send a quote with bid & offer size set to 0 to imply rejection
  quote.quotereqid = quoterequest.quotereqid;
  quote.symbol = quoterequest.symbol;
  quote.bidsize = 0;
  quote.offersize = 0;
  quote.quoterejectreason = reason;
  quote.reasondesc = getReasonDesc(reason);

  conn.write("{\"quote\":" + JSON.stringify(quote) + "}");
}*/

function getSideDesc(side) {
  if (parseInt(side) == 1) {
    return "Buy";
  } else {
    return "Sell";
  }
}

function getSendOrder(orderid) {
  db.hgetall("order:" + orderid, function(err, order) {
    if (err) {
      console.log(err);
      return;
    }

    if (order == null) {
      console.log("Order #" + orderid + " not found");
      return;
    }

    // send to client, if connected
    if (order.operatorid in connections) {
      sendOrder(order, connections[order.operatorid]);
    }
  });
}

function getSendTrade(tradeid) {
  db.hgetall("trade:" + tradeid, function(err, trade) {
    if (err) {
      console.log(err);
      return;
    }

    if (trade == null) {
      console.log("Trade #" + tradeid + " not found");
      return;
    }

    db.hget("order:" + trade.orderid, "operatorid", function(err, operatorid) {
      if (err) {
        console.log(err);
        return;
      }

      if (operatorid in connections) {
        sendTrade(trade, connections[operatorid]);
      }
    });
  });
}

function getSendPosition(positionid, orgclientid) {
  var position = {};

  db.hgetall("position:" + positionid, function(err, pos) {
    if (err) {
      console.log(err);
      return;
    }

    // send anyway, even if no position, as may need to clear f/e - todo: review
    if (pos == null) {
      position.positionid = positionid;
      position.quantity = "0";
      position.cost = "0";
      //position.symbol = symbol;
      //position.currency = currency;
    } else {
      position = pos;
    }

    // send to client, if connected
    if (orgclientid in connections) {
      sendPosition(position, connections[orgclientid]);
    }
  });
}

function getSendCash(orgclientkey, currency) {
  var cash = {};

  db.get(orgclientkey + ":cash:" + currency, function(err, amount) {
    if (err) {
      console.log(err);
      return;
    }

    // send regardless, as may need to clear f/e
    if (amount == null) {
      cash.amount = "0";      
    } else {
      cash.amount = amount;
    }

    cash.currency = currency;

    // send to client, if connected
    if (orgclientkey in connections) {
      sendCashItem(cash, connections[orgclientkey]);
    }
  });
}

function getSendMargin(orgclientkey, currency) {
  var margin = {};

  db.get(orgclientkey + ":margin:" + currency, function(err, amount) {
    if (err) {
      console.log(err);
      return;
    }

    // send regardless, as may need to clear f/e
    if (amount == null) {
      margin.amount = "0";
    } else {
      margin.amount = amount;
    }

    margin.currency = currency;

    if (orgclientkey in connections) {
      sendMargin(margin, connections[orgclientkey]);
    }
  });
}

function getSendReserve(orgclientkey, symbol, currency) {
  var reserve = {};

  db.get(orgclientkey + ":reserve:" + symbol + ":" + currency, function(err, res) {
    if (err) {
      console.log(err);
      return;
    }

    // send regardless, as may need to clear f/e
    if (res == null) {
      reserve.quantity = "0";
    } else {
      reserve.quantity = res;
    }

    reserve.symbol = symbol;
    reserve.currency = currency;

    if (orgclientkey in connections) {
      sendReserve(reserve, connections[orgclientkey]);
    }
  });
}

function orderCancelReject(orgclientkey, ocr, reason) {
  var ordercancelreject = {};

  ordercancelreject.orderid = ocr.orderid;

  console.log("Order cancel request #" + ordercancelreject.orderid + " rejected, reason: " + getReasonDesc(reason));

  if (orgclientkey in connections) {
    connections[orgclientkey].write("{\"ordercancelreject\":" + JSON.stringify(ordercancelreject) + "}");
  }
}

function getValue(trade) {
  if (parseInt(trade.side) == 1) {
    return trade.quantity * trade.price + trade.commission + trade.costs;
  } else {
    return trade.quantity * trade.price - trade.commission - trade.costs;
  }
}

function sendInstruments(conn) {
  // get sorted subset of instruments for this client
  db.eval(scriptgetinst, 0, function(err, ret) {
    conn.write("{\"instruments\":" + ret + "}");
  });
}

function sendOrderTypes(conn) {
  var o = {ordertypes: []};
  var count;

  db.smembers("ordertypes", function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      console.log("Error: no order types in database");
      return;
    }

    replies.forEach(function(ordertypeid, i) {
      db.get("ordertype:" + ordertypeid, function(err, description) {
        var ordertype = {};

        ordertype.id = ordertypeid;
        ordertype.description = description;

        // add the order to the array
        o.ordertypes.push(ordertype);

        // our own global
        ordertypes[ordertypeid] = description;

        // send array if we have added the last item
        count--;
        if (count <= 0) {
          conn.write(JSON.stringify(o));
        }
      });
    });
  });
}

function sendIndex(orgclientkey, index, conn) {
  var i = {symbols: []};
  var count;

  // todo: remove this stuff?
  i.name = index;

  db.smembers("index:" + index, function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      console.log("Index: " + index + " not found");
      return;
    }

    replies.forEach(function(symbol, j) {
      db.hgetall("symbol:" + symbol, function(err, inst) {
        var instrument = {};
        if (err) {
          console.log(err);
          return;
        }

        if (inst == null) {
          console.log("Symbol " + symbol + " not found");
          count--;
          return;
        }

        instrument.symbol = symbol;

        // add the order to the array
        i.symbols.push(instrument);

        // send array if we have added the last item
        //count--;
        //if (count <= 0) {
          //conn.write("{\"index\":" + JSON.stringify(i) + "}");
          orderBookOut(orgclientkey, symbol, conn);
        //}
      });
    });
  });
}

function sendOrder(order, conn) {
  if (conn != null) {
    conn.write("{\"order\":" + JSON.stringify(order) + "}");
  }
}

function sendOrders(orgclientkey, conn) {
  var o = {orders: []};
  var count;

  db.smembers(orgclientkey + ":orders", function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function (orderid, i) {
      db.hgetall("order:" + orderid, function(err, order) {
        // only active orders
        // todo: check statii
        if (order.status == "0" || order.status == "1") {
          // add the order to the array
          o.orders.push(order);
        }

        // send array if we have added the last item
        count--;
        if (count <= 0) {
          conn.write(JSON.stringify(o));
        }
      });
    });
  });
}

function sendTrade(trade, conn) {
  if (conn != null) {
    conn.write("{\"trade\":" + JSON.stringify(trade) + "}");
  }
}

function sendTrades(orgclientkey, conn) {
  var t = {trades: []};
  var count;

  db.smembers(orgclientkey + ":trades", function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function (tradeid, i) {
      db.hgetall("trade:" + tradeid, function(err, trade) {
        t.trades.push(trade);

        // send array if we have added the last item
        count--;
        if (count <= 0) {
          conn.write(JSON.stringify(t));
        }
      });
    });
  });
}

function sendPosition(position, conn) {
  conn.write("{\"position\":" + JSON.stringify(position) + "}");
}

function sendPositions(orgclientkey, conn) {
  var posarray = {positions: []};
  var count;

  db.smembers(orgclientkey + ":positions", function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function(poskey, i) {
      db.hgetall("position:" + poskey, function(err, position) {
        if (err) {
          console.log(err);
          return;
        }

        posarray.positions.push(position);

        // send array if we have added the last item
        count--;
        console.log(count);
        if (count <= 0) {
          conn.write(JSON.stringify(posarray));
        }
      });
    });
  });
}

function sendCashItem(cash, conn) {
  conn.write("{\"cashitem\":" + JSON.stringify(cash) + "}");
}

function sendCash(orgclientkey, conn) {
  var arr = {cash: []};
  var count;

  db.smembers(orgclientkey + ":cash", function(err, replies) {
    if (err) {
      console.log("sendCash:" + err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function (currency, i) {
      db.get(orgclientkey + ":cash:" + currency, function(err, amount) {
        if (err) {
          console.log(err);
          return;
        }

        var cash = {};
        cash.currency = currency;
        cash.amount = amount;

        arr.cash.push(cash);

        // send array if we have added the last item
        count--;
        if (count <= 0) {
          conn.write(JSON.stringify(arr));
        }
      });
    });
  });
}

function sendMargin(margin, conn) {
  conn.write("{\"margin\":" + JSON.stringify(margin) + "}");
}

function sendMargins(orgclientkey, conn) {
  var arr = {margins: []};
  var count;

  db.smembers(orgclientkey + ":margins", function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function (currency, i) {
      db.get(orgclientkey + ":margin:" + currency, function(err, amount) {
        if (err) {
          console.log(err);
          return;
        }

        var margin = {};
        margin.currency = currency;
        margin.amount = amount;

        arr.margins.push(margin);

        // send array if we have added the last item
        count--;
        if (count <= 0) {
          conn.write(JSON.stringify(arr));
        }
      });
    });
  });
}

function sendReserve(reserve, conn) {
  conn.write("{\"reserve\":" + JSON.stringify(reserve) + "}");
}

function sendReserves(orgclientkey, conn) {
  var arr = {reserves: []};
  var count;

  db.smembers(orgclientkey + ":reserves", function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function (reskey, i) {
      db.hgetall(orgclientkey + ":reserve:" + reskey, function(err, reserve) {
        if (err) {
          console.log(err);
          return;
        }

        arr.reserves.push(reserve);

        // send array if we have added the last item
        count--;
        if (count <= 0) {
          conn.write(JSON.stringify(arr));
        }
      });
    });
  });
}

function sendAllOrderBooks(conn) {
  var level1arr = [];
  var count;
  var send = false;

  // get all the instruments in the order book
  db.smembers("orderbooks", function(err, instruments) {
    if (err) {
      console.log("error in sendAllOrderBooks:" + err);
      return;
    }

    count = instruments.length;
    if (count == 0) {
      return;
    }

    // send the order book for each instrument
    instruments.forEach(function(symbol, i) {
      count--;
      if (count <= 0) {
        send = true;
      }

      sendOrderBook(symbol, level1arr, send, conn);
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
function sendOrderBooks(userid, conn) {
  // get all the instruments in the order book for this user
  db.smembers("user:" + userid + ":orderbooks", function(err, instruments) {
    if (err) {
      console.log("Error in sendOrderBooks:" + err);
      return;
    }

    // send the order book for each instrument
    instruments.forEach(function (symbol, i) {
      orderBookOut(userid, symbol, conn);
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

function getTimeInForceDesc(timeinforce) {
/*0 = Day

1 = Good Till Cancel (GTC)

2 = At the Opening (OPG)

3 = Immediate or Cancel (IOC)

4 = Fill or Kill (FOK)

5 = Good Till Crossing (GTX)

6 = Good Till Date*/
}

function getReasonDesc(reason) {
  var desc;

  switch (parseInt(reason)) {
    case 1001:
      desc = "No currency held for this instrument";
      break;
    case 1002:
      desc = "Insufficient cash in settlement currency";
      break;
    case 1003:
      desc = "No position held in this instrument";
      break;
    case 1004:
      desc = "Insufficient position size in this instrument";
      break;
    case 1005:
      desc = "System error";
      break;
    case 1006:
      desc = "Invalid order";
      break;
    case 1007:
      desc = "Invalid instrument";
      break;
    case 1008:
      desc = "Order already cancelled";
      break;
    case 1009:
      desc = "Order not found";
      break;
    case 1010:
      desc = "Order already filled";
      break;
    case 1011:
      desc = "Order currency does not match symbol currency";
      break;
    case 1012:
      desc = "Order already rejected";
      break;
    case 1013:
      desc = "Ordercancelrequest not found";
      break;
    case 1014:
      desc = "Quoterequest not found";
      break;
    case 1015:
      desc = "Symbol not found";
      break;
    case 1016:
      desc = "Proquote symbol not found";
      break;
    case 1017:
      desc = "Client not found";
      break;
    case 1018:
      desc = "Client not authorised to trade this type of product";
      break;
    default:
      desc = "Unknown reason";
  }

  return desc;
}

function start(userid, brokerid, conn) {
  sendOrderBooks(userid, conn);
  sendInstruments(conn);
  sendBrokers(conn);
  sendIfas(conn);
  sendInstrumentTypes(conn);
  sendOrderTypes(conn);
  sendCashTransTypes(conn);
  sendCurrencies(conn);
  sendClientTypes(conn);
  sendHedgebooks(conn);
  sendCosts(conn);

  // make this the last one, as sends ready status to f/e
  sendClients(userid, brokerid, conn);
}

function sendUserid(userid, conn) {
  conn.write("{\"userid\":" + userid + "}");
}

function replySignIn(reply, conn) {
    conn.write("{\"signinreply\":" + JSON.stringify(reply) + "}");
}

function initDb() {
  registerScripts();
}

function registerClient(reg, conn) {
  var reply = {};

  db.sismember("clients", reg.email, function(err, found) {
    if (found) {
      reply.success = "false";
      reply.email = reg.email;
      reply.reason = "Email address already exists";
      conn.write("{\"registerreply\":" + JSON.stringify(reply) + "}");
      return;
    }

    // get the current client id
    db.get("clientid", function(err, nextclientid) {
      if (err) {
        console.log(err);
        return;
      }

      db.sadd("clients", reg.email);

      db.hmset(reg.email, "password", "cw2t", "clientid", nextclientid);

      reply.success = "true";
      reply.email = reg.email;
      reply.password = "cw2t";

      conn.write("{\"registerreply\":" + JSON.stringify(reply) + "}");
    });

    // increment client id
    db.incr("clientid");
  });
}

function getUTCTimeStamp() {
    var timestamp = new Date();

    var year = timestamp.getUTCFullYear();
    var month = timestamp.getUTCMonth() + 1; // flip 0-11 -> 1-12
    var day = timestamp.getUTCDate();
    var hours = timestamp.getUTCHours();
    var minutes = timestamp.getUTCMinutes();
    var seconds = timestamp.getUTCSeconds();
    //var millis = timestamp.getUTCMilliseconds();

    if (month < 10) {month = '0' + month;}

    if (day < 10) {day = '0' + day;}

    if (hours < 10) {hours = '0' + hours;}

    if (minutes < 10) {minutes = '0' + minutes;}

    if (seconds < 10) {seconds = '0' + seconds;}

    /*if (millis < 10) {
        millis = '00' + millis;
    } else if (millis < 100) {
        millis = '0' + millis;
    }*/

    //var ts = [year, month, day, '-', hours, ':', minutes, ':', seconds, '.', millis].join('');
    var ts = [year, month, day, '-', hours, ':', minutes, ':', seconds].join('');

    return ts;
}

function getUTCDateString(date) {
    var year = date.getUTCFullYear();
    var month = date.getUTCMonth() + 1; // flip 0-11 -> 1-12
    var day = date.getUTCDate();

    if (month < 10) {month = '0' + month;}

    if (day < 10) {day = '0' + day;}

    var utcdate = "" + year + month + day;

    return utcdate;
}

/*
* Get the nuber of seconds between two UTC datetimes
*/
function getSeconds(startutctime, finishutctime) {
  var startdt = new Date(getDateString(startutctime));
  var finishdt = new Date(getDateString(finishutctime));
  return ((finishdt - startdt) / 1000);
}

/*
* Convert a UTC datetime to a valid string for creating a date object
*/
function getDateString(utcdatetime) {
    return (utcdatetime.substr(0,4) + "/" + utcdatetime.substr(4,2) + "/" + utcdatetime.substr(6,2) + " " + utcdatetime.substr(9,8));
}

function sendClients(userid, brokerid, conn) {
  // get sorted set of clients for specified broker
  db.eval(scriptgetclients, 1, brokerid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"clients\":" + ret + "}");

    sendUserid(userid, conn);
  });
}

function sendBrokers(conn) {
  // get sorted set of brokers
  db.eval(scriptgetbrokers, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"brokers\":" + ret + "}");
  });
}

function sendIfas(conn) {
  // get sorted set of ifas
  db.eval(scriptgetifas, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"ifas\":" + ret + "}");
  });
}

function sendInstrumentTypes(conn) {
  db.eval(scriptgetinstrumenttypes, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"instrumenttypes\":" + ret + "}");
  });
}

function sendCashTransTypes(conn) {
  db.eval(scriptgetcashtranstypes, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"cashtranstypes\":" + ret + "}");
  });  
}

function sendCurrencies(conn) {
  db.eval(scriptgetcurrencies, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"currencies\":" + ret + "}");
  });  
}

function sendClientTypes(conn) {
  db.eval(scriptgetclienttypes, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"clienttypes\":" + ret + "}");
  });    
}

function sendHedgebooks(conn) {
  db.eval(scriptgethedgebooks, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"hedgebooks\":" + ret + "}");
  });      
}

function sendCosts(conn) {
  db.eval(scriptgetcosts, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"costs\":" + ret + "}");
  });      
}

function sendQuoteack(quotereqid) {
  var quoteack = {};

  db.hgetall("quoterequest:" + quotereqid, function(err, quoterequest) {
    if (err) {
      console.log(err);
      return;
    }

    if (quoterequest == null) {
      console.log("can't find quote request id " + quotereqid);
      return;
    }

    quoteack.quotereqid = quoterequest.quotereqid;
    quoteack.clientid = quoterequest.clientid;
    quoteack.symbol = quoterequest.symbol;
    quoteack.quoterejectreasondesc = getPTPQuoteRejectReason(quoterequest.quoterejectreason);
    if ('text' in quoterequest) {
      quoteack.text = quoterequest.text;
    }

    // send the quote acknowledgement
    if (quoterequest.operatorid in connections) {
      connections[quoterequest.operatorid].write("{\"quoteack\":" + JSON.stringify(quoteack) + "}");
    }
  });
}

function sendQuote(quoteid) {
  // get the quote
  db.hgetall("quote:" + quoteid, function(err, quote) {
    if (err) {
      console.log(err);
      return;
    }

    if (quote == null) {
      console.log("Unable to find quote:" + quoteid);
      return;
    }

    // get the number of seconds the quote is valid for
    quote.noseconds = getSeconds(quote.transacttime, quote.validuntiltime);

    // send quote to the user who placed the quote request
    db.hget("quoterequest:" + quote.quotereqid, "operatorid", function(err, operatorid) {
      if (err) {
        console.log(err);
        return;
      }

      if (operatorid in connections) {
        connections[operatorid].write("{\"quote\":" + JSON.stringify(quote) + "}");
      }
    });
  });
}

//
// todo: make common
//
function getPTPQuoteRejectReason(reason) {
  var desc;
  console.log(reason);

  switch (parseInt(reason)) {
  case 1:
    desc = "Unknown symbol";
    break;
  case 2:
    desc = "Exchange closed";
    break;
  case 3:
    desc = "Quote Request exceeds limit";
    break;
  case 4:
    desc = "Too late to enter";
    break;
  case 5:
    desc = "Unknown Quote";
    break;
  case 6:
    desc = "Duplicate Quote";
    break;
  case 7:
    desc = "Invalid bid/ask spread";
    break;
  case 8:
    desc = "Invalid price";
    break;
  case 9:
    desc = "Not authorized to quote security";
    break;
  }

  return desc;
}

function registerScripts() {
  var stringsplit;
  var updatecash;
  var subscribeinstrument;
  var unsubscribeinstrument;

  //
  // function to split a string into an array of substrings, based on a character
  // parameters are the string & character
  // i.e. stringsplit("abc,def,hgi", ",") = ["abc", "def", "hgi"]
  //
  stringsplit = '\
  local stringsplit = function(str, inSplitPattern) \
    local outResults = {} \
    local theStart = 1 \
    local theSplitStart, theSplitEnd = string.find(str, inSplitPattern, theStart) \
    while theSplitStart do \
      table.insert(outResults, string.sub(str, theStart, theSplitStart-1)) \
      theStart = theSplitEnd + 1 \
      theSplitStart, theSplitEnd = string.find(str, inSplitPattern, theStart) \
    end \
    table.insert(outResults, string.sub(str, theStart)) \
    return outResults \
  end \
  ';

  //
  // todo: tie in with server
  //
  updatecash = '\
  local updatecash = function(clientid, currency, transtype, amount, desc, timestamp, operatortype, operatorid) \
    local cashtransid = redis.call("incr", "cashtransid") \
    if not cashtransid then return {1005} end \
    redis.call("hmset", "cashtrans:" .. cashtransid, "clientid", clientid, "currency", currency, "transtype", transtype, "amount", amount, "desc", desc, "timestamp", timestamp, "operatortype", operatortype, "operatorid", operatorid, "cashtransid", cashtransid) \
    local cashkey = clientid .. ":cash:" .. currency \
    local cashskey = clientid .. ":cash" \
    local cash = redis.call("get", cashkey) \
    --[[ take account of +/- transaction type ]] \
    if transtype == "TB" or transtype == "CO" or transtype == "JO" then \
      amount = -tonumber(amount) \
    end \
    if not cash then \
      redis.call("set", cashkey, amount) \
      redis.call("sadd", cashskey, currency) \
    else \
      local adjamount = tonumber(cash) + tonumber(amount) \
      if adjamount == 0 then \
        redis.call("del", cashkey) \
        redis.call("srem", cashskey, currency) \
      else \
        redis.call("set", cashkey, adjamount) \
      end \
    end \
    return {0, cashtransid} \
  end \
  ';

  //
  // get alpha sorted list of clients for a specified broker
  //
  scriptgetclients = '\
  local clients = redis.call("sort", "clients", "ALPHA") \
  local fields = {"brokerid", "clientid", "email", "name", "address", "mobile", "ifaid", "type", "hedge", "brokerclientcode"} \
  local vals \
  local tblclient = {} \
  local tblinsttype = {} \
  for index = 1, #clients do \
    vals = redis.call("hmget", "client:" .. clients[index], unpack(fields)) \
    if KEYS[1] == vals[1] then \
      tblinsttype = redis.call("smembers", vals[2] .. ":instrumenttypes") \
      table.insert(tblclient, {brokerid = vals[1], clientid = vals[2], email = vals[3], name = vals[4], address = vals[5], mobile = vals[6], ifaid = vals[7], insttypes = tblinsttype, type = vals[8], hedge = vals[9], brokerclientcode = vals[10]}) \
    end \
  end \
  return cjson.encode(tblclient) \
  ';

  scriptnewclient = stringsplit + '\
  local clientid = redis.call("incr", "clientid") \
  if not clientid then return {1005} end \
  --[[ store the client ]] \
  redis.call("hmset", "client:" .. clientid, "clientid", clientid, "brokerid", KEYS[1], "name", KEYS[2], "email", KEYS[3], "password", KEYS[3], "mobile", KEYS[4], "address", KEYS[5], "ifaid", KEYS[6], "type", KEYS[7], "hedge", KEYS[9], "brokerclientcode", KEYS[10], "marketext", "D") \
  --[[ add to set of clients ]] \
  redis.call("sadd", "clients", clientid) \
  --[[ add route to find client from email ]] \
  redis.call("set", "client:" .. KEYS[3], clientid) \
  --[[ add tradeable instrument types ]] \
  if KEYS[8] ~= "" then \
    local insttypes = stringsplit(KEYS[8], ",") \
    for i = 1, #insttypes do \
      redis.call("sadd", clientid .. ":instrumenttypes", insttypes[i]) \
    end \
  end \
  return {0, clientid} \
  ';

  scriptupdateclient = stringsplit + '\
  local clientkey = "client:" .. KEYS[1] \
  --[[ get existing email, in case we need to change email->client link ]] \
  local email = redis.call("hget", clientkey, "email") \
  if not email then return 1017 end \
  --[[ update client ]] \
  redis.call("hmset", "client:" .. KEYS[1], "clientid", KEYS[1], "brokerid", KEYS[2], "name", KEYS[3], "email", KEYS[4], "mobile", KEYS[5], "address", KEYS[6], "ifaid", KEYS[7], "type", KEYS[8], "hedge", KEYS[10], "brokerclientcode", KEYS[11]) \
  --[[ remove old email link and add new one ]] \
  if KEYS[4] ~= email then \
    redis.call("del", "client:" .. email) \
    redis.call("set", "client:" .. KEYS[4], KEYS[1]) \
  end \
  --[[ add/remove tradeable instrument types ]] \
  local insttypes = redis.call("smembers", "instrumenttypes") \
  local clientinsttypes = stringsplit(KEYS[9], ",") \
  for i = 1, #insttypes do \
    local found = false \
    for j = 1, #clientinsttypes do \
      if clientinsttypes[j] == insttypes[i] then \
        redis.call("sadd", KEYS[1] .. ":instrumenttypes", insttypes[i]) \
        found = true \
        break \
      end \
    end \
    if not found then \
      redis.call("srem", KEYS[1] .. ":instrumenttypes", insttypes[i]) \
    end \
  end \
  return 0 \
  ';

  scriptcashtrans = updatecash + '\
  local ret = updatecash(KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8]) \
  return ret \
  ';

  scriptgetbrokers = '\
  local brokers = redis.call("sort", "brokers", "ALPHA") \
  local fields = {"brokerid", "name"} \
  local vals \
  local broker = {} \
  for index = 1, #brokers do \
    vals = redis.call("hmget", "broker:" .. brokers[index], unpack(fields)) \
    table.insert(broker, {brokerid = vals[1], name = vals[2]}) \
  end \
  return cjson.encode(broker) \
  ';

  scriptgetclienttypes = '\
  local clienttypes = redis.call("sort", "clienttypes", "ALPHA") \
  local clienttype = {} \
  local val \
  for index = 1, #clienttypes do \
    val = redis.call("get", "clienttype:" .. clienttypes[index]) \
    table.insert(clienttype, {clienttypeid = clienttypes[index], description = val}) \
  end \
  return cjson.encode(clienttype) \
  ';

  scriptgetinstrumenttypes = '\
  local instrumenttypes = redis.call("smembers", "instrumenttypes") \
  local instrumenttype = {} \
  local val \
  for index = 1, #instrumenttypes do \
    val = redis.call("get", "instrumenttype:" .. instrumenttypes[index]) \
    table.insert(instrumenttype, {instrumenttypeid = instrumenttypes[index], description = val}) \
  end \
  return cjson.encode(instrumenttype) \
  ';

  scriptgetcashtranstypes = '\
  local cashtranstypes = redis.call("sort", "cashtranstypes", "ALPHA") \
  local cashtranstype = {} \
  local val \
  for index = 1, #cashtranstypes do \
    val = redis.call("get", "cashtranstype:" .. cashtranstypes[index]) \
    table.insert(cashtranstype, {cashtranstypeid = cashtranstypes[index], description = val}) \
  end \
  return cjson.encode(cashtranstype) \
  ';

  scriptgetcurrencies = '\
  local currencies = redis.call("sort", "currencies", "ALPHA") \
  local currency = {} \
  for index = 1, #currencies do \
    table.insert(currency, currencies[index]) \
  end \
  return cjson.encode(currency) \
  ';

  scriptgethedgebooks = '\
  local hedgebooks = redis.call("sort", "hedgebooks", "ALPHA") \
  local hedgebook = {} \
  local val \
  for index = 1, #hedgebooks do \
    val = redis.call("get", "hedgebook:" .. hedgebooks[index]) \
    table.insert(hedgebook, {hedgebookkey = hedgebooks[index], hedgebookid = val}) \
  end \
  return cjson.encode(hedgebook) \
  ';

  //
  // get alpha sorted list of instruments based on set of valid instrument types
  //
  scriptgetinst = '\
  local instruments = redis.call("sort", "instruments", "ALPHA") \
  local fields = {"instrumenttype", "description", "currency", "marginpercent", "market", "isin", "sedol", "sector", "hedge"} \
  local vals \
  local inst = {} \
  local marginpc \
  for index = 1, #instruments do \
    vals = redis.call("hmget", "symbol:" .. instruments[index], unpack(fields)) \
    if redis.call("sismember", "instrumenttypes", vals[1]) == 1 then \
      if vals[4] then \
        marginpc = vals[4] \
      else \
        marginpc = 100 \
      end \
      table.insert(inst, {symbol = instruments[index], description = vals[2], currency = vals[3], instrumenttype = vals[1], marginpercent = marginpc, market = vals[5], isin = vals[6], sedol = vals[7], sector = vals[8], hedge = vals[9]}) \
    end \
  end \
  return cjson.encode(inst) \
  ';

  scriptinstupdate = '\
  redis.call("hmset", "symbol:" .. KEYS[1], "marginpercent", KEYS[2], "hedge", KEYS[3]) \
  ';

  scripthedgeupdate = '\
  local hedgebookkey = KEYS[1] .. ":" .. KEYS[2] \
  redis.call("set", "hedgebook:" .. hedgebookkey, KEYS[3]) \
  redis.call("sadd", "hedgebooks", hedgebookkey) \
  ';

  scriptcost = '\
  local costkey = KEYS[1] .. ":" .. KEYS[2] .. ":" .. KEYS[3] \
  redis.call("hmset", "cost:" .. costkey, "costkey", costkey, "commissionpercent", KEYS[4], "commissionmin", KEYS[5], "ptmlevylimit", KEYS[6], "ptmlevy", KEYS[7], "stampdutylimit", KEYS[8], "stampdutypercent", KEYS[9], "contractcharge", KEYS[10], "finance", KEYS[11]) \
  redis.call("sadd", "costs", costkey) \
  ';

  // todo: use 'hgetall' & ipair to get all vals
  scriptgetcosts = '\
  local costs = redis.call("smembers", "costs") \
  local fields = {"commissionpercent", "commissionmin", "ptmlevylimit", "ptmlevy", "stampdutylimit", "stampdutypercent", "contractcharge", "finance"} \
  local vals \
  local cost = {} \
  for index = 1, #costs do \
    vals = redis.call("hmget", "cost:" .. costs[index], unpack(fields)) \
    table.insert(cost, {costkey = costs[index], commissionpercent = vals[1], commissionmin = vals[2], ptmlevylimit = vals[3], ptmlevy = vals[4], stampdutylimit = vals[5], stampdutypercent = vals[6], contractcharge = vals[7], finance = vals[8]}) \
  end \
  return cjson.encode(cost) \
  ';

  scriptgetifas = '\
  local ifas = redis.call("sort", "ifas", "ALPHA") \
  local fields = {"ifaid", "name", "email", "address", "mobile"} \
  local vals \
  local ifa = {} \
  for index = 1, #ifas do \
    vals = redis.call("hmget", "ifa:" .. ifas[index], unpack(fields)) \
    table.insert(ifa, {ifaid = vals[1], name = vals[2], email = vals[3], address = vals[4], mobile = vals[5]}) \
  end \
  return cjson.encode(ifa) \
  ';

  scriptifa = '\
  local ifaid \
  if KEYS[1] == "" then \
    ifaid = redis.call("incr", "ifaid") \
    --[[ set the default password to email ]] \
    redis.call("hmset", "ifa:" .. ifaid, "ifaid", ifaid, "name", KEYS[2], "email", KEYS[3], "password", KEYS[3], "address", KEYS[4], "mobile", KEYS[5]) \
  else \
    ifaid = KEYS[1] \
    --[[ do not update id/password ]] \
    redis.call("hmset", "ifa:" .. ifaid, "name", KEYS[2], "email", KEYS[3], "address", KEYS[4], "mobile", KEYS[5]) \
  end \
  redis.call("sadd", "ifas", ifaid) \
  return {0, ifaid} \
  ';

  subscribeinstrument = '\
  local subscribeinstrument = function(symbol, userid) \
    local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    local marketext = redis.call("hget", "user:" .. userid, "marketext") \
    if marketext then \
      topic = topic .. marketext \
    end \
    local needtosubscribe = 0 \
    if redis.call("sismember", "proquote:topics:user", topic) == 0 then \
      redis.call("publish", "proquote", "subscribe:" .. topic) \
      redis.call("sadd", "proquote:topics:user", topic) \
      needtosubscribe = 1 \
    end \
    redis.call("sadd", "topic:" .. topic .. ":users", userid) \
    redis.call("sadd", "user:" .. userid .. ":topics", topic) \
    redis.call("sadd", "topic:" .. topic .. ":user:" .. userid .. ":symbols", symbol) \
    redis.call("sadd", "topic:" .. topic .. ":symbol:" .. symbol .. ":users", userid) \
    return {needtosubscribe, topic} \
  end \
  ';

  // params: symbol, userid
  scriptsubscribeinstrument = subscribeinstrument + '\
  redis.call("sadd", "orderbook:" .. KEYS[1] .. ":users", KEYS[2]) \
  redis.call("sadd", "user:" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  local ret = subscribeinstrument(KEYS[1], KEYS[2]) \
  return ret \
  ';

  scriptsubscribeuser = subscribeinstrument + '\
  local orderbooks = redis.call("smembers", "user:" .. KEYS[1] .. ":orderbooks") \
  for i = 1, #orderbooks do \
    subscribeinstrument(orderbooks[i], KEYS[1]) \
  end \
  ';

  unsubscribeinstrument = '\
  local unsubscribeinstrument = function(symbol, userid) \
    local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    local marketext = redis.call("hget", "user:" .. userid, "marketext") \
    if marketext then \
      topic = topic .. marketext \
    end \
    local needtounsubscribe = 0 \
    redis.call("srem", "topic:" .. topic .. ":symbol:" .. symbol .. ":users", userid) \
    redis.call("srem", "topic:" .. topic .. ":user:" .. userid .. ":symbols", symbol) \
    if redis.call("scard", "topic:" .. topic .. ":user:" .. userid .. ":symbols") == 0 then \
      redis.call("srem", "user:" .. userid .. ":topics", topic) \
      redis.call("srem", "topic:" .. topic .. ":users", userid) \
      if redis.call("scard", "topic:" .. topic .. ":users") == 0 then \
        redis.call("publish", "proquote", "unsubscribe:" .. topic) \
        redis.call("srem", "proquote:topics:user", topic) \
        needtounsubscribe = 1 \
      end \
    end \
    return {needtounsubscribe, topic} \
  end \
  ';

  // params: symbol, userid
  scriptunsubscribeinstrument = unsubscribeinstrument + '\
  redis.call("srem", "orderbook:" .. KEYS[1] .. ":users", KEYS[2]) \
  redis.call("srem", "user:" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  local ret = unsubscribeinstrument(KEYS[1], KEYS[2]) \
  return ret \
  ';

  scriptunsubscribeuser = unsubscribeinstrument + '\
  local orderbooks = redis.call("smembers", "user:" .. KEYS[1] .. ":orderbooks") \
  for i = 1, #orderbooks do \
    unsubscribeinstrument(orderbooks[i], KEYS[1]) \
  end \
  ';

  /*local topics = redis.call("smembers", "user:" .. KEYS[1] .. ":topics") \
  local needtounsubscribe = {} \
  for i = 1, #topics do \
    local symbols = redis.call("smembers", "topic:" .. topics[i] .. ":user:" .. KEYS[1] .. ":symbols") \
    for j = 1, #symbols do \
      redis.call("srem", "topic:" .. topics[i] .. ":user:" .. KEYS[1] .. ":symbols", symbols[j]) \
      redis.call("srem", "topic:" .. topics[i] .. ":symbol:" .. symbols[j] .. ":users", KEYS[1]) \
    end \
    redis.call("srem", "user:" .. KEYS[1] .. ":topics", topics[i]) \
    redis.call("srem", "topic:" .. topics[i] .. ":users", KEYS[1]) \
    if redis.call("scard", "topic:" .. topics[i] .. ":users") == 0 then \
      redis.call("publish", "proquote", "unsubscribe:" .. topics[i]) \
      redis.call("srem", "proquote:topics:user", topics[i]) \
      table.insert(needtounsubscribe, topics[i]) \
    end \
  end \
  return needtounsubscribe \
  ';*/

  scriptnewprice = '\
  local symbols = redis.call("smembers", "topic:" .. KEYS[1] .. ":symbols") \
  local voyeurs = {} \
  for i = 1, #symbols do \
    local users = redis.call("smembers", "topic:" .. KEYS[1] .. ":symbol:" .. symbols[i] .. ":users") \
    for j = 1, #users do \
      table.insert(voyeurs, {symbol = symbols[i], userid = users[j]}) \
    end \
  end \
  return voyeurs \
';
}
