/****************
* server.js
* Front-office client server
* Cantwaittotrade Limited
* Terry Johnston
* September 2012
* 
* Server to direct trading clients
*
****************/

// node libraries
var http = require('http');
var net = require('net');

// external libraries
var sockjs = require('sockjs');
var node_static = require('node-static');
var redis = require('redis');

// cw2t libraries

// globals
var connections = {}; // added to if & when a client logs on
var static_directory = new node_static.Server(__dirname); // static files server
var cw2tport = 8080; // client listen port
var outofhours = false; // in or out of market hours - todo: replace with markettype?
var ordertypes = {};
var orgid = "1"; // todo: via logon
var defaultnosettdays = 3;
var tradeserverchannel = 3;
var clientserverchannel = 1;

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
var scriptmatchorder;
var scriptordercancelrequest;
var scriptordercancel;
var scriptorderack;
var scriptnewtrade;
var scriptrejectorder;
var scriptgetinst;

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
  console.log("Db Error:" + err);
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

    if (message.substr(0, 8) == "quoteack") {
      sendQuoteack(message.substr(9));
    } else if (message.substr(0, 5) == "quote") {
      sendQuote(message.substr(6));
    } else if (message.substr(0, 5) == "order") {
      getSendOrder(message.substr(6), true, true);
    } else if (message.substr(0, 5) == "trade") {
      getSendTrade(message.substr(6));
    }

    // get the set of symbols linked to this topic (i.e. BARC.L & BARC.L.CFD are both linked to TIT.BARC.L)
    /*db.smembers("topicsymbol:" + channel, function(err, replies) {
      if (err) {
        console.log(err);
        return;
      }

      replies.forEach(function(symbol, i) {
        db.smembers("proquote:" + symbol, function(err, replies) {
          if (err) {
            console.log(err);
            return;
          }

          if (orgclientkey in connections) {
            connections[orgclientkey].write(message);
          }
        });
      });*/

    /*db.smembers("proquote:" + channel, function(err, replies) {
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
        replies.forEach(function(clientid, i) {
          if (clientid in connections) {
            connections[clientid].write(message);
          }
        });
      });
    });*/
  });

  dbsub.subscribe(clientserverchannel);
}

// sockjs server
var sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.3.min.js"};
var sockjs_svr = sockjs.createServer(sockjs_opts);

// ssl
/*var options = {
  key: fs.readFileSync('ssl.key'),
  cert: fs.readFileSync('ssl.crt')
};
var server = http.createServer();*/

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

    console.log('new connection');

    // data callback
    // todo: multiple messages in one data event
    conn.on('data', function(msg) {
      var obj;
      console.log('recd:' + msg);

      if (msg.substr(2, 18) == "ordercancelrequest") {
        obj = JSON.parse(msg);
        orderCancelRequest(clientid, obj.ordercancelrequest);
      } else if (msg.substr(2, 16) == "orderbookrequest") {
        obj = JSON.parse(msg);
        orderBookRequest(clientid, obj.orderbookrequest, conn);
      } else if (msg.substr(2, 22) == "orderbookremoverequest") {
        obj = JSON.parse(msg);
        orderBookRemoveRequest(clientid, obj.orderbookremoverequest, conn);
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
        sendIndex(clientid, obj.index, conn);   
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
        if (clientid in connections) {
          console.log("client:" + clientid + " logged off");
          // remove from list
          delete connections[clientid];

          // remove from database
          db.srem("connections:clients", clientid);
        }

        unsubscribeTopics(clientid);
        clientid = "0";
      }
    });

    // client sign on
    function signIn(signin) {
      var reply = {};
      reply.success = false;

      db.get("client:" + signin.email, function(err, cid) {
        if (err) {
          console.log("Error in signIn:" + err);
          return;
        }

        if (!cid) {
          console.log("Email not found:" + signin.email);
          reply.reason = "Email or password incorrect. Please try again.";
          replySignIn(reply, conn);
          return;
        }

        // validate email/password
        db.hgetall("client:" + cid, function(err, client) {
          if (err) {
            console.log("Error in signIn:" + err);
            return;
          }

          if (!client || signin.email != client.email || signin.password != client.password) {
            reply.reason = "Email or password incorrect. Please try again.";
            replySignIn(reply, conn);
            return;
          }

          // validated, so set client id & add client to list of connections
          clientid = client.clientid;
          connections[clientid] = conn;

          // keep a record
          db.sadd("connections:clients", clientid);

          // send a successful logon reply
          reply.success = true;
          reply.email = signin.email;
          reply.clientid = clientid;
          replySignIn(reply, conn);

          console.log("client:" + clientid + " logged on");

          // send the data
          start(clientid, conn);
        });
      });
    }
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

function matchOrder(orderid) {
  db.eval(scriptmatchorder, 1, orderid, function(err, ret) {
    if (err) throw err;

    /*if (orderid == 2000 || orderid == 4000) {
      var now = new Date().getTime();
      var elapsed = now-startms;
      console.log("done,"+elapsed);
    }*/

    // todo: replace with getsendorder?

    // todo: check ret value

    // get order here as we need a number of order values
    db.hgetall("order:" + orderid, function(err, order) {
      if (err) {
        console.log(err);
        return;
      }

      // send to client, if connected
      if (order.clientid in connections) {
        sendOrder(order, connections[order.clientid]);
      }

      // broadcast market - todo: timer based? - pubsub?
      broadcastLevelTwo(order.symbol, null);

      // send any trades for active order client
      for (var i = 0; i < ret[1].length; ++i) {
        getSendTrade(ret[1][i]);
      }

      // send cash
      if (ret[1].length > 0) {
        getSendCash(order.clientid, order.settlcurrency);
      }

      // send margin/reserve
      if (parseInt(order.side) == 1) {
        getSendMargin(order.clientid, order.settlcurrency);
      } else {
        getSendReserve(order.clientid, order.symbol, order.settlcurrency);
      }

      // send any matched orders
      for (var i = 0; i < ret[0].length; ++i) {
        // todo: send margin/reserve/cash
        getSendOrder(ret[0][i], false, false);
      }

      // send any matched trades
      for (var i = 0; i < ret[2].length; ++i) {
        getSendTrade(ret[2][i]);
      }

      // send cash/position/margin/reserve for matched clients
      for (var i = 0; i < ret[3].length; ++i) {
        getSendCash(ret[3][i], order.settlcurrency);
        getSendMargin(ret[3][i], order.settlcurrency);
        getSendReserve(ret[3][i], order.symbol, order.settlcurrency);
      }
    });
  });
}

function getSendOrder(orderid, sendmarginreserve, sendcash) {
  db.hgetall("order:" + orderid, function(err, order) {
    if (err) {
      console.log(err);
      return;
    }

    if (order == null) {
      console.log("Order:" + orderid + " not found");
      return;
    }

    // send to client, if connected
    if (order.clientid in connections) {
      sendOrder(order, connections[order.clientid]);
    }

    if (sendmarginreserve) {
      getSendMargin(order.clientid, order.currency);
      getSendReserve(order.clientid, order.symbol, order.settlcurrency);
    }

    if (sendcash) {
      getSendCash(order.clientid, order.settlcurrency);
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
      console.log("Trade:" + tradeid + " not found");
      return;
    }

    // send to client, if connected
    if (trade.clientid in connections) {
      sendTrade(trade, connections[trade.clientid]);
    }

    if ("positionid" in trade) {
      getSendPosition(trade.positionid, trade.clientid);
    }
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

function getSendCash(clientid, currency) {
  var cash = {};

  db.get(clientid + ":cash:" + currency, function(err, amount) {
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
    if (clientid in connections) {
      sendCashItem(cash, connections[clientid]);
    }
  });
}

function getSendMargin(clientid, currency) {
  var margin = {};

  db.get(clientid + ":margin:" + currency, function(err, amount) {
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

    if (clientid in connections) {
      sendMargin(margin, connections[clientid]);
    }
  });
}

function getSendReserve(clientid, symbol, currency) {
  var reserve = {};

  db.get(clientid + ":reserve:" + symbol + ":" + currency, function(err, res) {
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

    if (clientid in connections) {
      sendReserve(reserve, connections[clientid]);
    }
  });
}

/*function orderCancelRequest(clientid, ocr) {
  console.log("Order cancel request received for order#" + ocr.orderid);

  ocr.timestamp = getUTCTimeStamp();

  db.eval(scriptordercancelrequest, 4, orgid, clientid, ocr.orderid, ocr.timestamp, function(err, ret) {
    if (err) throw err;

    console.log(ret);

    var orgclientkey = orgid + ":" + clientid;

    // error, so send a cancel reject message
    if (ret[0] != 0) {
      orderCancelReject(orgclientkey, ocr, ret[0]);
      return;
    }

    // forward to Proquote
    if (ret[1] == "0") {
      ocr.ordercancelreqid = ret[2];
      ocr.symbol = ret[3];
      ocr.isin = ret[4];
      ocr.proquotesymbol = ret[5];
      ocr.exchange = ret[6];
      ocr.side = ret[7];
      ocr.quantity = ret[8];

      ptp.orderCancelRequest(ocr);
      return;
    }

    // ok, so send confirmation
    getSendOrder(ocr.orderid, true, false);

    // send margin & reserve
    //getSendMargin(orgclientkey, ret[1][31]); // for this currency
    //getSendReserve(orgclientkey, ret[1][5]); // for this symbol

    // distribute any changes to the order book
    broadcastLevelTwo(ret[3], null);
  });
}*/

function orderCancelReject(clientid, ocr, reason) {
  var ordercancelreject = {};

  ordercancelreject.orderid = ocr.orderid;

  console.log("Order cancel request #" + ordercancelreject.orderid + " rejected, reason: " + getReasonDesc(reason));

  if (clientid in connections) {
    connections[clientid].write("{\"ordercancelreject\":" + JSON.stringify(ordercancelreject) + "}");
  }
}

/*function processOrder(order, hedgeorderid, tradeid, hedgetradeid, conn) {
  // either forward to Proquote or trade immediately or attempt to match the order, depending on the type of instrument & whether the market is open
  if (order.markettype == 1) {
    matchOrder(order.orderid, conn);
  } else {
    // forward client equity orders to the market
    if (order.instrumenttype == "DE") {
      ptp.newOrder(order);
    } else {
      // the trade has been done, so send the order & trade to the client
      getSendOrder(order.orderid, true, true);
      getSendTrade(tradeid);

      // & send the other side to the hedge book
      getSendTrade(hedgetradeid);

      // if we are hedging, change the order id to that of the hedge & forward to proquote
      if (hedgeorderid != "") {
        order.orderid = hedgeorderid;
        ptp.newOrder(order);
      }
    }
  }
}*/

function displayOrderBook(symbol, lowerbound, upperbound) {
  db.zrangebyscore(symbol, lowerbound, upperbound, function(err, matchorders) {
    console.log("order book for instrument " + symbol + " has " + matchorders.length + " order(s)");

    matchorders.forEach(function (matchorderid, i) {
      db.hgetall("order:" + matchorderid, function(err, matchorder) {
        console.log("orderid="+matchorder.orderid+", clientid="+matchorder.clientid+", price="+matchorder.price+", side="+matchorder.side+", quantity="+matchorder.remquantity);
      });
    });
  });
}

function getValue(trade) {
  if (parseInt(trade.side) == 1) {
    return trade.quantity * trade.price + trade.commission + trade.costs;
  } else {
    return trade.quantity * trade.price - trade.commission - trade.costs;
  }
}

/*function sendInstruments(conn) {
  console.log("sending Instruments");

  db.sort("instruments", "ALPHA", function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    if (replies.length == 0) {
      console.log("Error: no instruments in database");
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
          console.log("symbol " + symbol + " not found");
          return;
        }

        // todo: reinstate/choice
        if (inst.instrumenttype != "DE") {
          return;
        }

        instrument.symbol = symbol;
        instrument.currency = inst.currency;

        if ("description" in inst) {
          instrument.description = inst.description;
        } else {
          console.log("no desc for "+symbol);
          instrument.description = "";
        }

        conn.write("{\"instrument\":" + JSON.stringify(instrument) + "}");
      });
    });
  });
}*/

function sendInstruments(clientid, conn) {
  // get sorted subset of instruments for this client
  db.eval(scriptgetinst, 1, clientid, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }
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

function sendIndex(clientid, index, conn) {
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
      console.log("Index:" + index + " not found");
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
          console.log("Symbol:" + symbol + " not found");
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
          orderBookOut(clientid, symbol, conn);
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

function sendOrders(clientid, conn) {
  var o = {orders: []};
  var count;

  db.smembers(clientid + ":orders", function(err, replies) {
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

function sendTrades(clientid, conn) {
  var t = {trades: []};
  var count;

  db.smembers(clientid + ":trades", function(err, replies) {
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

function sendPositions(clientid, conn) {
  var posarray = {positions: []};
  var count;

  db.smembers(clientid + ":positions", function(err, replies) {
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

function sendCash(clientid, conn) {
  var arr = {cash: []};
  var count;

  db.smembers(clientid + ":cash", function(err, replies) {
    if (err) {
      console.log("sendCash:" + err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function (currency, i) {
      db.get(clientid + ":cash:" + currency, function(err, amount) {
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

function sendMargins(clientid, conn) {
  var arr = {margins: []};
  var count;

  db.smembers(clientid + ":margins", function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function (currency, i) {
      db.get(clientid + ":margin:" + currency, function(err, amount) {
        if (err) {
          console.log(err);
          return;
        }

        if (amount == null) {
          console.log("Margin not found:" + clientid + ":margin:" + currency);
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

function sendReserves(clientid, conn) {
  var arr = {reserves: []};
  var count;

  db.smembers(clientid + ":reserves", function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function (reskey, i) {
      db.hgetall(clientid + ":reserve:" + reskey, function(err, reserve) {
        if (err) {
          console.log(err);
          return;
        }

        if (reserve == null) {
          console.log("Reserve not found:" + clientid + ":reserve:" + reskey);
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
      console.log("Error in sendAllOrderBooks:" + err);
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
function sendOrderBooksClient(clientid, conn) {
  // get all the instruments in the order book for this client
  db.smembers(clientid + ":orderbooks", function(err, instruments) {
    if (err) {
      console.log("Error in sendOrderBooksClient:" + err);
      return;
    }

    // send the order book for each instrument
    instruments.forEach(function (symbol, i) {
      orderBookOut(clientid, symbol, conn);
    });
  });
}

function broadcastLevelOne(symbol) {
  var level1 = {};
  var count;

  level1.bestbid = 0;
  level1.bestoffer = 0;

  db.zrange(symbol, 0, -1, "WITHSCORES", function(err, orders) {
    if (err) {
      console.log(err);
      return;
    }

    count = orders.length;
    if (count == 0) {
      publishMessage("{\"orderbook\":" + JSON.stringify(level1) + "}");
      return;
    }

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

      count--;
      if (count <= 0) {
        publishMessage("{\"orderbook\":" + JSON.stringify(level1) + "}");
      }
    });
  });
}

/*
 * pass conn=null to send to all interested parties
 */
function broadcastLevelTwo(symbol, conn) {
  var orderbook = {pricelevels : []};
  var lastprice = 0;
  var lastside = 0;
  var firstbid = true;
  var firstoffer = true;
  var bidlevel = 0;
  var offerlevel = 0;
  var count;

  console.log("broadcastLevelTwo:"+symbol);

  orderbook.symbol = symbol;

  // get bids & offers in the same call as, despite code complexity, seems safest
  db.zrange(symbol, 0, -1, function(err, orders) {
    if (err) {
      console.log("zrange error:" + err + ", symbol:" + symbol);
      return;
    }

    count = orders.length;
    if (count == 0) {
      // build & send a message showing no orders in the order book
      var pricelevel = {};
      pricelevel.bid = 0;
      pricelevel.bidsize = 0;
      pricelevel.offer = 0;
      pricelevel.offersize = 0;
      orderbook.pricelevels[0] = pricelevel;

      if (conn != null) {
        conn.write("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
      } else {
        publishMessage("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
      }
      return;
    }

    orders.forEach(function (orderid, i) {
      if (err) {
        console.log(err);
        return;
      }

      // get order hash
      db.hgetall("order:" + orderid, function(err, order) {
        var level1 = {};

        if (err) {
          console.log(err);
          return;
        }

        if (order.price != lastprice || order.side != lastside) {
          if (parseInt(order.side) == 1) {
            if (!firstbid) {
              bidlevel++;
            } else {
              firstbid = false;
            }

            level1.bid = order.price;
            level1.bidsize = parseInt(order.remquantity);
            level1.offer = 0;
            level1.offersize = 0;
            orderbook.pricelevels[bidlevel] = level1;
          } else {
            if (!firstoffer) {
              offerlevel++;
            } else {
              firstoffer = false;
            }

            if (offerlevel <= bidlevel && !firstbid) {
              orderbook.pricelevels[offerlevel].offer = order.price;
              orderbook.pricelevels[offerlevel].offersize = parseInt(order.remquantity);
            } else {
              level1.bid = 0;
              level1.bidsize = 0;
              level1.offer = order.price;
              level1.offersize = parseInt(order.remquantity);
              orderbook.pricelevels[offerlevel] = level1;
            }
          }

          lastprice = order.price;
          lastside = order.side;
        } else {
          if (parseInt(order.side) == 1) {
            orderbook.pricelevels[bidlevel].bidsize += parseInt(order.remquantity);
          } else {
            orderbook.pricelevels[offerlevel].offersize += parseInt(order.remquantity);
          }
        }

        count--;
        if (count <= 0) {
          if (conn != null) {
            conn.write("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
          } else {
            // broadcast to all interested parties
            publishMessage("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
          }
        }
      });
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

// todo: don't think we need this
/*function cancelOrder(order, conn) {
  // update stored order status
  order.status = "4";
  db.hset("order:" + order.orderid, "status", order.status);

  console.log("Order #" + order.orderid + " cancelled");

  // send to client, if connected
  if (conn == null) {
    if (order.clientid in connections) {
        conn = connections[order.clientid];
    }
  }

  if (conn != null) {
    conn.write("{\"order\":" + JSON.stringify(order) + "}");
  }
}*/

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

function start(clientid, conn) {
  sendOrderBooksClient(clientid, conn);
  sendInstruments(clientid, conn);
  sendOrderTypes(conn);
  sendPositions(clientid, conn);
  sendOrders(clientid, conn);
  sendCash(clientid, conn);
  sendMargins(clientid, conn);
  sendReserves(clientid, conn);

  // may not be last, but...
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

function orderBookRequest(clientid, symbol, conn) {
  // add the client to the order book set for this instrument
  db.sadd("orderbook:" + symbol, clientid);

  // & add the instrument to the watchlist for this client
  db.sadd(clientid + ":orderbooks", symbol);

  orderBookOut(clientid, symbol, conn);
}

function orderBookOut(clientid, symbol, conn) {
  if (outofhours) {
    broadcastLevelTwo(symbol, conn);
  } else {
    subscribeAndSend(clientid, symbol, conn);
  }
}

function subscribeAndSend(clientid, symbol, conn) {
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
    db.hgetall("client:" + clientid, function(err, client) {
      if (err) {
        console.log(err);
        return;
      }

      if (!client) {
        console.log("client:" + clientid + " not found");
        return;
      }

      // may need to adjust the topic to delayed
      inst.topic += "D";

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
        db.sismember("proquote:" + inst.topic, clientid, function(err, clientfound) {
          if (err) {
            console.log(err);
            return;
          }

          if (!clientfound) {
            console.log("subscribing client:" + clientid + " to " + inst.topic)
            // add client to the set for this instrument
            db.sadd("proquote:" + inst.topic, clientid);
          }
        });
      });

      // send the orderbook, with the current stored prices
      sendCurrentOrderBook(clientid, symbol, inst.topic, conn);
    });
  });
}

function sendCurrentOrderBook(clientid, symbol, topic, conn) {
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

function orderBookRemoveRequest(clientid, symbol, conn) {
  // remove the client from the order book set for this instrument
  db.srem("orderbook:" + symbol, clientid);

  // & remove the instrument from the order book set for this client
  db.srem(clientid + ":orderbooks", symbol);

  // get the market extension for this client, as topic may need to be adjusted for live/delayed
  db.hget("client:" + clientid, "marketext", function(err, marketext) {
    if (err) {
      console.log("Error in orderBookRemoveRequest:" + err);
      return;
    }

    unsubscribeTopic(clientid, symbol, marketext);
  });
}

function unsubscribeTopic(clientid, symbol, marketext) {
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
    inst.topic += marketext;

    // remove client from set for this topic
    db.srem("proquote:" + inst.topic, clientid);

    // unsubscribe from this topic if no clients are looking at it
    db.scard("proquote:" + inst.topic, function(err, numelements) {
      if (numelements == 0) {
        dbsub.unsubscribe(inst.topic);
        db.srem("proquote", inst.topic);
        // todo: change from dbpub?
        dbpub.publish("cw2t", "unsubscribe:" + inst.topic);
      }
    });
  });
}

function unsubscribeTopics(clientid) {
  // get the market extension for this client, as topic may need to be adjusted for live/delayed
  db.hget("client:" + clientid, "marketext", function(err, marketext) {
    if (err) {
      console.log("Error in unsubscribeTopics:" + err);
      return;
    }

    // get all the orderbooks
    db.smembers(clientid + ":orderbooks", function(err, orderbooks) {
      if (err) {
        console.log("Error in unsubscribeTopics:" + err);
        return;
      }

      // & unsubscibe
      orderbooks.forEach(function(symbol, i) {
        unsubscribeTopic(clientid, symbol, marketext);
      });
    });
  });
}

function positionHistory(clientid, symbol, conn) {
  // todo: remove
  clientid = 5020;
  symbol = "LOOK";

  //bo.getPositionHistory(clientid, symbol);
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

/*ptp.on("orderReject", function(exereport) {
  var text = "";
  var ordrejreason = "";
  console.log(exereport);

  console.log("order rejected, id:" + exereport.clordid);

  // execution reports vary as to whether they contain a reject reason &/or text
  if ('ordrejreason' in exereport) {
    ordrejreason = exereport.ordrejreason;
  }
  if ('text' in exereport) {
    text = exereport.text;
  }

  db.eval(scriptrejectorder, 3, exereport.clordid, ordrejreason, text, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    if (ret != 0) {
      // todo: message to client
      console.log("Error in scriptrejectorder, reason:" + getReasonDesc(ret));
      return;
    }

    // send to client if connected
    getSendOrder(exereport.clordid, false, false);
  });
});*/

//
// Limit order acknowledgement
//
/*ptp.on("orderAck", function(exereport) {
  var text = "";
  console.log("Order acknowledged, id:" + exereport.clordid);
  console.log(exereport);

  if ('text' in exereport) {
    text = exereport.text;
  }

  db.eval(scriptorderack, 5, exereport.clordid, exereport.orderid, exereport.ordstatus, exereport.execid, text, function(err, ret) {
    if (err) throw err;

    // ok, so send confirmation
    getSendOrder(exereport.clordid, true, false);
  });
});*/

/*ptp.on("orderCancel", function(exereport) {
  console.log("Order cancelled by Proquote, ordercancelrequest id:" + exereport.clordid);

  db.eval(scriptordercancel, 1, exereport.clordid, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      // todo: send to client
      console.log("Error in scriptordercancel, reason:" + getReasonDesc(ret[0]));
      return;
    }

    //var orgclientkey = ret[1][1] + ":" + ret[1][3];

    // send confirmation
    getSendOrder(ret[1], true, false);

    // send margin & reserve
    // todo: wrap these in send order?
    //getSendMargin(orgclientkey, ret[1][31]); // orgclientkey, currency
    //getSendReserve(orgclientkey, ret[1][5]); // orgclientkey, symbol
  });
});*/

/*ptp.on("orderExpired", function(exereport) {
  console.log(exereport);
  console.log("order expired, id:" + exereport.clordid);

  db.eval(scriptorderexpire, 1, exereport.clordid, function(err, ret) {
    if (err) {
      console.log(err);
      return
    }

    if (ret != 0) {
      // todo: send to client
      console.log("Error in scriptorderexpire, reason:" + getReasonDesc(ret));
      return;
    }

    getSendOrder(exereport.clordid, true, false);
  });
});*/

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

    // send quote to the client who placed the quote request
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

function registerScripts() {
  var updateposition;
  var updatecash;
  var updateordermargin;
  var updatereserve;
  var removefromorderbook;
  var cancelorder;
  var newtrade;
  var round;
  var rejectorder;
  var adjustmarginreserve;
  var creditcheck;
  var getproquotesymbol;
  var updatetrademargin;
  var calcfinance;

  round = '\
  local round = function(num, dp) \
    local mult = 10 ^ (dp or 0) \
    return math.floor(num * mult + 0.5) / mult \
  end \
  ';

  rejectorder = '\
  local rejectorder = function(orderid, reason, text) \
    redis.call("hmset", "order:" .. orderid, "status", "8", "reason", reason, "text", text) \
  end \
  ';

  //
  // parameter: symbol
  // returns: isin, proquote symbol, market & hedge symbol
  //
  getproquotesymbol = '\
  local getproquotesymbol = function(symbol) \
    local symbolkey = "symbol:" .. symbol \
    local fields = {"isin", "proquotesymbol", "market", "hedgesymbol"} \
    local vals = redis.call("hmget", symbolkey, unpack(fields)) \
    return {vals[1], vals[2], vals[3], vals[4]} \
  end \
  ';

  //
  // get proquote quote id & quoting rsp
  //
  getproquotequote = '\
    local getproquotequote = function(quoteid, side) \
      local quotekey = "quote:" .. quoteid \
      local pqquoteid = "" \
      local qbroker = "" \
      if quoteid ~= "" then \
        if side == 1 then \
          pqquoteid = redis.call("hget", quotekey, "offerquoteid") \
          qbroker = redis.call("hget", quotekey, "offerqbroker") \
        else \
          pqquoteid = redis.call("hget", quotekey, "bidquoteid") \
          qbroker = redis.call("hget", quotekey, "bidqbroker") \
        end \
      end \
      return {pqquoteid, qbroker} \
    end \
  ';

  addtoorderbook = '\
  local addtoorderbook = function(symbol, orderid, side, price) \
    --[[ buy orders need a negative price ]] \
    if tonumber(side) == 1 then \
      price = "-" .. price \
    end \
    --[[ add order to order book ]] \
    redis.call("zadd", symbol, price, orderid) \
    redis.call("sadd", "orderbooks", symbol) \
  end \
  ';

  removefromorderbook = '\
  local removefromorderbook = function(symbol, orderid) \
    redis.call("zrem", symbol, orderid) \
    if (redis.call("zcount", symbol, "-inf", "+inf") == 0) then \
      redis.call("srem", "orderbooks", symbol) \
    end \
  end \
  ';

  calcfinance = round + '\
  local calcfinance = function(consid, currency, longshort, nosettdays) \
    local finance = 0 \
    local financerate = redis.call("get", "financerate:" .. currency .. ":" .. longshort) \
    if financerate then \
      --[[ nosettdays = 0 represents rolling settlement, so set it to 1 day for interest calculation ]] \
      if tonumber(nosettdays) == 0 then nosettdays = 1 end \
      finance = round(consid * tonumber(nosettdays) / 365 * tonumber(financerate) / 100, 2) \
   end \
   return finance \
  end \
  ';

  scriptordercancelrequest = removefromorderbook + cancelorder + getproquotesymbol + '\
  local errorcode = 0 \
  local ordercancelreqid = redis.call("incr", "ordercancelreqid") \
  redis.call("hmset", "ordercancelrequest:" .. ordercancelreqid, "orgid", KEYS[1], "clientid", KEYS[2], "orderid", KEYS[3], "timestamp", KEYS[4]) \
  local fields = {"status", "markettype", "symbol", "side", "quantity"} \
  local vals = redis.call("hmget", "order:" .. KEYS[3], unpack(fields)) \
  local markettype = "" \
  local symbol = "" \
  local side \
  local quantity = "" \
  if vals == nil then \
    --[[ order not found ]] \
    errorcode = 1009 \
  else \
    markettype = vals[2] \
    symbol = vals[3] \
    side = vals[4] \
    quantity = vals[5] \
    if vals[1] == "2" then \
      --[[ already filled ]] \
      errorcode = 1010 \
    elseif vals[1] == "4" then \
      --[[ already cancelled ]] \
      errorcode = 1008 \
    elseif vals[1] == "8" then \
      --[[ already rejected ]] \
      errorcode = 1012 \
    end \
  end \
  --[[ process according to market type ]] \
  local proquotesymbol = {"", "", ""} \
  if vals[2] == "1" then \
    if errorcode ~= 0 then \
      redis.call("hset", "ordercancelrequest:" .. ordercancelreqid, "reason", errorcode) \
    else \
      removefromorderbook(symbol, KEYS[3]) \
      cancelorder(KEYS[3], "4") \
    end \
  else \
    --[[ get required instrument values for proquote ]] \
    if errorcode == 0 then \
      proquotesymbol = getproquotesymbol(symbol) \
    end \
  end \
  return {errorcode, markettype, ordercancelreqid, symbol, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], side, quantity} \
  ';

  scriptordercancel = cancelorder + '\
  local errorcode = 0 \
  local orderid = redis.call("hget", "ordercancelrequest:" .. KEYS[1], "orderid") \
  if not orderid then \
    errorcode = 1013 \
  else \
    errorcode = cancelorder(orderid, "4") \
    if errorcode ~= 0 then \
      redis.call("hset", "order:" .. orderid, "ordercancelrequestid", KEYS[1]) \
    end \
  end \
  return {errorcode, orderid} \
  ';

  scriptorderack = '\
  --[[ update external limit reference ]] \
  redis.call("hmset", "order:" .. KEYS[1], "externalorderid", KEYS[2], "status", KEYS[3], "execid", KEYS[4], "text", KEYS[5]) \
  return \
  ';

  scriptorderexpire = cancelorder + '\
  local ret = cancelorder(KEYS[1], "C") \
  return ret \
  ';

  //
  // get alpha sorted list of instruments for a specified client
  // uses set of valid instrument types per client i.e. 1:instrumenttypes CFD
  //
  scriptgetinst = '\
  local instruments = redis.call("sort", "instruments", "ALPHA") \
  local fields = {"instrumenttype", "description", "currency", "marginpercent"} \
  local vals \
  local inst = {} \
  local marginpc \
  for index = 1, #instruments do \
    vals = redis.call("hmget", "symbol:" .. instruments[index], unpack(fields)) \
    if redis.call("sismember", KEYS[1] .. ":instrumenttypes", vals[1]) == 1 then \
      if vals[4] then \
        marginpc = vals[4] \
      else \
        marginpc = 100 \
      end \
      table.insert(inst, {symbol = instruments[index], description = vals[2], currency = vals[3], instrumenttype = vals[1], marginpercent = marginpc}) \
    end \
  end \
  return cjson.encode(inst) \
  ';
}