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
var orgid = "1"; // todo: via logon
var defaultnosettdays = 3;

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
  listen();
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
    var orguserkey;

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
        orderBookRequest(orgclientkey, obj.orderbookrequest, conn);
      } else if (msg.substr(2, 22) == "orderbookremoverequest") {
        obj = JSON.parse(msg);
        orderBookRemoveRequest(orgclientkey, obj.orderbookremoverequest, conn);
      } else if (msg.substr(2, 5) == "order") {
        obj = JSON.parse(msg);
        newOrder(clientid, obj.order, conn);
      } else if (msg.substr(2, 12) == "quoterequest") {
        obj = JSON.parse(msg);
        quoteRequest(clientid, obj.quoterequest);
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
      } else if (msg.substr(0, 4) == "ping") {
        conn.write("pong");
      } else {
        console.log("unknown msg received:" + msg);
      }
    });

    // close connection callback
    conn.on('close', function() {
      // todo: check for existence
      userid = "0";
    });

    // user sign in
    function signIn(signin) {
      var reply = {};
      reply.success = false;

      db.get("user:" + signin.email, function(err, orguserid) {
        if (err) {
          console.log("Error in signIn:" + err);
          return;
        }

        if (!orguserid) {
          console.log("Email not found:" + signin.email);
          reply.reason = "Email or password incorrect. Please try again.";
          replySignIn(reply, conn);
          return;
        }

        // validate email/password
        db.hgetall("user:" + orguserid, function(err, user) {
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
          orguserkey = orguserid;
          connections[orguserkey] = conn;

          // send a successful logon reply
          reply.success = true;
          reply.email = signin.email;
          replySignIn(reply, conn);

          console.log("user " + orguserkey + " logged on");

          // send the data
          start(user.orgid, conn);
        });
      });
    }
  });
}

function newClient(client, conn) {
  console.log("new client");
  var orgclientkey = client.orgid + ":" + client.clientid;

  db.hmset("client:" + orgclientkey, client);

  sendClientAllUsers(orgclientkey);
}

function sendClientAllUsers(orgclientkey) {
  for (var user in connections) {
    getSendClient(orgclientkey, user);
  }
}

function getSendClient(orgclientkey, user) {
  db.hgetall("client:" + orgclientkey, function(err, client) {
    if (err) {
      console.log(err);
      return;
    }

    // send anyway, even if no position, as may need to clear f/e - todo: review
    if (client == null) {
      console.log("Client not found, id:" + orgclientkey);
      return;
    }

    connections[user].write("{\"client\":" + JSON.stringify(client) + "}");
  });
}

function quoteRequest(clientid, quoterequest) {
  console.log("quoterequest");
  console.log(quoterequest);

  quoterequest.timestamp = getUTCTimeStamp();

  // get settlement date from T+n no. of days
  quoterequest.futsettdate = getUTCDateString(getSettDate(quoterequest.nosettdays));

  // store the quote request & get an id
  db.eval(scriptquoterequest, 10, orgid, clientid, quoterequest.symbol, quoterequest.quantity, quoterequest.cashorderqty, quoterequest.currency, quoterequest.settlcurrency, quoterequest.nosettdays, quoterequest.futsettdate, quoterequest.timestamp, function(err, ret) {
    if (err) throw err;
  
    if (ret[0] != 0) {
      // todo: send a quote ack to client

      console.log("Error in scriptquoterequest:" + getReasonDesc(ret[0]));
      return;
    }

    // add the quote request id & symbol details required for proquote
    quoterequest.quotereqid = ret[1];
    quoterequest.isin = ret[2];
    quoterequest.proquotesymbol = ret[3];
    quoterequest.exchange = ret[4];

    // for derivatives, make the quote request for the default settlement date
    if (ret[5] != "DE") {
      if (quoterequest.nosettdays != defaultnosettdays) {
        quoterequest.futsettdate = getUTCDateString(getSettDate(defaultnosettdays));
        quoterequest.nosettdays = defaultnosettdays;
      }
    }

    // forward the request to Proquote
    ptp.quoteRequest(quoterequest);
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

function newOrder(clientid, order, conn) {
  var currencyratetoorg = 1; // product currency to org curreny rate
  var currencyindtoorg = 1;
  var settlcurrfxrate = 1; // settlement currency to product currency rate
  var settlcurrfxratecalc = 1;

  console.log(order);

  // todo: tie this in with in/out of hours
  order.timestamp = getUTCTimeStamp();
  order.markettype = 0; // 0 = main market, 1 = ooh
  order.partfill = 1; // accept part-fill

  // always put a price in the order
  if (!("price" in order)) {
    order.price = "";
  }

  // and always have a quote id
  if (!("quoteid" in order)) {
    order.quoteid = "0";
  }

  // store the order, get an id & credit check it
  db.eval(scriptneworder, 24, orgid, clientid, order.symbol, order.side, order.quantity, order.price, order.ordertype, order.markettype, order.futsettdate, order.partfill, order.quoteid, order.currency, currencyratetoorg, currencyindtoorg, order.timestamp, order.timeinforce, order.expiredate, order.expiretime, order.settlcurrency, settlcurrfxrate, settlcurrfxratecalc, order.nosettdays, order.positionopenclose, order.positioncloseid, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // credit check failed
    if (ret[0] == 0) {
      getSendOrder(ret[1], false, false);
      return;
    }

    // update order details
    order.orderid = ret[1];
    order.instrumenttype = ret[7];

    // use the returned instrument values required by proquote
    if (order.markettype == 0) {
      order.isin = ret[2];
      order.proquotesymbol = ret[3];
      order.exchange = ret[4];
    }

    // use the returned quote values required by proquote
    if (order.ordertype == "D") {
      order.proquotequoteid = ret[5];
      order.qbroker = ret[6];
    }

    processOrder(order, ret[8], ret[9], ret[10], conn);
  });
}

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

      var orgclientkey = order.orgid + ":" + order.clientid;

      // send to client, if connected
      if (orgclientkey in connections) {
        sendOrder(order, connections[orgclientkey]);
      }

      // broadcast market - todo: timer based? - pubsub?
      broadcastLevelTwo(order.symbol, null);

      // send any trades for active order client
      for (var i = 0; i < ret[1].length; ++i) {
        getSendTrade(ret[1][i]);
      }

      // send cash
      if (ret[1].length > 0) {
        getSendCash(orgclientkey, order.settlcurrency);
      }

      // send margin/reserve
      if (parseInt(order.side) == 1) {
        getSendMargin(orgclientkey, order.settlcurrency);
      } else {
        getSendReserve(orgclientkey, order.symbol, order.settlcurrency);
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
  var orgclientkey;

  db.hgetall("order:" + orderid, function(err, order) {
    if (err) {
      console.log(err);
      return;
    }

    if (order == null) {
      console.log("Order #" + orderid + " not found");
      return;
    }

    // required for identifying the client connection
    orgclientkey = order.orgid + ":" + order.clientid;

    // send to client, if connected
    if (orgclientkey in connections) {
      sendOrder(order, connections[orgclientkey]);
    }

    if (sendmarginreserve) {
      getSendMargin(orgclientkey, order.currency);
      getSendReserve(orgclientkey, order.symbol, order.settlcurrency);
    }

    if (sendcash) {
      getSendCash(orgclientkey, order.settlcurrency);
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

    // send to client, if connected
    var orgclientkey = trade.orgid + ":" + trade.clientid;

    if (orgclientkey in connections) {
      sendTrade(trade, connections[orgclientkey]);
    }

    if ("positionid" in trade) {
      getSendPosition(trade.positionid, orgclientkey);
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

function orderCancelRequest(clientid, ocr) {
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
}

function orderCancelReject(orgclientkey, ocr, reason) {
  var ordercancelreject = {};

  ordercancelreject.orderid = ocr.orderid;

  console.log("Order cancel request #" + ordercancelreject.orderid + " rejected, reason: " + getReasonDesc(reason));

  if (orgclientkey in connections) {
    connections[orgclientkey].write("{\"ordercancelreject\":" + JSON.stringify(ordercancelreject) + "}");
  }
}

function processOrder(order, hedgeorderid, tradeid, hedgetradeid, conn) {
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
}

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

function sendInstruments(conn) {
  // get sorted subset of instruments for this client
  db.eval(scriptgetinst, 1, orgclientkey, function(err, ret) {
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
    default:
      desc = "Unknown reason";
  }

  return desc;
}

function start(orgid, conn) {
  //sendInstruments(conn);
  sendOrderTypes(conn);
  sendClients(orgid, conn);

  // may not be last, but...
  sendReadyToManage(conn);
}

function sendReadyToManage(conn) {
  conn.write("{\"status\":\"readytomanage\"}");
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

function sendQuote(quote, conn) {
  conn.write("{\"quote\":" + JSON.stringify(quote) + "}");
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

function sendClients(orgid, conn) {
  // get sorted set of clients for specified organisation
  db.eval(scriptgetclients, 1, orgid, function(err, ret) {
    conn.write("{\"clients\":" + ret + "}");
  });
}

function registerScripts() {
  //
  // get alpha sorted list of clients for a specified organisation
  //
  scriptgetclients = '\
  local clients = redis.call("sort", "clients", "ALPHA") \
  local fields = {"orgid", "clientid", "email", "name", "address", "mobile"} \
  local vals \
  local client = {} \
  local marginpc \
  for index = 1, #clients do \
    vals = redis.call("hmget", "client:" .. clients[index], unpack(fields)) \
    if KEYS[1] == vals[1] then \
      table.insert(client, {orgid = vals[1], clientid = vals[2], email = vals[3], name = vals[4], address = vals[5], mobile = vals[6]}) \
    end \
  end \
  return cjson.encode(client) \
  ';
}
