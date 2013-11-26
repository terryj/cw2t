/****************
* tradeserver.js
* Front-office trading server
* Cantwaittotrade Limited
* Terry Johnston
* November 2013
****************/

// node libraries

// external libraries
var redis = require('redis');

// cw2t libraries
//var winnclient = require('./winnclient.js'); // Winner API connection
var ptpclient = require('./ptpclient.js'); // Proquote API connection

// globals
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
var scriptquoterequest;
var scriptneworder;
var scriptmatchorder;
var scriptordercancelrequest;
var scriptordercancel;
var scriptorderack;
var scriptnewtrade;
var scriptquote;
var scriptquoteack;
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
  console.log(err);
});

function initialise() {
  initDb();
  pubsub();
  connectToTrading();
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
    var obj;
    console.log("channel:" + channel + " " + message);

    if (message.substr(2, 12) == "quoterequest") {
      obj = JSON.parse(message);
      quoteRequest(obj.quoterequest);
    } else if (message.substr(2, 5) == "order") {
      obj = JSON.parse(message);
      newOrder(obj.order);
    }
  });

  dbsub.subscribe(3);
}

// connection to Winner
/*var winner = new winnclient.Winner();
winner.on("connected", function() {
  console.log("Connected to Winner");
});
winner.on("loggedon", function() {
  console.log("Logged on to Winner");
});
winner.on('finished', function(message) {
    console.log(message);
});*/

// connection to Proquote
var ptp = new ptpclient.Ptp();
ptp.on("connected", function() {
  console.log("Connected to Proquote");
});
ptp.on('finished', function(message) {
    console.log(message);
});

function connectToTrading() {
  // try to connect
  //winner.connect();
  ptp.connect();
}

function quoteRequest(quoterequest) {
  console.log("quoterequest");
  console.log(quoterequest);

  quoterequest.timestamp = getUTCTimeStamp();

  // get settlement date from T+n no. of days
  quoterequest.futsettdate = getUTCDateString(getSettDate(quoterequest.nosettdays));

  // store the quote request & get an id
  db.eval(scriptquoterequest, 11, quoterequest.clientid, quoterequest.symbol, quoterequest.quantity, quoterequest.cashorderqty, quoterequest.currency, quoterequest.settlcurrency, quoterequest.nosettdays, quoterequest.futsettdate, quoterequest.timestamp, quoterequest.operatortype, quoterequest.operatorid, function(err, ret) {
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

function newOrder(order) {
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
  db.eval(scriptneworder, 25, order.clientid, order.symbol, order.side, order.quantity, order.price, order.ordertype, order.markettype, order.futsettdate, order.partfill, order.quoteid, order.currency, currencyratetoorg, currencyindtoorg, order.timestamp, order.timeinforce, order.expiredate, order.expiretime, order.settlcurrency, settlcurrfxrate, settlcurrfxratecalc, order.nosettdays, order.positionopenclose, order.positioncloseid, order.operatortype, order.operatorid, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // credit check failed
    if (ret[0] == 0) {
      db.publish(order.operatortype, "order:" + ret[1]);
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

    processOrder(order, ret[8], ret[9], ret[10]);
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

function processOrder(order, hedgeorderid, tradeid, hedgetradeid) {
  // either forward to Proquote or trade immediately or attempt to match the order, depending on the type of instrument & whether the market is open
  if (order.markettype == 1) {
    // todo: remove the conn
    matchOrder(order.orderid, conn);
  } else {
    // forward client equity orders to the market
    if (order.instrumenttype == "DE") {
      ptp.newOrder(order);
    } else {
      // publish the regular order & trade to whence it came
      db.publish(order.operatortype, "order:" + order.orderid);
      db.publish(order.operatortype, "trade:" + tradeid);

      // publish the hedge to the client server, so anyone viewing the hedgebook receives it
      db.publish(1, "trade:" + hedgetradeid);

      // & publish it to the user server
      db.publish(2, "trade:" + hedgetradeid);

      // the trade has been done, so send the order & trade to the client
      //getSendOrder(order.orderid, true, true);
      //getSendTrade(tradeid);

      // & send the other side to the hedge book
      //getSendTrade(hedgetradeid);

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

function sendInstruments(orgclientkey, conn) {
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
      desc = "Client not authorised to trade this type of product";
      break;
    default:
      desc = "Unknown reason";
  }

  return desc;
}

function start(orgclientkey, conn) {
  sendOrderBooksClient(orgclientkey, conn);
  sendInstruments(orgclientkey, conn);
  sendOrderTypes(conn);
  sendPositions(orgclientkey, conn);
  sendOrders(orgclientkey, conn);
  sendCash(orgclientkey, conn);
  sendMargins(orgclientkey, conn);
  sendReserves(orgclientkey, conn);

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

function orderBookRequest(orgclientkey, symbol, conn) {
  // add the client to the order book set for this instrument
  db.sadd("orderbook:" + symbol, orgclientkey);

  // & add the instrument to the watchlist for this client
  db.sadd(orgclientkey + ":orderbooks", symbol);

  orderBookOut(orgclientkey, symbol, conn);
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

function orderBookRemoveRequest(orgclientkey, symbol, conn) {
  // remove the client from the order book set for this instrument
  db.srem("orderbook:" + symbol, orgclientkey);

  // & remove the instrument from the order book set for this client
  db.srem(orgclientkey + ":orderbooks", symbol);

  // get the market extension for this client, as topic may need to be adjusted for live/delayed
  db.hget("client:" + orgclientkey, "marketext", function(err, marketext) {
    if (err) {
      console.log("Error in orderBookRemoveRequest:" + err);
      return;
    }

    unsubscribeTopic(orgclientkey, symbol, marketext);
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

ptp.on("orderReject", function(exereport) {
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
});

//
// Limit order acknowledgement
//
ptp.on("orderAck", function(exereport) {
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
});

ptp.on("orderCancel", function(exereport) {
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
});

ptp.on("orderExpired", function(exereport) {
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
});

ptp.on("orderFill", function(exereport) {
  var currencyratetoorg = 1; // product currency rate back to org 
  var currencyindtoorg = 1;

  console.log(exereport);

  if (!('settlcurrfxrate' in exereport)) {
    exereport.settlcurrfxrate = 1;
  }

  // we don't get this from proquote - todo: set based on currency pair
  exereport.settlcurrfxratecalc = 1;    

  db.eval(scriptnewtrade, 20, exereport.clordid, exereport.symbol, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currency, currencyratetoorg, currencyindtoorg, exereport.execbroker, exereport.execid, exereport.futsettdate, exereport.transacttime, exereport.ordstatus, exereport.lastmkt, exereport.leavesqty, exereport.orderid, exereport.settlcurrency, exereport.settlcurramt, exereport.settlcurrfxrate, exereport.settlcurrfxratecalc, function(err, ret) {
    if (err) {
      console.log(err);
      return
    }

    // send the order & trade
    //getSendOrder(exereport.clordid, true, true);
    //getSendTrade(ret);

    db.publish(ret[1], "order:" + exereport.clordid);
    db.publish(ret[1], "trade:" + ret[0]);
  });
});

ptp.on("orderCancelReject", function(ordercancelreject) {
  var reasondesc;

  console.log("Order cancel reject, order cancel request id:" + ordercancelreject.clordid);

  reasondesc = ordercancelreject.cxlrejreason;
  if ('text' in ordercancelreject) {
    reasondesc += ordercancelreject.text;
  }

  // update the db - todo: update status?
  db.hset("ordercancelrequest:" + ordercancelreject.clordid, "reason", reasondesc);

  // get the request
  db.hgetall("ordercancelrequest:" + ordercancelreject.clordid, function(err, ordercancelrequest) {
    if (err) {
      console.log(err); // can't get order cancel request, so just log
      return;
    }

    if (ordercancelrequest == null) {
      console.log("Order cancel request not found, id:" + ordercancelreject.clordid);
      return;
    }

    var orgclientkey = ordercancelrequest.orgid + ":" + ordercancelrequest.clientid;

    // send to the client
    orderCancelReject(orgclientkey, ordercancelrequest, ordercancelreject.cxlrejreason);
  });
});

ptp.on("quote", function(quote, header) {
  console.log("quote received");
  console.log(quote);

  // check to see if the quote may be a duplicate &, if it is, reject messages older than a limit
  // - todo: inform client & limit as parameter?
  if ('possdupflag' in header) {
    if (header.possdupflag == 'Y') {
      var sendingtime = new Date(getDateString(header.sendingtime));
      var validuntiltime = new Date(getDateString(quote.validuntiltime));
      if (validuntiltime < sendingtime) {
        console.log("Quote received is past valid until time, discarding");
        return;
      }
    }
  }

  // a two-way quote arrives in two bits, so fill in 'missing' price/size/quote id (note: separate external quote id for bid & offer)
  if (!('bidpx' in quote)) {
    quote.bidpx = '';
    quote.bidsize = '';
    quote.bidquoteid = '';
    quote.offerquoteid = quote.quoteid;
    quote.offerqbroker = quote.qbroker;
  }
  if (!('offerpx' in quote)) {
    quote.offerpx = '';
    quote.offersize = '';
    quote.offerquoteid = '';
    quote.bidquoteid = quote.quoteid;
    quote.bidqbroker = quote.qbroker;
  }

  // quote script
  // note: not passing securityid & idsource as proquote symbol should be enough
  db.eval(scriptquote, 15, quote.quotereqid, quote.bidquoteid, quote.offerquoteid, quote.symbol, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.transacttime, quote.currency, quote.settlcurrency, quote.bidqbroker, quote.offerqbroker, quote.futsettdate, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    if (ret[0] != 0) {
      // can't find quote request, so don't know which client to inform
      console.log("Error in scriptquote:" + getReasonDesc(ret[0]));
      return;
    }

    // exit if this is the first part of the quote as waiting for a two-way quote
    // todo: do we need to handle event of only getting one leg?
    if (ret[1] == 1) {return};

    db.publish(ret[3], "quote:" + ret[2]);
  });
});

function sendQuote(quote, conn) {
  conn.write("{\"quote\":" + JSON.stringify(quote) + "}");
}

//
// quote rejection
//
ptp.on("quoteack", function(quoteack) {
  console.log("quote ack, request id " + quoteack.quotereqid);
  console.log(quoteack);

  db.eval(scriptquoteack, 4, quoteack.quotereqid, quoteack.quotestatus, quoteack.quoterejectreason, quoteack.text, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    console.log(ret);

    if (ret[0] == 1) {
      console.log("already received rejection for this quote request, ignoring");
      return;
    }

    db.publish(ret[1], "quoteack:" + quoteack.quotereqid);

    /*db.hgetall("quoterequest:" + quoteack.quotereqid, function(err, quoterequest) {
      if (err) {
        console.log(err);
        return;
      }

      if (quoterequest == null) {
        console.log("can't find quote request id " + quoteack.quotereqid);
        return;
      }

      orgclientkey = quoterequest.orgid + ":" + quoterequest.clientid;

      quoteack.symbol = quoterequest.symbol;
      quoteack.quoterejectreasondesc = ptp.getQuoteRejectReason(quoteack.quoterejectreason);

      // send the quote acknowledgement
      if (orgclientkey in connections) {
        connections[orgclientkey].write("{\"quoteack\":" + JSON.stringify(quoteack) + "}");
      }
    });*/
  });
});

/*function sendQuoteAck(quotereqid) {
  var quoteack = {};
  var orgclientkey;

  db.hgetall("quoterequest:" + quotereqid, function(err, quoterequest) {
    if (err) {
      console.log(err);
      return;
    }

    if (quoterequest == null) {
      console.log("can't find quote request id " + quotereqid);
      return;
    }

    quoteack.quotereqid = quotereqid;
    quoteack.symbol = quoterequest.symbol;
    quoteack.quoterejectreason = quoterequest.quoterejectreason;

    orgclientkey = quoterequest.orgid + ":" + quoterequest.clientid;

    // send the quote acknowledgement
    if (orgclientkey in connections) {
      connections[orgclientkey].write("{\"quoteack\":" + JSON.stringify(quoteack) + "}");
    }
  });
}*/

//
// message error
//
ptp.on("reject", function(reject) {
  console.log("Error: reject received, fixseqnum:" + reject.refseqnum);
  console.log("Text:" + reject.text);
  console.log("Tag id:" + reject.reftagid);
  console.log("Msg type:" + reject.refmsgtype);
  if ('sessionrejectreason' in reject) {
    var reasondesc = ptp.getSessionRejectReason(reject.sessionrejectreason);
    console.log("Reason:" + reasondesc);
  }
});

ptp.on("businessReject", function(businessreject) {
  console.log(businessreject);
  // todo: lookup fix msg to get details of what it relates to
});

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

/*
local results = redis.call("HGETALL", KEYS[1]);
if results ~= 0 then
    for key, value in ipairs(results) do
        -- do something here
    end
end

local data = redis.call('GET', key);
local json;

if not data then
    json = {};
else
    json = cjson.decode(data);
end

json[name] = value;
redis.call('SET', key, cjson.encode(json));
return cjson.encode(json);

string luaScript = "local tDecoded = redis.call('GET', KEYS[1]);\n"
                    + "local tFinal = {};\n"
                    + "for iIndex, tValue in ipairs(tDecoded) do\n"
                    + "     if tonumber( tValue.Value ) < 20 then\n"
                    + "        table.insert(tFinal, { ID = tValue.ID, Value = tValue.Value});\n"
                    + "     else\n"
                    + "         table.insert(tFinal, { ID = 999, Value = 0});\n"
                    + "     end\n"
                    + "end\n"
                    + "return cjson.encode(tFinal);";

                var test = redisClient.ExecLuaAsString(luaScript, "Meds25");

*/

function registerScripts() {
  var updateposition;
  var updatecash;
  var updateordermargin;
  var updatereserve;
  var removefromorderbook;
  var cancelorder;
  var newtrade;
  var getcosts;
  var round;
  var gettotalcost;
  var rejectorder;
  var adjustmarginreserve;
  var creditcheck;
  var getproquotesymbol;
  var getinitialmargin;
  var updatetrademargin;
  var calcfinance;
  var neworder;

  round = '\
  local round = function(num, dp) \
    local mult = 10 ^ (dp or 0) \
    return math.floor(num * mult + 0.5) / mult \
  end \
  ';

  getcosts = round + '\
  local getcosts = function(instrumenttype, side, consid, currency) \
    local commission = 0 \
    local ptmlevy = 0 \
    local stampduty = 0 \
    local contractcharge = 0 \
    local commpercent = redis.call("get", "commissionpercent") \
    local commmin = redis.call("get", "commissionmin:" .. currency) \
    if commpercent then \
      commission = round(consid * tonumber(commpercent) / 100, 2) \
    end \
    if commmin then \
      if commission < tonumber(commmin) then \
        commission = tonumber(commmin) \
      end \
    end \
    if currency == "GBP" then \
      if instrumenttype == "DE" then \
        local ptmlevylimit = redis.call("get", "ptmlevylimit") \
        if ptmlevylimit then \
          if consid > tonumber(ptmlevylimit) then \
            ptmlevy = tonumber(redis.call("get", "ptmlevy")) \
          end \
        end \
        if tonumber(side) == 1 then \
          local stampdutylimit = redis.call("get", "stampdutylimit") \
          if stampdutylimit then \
            if consid > tonumber(stampdutylimit) then \
              local stampdutypercent = redis.call("get", "stampdutypercent") \
              if stampdutypercent then \
                stampduty = round(consid * tonumber(stampdutypercent) / 100, 2) \
              end \
            end \
          end \
        end \
      end \
    end \
    local contractchargeamt = redis.call("get", "contractcharge") \
    if contractchargeamt then \
      contractcharge = tonumber(contractchargeamt) \
    end \
    return {commission, ptmlevy, stampduty, contractcharge} \
  end \
  ';

  gettotalcost = getcosts + '\
  local gettotalcost = function(instrumenttype, side, quantity, price, currency) \
    local consid = tonumber(quantity) * tonumber(price) \
    local costs =  getcosts(instrumenttype, side, consid, currency) \
    return consid + costs[1] + costs[2] + costs[3] + costs[4] \
  end \
  ';

  //
  // get initial margin to include costs
  //
  getinitialmargin = getcosts + '\
  local getinitialmargin = function(symbol, instrumenttype, quantity, price, currency) \
    local marginpercent = redis.call("hget", "symbol:" .. symbol, "marginpercent") \
    if not marginpercent then marginpercent = 100 end \
    local consid = tonumber(quantity) * tonumber(price) \
    local initialmargin = consid * tonumber(marginpercent) / 100 \
    local instrumenttype = redis.call("hget", "symbol:" .. symbol, "instrumenttype") \
    local costs = getcosts(instrumenttype, 1, consid, currency) \
    return {initialmargin + costs[1] + costs[2] + costs[3] + costs[4], costs} \
  end \
  ';

  updateordermargin = '\
  local updateordermargin = function(orderid, clientid, ordmargin, currency, newordmargin) \
    if newordmargin == ordmargin then return end \
    local marginkey = clientid .. ":margin:" .. currency \
    local marginskey = clientid .. ":margins" \
    local margin = redis.call("get", marginkey) \
    if not margin then \
      redis.call("set", marginkey, newordmargin) \
      redis.call("sadd", marginskey, currency) \
    else \
      local adjmargin = tonumber(margin) - tonumber(ordmargin) + tonumber(newordmargin) \
      if adjmargin == 0 then \
        redis.call("del", marginkey) \
        redis.call("srem", marginskey, currency) \
      else \
        redis.call("set", marginkey, adjmargin) \
      end \
    end \
    redis.call("hset", "order:" .. orderid, "margin", newordmargin) \
  end \
  ';

  updatetrademargin = '\
  local updatetrademargin = function(tradeid, clientid, currency, initialmargin) \
    local marginkey = clientid .. ":margin:" .. currency \
    local marginskey = clientid .. ":margins" \
    local margin = redis.call("get", marginkey) \
    if not margin then \
      redis.call("set", marginkey, margin) \
      redis.call("sadd", marginskey, currency) \
    else \
      local adjmargin = tonumber(margin) + tonumber(initialmargin) \
      if adjmargin == 0 then \
        redis.call("del", marginkey) \
        redis.call("srem", marginskey, currency) \
      else \
        redis.call("set", marginkey, adjmargin) \
      end \
    end \
    redis.call("hset", "trade:" .. tradeid, "margin", initialmargin) \
  end \
  ';

  updatereserve = '\
  local updatereserve = function(clientid, symbol, currency, quantity) \
    local poskey = symbol .. ":" .. currency \
    local reservekey = clientid .. ":reserve:" .. poskey \
    local reserveskey = clientid .. ":reserves" \
    local reserve = redis.call("get", reservekey) \
    if not reserve then \
      redis.call("set", reservekey, quantity) \
      redis.call("sadd", reserveskey, poskey) \
    else \
      local adjquantity = tonumber(reserve) + tonumber(quantity) \
      if adjquantity == 0 then \
        redis.call("del", reservekey) \
        redis.call("srem", reserveskey, poskey) \
      else \
        redis.call("set", reservekey, adjquantity) \
      end \
    end \
  end \
  ';

  updatecash = '\
  local updatecash = function(clientid, currency, amount) \
    local cashkey = clientid .. ":cash:" .. currency \
    local cashskey = clientid .. ":cash" \
    local cash = redis.call("get", cashkey) \
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
  end \
  ';

  //
  // tradecost & currency should be in settlement currency
  //
  updateposition = '\
  local updateposition = function(clientid, symbol, side, tradequantity, tradeprice, tradecost, currency, initialmargin, positionopenclose, positioncloseid) \
    local positionskey = clientid .. ":positions" \
    local positionid \
    if tonumber(positionopenclose) == 1 then \
      --[[ opening position, so get a new id ]] \
      positionid = redis.call("incr", "positionid") \
      --[[ create the position ]] \
      redis.call("hmset", "position:" .. positionid, "clientid", clientid, "symbol", symbol, "longshort", side, "quantity", tradequantity, "cost", tradecost, "currency", currency, "initialmargin", initialmargin, "positionid", positionid) \
      redis.call("sadd", positionskey, positionid) \
    else \
      --[[ closing, so get the position ]] \
      positionid = positioncloseid \
      local fields = {"quantity", "cost", "initialmargin"} \
      local vals = redis.call("hmget", "position:" .. positionid, unpack(fields)) \
      local posqty = tonumber(vals[1]) \
      local poscost = tonumber(vals[2]) \
      local posinitialmargin = tonumber(vals[3]) \
      posqty = posqty - tradequantity \
    end \
    return positionid \
  end \
  ';

    /*local symbolcurrency = symbol .. ":" .. currency \
    local positionkey = orgclientkey .. ":position:" .. symbolcurrency \
    local positionskey = orgclientkey .. ":positions" \
    local posqty = 0 \
    local poscost = 0 \
    local posinitialmargin = 0 \
    local fields = {"quantity", "cost", "initialmargin"} \
    local vals = redis.call("hmget", positionkey, unpack(fields)) \
    if vals[1] then \
      posqty = vals[1] \
      poscost = vals[2] \
      posinitialmargin = vals[3] \
    end \
    if side == "1" then \
      if posqty ~= 0 then \
        local adjqty = tonumber(posqty) + tonumber(tradequantity) \
        local adjcost = tonumber(poscost) + tonumber(tradecost) \
        local adjinitialmargin = tonumber(posinitialmargin) + tonumber(initialmargin) \
        if adjqty == 0 then \
          redis.call("hdel", positionkey, "symbol", "quantity", "cost", "currency", "initialmargin") \
          redis.call("srem", positionskey, symbolcurrency) \
        else \
          redis.call("hmset", positionkey, "quantity", adjqty, "price", tradeprice, "cost", adjcost, "initialmargin", adjinitialmargin) \
        end \
      else \
        redis.call("hmset", positionkey, "symbol", symbol, "quantity", tradequantity, "cost", tradecost, "currency", currency, "initialmargin", initialmargin) \
        redis.call("sadd", positionskey, symbolcurrency) \
      end \
    else \
      if posqty ~= 0 then \
        local adjqty = tonumber(posqty) - tradequantity \
        local adjcost = tonumber(poscost) - tradecost \
        local adjinitialmargin = tonumber(posinitialmargin) - tonumber(initialmargin) \
        if adjqty == 0 then \
          redis.call("hdel", positionkey, "symbol", "quantity", "cost", "currency", "initialmargin") \
          redis.call("srem", positionskey, symbolcurrency) \
        else \
          redis.call("hmset", positionkey, "quantity", adjqty, "cost", adjcost, "initialmargin", adjinitialmargin) \
        end \
      else \
        redis.call("hmset", positionkey, "symbol", symbol, "quantity", -tradequantity, "cost", -tradecost, "currency", currency, "initialmargin", initialmargin) \
        redis.call("sadd", positionskey, symbolcurrency) \
      end \
    end \
  end \
  ';*/

  adjustmarginreserve = getinitialmargin + updateordermargin + updatereserve + '\
  local adjustmarginreserve = function(orderid, clientid, symbol, side, price, ordmargin, currency, remquantity, newremquantity) \
    local instrumenttype = redis.call("hget", "symbol:" .. symbol, "instrumenttype") \
    if tonumber(side) == 1 then \
      if tonumber(newremquantity) ~= tonumber(remquantity) then \
        local newordmargin = {0} \
        if tonumber(newremquantity) ~= 0 then \
          newordmargin = getinitialmargin(symbol, instrumenttype, newremquantity, price, currency) \
        end \
        updateordermargin(orderid, clientid, ordmargin, currency, newordmargin[1]) \
      end \
    else \
      if tonumber(newremquantity) ~= tonumber(remquantity) then \
        updatereserve(clientid, symbol, currency, -tonumber(remquantity) + tonumber(newremquantity)) \
      end \
    end \
  end \
  ';

  rejectorder = '\
  local rejectorder = function(orderid, reason, text) \
    redis.call("hmset", "order:" .. orderid, "status", "8", "reason", reason, "text", text) \
  end \
  ';

  creditcheck = getinitialmargin + rejectorder + updateordermargin + updatereserve + '\
  local creditcheck = function(orderid, clientid, symbol, side, quantity, price, currency, instrumenttype, positionopenclose) \
    --[[ calculate initial margin, including costs ]] \
    local initialmargin = getinitialmargin(symbol, instrumenttype, quantity, price, currency) \
    --[[ always allow closing trades ]] \
    if tonumber(positionopenclose) == 2 then \
      return {1, initialmargin[2]} \
    end \
    --[[ see if client is allowed to trade this product ]] \
    if redis.call("sismember", clientid .. ":instrumenttypes", instrumenttype) == 0 then \
      rejectorder(orderid, 1017, "") \
      return {0} \
    end \
    --[[ check margin for all derivative trades & equity buys ]] \
    if instrumenttype == "CFD" or instrumenttype == "SPB" or tonumber(side) == 1 then \
      --[[ get existing margin ]] \
      local margin = redis.call("get", clientid .. ":margin:" .. currency) \
      if not margin then margin = 0 end \
      --[[ get cash ]] \
      local cash = redis.call("get", clientid .. ":cash:" .. currency) \
      if not cash then \
        rejectorder(orderid, 1001, "") \
        return {0} \
      end \
      --[[ check there is enough cash to cover total margin ]] \
      if tonumber(cash) < tonumber(margin) + initialmargin[1] then \
        rejectorder(orderid, 1002, "") \
        return {0} \
      end \
      updateordermargin(orderid, clientid, 0, currency, initialmargin[1]) \
    else \
      --[[ check there is a large enough position in this symbol/currency ]] \
      local poskey = symbol .. ":" .. currency \
      local position = redis.call("hget", clientid .. ":position:" .. poskey, "quantity") \
      if not position then \
        rejectorder(orderid, 1003, "") \
        return {0} \
      end \
      local netpos = tonumber(position) \
      local reserve = redis.call("get", clientid .. ":reserve:" .. poskey) \
      if reserve then \
        netpos = netpos - tonumber(reserve) \
      end \
      --[[ check what we have against quantity of order ]] \
      if netpos < tonumber(quantity) then \
        rejectorder(orderid, 1004, "") \
        return {0} \
      end \
      updatereserve(clientid, symbol, currency, quantity) \
    end \
    return {1, initialmargin[2]} \
  end \
  ';

  cancelorder = adjustmarginreserve + '\
  local cancelorder = function(orderid, status) \
    local orderkey = "order:" .. orderid \
    redis.call("hset", orderkey, "status", status) \
    local fields = {"clientid", "symbol", "side", "price", "settlcurrency", "margin", "remquantity"} \
    local vals = redis.call("hmget", orderkey, unpack(fields)) \
    if not vals[1] then \
      return 1009 \
    end \
    adjustmarginreserve(orderid, vals[1], vals[2], vals[3], vals[4], vals[6], vals[5], vals[7], 0) \
    return 0 \
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

  newtrade = updateposition + updatecash + '\
  local newtrade = function(clientid, orderid, symbol, side, quantity, price, currency, currencyratetoorg, currencyindtoorg, costs, counterpartyid, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrency, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, nosettdays, initialmargin, positionopenclose, positioncloseid) \
    local tradeid = redis.call("incr", "tradeid") \
    if not tradeid then return 0 end \
    local tradekey = "trade:" .. tradeid \
    redis.call("hmset", tradekey, "clientid", clientid, "orderid", orderid, "symbol", symbol, "side", side, "quantity", quantity, "price", price, "currency", currency, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "commission", costs[1], "ptmlevy", costs[2], "stampduty", costs[3], "contractcharge", costs[4], "counterpartyid", counterpartyid, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", tradeid, "settlcurrency", settlcurrency, "settlcurramt", settlcurramt, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "nosettdays", nosettdays, "positionopenclose", positionopenclose, "positioncloseid", positioncloseid) \
    redis.call("sadd", "trades", tradeid) \
    redis.call("sadd", clientid .. ":trades", tradeid) \
    redis.call("sadd", "order:" .. orderid .. ":trades", tradeid) \
    local totalcost = costs[1] + costs[2] + costs[3] + costs[4] \
    local tradecost \
    if tonumber(side) == 1 then \
      tradecost = settlcurramt + totalcost \
      updatecash(clientid, settlcurrency, -tradecost) \
    else \
      tradecost = settlcurramt - totalcost \
      updatecash(clientid, settlcurrency, tradecost) \
    end \
    local positionid = updateposition(clientid, symbol, side, quantity, price, tradecost, settlcurrency, initialmargin, positionopenclose, positioncloseid) \
    redis.call("hset", tradekey, "positionid", positionid) \
    return tradeid \
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

  neworder = '\
  local neworder = function(clientid, symbol, side, quantity, price, ordertype, remquantity, status, markettype, futsettdate, partfill, quoteid, currency, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforce, expiredate, expiretime, settlcurrency, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, nosettdays, positionopenclose, positioncloseid, operatortype, operatorid, hedgeorderid) \
    --[[ get a new orderid & store the order ]] \
    local orderid = redis.call("incr", "orderid") \
    redis.call("hmset", "order:" .. orderid, "clientid", clientid, "symbol", symbol, "side", side, "quantity", quantity, "price", price, "ordertype", ordertype, "remquantity", remquantity, "status", status, "markettype", markettype, "futsettdate", futsettdate, "partfill", partfill, "quoteid", quoteid, "currency", currency, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "timestamp", timestamp, "margin", margin, "timeinforce", timeinforce, "expiredate", expiredate, "expiretime", expiretime, "settlcurrency", settlcurrency, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "orderid", orderid, "externalorderid", externalorderid, "execid", execid, "nosettdays", nosettdays, "positionopenclose", positionopenclose, "positioncloseid", positioncloseid, "operatortype", operatortype, "operatorid", operatorid, "hedgeorderid", hedgeorderid) \
    --[[ add to set of orders for this client ]] \
    redis.call("sadd", KEYS[1] .. ":orders", orderid) \
    --[[ add order id to associated quote, if there is one ]] \
    if quoteid ~= "" then \
      redis.call("hset", "quote:" .. quoteid, "orderid", orderid) \
    end \
    return orderid \
  end \
  ';

  scriptrejectorder = rejectorder + adjustmarginreserve + '\
  rejectorder(KEYS[1], KEYS[2], KEYS[3]) \
  local fields = {"clientid", "symbol", "side", "price", "margin", "settlcurrency", "remquantity"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  if not vals[1] then \
    return 1009 \
  end \
  adjustmarginreserve(KEYS[1], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7], 0) \
  return 0 \
  ';

  scriptneworder = neworder + creditcheck + newtrade + getproquotesymbol + getproquotequote + '\
  local orderid = neworder(KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[4], "0", KEYS[7], KEYS[8], KEYS[9], KEYS[10], KEYS[11], KEYS[12], KEYS[13], KEYS[14], 0, KEYS[15], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], "", "", KEYS[21], KEYS[22], KEYS[23], KEYS[24], KEYS[25], "") \
  local side = tonumber(KEYS[3]) \
  local markettype = tonumber(KEYS[7]) \
  --[[ calculate consideration in settlement currency, costs will be added later ]] \
  local settlcurramt = tonumber(KEYS[4]) * tonumber(KEYS[5]) \
  local instrumenttype = redis.call("hget", "symbol:" .. KEYS[2], "instrumenttype") \
  local ret = creditcheck(orderid, KEYS[1], KEYS[2], side, KEYS[4], KEYS[5], KEYS[18], instrumenttype, KEYS[22]) \
  --[[ return order id for lookup, if credit check has failed, error message will be in order ]] \
  if ret[1] == 0 then \
    return {ret[1], orderid} \
  end \
  local proquotesymbol = {"", "", "", ""} \
  local proquotequote = {"", ""} \
  local hedgebookid = "" \
  local tradeid = "" \
  local hedgeorderid = "" \
  local hedgetradeid = "" \
  if instrumenttype == "CFD" or instrumenttype == "SPB" then \
    --[[ create trades for client & hedge book for off-exchange products ]] \
    hedgebookid = redis.call("hget", "hedge:" .. instrumenttype .. ":" .. KEYS[18], "hedgebookid") \
    if not hedgebookid then hedgebookid = 999999 end \
    local reverseside \
    if side == 1 then \
      reverseside = 2 \
    else \
      reverseside = 1 \
    end \
    local hedgecosts = {0,0,0,0} \
    tradeid = newtrade(KEYS[1], orderid, KEYS[2], side, KEYS[4], KEYS[5], KEYS[11], 1, 1, ret[2], hedgebookid, KEYS[7], "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], 5, KEYS[22], KEYS[23]) \
    hedgetradeid = newtrade(hedgebookid, orderid, KEYS[2], reverseside, KEYS[4], KEYS[5], KEYS[11], 1, 1, hedgecosts, KEYS[2], KEYS[7], "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], 5, KEYS[22], KEYS[23]) \
    --[[ adjust order as filled ]] \
    redis.call("hmset", "order:" .. orderid, "remquantity", 0, "status", 2) \
    --[[ todo: may need to adjust margin here ]] \
    --[[ see if we need to hedge this trade in the market ]] \
    local hedgeclient = tonumber(redis.call("hget", "client:" .. KEYS[1], "hedge")) \
    local hedgeinst = tonumber(redis.call("hget", "symbol:" .. KEYS[2], "hedge")) \
    if hedgeclient == 1 or hedgeinst == 1 then \
      --[[ create a hedge order in the underlying product ]] \
      proquotesymbol = getproquotesymbol(KEYS[2]) \
      if proquotesymbol[4] then \
        hedgeorderid = neworder(hedgebookid, proquotesymbol[4], KEYS[3], KEYS[4], KEYS[5], "X", KEYS[4], 0, KEYS[7], KEYS[8], KEYS[9], "", KEYS[11], KEYS[12], KEYS[13], KEYS[14], 0, KEYS[15], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], "", "", KEYS[21], KEYS[22], KEYS[23], KEYS[24], KEYS[25], orderid) \
        --[[ get proquote values ]] \
        proquotequote = getproquotequote(KEYS[10], side) \
      end \
    end \
  else \
    --[[ get proquote values if order is external ]] \
    if markettype == 0 then \
      proquotesymbol = getproquotesymbol(KEYS[2]) \
      proquotequote = getproquotequote(KEYS[10], side) \
    end \
  end \
  return {ret[1], orderid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], proquotequote[1], proquotequote[2], instrumenttype, hedgeorderid, tradeid, hedgetradeid} \
  ';

  // todo: add lastmkt
  // quantity required as number of shares
  // todo: settlcurrency
  scriptmatchorder = addtoorderbook + removefromorderbook + adjustmarginreserve + newtrade + getcosts + '\
  local fields = {"orgid", "clientid", "symbol", "side", "quantity", "price", "currency", "margin", "remquantity", "futsettdate", "timestamp"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  local orgclientkey = vals[1] .. ":" .. vals[2] \
  local remquantity = tonumber(vals[9]) \
  if remquantity <= 0 then return "1010" end \
  local instrumenttype = redis.call("hget", "symbol:" .. vals[3], "instrumenttype") \
  local lowerbound \
  local upperbound \
  local matchside \
  if tonumber(vals[4]) == 1 then \
    lowerbound = 0 \
    upperbound = vals[6] \
    matchside = 2 \
  else \
    lowerbound = "-inf" \
    upperbound = "-" .. vals[6] \
    matchside = 1 \
  end \
  local mo = {} \
  local t = {} \
  local mt = {} \
  local mc = {} \
  local j = 1 \
  local matchorders = redis.call("zrangebyscore", vals[3], lowerbound, upperbound) \
  for i = 1, #matchorders do \
    local matchvals = redis.call("hmget", "order:" .. matchorders[i], unpack(fields)) \
    local matchorgclientkey = matchvals[1] .. ":" .. matchvals[2] \
    local matchremquantity = tonumber(matchvals[9]) \
    if matchremquantity > 0 and matchorgclientkey ~= orgclientkey then \
      local tradequantity \
      --[[ calculate trade & remaining order quantities ]] \
      if matchremquantity >= remquantity then \
        tradequantity = remquantity \
        matchremquantity = matchremquantity - remquantity \
        remquantity = 0 \
      else \
        tradequantity = matchremquantity \
        remquantity = remquantity - matchremquantity \
        matchremquantity = 0 \
      end \
      --[[ adjust order book & update passive order ]] \
      local matchorderstatus \
      if matchremquantity == 0 then \
        removefromorderbook(matchvals[3], matchorders[i]) \
        matchorderstatus = 2 \
      else \
        matchorderstatus = 1 \
      end \
      redis.call("hmset", "order:" .. matchorders[i], "remquantity", matchremquantity, "status", matchorderstatus) \
      --[[ adjust margin/reserve ]] \
      adjustmarginreserve(matchorders[i], matchorgclientkey, matchvals[3], matchvals[4], matchvals[6], matchvals[8], matchvals[7], matchvals[9], matchremquantity) \
      --[[ trade gets done at passive order price ]] \
      local tradeprice = tonumber(matchvals[6]) \
      local consid = tradequantity * tradeprice \
      --[[ create trades for active & passive orders ]] \
      local costs = getcosts(instrumenttype, vals[4], consid, vals[7]) \
      local matchcosts = getcosts(instrumenttype, matchside, consid, matchvals[7]) \
      local tradeid = newtrade(vals[1], vals[2], KEYS[1], vals[3], vals[4], tradequantity, tradeprice, vals[7], costs, matchorder[2], matchorder[4], "1", "", vals[10], vals[11], "", "", vals[7], "", "") \
      local matchtradeid = newtrade(matchvals[2], matchvals[4], matchorders[i], matchvals[3], matchside, tradequantity, tradeprice, matchvals[7], matchcosts, order[2], order[4], "1", "", matchvals[10], matchvals[11], "", "", vals[7], "", "") \
      --[[ update return values ]] \
      mo[j] = matchorders[i] \
      t[j] = tradeid \
      mt[j] = matchtradeid \
      mc[j] = matchorgclientkey \
      j = j + 1 \
    end \
    if remquantity == 0 then break end \
  end \
  local orderstatus = "0" \
  if remquantity > 0 then \
    addtoorderbook(vals[3], KEYS[1], vals[4], vals[6]) \
  else \
    orderstatus = "2" \
  end \
  if remquantity < tonumber(vals[5]) then \
    --[[ reduce margin/reserve that has been added in the credit check ]] \
    adjustmarginreserve(KEYS[1], orgclientkey, vals[3], vals[4], vals[6], vals[8], vals[7], vals[9], remquantity) \
    if remquantity ~= 0 then \
      orderstatus = "1" \
    end \
  end \
  --[[ update active order ]] \
  redis.call("hmset", "order:" .. KEYS[1], "remquantity", remquantity, "status", orderstatus) \
  return {mo, t, mt, mc} \
  ';

  //
  // fill
  //
  // todo: should currency be settlcurrency?
  scriptnewtrade = newtrade + getcosts + adjustmarginreserve + updatetrademargin + '\
  local fields = {"clientid", "symbol", "side", "quantity", "price", "margin", "remquantity", "nosettdays", "positionopenclose", "positioncloseid", "operatortype"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  local quantity = tonumber(KEYS[4]) \
  local price = tonumber(KEYS[5]) \
  local instrumenttype = redis.call("hget", "symbol:" .. vals[2], "instrumenttype") \
  local initialmargin = getinitialmargin(vals[2], instrumenttype, quantity, price, KEYS[18]) \
  local tradeid = newtrade(vals[1], KEYS[1], vals[2], KEYS[3], quantity, price, KEYS[6], KEYS[7], KEYS[8], initialmargin[2], KEYS[9], 0, KEYS[10], KEYS[11], KEYS[12], KEYS[14], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], vals[8], initialmargin[1], vals[9], vals[10]) \
  --[[ adjust order related margin/reserve ]] \
  adjustmarginreserve(KEYS[1], vals[1], vals[2], vals[3], vals[5], vals[6], KEYS[18], vals[7], KEYS[15]) \
  --[[ adjust order ]] \
  redis.call("hmset", "order:" .. KEYS[1], "remquantity", KEYS[15], "status", KEYS[13]) \
  --[[ adjust trade related margin ]] \
  updatetrademargin(tradeid, vals[1], KEYS[17], initialmargin[1]) \
  return {tradeid, vals[11]} \
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

  scriptquoterequest = getproquotesymbol + '\
  local quotereqid = redis.call("incr", "quotereqid") \
  if not quotereqid then return 1005 end \
  --[[ store the quote request ]] \
  redis.call("hmset", "quoterequest:" .. quotereqid, "clientid", KEYS[1], "symbol", KEYS[2], "quantity", KEYS[3], "cashorderqty", KEYS[4], "currency", KEYS[5], "settlcurrency", KEYS[6], "nosettdays", KEYS[7], "futsettdate", KEYS[8], "quotestatus", "", "timestamp", KEYS[9], "quoteid", "", "quoterejectreason", "", "quotereqid", quotereqid, "operatortype", KEYS[10], "operatorid", KEYS[11]) \
  --[[ add to set of quoterequests for this client ]] \
  redis.call("sadd", KEYS[1] .. ":quoterequests", quotereqid) \
  --[[ get required instrument values for proquote ]] \
  local proquotesymbol = getproquotesymbol(KEYS[2]) \
  local instrumenttype = redis.call("hget", "symbol:" .. KEYS[2], "instrumenttype") \
  return {0, quotereqid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], instrumenttype} \
  ';

  // todo: check proquotsymbol against quote request symbol?
  scriptquote = calcfinance + '\
  local errorcode = 0 \
  local sides = 0 \
  local quoteid = "" \
  --[[ get the quote request ]] \
  local fields = {"clientid", "quoteid", "symbol", "quantity", "cashorderqty", "nosettdays", "settlcurrency", "operatortype"} \
  local vals = redis.call("hmget", "quoterequest:" .. KEYS[1], unpack(fields)) \
  if not vals[1] then \
    errorcode = 1014 \
    return {errorcode, sides, quoteid, ""} \
  end \
  local instrumenttype = redis.call("hget", "symbol:" .. vals[3], "instrumenttype") \
  local bidquantity = "" \
  local offerquantity = "" \
  local bidfinance = 0 \
  local offerfinance = 0 \
  --[[ calculate the quantity from the cashorderqty, if necessary, & calculate any finance ]] \
  if KEYS[2] == "" then \
    local offerprice = tonumber(KEYS[6]) \
    if vals[4] == "" then \
      offerquantity = round(tonumber(vals[5]) / offerprice, 0) \
    else \
      offerquantity = tonumber(vals[4]) \
    end \
    if instrumenttype == "CFD" or instrumenttype == "SPB" then \
      offerfinance = calcfinance(offerquantity * offerprice, vals[7], 1, vals[6]) \
    end \
  else \
    local bidprice = tonumber(KEYS[5]) \
    if vals[4] == "" then \
      bidquantity = round(tonumber(vals[5]) / bidprice, 0) \
    else \
      bidquantity = tonumber(vals[4]) \
    end \
    if instrumenttype == "CFD" or instrumenttype == "SPB" then \
      bidfinance = calcfinance(bidquantity * bidprice, vals[7], 2, vals[6]) \
    end \
  end \
  --[[ quotes for bid/offer arrive separately, so see if this is the first by checking for a quote id ]] \
  if vals[2] == "" then \
    --[[ get touch prices - using delayed - todo: may need to look up delayed/live ]] \
    local bestbid = "" \
    local bestoffer = "" \
    local pricefields = {"bid1", "offer1"} \
    local pricevals = redis.call("hmget", "topic:TIT." .. KEYS[4] .. ".LD", unpack(pricefields)) \
    if pricevals[1] then \
      bestbid = pricevals[1] \
      bestoffer = pricevals[2] \
    end \
    --[[ create a quote id as different from external quote ids (one for bid, one for offer)]] \
    quoteid = redis.call("incr", "quoteid") \
    --[[ store the quote ]] \
    redis.call("hmset", "quote:" .. quoteid, "quotereqid", KEYS[1], "clientid", vals[1], "quoteid", quoteid, "bidquoteid", KEYS[2], "offerquoteid", KEYS[3], "symbol", vals[3], "bestbid", bestbid, "bestoffer", bestoffer, "bidpx", KEYS[5], "offerpx", KEYS[6], "bidquantity", bidquantity, "offerquantity", offerquantity, "bidsize", KEYS[7], "offersize", KEYS[8], "validuntiltime", KEYS[9], "transacttime", KEYS[10], "currency", KEYS[11], "settlcurrency", KEYS[12], "bidqbroker", KEYS[13], "offerqbroker", KEYS[14], "nosettdays", vals[6], "futsettdate", KEYS[15], "bidfinance", bidfinance, "offerfinance", offerfinance) \
    --[[ quoterequest status - 0=new, 1=quoted, 2=rejected ]] \
    local status \
    --[[ bid or offer size needs to be non-zero ]] \
    if KEYS[9] == 0 and KEYS[10] == 0 then \
      status = "5" \
    else \
      status = "0" \
    end \
    --[[ add quote id, status to stored quoterequest ]] \
    redis.call("hmset", "quoterequest:" .. KEYS[1], "quoteid", quoteid, "quotestatus", status) \
    --[[ return to show only one side of quote received ]] \
    sides = 1 \
  else \
    quoteid = vals[2] \
    --[[ update quote, either bid or offer price/size/quote id ]] \
    if KEYS[2] == "" then \
      redis.call("hmset", "quote:" .. quoteid, "offerquoteid", KEYS[3], "offerpx", KEYS[6], "offerquantity", offerquantity, "offersize", KEYS[8], "offerqbroker", KEYS[14], "offerfinance", offerfinance) \
    else \
      redis.call("hmset", "quote:" .. quoteid, "bidquoteid", KEYS[2], "bidpx", KEYS[5], "bidquantity", bidquantity, "bidsize", KEYS[7], "bidqbroker", KEYS[13], "bidfinance", bidfinance) \
    end \
    sides = 2 \
  end \
  return {errorcode, sides, quoteid, vals[8]} \
  ';

  scriptquoteack = '\
  --[[ see if the quote request has already been rejected - this can happen as there may be a rejection message for bid & offer ]] \
  local quotestatus = redis.call("hget", "quoterequest:" .. KEYS[1], "quotestatus") \
  if quotestatus ~= "" then \
    return {1} \
  end \
  redis.call("hmset", "quoterequest:" .. KEYS[1], "quotestatus", KEYS[2], "quoterejectreason", KEYS[3], "text", KEYS[4]) \
  local operatortype = redis.call("hget", "quoterequest:" .. KEYS[1], "operatortype") \
  return {0, operatortype} \
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