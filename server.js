/****************
* server.js
* Front-office server
* Cantwaittotrade Limited
* Terry Johnston
* September 2012
****************/

// node libraries
var http = require('http');
var net = require('net');

// external libraries
var sockjs = require('sockjs');
var node_static = require('node-static');
var redis = require('redis');

// cw2t libraries
//var winnclient = require('./winnclient.js'); // Winner API connection
var ptpclient = require('./ptpclient.js'); // Proquote API connection

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
var scriptquoterequest;
var scriptneworder;
var scriptcreditcheck;
var scriptmatchorder;
var scriptordercancelrequest;
var scriptordercancel;
var scriptorderack;
var scriptnewtrade;
var scriptquote;
var scriptquoteack;
var scriptrejectorder;
var scriptgetinst;

// redis server
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

function quoteRequest(clientid, quoterequest) {
  console.log("quoterequest");
  console.log(quoterequest);

  quoterequest.timestamp = getUTCTimeStamp();

  // store the quote request & get an id
  db.eval(scriptquoterequest, 9, orgid, clientid, quoterequest.symbol, quoterequest.quantity, quoterequest.cashorderqty, quoterequest.currency, quoterequest.settlcurrency, quoterequest.futsettdate, quoterequest.timestamp, function(err, ret) {
    if (err) throw err;
  
    if (ret[0] != 0) {
      // todo: send error to client

      // send a quote ack
      console.log("Error in scriptquoterequest:" + getReasonDesc(ret[0]));
      return;
    }

    // add the quote request id & symbol details required for proquote
    quoterequest.quotereqid = ret[1];
    quoterequest.isin = ret[2];
    quoterequest.proquotesymbol = ret[3];
    quoterequest.exchange = ret[4];

    // specify broker - todo: remove
    //quoterequest.qbroker = "WNTSGB2LBIC";
 
    // forward the request to Proquote
    ptp.quoteRequest(quoterequest);
  });
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
  console.log("new order");

  // todo: tie this in with in/out of hours
  order.timestamp = getUTCTimeStamp();
  order.markettype = "0"; // "0" = main market, "1" = ooh - todo: "0" = any?
  order.partfill = "1"; // accept part-fill

  // always put a price in the order
  if (!("price" in order)) {
    order.price = "";
  }

  // and always have a quote id
  if (!("quoteid" in order)) {
    order.quoteid = "";
  }

  // store the order, get an id & credit check it
  db.eval(scriptneworder, 17, orgid, clientid, order.symbol, order.side, order.quantity, order.price, order.ordertype, order.markettype, order.futsettdate, order.partfill, order.quoteid, order.currency, order.timestamp, order.timeinforce, order.expiredate, order.expiretime, order.settlcurrency, function(err, ret) {
    if (err) throw err;

    if (ret[0] == 0) {
      getSendOrder(ret[1], false, false);
      return;
    }

    order.orderid = ret[1];

    // use the returned instrument values required by proquote
    if (order.markettype == "0") {
      order.isin = ret[2];
      order.proquotesymbol = ret[3];
      order.exchange = ret[4];
    }

    // use the returned quote values required by proquote
    if (order.ordertype == "D") {
      order.proquotequoteid = ret[5];
      order.qbroker = ret[6];
    }

    processOrder(order, conn);
  });
}

function getSideDesc(side) {
  if (side == '1') {
    return 'Buy';
  } else {
    return 'Sell';
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

      // send cash/positions
      if (ret[1].length > 0) {
        getSendPosition(orgclientkey, order.symbol, order.settlcurrency);
        getSendCash(orgclientkey, order.settlcurrency);
      }

      // send margin/reserve
      if (order.side == "1") {
        getSendMargin(orgclientkey, order.settlcurrency);
      } else {
        getSendReserve(orgclientkey, order.symbol, order.settlcurrency);
      }

      // send any matched orders
      for (var i = 0; i < ret[0].length; ++i) {
        // todo: send margin/reserve/cash/position?
        getSendOrder(ret[0][i], false, false);
      }

      // send any matched trades
      for (var i = 0; i < ret[2].length; ++i) {
        getSendTrade(ret[2][i]);
      }

      // send cash/position/margin/reserve for matched clients
      for (var i = 0; i < ret[3].length; ++i) {
        getSendPosition(ret[3][i], order.symbol, order.settlcurrency);
        getSendCash(ret[3][i], order.settlcurrency);
        getSendMargin(ret[3][i], order.settlcurrency);
        getSendReserve(ret[3][i], order.symbol, order.settlcurrency);
      }
    });
  });
}

function getSendOrder(orderid, sendmarginreserve, sendcashposition) {
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

    if (sendcashposition) {
      getSendCash(orgclientkey, order.settlcurrency);
      getSendPosition(orgclientkey, order.symbol, order.settlcurrency);
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
  });
}

function getSendPosition(orgclientid, symbol, currency) {
  var position = {};

  db.hgetall(orgclientid + ":position:" + symbol + ":" + currency, function(err, pos) {
    if (err) {
      console.log(err);
      return;
    }

    // send anyway, even if no position, as may need to clear f/e
    if (pos == null) {
      position.quantity = "0";
      position.cost = "0";
      position.symbol = symbol;
      position.currency = currency;
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

function processOrder(order, conn) {
  // either forward to Proquote or attempt to match the order, depending on whether the market is open
  if (order.markettype == "1") {
    matchOrder(order.orderid, conn);
  } else {
    ptp.newOrder(order);
  }

    /*order.orderid = orderid;

    // get the instrument, as we need the proquote symbol & isin
    db.hgetall("symbol:" + order.symbol, function(err, inst) {
      if (err) {
          console.log(err);
          return;
      }

      if (inst == null) {
        console.log("instrument not found, symbol:" + order.symbol);
        return;
      }

      // add instrument details to order
      order.proquotesymbol = inst.proquotesymbol;
      order.isin = inst.isin;

      // if there is an associated quote, we need the proquote quote id
      if (order.quoteid != "") {
        db.hgetall("quote:" + order.quoteid, function(err, quote) {
          if (err) {
            console.log(err);
            return;
          }

          if (quote == null) {
            console.log("quote not found, id:" + order.quoteid);
            return;
          }

          // add the proquote quote id, different for bid/offer
          if (order.side == "1") {
            order.qbroker = quote.offerqbroker;
            order.proquotequoteid = quote.offerquoteid;
          } else {
            order.qbroker = quote.bidqbroker;
            order.proquotequoteid = quote.bidquoteid;
          }

          ptp.newOrder(order);
        });
      } else {
        ptp.newOrder(order);
      }
    });
  }*/
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
  if (trade.side == '1') {
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

function sendInstruments(conn) {
  // get sorted subset of instruments
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

function sendIndex(index, conn) {
  var i = {symbols: []};
  var count;

  db.smembers("index:" + index, function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      console.log("Error: index " + index + " not found");
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
        //instrument.description = inst.description;

        // add the order to the array
        i.symbols.push(instrument);

        // send array if we have added the last item
        count--;
        if (count <= 0) {
          conn.write("{\"index" + index + "\":" + JSON.stringify(i) + "}");
        }
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

    replies.forEach(function (poskey, i) {
      db.hgetall(orgclientkey + ":position:" + poskey, function(err, position) {
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
  console.log("sendOrderBooksClient");
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
  var lastside = "";
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
          if (order.side == "1") {
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
          if (order.side == "1") {
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
    default:
      desc = "Unknown reason";
  }

  return desc;
}

function start(orgclientkey, conn) {
  sendOrderBooksClient(orgclientkey, conn);
  sendInstruments(conn);
  sendOrderTypes(conn);
  sendPositions(orgclientkey, conn);
  sendOrders(orgclientkey, conn);
  //sendTrades(orgclientkey, conn);
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

function orderBookRequest(orgclientkey, symbol, conn) {
  // add the client to the order book set for this instrument
  db.sadd("orderbook:" + symbol, orgclientkey);

  // & add the instrument to the order book set for this client
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

    var interval = setInterval(function() {
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
    }, 9000);
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

ptp.on("orderReject", function(exereport) {
  var ordrejreason = "";
  var text = "";
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

  db.eval(scriptorderack, 4, exereport.clordid, exereport.ordstatus, exereport.execid, text, function(err, ret) {
    if (err) throw err;

    // ok, so send confirmation
    getSendOrder(exereport.clordid, true, false);
  });
});

ptp.on("orderCancel", function(exereport) {
  console.log("Order cancelled by Proquote, ordercancelrequest id:" + exereport.clordid);

  db.eval(scriptordercancel, 1, exereport.clordid, function(err, ret) {
    if (err) throw err;

    if (ret != 0) {
      // todo: send to client
      console.log("Error in scriptordercancel, reason:" + getReasonDesc(ret));
      return;
    }

    //var orgclientkey = ret[1][1] + ":" + ret[1][3];

    // send confirmation
    getSendOrder(exereport.clordid, true, false);

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
  console.log(exereport);
  console.log("order executed, id:" + exereport.clordid);

  // default fx rate
  if (exereport.currency == exereport.settlcurrency) {
    exereport.settlcurrfxrate = "1";
  }

  db.eval(scriptnewtrade, 17, exereport.clordid, exereport.symbol, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currency, exereport.execbroker, exereport.execid, exereport.futsettdate, exereport.transacttime, exereport.ordstatus, exereport.lastmkt, exereport.leavesqty, exereport.orderid, exereport.settlcurrency, exereport.settlcurramt, exereport.settlcurrfxrate, function(err, ret) {
    if (err) {
      console.log(err);
      return
    }

    // send the order & trade
    getSendOrder(exereport.clordid, true, true);
    getSendTrade(ret);

    // send cash & position
    // todo: send as part of getsendorder?
    //getSendCash(orgclientkey, ret[0][31]); // orgclientkey, currency
    //getSendPosition(orgclientkey, ret[0][5]); // orgclientkey, symbol
  
    // send margin & reserve for limit orders
    /*if (ret[1][13] == "2") {
      getSendMargin(orgclientkey, ret[0][31]); // orgclientkey, currency
      getSendReserve(orgclientkey, ret[0][5]); // orgclientkey, symbol
    }*/
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

    // get the quote
    db.hgetall("quote:" + ret[2], function(err, storedquote) {
      var orgclientkey;

      if (err) {
        console.log(err);
        return;
      }

      if (storedquote == null) {
        console.log("Unable to find quote #" + ret[2]);
        return;
      }

      // get the number of seconds the quote is valid for
      storedquote.noseconds = getSeconds(storedquote.transacttime, storedquote.validuntiltime);

      // send quote to the client, if connected
      orgclientkey = storedquote.orgid + ":" + storedquote.clientid;
      if (orgclientkey in connections) {
        sendQuote(storedquote, connections[orgclientkey]);
      }
    });
  });
});

function sendQuote(quote, conn) {
  conn.write("{\"quote\":" + JSON.stringify(quote) + "}");
}

//
// quote rejection
//
ptp.on("quoteack", function(quoteack) {
  var orgclientkey;

  console.log("quote ack, request id " + quoteack.quotereqid);
  console.log(quoteack);

  db.eval(scriptquoteack, 4, quoteack.quotereqid, quoteack.quotestatus, quoteack.quoterejectreason, quoteack.text, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    db.hgetall("quoterequest:" + quoteack.quotereqid, function(err, quoterequest) {
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
    });
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
  var updatemargin;
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

  round = '\
  local round = function(num, dp) \
    local mult = 10 ^ (dp or 0) \
    return math.floor(num * mult + 0.5) / mult \
  end \
  ';

  getcosts = round + '\
  local getcosts = function(side, consid, currency) \
    local commission  = 0 \
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
      local ptmlevylimit = redis.call("get", "ptmlevylimit") \
      if ptmlevylimit then \
        if consid > tonumber(ptmlevylimit) then \
          local ptmlevyamt = redis.call("get", "ptmlevy") \
          if ptmlevyamt then \
            ptmlevy = tonumber(ptmlevyamt) \
          end \
        end \
      end \
      if side == "1" then \
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
    local contractchargeamt = redis.call("get", "contractcharge") \
    if contractchargeamt then \
      contractcharge = tonumber(contractchargeamt) \
    end \
    return {commission, ptmlevy, stampduty, contractcharge} \
  end \
  ';

  gettotalcost = getcosts + '\
  local gettotalcost = function(side, quantity, price, currency) \
    local consid = tonumber(quantity) * tonumber(price) \
    local costs =  getcosts(side, consid, currency) \
    return consid + costs[1] + costs[2] + costs[3] + costs[4] \
  end \
  ';

  updatemargin = '\
  local updatemargin = function(orderid, orgclientkey, ordmargin, currency, newordmargin) \
    if newordmargin == ordmargin then return end \
    local marginkey = orgclientkey .. ":margin:" .. currency \
    local marginskey = orgclientkey .. ":margins" \
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

  updatereserve = '\
  local updatereserve = function(orgclientkey, symbol, currency, quantity) \
    local poskey = symbol .. ":" .. currency \
    local reservekey = orgclientkey .. ":reserve:" .. poskey \
    local reserveskey = orgclientkey .. ":reserves" \
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
  local updatecash = function(orgclientkey, currency, amount) \
    local cashkey = orgclientkey .. ":cash:" .. currency \
    local cashskey = orgclientkey .. ":cash" \
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
  local updateposition = function(orgclientkey, symbol, side, tradequantity, tradecost, currency) \
    local symbolcurrency = symbol .. ":" .. currency \
    local positionkey = orgclientkey .. ":position:" .. symbolcurrency \
    local positionskey = orgclientkey .. ":positions" \
    local posqty = redis.call("hget", positionkey, "quantity") \
    local poscost = redis.call("hget", positionkey, "cost") \
    if side == "1" then \
      if posqty then \
        local adjqty = tonumber(posqty) + tonumber(tradequantity) \
        local adjcost = tonumber(poscost) + tonumber(tradecost) \
        if adjqty == 0 then \
          redis.call("hdel", positionkey, "symbol", "quantity", "cost", "currency") \
          redis.call("srem", positionskey, symbolcurrency) \
        else \
          redis.call("hmset", positionkey, "quantity", adjqty, "cost", adjcost) \
        end \
      else \
        redis.call("hmset", positionkey, "symbol", symbol, "quantity", tradequantity, "cost", tradecost, "currency", currency) \
        redis.call("sadd", positionskey, symbolcurrency) \
      end \
    else \
      if posqty then \
        local adjqty = tonumber(posqty) - tradequantity \
        local adjcost = tonumber(poscost) - tradecost \
        if adjqty == 0 then \
          redis.call("hdel", positionkey, "symbol", "quantity", "cost", "currency") \
          redis.call("srem", positionskey, symbolcurrency) \
        else \
          redis.call("hmset", positionkey, "quantity", adjqty, "cost", adjcost) \
        end \
      else \
        redis.call("hmset", positionkey, "symbol", symbol, "quantity", -tradequantity, "cost", -tradecost, "currency", currency) \
        redis.call("sadd", positionskey, symbolcurrency) \
      end \
    end \
  end \
  ';

  adjustmarginreserve = gettotalcost + updatemargin + updatereserve + '\
  local adjustmarginreserve = function(orderid, orgclientkey, symbol, side, price, ordmargin, currency, remquantity, newremquantity) \
    if side == "1" then \
      if tonumber(newremquantity) ~= tonumber(remquantity) then \
        local newordmargin = 0 \
        if tonumber(newremquantity) ~= 0 then \
          newordmargin = gettotalcost("1", tonumber(newremquantity), tonumber(price), currency) \
        end \
        updatemargin(orderid, orgclientkey, ordmargin, currency, newordmargin) \
      end \
    else \
      if tonumber(newremquantity) ~= tonumber(remquantity) then \
        updatereserve(orgclientkey, symbol, currency, -tonumber(remquantity) + tonumber(newremquantity)) \
      end \
    end \
  end \
  ';

  rejectorder = '\
  local rejectorder = function(orderid, reason, text) \
    redis.call("hmset", "order:" .. orderid, "status", "8", "reason", reason, "text", text) \
  end \
  ';

  //
  // note the currency should be the settlement currency
  //
  creditcheck = rejectorder + gettotalcost + updatemargin + updatereserve + '\
  local creditcheck = function(orderid, orgclientkey, symbol, side, quantity, price, currency, margin) \
    --[[ buy/sell ]] \
    if side == "1" then \
      local cash = redis.call("get", orgclientkey .. ":cash:" .. currency) \
      if not cash then \
        rejectorder(orderid, 1001, "") \
        return 0 \
      end \
      local margin = redis.call("get", orgclientkey .. ":margin:" .. currency) \
      if not margin then margin = "0" end \
      local totalcost = gettotalcost(side, quantity, price, currency) \
      if tonumber(cash) - tonumber(margin) < totalcost then \
        rejectorder(orderid, 1002, "") \
        return 0 \
      end \
      updatemargin(orderid, orgclientkey, margin, currency, totalcost) \
    else \
      local poskey = symbol .. ":" .. currency \
      local position = redis.call("hget", orgclientkey .. ":position:" .. poskey, "quantity") \
      if not position then \
        rejectorder(orderid, 1003, "") \
        return 0 \
      end \
      local netpos = tonumber(position) \
      local reserve = redis.call("get", orgclientkey .. ":reserve:" .. poskey) \
      if reserve then \
        netpos = netpos - tonumber(reserve) \
      end \
      --[[ check what we have against quantity of order ]] \
      if netpos < tonumber(quantity) then \
        rejectorder(orderid, 1004, "") \
        return 0 \
      end \
      updatereserve(orgclientkey, symbol, currency, quantity) \
    end \
    return 1 \
  end \
  ';

  cancelorder = adjustmarginreserve + '\
  local cancelorder = function(orderid, status) \
    local orderkey = "order:" .. orderid \
    redis.call("hset", orderkey, "status", status) \
    local fields = {"orgid", "clientid", "symbol", "side", "quantity", "price", "settlcurrency", "margin", "remquantity"} \
    local vals = redis.call("hmget", orderkey, unpack(fields)) \
    if not vals[1] then \
      return 1009 \
    end \
    local orgclientkey = vals[1] .. ":" .. vals[2] \
    adjustmarginreserve(orderid, orgclientkey, vals[3], vals[4], vals[6], vals[8], vals[7], vals[9], 0) \
    return 0 \
  end \
  ';

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

  addtoorderbook = '\
  local addtoorderbook = function(symbol, orderid, side, price) \
    --[[ buy orders need a negative price ]] \
    if side == "1" then \
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
  local newtrade = function(orgid, clientid, orderid, symbol, side, quantity, price, currency, costs, counterpartyorgid, counterpartyid, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrency, settlcurramt, settlcurrfxrate) \
    local tradeid = redis.call("incr", "tradeid") \
    if not tradeid then return 0 end \
    redis.call("hmset", "trade:" .. tradeid, "orgid", orgid, "clientid", clientid, "orderid", orderid, "symbol", symbol, "side", side, "quantity", quantity, "price", price, "currency", currency, "commission", costs[1], "ptmlevy", costs[2], "stampduty", costs[3], "contractcharge", costs[4], "counterpartyorgid", counterpartyorgid, "counterpartyid", counterpartyid, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", tradeid, "settlcurrency", settlcurrency, "settlcurramt", settlcurramt, "settlcurrfxrate", settlcurrfxrate) \
    redis.call("rpush", "trades", tradeid) \
    local orgclientkey = orgid .. ":" .. clientid \
    redis.call("sadd", orgclientkey .. ":trades", tradeid) \
    redis.call("sadd", "order:" .. orderid .. ":trades", tradeid) \
    local totalcost = costs[1] + costs[2] + costs[3] + costs[4] \
    local tradecost \
    if side == "1" then \
      tradecost = settlcurramt + totalcost \
      updatecash(orgclientkey, settlcurrency, -tradecost) \
    else \
      tradecost = settlcurramt - totalcost \
      updatecash(orgclientkey, settlcurrency, tradecost) \
    end \
    updateposition(orgclientkey, symbol, side, quantity, tradecost, settlcurrency) \
    return tradeid \
  end \
  ';

  scriptrejectorder = rejectorder + adjustmarginreserve + '\
  rejectorder(KEYS[1], KEYS[2], KEYS[3]) \
  local fields = {"orgid", "clientid", "symbol", "side", "price", "margin", "settlcurrency", "remquantity"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  if not vals[1] then \
    return 1009 \
  end \
  local orgclientkey = vals[1] .. ":" .. vals[2] \
  adjustmarginreserve(KEYS[1], orgclientkey, vals[3], vals[4], vals[5], vals[6], vals[7], vals[8], 0) \
  return 0 \
  ';

// todo: may need to use indicative price if there isn't one in the order
  scriptcreditcheck = creditcheck + '\
  local fields = {"orgid", "clientid", "symbol", "side", "quantity", "price", "currency", "margin"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  local ret = creditcheck(KEYS[1], vals[1] .. ":" .. vals[2], vals[3], vals[4], vals[5], vals[6], vals[7], vals[8]) \
  return ret \
  ';

  scriptneworder = creditcheck + getproquotesymbol + '\
  local orderid = redis.call("incr", "orderid") \
  if not orderid then return 1005 end \
  redis.call("hmset", "order:" .. orderid, "orgid", KEYS[1], "clientid", KEYS[2], "symbol", KEYS[3], "side", KEYS[4], "quantity", KEYS[5], "price", KEYS[6], "ordertype", KEYS[7], "remquantity", KEYS[5], "status", "0", "reason", "", "markettype", KEYS[8], "futsettdate", KEYS[9], "partfill", KEYS[10], "quoteid", KEYS[11], "currency", KEYS[12], "timestamp", KEYS[13], "margin", "0", "timeinforce", KEYS[14], "expiredate", KEYS[15], "expiretime", KEYS[16], "settlcurrency", KEYS[17], "text", "", "orderid", orderid) \
  local orgclientkey = KEYS[1] .. ":" .. KEYS[2] \
  redis.call("sadd", orgclientkey .. ":orders", orderid) \
  local pqquoteid = "" \
  local qbroker = "" \
  --[[ add order id to associated quote, if there is one, & get quote values required by proquote ]] \
  if KEYS[11] ~= "" then \
    local quotekey = "quote:" .. KEYS[11] \
    redis.call("hset", quotekey, "orderid", orderid) \
    --[[ ids & broker depend on buy/sell ]] \
    if KEYS[4] == "1" then \
      pqquoteid = redis.call("hget", quotekey, "offerquoteid") \
      qbroker = redis.call("hget", quotekey, "offerqbroker") \
    else \
      pqquoteid = redis.call("hget", quotekey, "bidquoteid") \
      qbroker = redis.call("hget", quotekey, "bidqbroker") \
    end \
  end \
  local ret = creditcheck(orderid, orgclientkey, KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[17], 0) \
  local proquotesymbol = {"", "", ""} \
  --[[ get required instrument values for proquote if order is external ]] \
  if ret == 1 and KEYS[8] == "0" then \
    proquotesymbol = getproquotesymbol(KEYS[3]) \
  end \
  return {ret, orderid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], pqquoteid, qbroker} \
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
  local lowerbound \
  local upperbound \
  local matchside \
  if vals[4] == "1" then \
    lowerbound = "0" \
    upperbound = vals[6] \
    matchside = "2" \
  else \
    lowerbound = "-inf" \
    upperbound = "-" .. vals[6] \
    matchside = "1" \
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
      local matchcosts = getcosts(matchside, consid, matchvals[7]) \
      --[[ create trades for active & passive orders ]] \
      local costs = getcosts(vals[4], consid, vals[7]) \
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
  // External fill
  //
  scriptnewtrade = newtrade + getcosts + adjustmarginreserve + '\
  local fields = {"orgid", "clientid", "symbol", "side", "quantity", "price", "currency", "margin", "remquantity"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  local quantity = tonumber(KEYS[4]) \
  local price = tonumber(KEYS[5]) \
  local costs = getcosts(KEYS[3], tonumber(KEYS[16]), KEYS[6]) \
  local tradeid = newtrade(vals[1], vals[2], KEYS[1], vals[3], KEYS[3], quantity, price, KEYS[6], costs, "", KEYS[7], "0", KEYS[8], KEYS[9], KEYS[10], KEYS[12], KEYS[14], KEYS[15], KEYS[16], KEYS[17]) \
  --[[ adjust margin/reserve ]] \
  adjustmarginreserve(KEYS[1], vals[1] .. ":" .. vals[2], vals[3], vals[4], vals[6], vals[8], KEYS[15], vals[9], KEYS[13]) \
  --[[ adjust order ]] \
  redis.call("hmset", "order:" .. KEYS[1], "remquantity", KEYS[13], "status", KEYS[11]) \
  return tradeid \
  ';

  scriptordercancelrequest = removefromorderbook + cancelorder + getproquotesymbol + '\
  local errorcode = 0 \
  local ordercancelreqid = redis.call("incr", "ordercancelreqid") \
  redis.call("hmset", "ordercancelrequest:" .. ordercancelreqid, "orgid", KEYS[1], "clientid", KEYS[2], "orderid", KEYS[3], "timestamp", KEYS[4]) \
  local fields = {"status", "markettype", "symbol", "side", "quantity"} \
  local vals = redis.call("hmget", "order:" .. KEYS[3], unpack(fields)) \
  local markettype = "" \
  local symbol = "" \
  local side = "" \
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
  return errorcode \
  ';

  scriptorderack = '\
  --[[ update external limit reference ]] \
  redis.call("hmset", "order:" .. KEYS[1], "status", KEYS[2], "ackid", KEYS[3], "text", KEYS[4]) \
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
  redis.call("hmset", "quoterequest:" .. quotereqid, "orgid", KEYS[1], "clientid", KEYS[2], "symbol", KEYS[3], "quantity", KEYS[4], "cashorderqty", KEYS[5], "currency", KEYS[6], "settlcurrency", KEYS[7], "futsettdate", KEYS[8], "quotestatus", "", "timestamp", KEYS[9], "quoteid", "", "quoterejectreason", "", "quotereqid", quotereqid) \
  redis.call("sadd", KEYS[1] .. ":" .. KEYS[2] .. ":quoterequests", quotereqid) \
  --[[ get required instrument values for proquote ]] \
  local proquotesymbol = getproquotesymbol(KEYS[3]) \
  return {0, quotereqid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3]} \
  ';

  /*
    --[[ get our symbol from the proquote symbol ]] \
  local symbol = redis.call("get", "proquotesymbol:" .. KEYS[4]) \
  if not symbol then \
    errorcode = 1016 \
    return {errorcode, sides, quoteid} \
  end \
  */

  // todo: check proquotsymbol against quote request symbol?
  scriptquote = '\
  local errorcode = 0 \
  local sides = 0 \
  local quoteid = "" \
  --[[ get the quote request ]] \
  local fields = {"orgid", "clientid", "quoteid", "symbol", "quantity", "cashorderqty"} \
  local vals = redis.call("hmget", "quoterequest:" .. KEYS[1], unpack(fields)) \
  if not vals[1] then \
    errorcode = 1014 \
    return {errorcode, sides, quoteid, ""} \
  end \
  --[[ quotes for bid/offer arrive separately, so see if this is the first by checking for a quote id ]] \
  if vals[3] == "" then \
    --[[ create a quote id as different from external quote ids (one for bid, one for offer)]] \
    quoteid = redis.call("incr", "quoteid") \
    --[[ store the quote, use quantity from quote request ]] \
    redis.call("hmset", "quote:" .. quoteid, "quotereqid", KEYS[1], "orgid", vals[1], "clientid", vals[2], "quoteid", quoteid, "bidquoteid", KEYS[2], "offerquoteid", KEYS[3], "symbol", vals[4], "quantity", vals[5], "cashorderqty", vals[6], "bidpx", KEYS[5], "offerpx", KEYS[6], "bidsize", KEYS[7], "offersize", KEYS[8], "validuntiltime", KEYS[9], "transacttime", KEYS[10], "currency", KEYS[11], "settlcurrency", KEYS[12], "bidqbroker", KEYS[13], "offerqbroker", KEYS[14], "futsettdate", KEYS[15]) \
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
    quoteid = vals[3] \
    local quotekey = "quote:" .. quoteid \
    --[[ update quote, either bid or offer price/size/quote id ]] \
    if KEYS[2] == "" then \
      redis.call("hmset", quotekey, "offerquoteid", KEYS[3], "offerpx", KEYS[6], "offersize", KEYS[8], "offerqbroker", KEYS[14]) \
    else \
      redis.call("hmset", quotekey, "bidquoteid", KEYS[2], "bidpx", KEYS[5], "bidsize", KEYS[7], "bidqbroker", KEYS[13]) \
    end \
    sides = 2 \
  end \
  return {errorcode, sides, quoteid} \
  ';

  scriptquoteack = '\
  redis.call("hmset", "quoterequest:" .. KEYS[1], "quotestatus", KEYS[2], "quoterejectreason", KEYS[3], "text", KEYS[4]) \
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