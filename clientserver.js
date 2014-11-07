/****************
* clientserver.js
* Front-office server for clients
* Cantwaittotrade Limited
* Terry Johnston
* September 2012
****************/

// node libraries
var https = require('https');
var fs = require("fs");

// external libraries
var sockjs = require('sockjs');
var node_static = require('node-static');
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

// globals
var connections = {}; // added to if & when a client logs on
var static_directory = new node_static.Server(__dirname); // static files server
var cw2tport = 443; // client listen port
var ordertypes = {};
var orgid = "1"; // todo: via logon
var defaultnosettdays = 3;
var serverid = 1; // must be unique, use .ini file?
var servertype = "client";
var feedtype = "nbtrader";

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
var scriptgetinst;
var scriptupdatepassword;
var scriptgetorderbooks;

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
  common.registerCommonScripts();
  registerScripts();
  initDb();
  clearSubscriptions();
  pubsub();
  listen();
}

// pubsub connections
function pubsub() {
  dbsub = redis.createClient(redisport, redishost);

  dbsub.on("subscribe", function(channel, count) {
    console.log("subscribed to: " + channel + ", num. channels:" + count);
  });

  dbsub.on("unsubscribe", function(channel, count) {
    console.log("unsubscribed from: " + channel + ", num. channels:" + count);
  });

  dbsub.on("message", function(channel, message) {
    console.log("channel: " + channel + ", message: " + message);

    if (channel.substr(0, 6) == "price:") {
      common.newPrice(channel.substr(6), serverid, message, connections, feedtype);
    } else {
      try {
        var obj = JSON.parse(message);

        if ("quote" in obj) {
          forwardQuote(obj.quote, message);
        } else if ("order" in obj) {
          forwardOrder(obj.order, message);
        } else if ("trade" in obj) {
          forwardTrade(obj.trade, message);
        } else if ("quoterequest" in obj) {
          forwardQuoterequest(obj.quoterequest, message);
        } else if ("quoteack" in obj) {
          forwardQuoteAck(obj.quoteack, message);
        }
      } catch (e) {
        console.log(e);
        console.log(message);
        return;
      }
    }

    /*if (message.substr(1, 6) == "prices") {
      common.newPrice(channel, servertype, message, connections, feedtype);
    } else if (message.substr(0, 8) == "quoteack") {
      sendQuoteack(message.substr(9));
    } else if (message.substr(0, 5) == "quote") {
      sendQuote(message.substr(6));
    } else if (message.substr(0, 15) == "orderbookupdate") {
      common.broadcastLevelOne(message.substr(16), connections);
    } else if (message.substr(0, 5) == "order") {
      getSendOrder(message.substr(6));
    } else if (message.substr(0, 5) == "trade") {
      getSendTrade(message.substr(6));
    } else if (message.substr(2, 4) == "chat") {
      newChat(message);
    } else {
      console.log("unknown message, channel=" + channel + ", message=" + message);
    }*/
  });

  // listen for client related messages
  dbsub.subscribe(clientserverchannel);

  // listen for trading messages
  dbsub.subscribe(tradechannel);

  // listen for ooh quoterequests
  dbsub.subscribe("quoterequest");
}

// sockjs server
var sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.3.min.js"};
var sockjs_svr = sockjs.createServer(sockjs_opts);

// options for https server
var options = {
  key: fs.readFileSync('key.pem'),
  cert: fs.readFileSync('cert.pem')
};

// https server
function listen() {
  var server = https.createServer(options);
 
  server.addListener('request', function(req, res) {
    static_directory.serve(req, res);
  });
  server.addListener('upgrade', function(req, res){
    res.end();
  });

  sockjs_svr.installHandlers(server, {prefix:'/echo'});
 
  server.listen(cw2tport, function() {
    console.log('Listening on port ' + cw2tport);
  });
 
  sockjs_svr.on('connection', function(conn) {
    // this will be overwritten if & when a client logs on
    var clientid = "0";

    // todo: remove
    //clientid = "1";
    //connections[clientid] = conn;
    //sendInstruments(1, conn);
    //

    console.log('new connection');

    // data callback
    conn.on('data', function(msg) {
      console.log('recd:' + msg);

      if (msg.substr(2, 18) == "ordercancelrequest") {
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 6) == "order\"") {
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 6) == "quote\"") {
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 13) == "quoterequest\"") {
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 4) == "chat") {
        db.publish(userserverchannel, msg);
      } else {
        try {
          var obj = JSON.parse(msg);

          if ("orderbookrequest" in obj) {
            orderbookRequest(clientid, obj.orderbookrequest, conn);
          } else if ("orderbookremoverequest" in obj) {
            orderbookRemoveRequest(clientid, obj.orderbookremoverequest, conn);
          } else if ("positionrequest" in obj) {
            positionRequest(obj.positionrequest, clientid, conn);
          } else if ("cashrequest" in obj) {
            cashRequest(obj.cashrequest, clientid, conn);
          } else if ("accountrequest" in obj) {
            accountRequest(obj.accountrequest, clientid, conn);
          } else if ("quoterequesthistoryrequest" in obj) {
            quoteRequestHistory(obj.quoterequesthistoryrequest, clientid, conn);
          } else if ("quotehistoryrequest" in obj) {
            quoteHistory(obj.quotehistoryrequest, clientid, conn);
          } else if ("tradehistoryrequest" in obj) {
            tradeHistory(obj.tradehistoryrequest, clientid, conn);
          } else if ("orderhistoryrequest" in obj) {
            orderHistory(obj.orderhistoryrequest, clientid, conn);
          } else if ("cashhistoryrequest" in obj) {
            cashHistory(obj.cashhistoryrequest, clientid, conn);
          } else if ("index" in obj) {
            common.sendIndex(obj.index, conn);
          } else if ("pwdrequest" in obj) {
            passwordRequest(clientid, obj.pwdrequest, conn);
          } else if ("register" in obj) {
            registerClient(obj.register, conn);
          } else if ("signin" in obj) {
            signIn(obj.signin);
          } else if ("ping" in obj) {
            conn.write("pong");
          } else {
            console.log("unknown msg received:" + msg);
          }
        } catch (e) {
          console.log(e);
          return;
        }
      }
    });

    conn.on('close', function() {
      tidy(clientid, conn);
      clientid = "0";
    });

    // client sign on
    function signIn(signin) {
      var reply = {};
      reply.success = false;

      db.get(servertype + ":" + signin.email, function(err, cid) {
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
        db.hgetall(servertype + ":" + cid, function(err, client) {
          if (err) {
            console.log("Error in signIn:" + err);
            return;
          }

          if (!client || signin.email != client.email || signin.password != client.password) {
            reply.reason = "Email or password incorrect. Please try again.";
            replySignIn(reply, conn);
            return;
          }

          // check to see if this client is already logged on
          db.sismember("connections:" + servertype, client.clientid, function(err, found) {
            if (err) {
              console.log(err);
              return;
            }

            if (found) {
              console.log("client " + client.clientid + " already logged on");

              if (client.clientid in connections) {
                connections[client.clientid].close();
              }
            }

            // validated, so set client id & add client to list of connections
            clientid = client.clientid;
            connections[clientid] = conn;

            // keep a record
            db.sadd("connections:" + servertype, clientid);

            // send a successful logon reply
            reply.success = true;
            reply.email = signin.email;
            reply.clientid = clientid;
            replySignIn(reply, conn);

            var timestamp = common.getUTCTimeStamp(new Date());
            console.log(timestamp + " - client:" + clientid + " logged on");

            // send the data
            start(clientid, conn);
            console.log("after start");
          });
        });
      });
    }
  });
}

function tidy(clientid, conn) {
  console.log("tidy");
  if (clientid != "0") {
    if (clientid in connections) {
      var timestamp = common.getUTCTimeStamp(new Date());
      console.log(timestamp + " - client:" + clientid + " logged off");

      if (connections[clientid] == conn) {
        // remove from list
        delete connections[clientid];

        // remove from database
        db.srem("connections:" + servertype, clientid);

        unsubscribeConnection(clientid);
      }
    }
  }
}

function unsubscribeConnection(id) {
  db.eval(common.scriptunsubscribeid, 3, id, serverid, feedtype, function(err, ret) {
    if (err) throw err;

    // unsubscribe returned topics
    for (var i = 0; i < ret.length; i++) {
      dbsub.unsubscribe("price:" + ret[i]);
    }
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
  quote.reasondesc = common.getReasonDesc(reason);

  conn.write("{\"quote\":" + JSON.stringify(quote) + "}");
}*/

function getSideDesc(side) {
  if (parseInt(side) == 1) {
    return "Buy";
  } else {
    return "Sell";
  }
}

/*function getSendOrder(orderid, sendmarginreserve, sendcash) {
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
}*/

/*function getSendTrade(tradeid) {
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
}*/

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
      connections[clientid].write("{\"cashitem\":" + JSON.stringify(cash) + "}");
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
  // get all the instruments in the order book for this client
/*function sendOrderBooksClient(clientid, conn) {
  db.smembers("client:" + clientid + ":orderbooks", function(err, instruments) {
    if (err) {
      console.log("Error in sendOrderBooksClient:" + err);
      return;
    }

    // send the order book for each instrument
    instruments.forEach(function (symbol, i) {
      orderBookRequest(clientid, symbol, conn);
    });
  });
}*/

//
// send all the orderbooks for a single client
//
function sendOrderBooksClient(clientid, conn) {
  db.eval(scriptgetorderbooks, 2, clientid, feedtype, function(err, ret) {
    if (err) throw err;

    conn.write("{\"orderbooks\":" + ret[1] + "}");

    // todo: is this needed or the right place?
    sendReadyToTrade(conn);

    // subscribe to any associated topics that are not already subscribed to
    for (var i = 0; i < ret[0].length; i++) {
      dbsub.subscribe("ticker:" + ret[0][i]);
    }
  });
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

function start(clientid, conn) {
  //sendOrderBooksClient(clientid, conn);
  //sendOrderTypes(conn);
  sendInstruments(clientid, conn);
  //sendPositions(clientid, conn);
  //sendOrders(clientid, conn);
  //sendCash(clientid, conn);
  //sendMargins(clientid, conn);
  //sendReserves(clientid, conn);

  // may not be last, but...
  //sendReadyToTrade(conn);
}

function sendReadyToTrade(conn) {
  var status = {};

  status.readytotrade = true;

  conn.write("{\"status\":" + JSON.stringify(status) + "}");
}

function replySignIn(reply, conn) {
    conn.write("{\"signinreply\":" + JSON.stringify(reply) + "}");
}

function clearSubscriptions() {
  // clears connections & subscriptions
  db.eval(common.scriptunsubscribeserver, 1, serverid, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    // unsubscribe to the returned topics
    for (var i = 0; i < ret.length; i++) {
      dbsub.unsubscribe("price:" + ret[i]);
    }
  });  
}

function initDb() {
  console.log("initDb");

  // clear any connected clients
  /*db.smembers("connections:" + serverid, function(err, connections) {
    if (err) {
      console.log(err);
      return;
    }

    connections.forEach(function(connection, i) {
      unsubscribeConnection(connection);

      db.srem("connections:" + serverid, connection);
    });
  });*/
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

function orderbookRequest(clientid, symbol, conn) {
  console.log("orderbookRequest:" + symbol);

  // get hour & minute for comparison with timezone to determine in/out of hours
  var today = new Date();

  var hour = today.getHours();
  var minute = today.getMinutes();
  var day = today.getDay();

  db.eval(common.scriptsubscribeinstrument, 7, symbol, clientid, serverid, feedtype, hour, minute, day, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    var subscribe = ret[0];
    var markettype = ret[4];

    // see if we need to subscribe
    if (subscribe == 1) {
      dbsub.subscribe("price:" + symbol);
    }

    //send latest price if ooh or already subscribed
    if (markettype == 1 || subscribe == 0) {
      var price = {};
      price.symbol = symbol;
      price.bid = ret[1];
      price.ask = ret[2];
      price.timestamp = ret[3];

      conn.write("{\"price\":" + JSON.stringify(price) + "}");
    }

    // send the current stored price
    //common.sendCurrentOrderbook(symbol, ret[1], conn, feedtype);
  });
}

function orderbookRemoveRequest(clientid, symbol, conn) {
  db.eval(common.scriptunsubscribeinstrument, 4, symbol, clientid, serverid, feedtype, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // the script will tell us if we need to unsubscribe from the topic
    if (ret[0]) {
      dbsub.unsubscribe("price:" + ret[1]);
    }
  });
}

/*function orderBookRequestDL(clientid, symbol, conn) {
  console.log("orderBookRequest");
  db.eval(common.scriptsubscribeinstrument, 4, symbol, clientid, servertype, feedtype, function(err, ret) {
    if (err) throw err;

    console.log(ret);

    // the script tells us if we need to subscribe to a topic
    if (ret[0]) {
      dbsub.subscribe("ticker:" + ret[1]);
    }

    // publish the ticker list if it has changed
    if (ret[2]) {
      db.publish("digitallook", ret[3]);
    }

    // send the orderbook, with the current stored prices
    common.sendCurrentOrderBook(symbol, ret[1], conn, feedtype);
  });
}

function orderBookRemoveRequestDL(clientid, symbol, conn) {
  db.eval(common.scriptunsubscribeinstrument, 4, symbol, clientid, servertype, feedtype, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // the script will tell us if we need to unsubscribe from the topic
    if (ret[0]) {
      dbsub.unsubscribe("ticker:" + ret[1]);
    }

    if (ret[2]) {
      db.publish("digitallook", ret[3]);
    }
  });
}*/

/*function positionHistory(clientid, symbol, conn) {
  // todo: remove
  clientid = 5020;
  symbol = "LOOK";

  //bo.getPositionHistory(clientid, symbol);
}*/

function quoteRequestHistory(req, clientid, conn) {
  db.eval(common.scriptgetquoterequests, 1, clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"quoterequests\":" + ret + "}");
  });
}

function quoteHistory(req, clientid, conn) {
  db.eval(common.scriptgetquotes, 1, clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"quotes\":" + ret + "}");
  });
}

function orderHistory(req, clientid, conn) {
  db.eval(common.scriptgetorders, 1, clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"orders\":" + ret + "}");
  });
}

function tradeHistory(req, clientid, conn) {
  if ("positionkey" in req) {
    db.eval(common.scriptgetpostrades, 2, clientid, req.positionkey, function(err, ret) {
      if (err) throw err;
      conn.write("{\"trades\":" + ret + "}");
    });
  } else {
    db.eval(common.scriptgettrades, 1, clientid, function(err, ret) {
      if (err) throw err;
      conn.write("{\"trades\":" + ret + "}");
    });
  }
}

function cashHistory(req, clientid, conn) {
  db.eval(common.scriptgetcashhistory, 2, clientid, req.currency, function(err, ret) {
    console.log(ret);
    if (err) throw err;
    conn.write("{\"cashhistory\":" + ret + "}");
  });  
}

function positionRequest(posreq, clientid, conn) {
  console.log("positionRequest");
  db.eval(common.scriptgetpositions, 1, clientid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
    conn.write("{\"positions\":" + ret + "}");
  });
}

function cashRequest(cashreq, clientid, conn) {
  db.eval(common.scriptgetcash, 1, clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"cash\":" + ret + "}");
  });  
}

function accountRequest(acctreq, clientid, conn) {
  db.eval(common.scriptgetaccount, 1, clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"account\":" + ret + "}");
  });
}

function passwordRequest(clientid, pwdrequest, conn) {
  db.eval(scriptupdatepassword, 3, clientid, pwdrequest.oldpwd, pwdrequest.newpwd, function(err, ret) {
    if (err) throw err;
    conn.write("{\"pwdupdate\":" + JSON.stringify(ret) + "}");
  });
}

function sendQuoteack(quoteack) {
  console.log(quoteack);
  return;

  db.hgetall("quoterequest:" + quoteack.quotereqid, function(err, quoterequest) {
    if (err) {
      console.log(err);
      return;
    }

    if (quoterequest == null) {
      console.log("can't find quote request id " + quotereqid);
      return;
    }

    //quoteack.quotereqid = quoterequest.quotereqid;
    quoteack.clientid = quoterequest.clientid;
    quoteack.symbol = quoterequest.symbol;
    quoteack.quoterejectreasondesc = common.getPTPQuoteRejectReason(quoterequest.quoterejectreason);
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
    quote.noseconds = common.getSeconds(quote.transacttime, quote.validuntiltime);

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

// forward quoteack to relevant client
function forwardQuoteAck(quoteack, msg) {
  console.log("forwardQuoteAck");
  console.log(quoteack);

  if (quoteack.clientid in connections) {
    connections[quoteack.clientid].write(msg);
  }
}

// forward quote to relevant client
function forwardQuote(quote, msg) {
  console.log("forwardQuote");
  console.log(quote);

  // forward to requesting client
  if (quote.clientid in connections) {
    connections[quote.clientid].write(msg);
  }

  // forward to quoting client
  if ('qclientid' in quote) {
    if (quote.qclientid in connections) {
      console.log("sending to " + quote.qclientid);
      connections[quote.qclientid].write(msg);
    }
  }
}

// forward order to relevant client
function forwardOrder(order, msg) {
  console.log("forwarding order to client");
  console.log(order);

  if (order.clientid in connections) {
    connections[order.clientid].write(msg);
  }
}

// forward trade to relevant client
function forwardTrade(trade, msg) {
  console.log("forwardTrade");
  console.log(trade);

  if (trade.clientid in connections) {
    connections[trade.clientid].write(msg);
  }
}

function forwardQuoterequest(quoterequest, msg) {
  console.log("forwardQuoterequest");
  console.log(quoterequest);

  // send to everyone?
  for (var i in connections) {
    // not to sender
    if (i != quoterequest.clientid) {
      connections[i].write(msg);
    }
  }
}

//
// this is chat received from a user that needs to be forwarded to a client
//
function newChat(chat) {
  // we need to parse the message to find out which client to forward it to
  try {
    var chatobj = JSON.parse(chat);

    if ('chat' in chatobj && chatobj.chat.clientid in connections) {
      connections[chatobj.chat.clientid].write(chat);
    }
  } catch (e) {
    console.log(e);
    return;
  }
}

function registerScripts() {
  //
  // get alpha sorted list of instruments for a specified client
  // uses set of valid instrument types per client i.e. 1:instrumenttypes CFD
  //
  scriptgetinst = '\
  local instruments = redis.call("sort", "instruments", "ALPHA") \
  local fields = {"instrumenttype", "description"} \
  local vals \
  local inst = {} \
  for index = 1, #instruments do \
    vals = redis.call("hmget", "symbol:" .. instruments[index], unpack(fields)) \
    if redis.call("sismember", KEYS[1] .. ":instrumenttypes", vals[1]) == 1 then \
      table.insert(inst, {symbol = instruments[index], description = vals[2]}) \
    end \
  end \
  return cjson.encode(inst) \
  ';

/* removed
      if vals[4] then \
        marginpc = vals[4] \
      else \
        marginpc = 100 \
      end \
*/

  scriptupdatepassword = '\
    local retval = 0 \
    local oldpwd = redis.call("hget", "client:" .. KEYS[1], "password") \
    if oldpwd == KEYS[2] then \
      redis.call("hset", "client:" .. KEYS[1], "password", KEYS[3]) \
      retval = 1 \
    end \
    return retval \
  ';

  scriptgetorderbooks = common.subscribeinstrument + common.subscribeinstrumentdl + '\
    local needtosubscribe = {} \
    local tblresults = {} \
    local vals \
    local fields = {"bid1", "offer1", "bid2", "offer2", "bid3", "offer3", "bid4", "offer4", "bid5", "offer5", "bid6", "offer6"} \
    local ret = {0, ""} \
    local orderbooks = redis.call("smembers", "client:" .. KEYS[1] .. ":orderbooks") \
    for index = 1, #orderbooks do \
      if KEYS[2] == "proquote" then \
        ret = subscribeinstrument(orderbooks[index], KEYS[1], "client") \
      elseif KEYS[2] == "digitallook" then \
        ret = subscribeinstrumentdl(orderbooks[index], KEYS[1], "client") \
      end \
      if ret[1] == 1 then \
        --[[ keep a list of topics/tickers we need to subscribe to as has to be done separately as on separate connection ]] \
        table.insert(needtosubscribe, ret[2]) \
      end \
      if KEYS[2] == "proquote" then \
        vals = redis.call("hmget", "topic:" .. ret[2], unpack(fields)) \
      elseif KEYS[2] == "digitallook" then \
        vals = redis.call("hmget", "ticker:" .. ret[2], unpack(fields)) \
      end \
      if vals[1] then \
        table.insert(tblresults, {symbol=orderbooks[index], prices={{bid=vals[1],offer=vals[2],level=1},{bid=vals[3],offer=vals[4],level=2},{bid=vals[5],offer=vals[6],level=3},{bid=vals[7],offer=vals[8],level=4},{bid=vals[9],offer=vals[10],level=5},{bid=vals[11],offer=vals[12],level=6}}}) \
      else \
        table.insert(tblresults, {symbol=orderbooks[index], prices={{bid=0,offer=0,level=1},{bid=0,offer=0,level=2},{bid=0,offer=0,level=3},{bid=0,offer=0,level=4},{bid=0,offer=0,level=5},{bid=0,offer=0,level=6}}}) \
      end \
    end \
    return {needtosubscribe, cjson.encode(tblresults)} \
  ';
}