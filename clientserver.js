/****************
* clientserver.js
* Front-office server for clients
* Cantwaittotrade Limited
* Terry Johnston
* September 2012
****************/

// http or https
var http = require('http');
//var https = require('https');

// comms
var socketio = require('socket.io')(http);
//var sockjs = require('sockjs');

// other
var node_static = require('node-static');
var redis = require('redis');
var fs = require("fs");

// cw2t libraries
var commonfo = require('./commonfo.js');
var commonbo = require('./commonbo.js');

// globals
var connections = {}; // added to if & when a client logs on
var static_directory = new node_static.Server(__dirname); // static files server
var defaultnosettdays = 3;
var serverid = 1; // must be unique, use .ini file?
var servertype = "client";
var feedtype = "nbtrader";
var operatortype = 1;
var brokerid = 1; // get from signin
var cw2tport = 8080; // http listen port
//var cw2tport = 443; // https listen port

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
  commonfo.registerScripts();
  commonbo.registerScripts();
  registerScripts();
  initDb();
  //clearSubscriptions();
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
    //console.log("channel: " + channel + ", message: " + message);

    if (channel.substr(0, 6) == "price:") {
      commonfo.newPrice(channel.substr(6), serverid, message, connections, feedtype);
    } else {
      console.log("channel: " + channel + ", message: " + message);
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
        } else if ("position" in obj) {
          forwardPosition(obj.position, message);
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
  dbsub.subscribe(commonbo.clientserverchannel);

  // listen for trading messages
  dbsub.subscribe(commonbo.tradechannel);

  // listen for ooh quoterequests
  dbsub.subscribe(commonbo.quoterequestchannel);

  // listen for position updates
  dbsub.subscribe(commonbo.positionchannel);
}

// sockjs server
// http
//var sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.3.min.js"};
// https
//var sockjs_opts = {sockjs_url: "https://d1fxtkz8shb9d2.cloudfront.net/sockjs-0.3.js"};
//var sockjs_svr = sockjs.createServer(sockjs_opts);

function handler(req, res) {
console.log("handler");
//  console.log(req);
//  console.log(res);
  fs.readFile(__dirname + '/chart.html',
  function (err, data) {
console.log(err);
console.log(data);
    if (err) {
      res.writeHead(500);
      return res.end('Error loading chart.html');
    }

    res.writeHead(200);
    res.end(data);
  });
}

// options for https server
var options = {
  key: fs.readFileSync('key.pem'),
  cert: fs.readFileSync('cert.pem')
};

// http/https server
function listen() {
console.log("listen");
  var server = http.createServer(handler);
  //var server = https.createServer(options);
 
  /*server.addListener('request', function(req, res) {
    static_directory.serve(req, res);
  });
  server.addListener('upgrade', function(req, res){
    res.end();
  });*/

  //sockjs_svr.installHandlers(server, {prefix:'/echo'});
   
  server.listen(cw2tport, function() {
    console.log('Listening on port ' + cw2tport);
  });
 
  //sockjs_svr.on('connection', function(conn) {
  socketio.on('connection', function(conn) {
    // this will be overwritten if & when a client logs on
    var clientid = "0";

    console.log('new connection');

    conn.emit('news', {hello: 'world'});

    // data callback
    conn.on('my other event', function(msg) {
      console.log('recd:' + msg);

      // just forward to trade server
      /*if (msg.substr(2, 18) == "ordercancelrequest") {
        db.publish(common.tradeserverchannel, msg);
      } else if (msg.substr(2, 6) == "order\"") {
        db.publish(common.tradeserverchannel, msg);
      } else if (msg.substr(2, 6) == "quote\"") {
        db.publish(common.tradeserverchannel, msg);
      } else if (msg.substr(2, 13) == "quoterequest\"") {
        db.publish(common.tradeserverchannel, msg);
      } else if (msg.substr(2, 4) == "chat") {
        db.publish(common.userserverchannel, msg);
      } else {*/
        try {
          var obj = JSON.parse(msg);

          if ("subscribepricerequest" in obj) {
            subscribePriceRequest(clientid, obj.subscribepricerequest, conn);
          } else if ("unsubscribepricerequest" in obj) {
            unsubscribePriceRequest(clientid, obj.unsubscribepricerequest, conn);
          } else if ("order" in obj) {
            newOrder(obj.order, clientid, conn);
          } else if ("quoterequest" in obj) {
            quoteRequest(obj.quoterequest, clientid, conn);
          } else if ("symbolrequest" in obj) {
            symbolRequest(obj.symbolrequest, clientid, conn);
          } else if ("positionrequest" in obj) {
            positionRequest(obj.positionrequest, clientid, conn);
          } else if ("cashrequest" in obj) {
            cashRequest(obj.cashrequest, clientid, conn);
          } else if ("accountsummaryrequest" in obj) {
            accountSummaryRequest(obj.accountsummaryrequest, clientid, conn);
          } else if ("quoterequesthistoryrequest" in obj) {
            quoteRequestHistory(obj.quoterequesthistoryrequest, clientid, conn);
          } else if ("openquoterequestrequest" in obj) {
            openQuoteRequestRequest(obj.openquoterequestrequest, conn);
          } else if ("myquotesrequest" in obj) {
            myQuotesRequest(obj.myquotesrequest, clientid, conn);
          } else if ("quotehistoryrequest" in obj) {
            quoteHistory(obj.quotehistoryrequest, clientid, conn);
          } else if ("tradehistoryrequest" in obj) {
            tradeHistory(obj.tradehistoryrequest, clientid, conn);
          } else if ("orderhistoryrequest" in obj) {
            orderHistory(obj.orderhistoryrequest, clientid, conn);
          } else if ("cashhistoryrequest" in obj) {
            cashHistory(obj.cashhistoryrequest, clientid, conn);
          /*} else if ("unsubscribepositionsrequest" in obj) {
            unsubscribePositionsRequest(obj.unsubscribepositionsrequest, clientid, conn);*/
          } else if ("watchlistrequest" in obj) {
            watchlistRequest(obj.watchlistrequest, clientid, conn);
          } else if ("unwatchlistrequest" in obj) {
            unwatchlistrequest(obj.unwatchlistrequest, clientid, conn);
          } else if ("statementrequest" in obj) {
            statementRequest(obj.statementrequest, clientid, conn);
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
      //}
    });

    conn.on('close', function() {
      tidy(clientid, conn);
      clientid = "0";
    });

    // client sign on
    function signIn(signin) {
      var reply = {};
      reply.success = false;
      var brokerkey = "broker:" + brokerid;

      console.log("signIn");

      // add servertype i.e. email:client:emailaddr?
      db.get(brokerkey + ":client:" + signin.email, function(err, cid) {
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
        db.hgetall(brokerkey + ":client:" + cid, function(err, client) {
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

            var timestamp = commonbo.getUTCTimeStamp(new Date());
            console.log(timestamp + " - client:" + clientid + " logged on");

            // send the data
            start(clientid, conn);
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
      var timestamp = commonbo.getUTCTimeStamp(new Date());
      console.log(timestamp + " - client:" + clientid + " logged off");

      if (connections[clientid] == conn) {
        console.log("removing");
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
  console.log("unsubscribeConnection");
  db.eval(commonfo.scriptunsubscribeid, 0, id, serverid, feedtype, function(err, ret) {
    if (err) throw err;
    console.log(ret);

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
  quote.symbolid = quoterequest.symbolid;
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
      getSendMargin(order.clientid, order.currencyid);
      getSendReserve(order.clientid, order.symbolid, order.settlcurrencyid);
    }

    if (sendcash) {
      getSendCash(order.clientid, order.settlcurrencyid);
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
      //position.symbolid = symbolid;
      //position.currencyid = currencyid;
    } else {
      position = pos;
    }

    // send to client, if connected
    if (orgclientid in connections) {
      sendPosition(position, connections[orgclientid]);
    }
  });
}

function getSendCash(clientid, currencyid) {
  var cash = {};

  db.get("client:" + clientid + ":cash:" + currencyid, function(err, amount) {
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

    cash.currencyid = currencyid;

    // send to client, if connected
    if (clientid in connections) {
      connections[clientid].write("{\"cashitem\":" + JSON.stringify(cash) + "}");
    }
  });
}

function getSendMargin(clientid, currencyid) {
  var margin = {};

  db.get(clientid + ":margin:" + currencyid, function(err, amount) {
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

    margin.currencyid = currencyid;

    if (clientid in connections) {
      sendMargin(margin, connections[clientid]);
    }
  });
}

function getSendReserve(clientid, symbolid, currencyid) {
  var reserve = {};

  db.get(clientid + ":reserve:" + symbolid + ":" + currencyid, function(err, res) {
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

    reserve.symbolid = symbolid;
    reserve.currencyid = currencyid;

    if (clientid in connections) {
      sendReserve(reserve, connections[clientid]);
    }
  });
}

function getValue(trade) {
  if (parseInt(trade.side) == 1) {
    return trade.quantity * trade.price + trade.commission + trade.costs;
  } else {
    return trade.quantity * trade.price - trade.commission - trade.costs;
  }
}

function symbolRequest(instreq, clientid, conn) {
  console.log("symbolRequest");
  console.log(instreq);

  // get alpha sorted set of instruments for this client
  db.eval(scriptgetinst, 1, "broker:" + brokerid, brokerid, clientid, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    conn.write("{\"symbols\":" + ret + "}");
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

  db.smembers("client:" + clientid + ":cash", function(err, replies) {
    if (err) {
      console.log("sendCash:" + err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      return;
    }

    replies.forEach(function (currencyid, i) {
      db.get("client:" + clientid + ":cash:" + currencyid, function(err, amount) {
        if (err) {
          console.log(err);
          return;
        }

        var cash = {};
        cash.currencyid = currencyid;
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

    replies.forEach(function (currencyid, i) {
      db.get(clientid + ":margin:" + currencyid, function(err, amount) {
        if (err) {
          console.log(err);
          return;
        }

        if (amount == null) {
          console.log("Margin not found:" + clientid + ":margin:" + currencyid);
          return;
        }

        var margin = {};
        margin.currencyid = currencyid;
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
  //sendOrderTypes(conn);
  //sendInstruments(clientid, conn);
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
  console.log("clearSubscriptions");
  // clears connections & subscriptions
  db.eval(commonfo.scriptunsubscribeserver, 0, serverid, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    // unsubscribe to the returned symbols
    for (var i = 0; i < ret.length; i++) {
      dbsub.unsubscribe("price:" + ret[i]);
    }
  });  
}

function initDb() {
  //console.log("initDb");

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

//
// request for a new symbol subscription
//
function subscribePriceRequest(clientid, symbol, conn) {
  console.log("subscribePriceRequest:" + symbol);

  db.eval(commonfo.scriptsubscribesymbol, 0, symbol.symbolid, clientid, serverid, feedtype, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // see if we need to subscribe
    if (ret[0] == 1) {
      console.log("subscribing to " + symbol.symbolid);
      dbsub.subscribe("price:" + symbol.symbolid);
      // todo - exit here as need to wait for price?
    }

    // send the current stored price
    var price = {};
    price.symbolid = symbol.symbolid;
    price.bid = ret[1];
    price.ask = ret[2];
    price.timestamp = ret[3];

    conn.write("{\"price\":" + JSON.stringify(price) + "}");
  });
}

function unsubscribePriceRequest(clientid, symbol, conn) {
  db.eval(commonfo.scriptunsubscribesymbol, 0, symbol.symbolid, clientid, serverid, feedtype, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // the script will tell us if we need to unsubscribe from the symbol
    if (ret[0] == 1) {
      console.log("unsubscribing from " + symbol.symbolid);
      dbsub.unsubscribe("price:" + ret[1]);
    }
  });
}

/*function positionHistory(clientid, currencyid, conn) {
  // todo: remove
  clientid = 5020;
  symbolid = "LOOK";

  //bo.getPositionHistory(clientid, symbolid);
}*/

//
// open quote requests for a symbol
//
function openQuoteRequestRequest(req, conn) {
  console.log("openQuoteRequestRequest");
  db.eval(commonfo.scriptgetopenquoterequests, 0, req.symbolid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"quoterequests\":" + ret + "}");
  });
}

//
// quotes made by a client for a quote request
//
function myQuotesRequest(req, clientid, conn) {
  console.log("myQuotesRequest");
  console.log(req);
  db.eval(commonfo.scriptgetmyquotes, 0, req.quotereqid, clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"myquotes\":" + ret + "}");
  });
}

function quoteRequestHistory(req, clientid, conn) {
  db.eval(commonbo.scriptgetquoterequests, 1, "broker:" + brokerid, req.accountid, brokerid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
    conn.write("{\"quoterequesthistory\":" + ret + "}");
  });
}

function quoteHistory(req, clientid, conn) {
  db.eval(commonbo.scriptgetquotes, 1, "broker:" + brokerid, req.accountid, brokerid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"quotes\":" + ret + "}");
  });
}

function orderHistory(req, clientid, conn) {
  db.eval(commonbo.scriptgetorders, 1, "broker:" + brokerid, req.accountid, brokerid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"orders\":" + ret + "}");
  });
}

function tradeHistory(req, clientid, conn) {
  db.eval(commonbo.scriptgettrades, 1, "broker:" + brokerid, req.accountid, brokerid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
    conn.write("{\"trades\":" + ret + "}");
  });
}

function positionRequest(posreq, clientid, conn) {
  console.log("positionRequest");
  console.log(posreq);

  if ('symbolid' in posreq && posreq.symbolid == "*") {
    // all positions
    db.eval(commonbo.scriptgetpositionvalues, 1, "broker:" + brokerid, posreq.accountid, brokerid, function(err, ret) {
      if (err) throw err;
      console.log(ret);
      conn.write("{\"positions\":" + ret + "}");
    });
  } else {
   // single symbol
    db.eval(commonbo.scriptgetpositionbysymbol, 0, posreq.accountid, posreq.symbolid, function(err, ret) {
      if (err) throw err;
      conn.write("{\"positions\":" + ret + "}");
    });    
  }
}

function unsubscribePositionsRequest(unsubposreq, clientid, conn) {
  console.log("unsubscribePositionsRequest");
  console.log(unsubposreq);

  // all positions
  db.eval(commonfo.scriptunsubscribepositions, 0, clientid, serverid, function(err, ret) {
    if (err) throw err;

    // unsubscribe to prices
    for (var i = 0; i < ret.length; i++) {
      dbsub.unsubscribe("price:" + ret[i]);
    }
  });
}

function accountSummaryRequest(acctreq, clientid, conn) {
  console.log("accountSummaryRequest");
  db.eval(commonbo.scriptgetaccountsummary, 1, "broker:" + brokerid, acctreq.accountid, brokerid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
    conn.write("{\"accountsummary\":" + ret + "}");
  });
}

function statementRequest(statementreq, clientid, conn) {
  console.log("statementRequest, account: " + statementreq.accountid);

  var startmilli = new Date("September 13, 2015 00:00:00").getTime();
  var endmilli = "inf";//new Date("September 19, 2015 00:00:00").getTime();

  console.log(startmilli);
  console.log(endmilli);

  db.eval(commonbo.scriptgetstatement, 1, "broker:" + brokerid, statementreq.accountid, brokerid, startmilli, endmilli, function(err, ret) {
    if (err) throw err;

    var obj = JSON.parse(ret);
    console.log(obj);
    conn.write("{\"statement\":" + ret + "}");
  });
}

function passwordRequest(clientid, pwdrequest, conn) {
  db.eval(scriptupdatepassword, 1, "broker:" + brokerid, brokerid, clientid, pwdrequest.oldpwd, pwdrequest.newpwd, function(err, ret) {
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
    quoteack.symbolid = quoterequest.symbolid;
    quoteack.quoterejectreasondesc = commonfo.getPTPQuoteRejectReason(quoterequest.quoterejectreason);
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
    quote.noseconds = commonfo.getSeconds(quote.transacttime, quote.validuntiltime);

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

  // forward to quoting client - todo: review
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

function forwardPosition(position, msg) {
  console.log("forwardPosition");
  console.log(position);

  if (position.clientid in connections) {
    connections[position.clientid].write(msg);
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

function watchlistRequest(watchlist, clientid, conn) {
  console.log("watchlistRequest");
  console.log(watchlist);

  if ("symbolid" in watchlist) {
    // add a symbol to the watchlist for this client
    db.eval(commonfo.scriptaddtowatchlist, 1, "broker:" + brokerid, watchlist.symbolid, brokerid, clientid, serverid, servertype, function(err, ret) {
      if (err) {
        console.log(err);
        return;
      }
      console.log(ret);

      conn.write("{\"watchlist\":" + ret[0] + "}");

      if (ret[1] == 1) {
        dbsub.subscribe("price:" + watchlist.symbolid);
      }
    });
  } else {
    // get the watchlist for this client
    db.eval(commonfo.scriptgetwatchlist, 1, "broker:" + brokerid, brokerid, clientid, serverid, servertype, function(err, ret) {
      if (err) {
        console.log(err);
        return;
      }
      console.log(ret);

      conn.write("{\"watchlist\":" + ret[0] + "}");
      console.log("{\"watchlist\":" + ret[0] + "}");

      // subscribe to symbols
      for (var i = 0; i < ret[1].length; i++) {
        dbsub.subscribe("price:" + ret[1][i]);
      }
   });
  }
}

function unwatchlistrequest(uwl, clientid, conn) {
  console.log("unwatchlistrequest");

  if ("symbolid" in uwl) {
    // unsubscribe from the watchlist for this client
    db.eval(commonfo.scriptremovewatchlist, 0, uwl.symbolid, clientid, serverid, servertype, function(err, ret) {
      if (err) {
        console.log(err);
        return;
      }

      // unsubscribe from symbol
      if (ret == 1) {
        dbsub.unsubscribe("price:" + uwl.symbolid);
      }

      conn.write("{\"unwatchlist\":[\"" + uwl.symbolid + "\"]}");
    });
  } else {
    // unsubscribe from the watchlist for this client
    db.eval(commonfo.scriptunwatchlist, 0, clientid, serverid, servertype, function(err, ret) {
      if (err) {
        console.log(err);
        return;
      }

      // unsubscribe from symbols
      for (var i = 0; i < ret.length; i++) {
        dbsub.unsubscribe("price:" + ret[i]);
      }
    });
  }
}

function quoteRequest(rfq, clientid, conn) {
  console.log("quoteRequest");
  console.log(rfq);

  // add clientid & operator
  rfq.brokerid = brokerid;
  rfq.clientid = clientid;
  rfq.operatorid = clientid;
  rfq.operatortype = operatortype;

  // send to tradeserver
  db.publish(commonbo.tradeserverchannel, "{\"quoterequest\":" + JSON.stringify(rfq) + "}");
}

function newOrder(order, clientid, conn) {
  console.log("newOrder");
  console.log(order);

  order.brokerid = brokerid;
  order.operatorid = clientid;
  order.operatortype = operatortype;

  if ("quoteid" in order) {
    db.hgetall("broker:" + brokerid + ":quote:" + order.quoteid, function(err, quote) {
      if (err) {
        console.log("Error getting quote:" + err);
        return;
      }

      if (quote == null) {
        console.log("Quote not found");
        return;
      }

      order.clientid = quote.clientid;
      order.accountid = quote.accountid;
      order.symbolid = quote.symbolid;

      if (quote.bidpx != "") {
        order.side = 2;
        order.price = quote.bidpx;
        order.quantity = quote.bidquantity;
      } else {
        order.side = 1;
        order.price = quote.offerpx;
        order.quantity = quote.offerquantity;
      }

      order.ordertype = "D";
      order.markettype = 0;
      order.currencyid = quote.currencyid;
      order.timeinforce = 4;
      order.expiredate = "";
      order.expiretime = "";
      order.settlcurrencyid = quote.settlcurrencyid;

      // send to tradeserver
      db.publish(commonbo.tradeserverchannel, "{\"order\":" + JSON.stringify(order) + "}");
    });
  } else {
      order.clientid = clientid;

      // send to tradeserver
      db.publish(commonbo.tradeserverchannel, "{\"order\":" + JSON.stringify(order) + "}");
  }
}

function registerScripts() {
  //
  // get alpha sorted list of instruments for a specified client
  // uses set of valid instrument types per client i.e. broker:client:1:instrumenttypes CFD
  //
  scriptgetinst = '\
  local symbols = redis.call("sort", "symbols", "ALPHA") \
  local fields = {"instrumenttypeid", "shortname", "currencyid", "marginpercent"} \
  local vals \
  local inst = {} \
  for index = 1, #symbols do \
    vals = redis.call("hmget", "symbol:" .. symbols[index], unpack(fields)) \
    if redis.call("sismember", "broker:" .. ARGV[1] .. ":client:" .. ARGV[2] .. ":instrumenttypes", vals[1]) == 1 then \
      table.insert(inst, {symbolid = symbols[index], instrumenttypeid = vals[1], shortname = vals[2], currencyid = vals[3], marginpercent = vals[4]}) \
    end \
  end \
  return cjson.encode(inst) \
  ';

  scriptupdatepassword = '\
    local retval = 0 \
    local clientkey = "broker:" .. ARGV[1] .. ":client:" .. ARGV[2] \
    local oldpwd = redis.call("hget", clientkey, "password") \
    if oldpwd == ARGV[3] then \
      redis.call("hset", clientkey, "password", ARGV[4]) \
      retval = 1 \
    end \
    return retval \
  ';
}
