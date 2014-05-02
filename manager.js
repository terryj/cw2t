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

// internal libraries
var common = require('./common.js');

// globals
var connections = {}; // added to if & when a client logs on
var static_directory = new node_static.Server(__dirname); // static files server
var cw2tport = 8080; // user listen port
var ordertypes = {};
var brokerid = "1"; // todo: via logon
var defaultnosettdays = 3;
var operatortype = 2;
var tradeserverchannel = 3;
var userserverchannel = 2;
var clientserverchannel = 1;
var servertype = "user";

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
var scriptupdateclient;
var scriptgetinst;
var scriptgetclienttypes;
var scriptinstupdate;
var scripthedgeupdate;
var scriptgethedgebooks;
var scriptcost;
var scriptgetcosts;
var scriptifa;
//var scriptgetquoterequests;
//var scriptgetquotes;
//var scriptgetorders;
//var scriptgettrades;
//var scriptgetpositions;
var scriptgetconnections;
var scriptnewchat;
var scriptgetchat;
var scriptgetpendingchat;
//var scriptgetcash;
var scriptgetreserves;
var scriptgetmargin;
//var scriptgetcashhistory;
//var scriptgetaccount;
var scriptendofday;
var scriptgetalltrades;
var scriptgetmarkets;
//var scriptgetholidays;

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
  common.registerCommonScripts();
  registerScripts();
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
    //console.log("channel:" + channel + ", " + message);

    if (message.substr(0, 8) == "quoteack") {
      sendQuoteack(message.substr(9));
    } else if (message.substr(0, 5) == "quote") {
      sendQuote(message.substr(6));
    } else if (message.substr(0, 17) == "ordercancelreject") {
      orderCancelReject(message.substr(18));
    } else if (message.substr(0, 15) == "orderbookupdate") {
      common.broadcastLevelOne(message.substr(16), connections);
    } else if (message.substr(0, 5) == "order") {
      getSendOrder(message.substr(6));
    } else if (message.substr(0, 5) == "trade") {
      getSendTrade(message.substr(6));
    } else if (message.substr(2, 4) == "chat") {
      newChatClient(message);
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
      console.log('recd:' + msg);

      // may be able to just forward to trade server
      if (msg.substr(2, 13) == "quoterequest\"") {
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 18) == "ordercancelrequest") {
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 16) == "orderfillrequest") {
        db.publish(tradeserverchannel, msg);
      } else if (msg.substr(2, 6) == "order\"") {
        db.publish(tradeserverchannel, msg);
      //} else if (msg.substr(0, 4) == "ping") {
        //conn.write("pong");
      } else {
        // need to parse
        try {
          var obj = JSON.parse(msg);

          if ("orderbookrequest" in obj) {
            orderBookRequest(userid, obj.orderbookrequest, conn);
          } else if ("orderbookremoverequest" in obj) {
            orderBookRemoveRequest(userid, obj.orderbookremoverequest, conn);
          } else if ("orderhistoryrequest" in obj) {
            orderHistory(obj.orderhistoryrequest, conn);
          } else if ("quoterequesthistoryrequest" in obj) {
            quoteRequestHistory(obj.quoterequesthistoryrequest, conn);
          } else if ("register" in obj) {
            registerClient(obj.register, conn);
          } else if ("signin" in obj) {
            signIn(obj.signin);
          } else if ("positionrequest" in obj) {
            positionRequest(obj.positionrequest, conn);
          } else if ("cashrequest" in obj) {
            cashRequest(obj.cashrequest, conn);
          } else if ("accountrequest" in obj) {
            accountRequest(obj.accountrequest, conn);
          } else if ("index" in obj) {
            sendIndex(obj.index, conn);        
          } else if ("newclient" in obj) {
            newClient(obj.newclient, conn);
          } else if ("cashtrans" in obj) {
            cashTrans(obj.cashtrans, userid, conn);
          } else if ("instupdate" in obj) {
            instUpdate(obj.instupdate, userid, conn);
          } else if ("hedgebookupdate" in obj) {
            hedgebookUpdate(obj.hedgebookupdate, userid, conn);
          } else if ("cost" in obj) {
            costUpdate(obj.cost, conn);
          } else if ("ifa" in obj) {
            newIfa(obj.ifa, conn);
          } else if ("tradehistoryrequest" in obj) {
            tradeHistory(obj.tradehistoryrequest, conn);
          } else if ("quotehistoryrequest" in obj) {
            quoteHistory(obj.quotehistoryrequest, conn);
          } else if ("cashhistoryrequest" in  obj) {
            cashHistory(obj.cashhistoryrequest, conn);
          } else if ("chathistoryrequest" in obj) {
            chatHistory(obj.chathistoryrequest, conn);
          } else if ("connectionrequest" in obj) {
            sendConnections(obj.connectionrequest, conn);
          } else if ("chat" in obj) {
            newChat(obj.chat, userid);
          } else if ("marginrequest" in obj) {
            marginRequest(obj.marginrequest, conn);
          } else if ("reservesrequest" in obj) {
            reservesRequest(obj.reservesrequest, conn);
          } else if ("pendingchatrequest" in obj) {
            pendingChatRequest(obj.pendingchatrequest, conn);
          } else if ("holidayrequest" in obj) {
            sendHolidays(obj.holidayrequest, conn);
          } else if ("endofday" in obj) {
            endOfDay(userid);
          } else {
            console.log("unknown msg received:" + msg);
          }
        } catch(e) {
          console.log(e);
          return;
        }
      }
      // todo: no orgclientkey
    });

    // close connection callback
    conn.on('close', function() {
      tidy(userid, conn);

      // todo: check for existence
      userid = "0";
    });

    // user sign in
    function signIn(signin) {
      var reply = {};
      reply.success = false;

      // todo: replace with script

      db.get(servertype + ":" + signin.email, function(err, emailuserid) {
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
        db.hgetall(servertype + ":" + emailuserid, function(err, user) {
          if (err) {
            console.log("Error in signIn:" + err);
            return;
          }

          if (!user || signin.email != user.email || signin.password != user.password) {
            reply.reason = "Email or password incorrect. Please try again.";
            replySignIn(reply, conn);
            return;
          }

          // check to see if this user is already logged on
          db.sismember("connections:" + servertype, user.userid, function(err, found) {
            if (err) {
              console.log(err);
              return;
            }

            if (found) {
              console.log("user " + user.userid + " already logged on");

              if (user.userid in connections) {
                connections[user.userid].close();
              }
            }

            // validated, so set user id
            userid = user.userid;
            connections[userid] = conn;

            // keep a record
            db.sadd("connections:" + servertype, userid);

            // send a successful logon reply
            reply.success = true;
            reply.email = signin.email;
            replySignIn(reply, conn);

            console.log("user:" + userid + " logged on");

            // send the data
            start(userid, user.brokerid, conn);
          });
        });
      });
    }
  });
}

function tidy(userid, conn) {
  if (userid != "0") {
    if (userid in connections) {
      console.log("user:" + userid + " logged off");

      // make sure this connection is still live, as may have been kicked off
      if (connections[userid] == conn) {
        // remove from list
        delete connections[userid];

        // remove from database
        db.srem("connections:" + servertype, userid);

        unsubscribeConnection(userid);
      }
    }
  }
}

function unsubscribeConnection(id) {
  db.eval(common.scriptunsubscribeid, 2, id, servertype, function(err, ret) {
    if (err) throw err;

    // unsubscribe returned topics
    for (var i = 0; i < ret.length; i++) {
      dbsub.unsubscribe(ret[i]);
    }
  });
}

function newPrice(topic, msg) {
  // which symbols are subscribed to for this topic (may be more than 1 as covers derivatives)
  db.smembers("topic:" + topic + ":" + servertype + ":symbols", function(err, symbols) {
    if (err) throw err;

    symbols.forEach(function(symbol, i) {
      // build the message according to the symbol
      var jsonmsg = "{\"orderbook\":{\"symbol\":\"" + symbol + "\"," + msg + "}}";

      // get the users watching this symbol
      db.smembers("topic:" + topic + ":symbol:" + symbol + ":" + servertype, function(err, users) {
        if (err) throw err;

        // send the message to each user
        users.forEach(function(user, j) {
          if (user in connections) {
            connections[user].write(jsonmsg);
          }
        });
      });
    });
  });
}

function orderBookRequest(userid, symbol, conn) {
  db.eval(common.scriptsubscribeinstrument, 3, symbol, userid, servertype, function(err, ret) {
    if (err) throw err;

    // the script tells us if we need to subscribe to a topic
    if (ret[0]) {
      dbsub.subscribe(ret[1]);
    }

    // send the orderbook, with the current stored prices
    sendCurrentOrderBook(symbol, ret[1], conn);
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
  db.eval(common.scriptunsubscribeinstrument, 3, symbol, userid, servertype, function(err, ret) {
    if (err) throw err;

    // the script will tell us if we need to unsubscribe from the topic
    if (ret[0]) {
      dbsub.unsubscribe(ret[1]);
    }
  });
}

function newClient(client, conn) {
  // maybe a new client or an updated client
  if (client.clientid == "") {
    db.eval(scriptnewclient, 11, client.brokerid, client.name, client.email, client.mobile, client.address, client.ifaid, client.type, client.insttypes, client.hedge, client.brokerclientcode, client.commissionpercent, function(err, ret) {
      if (err) throw err;

      if (ret[0] != 0) {
        console.log("Error in scriptnewclient:" + common.getReasonDesc(ret[0]));
        return;
      }

      getSendClient(ret[1], conn);
    });
  } else {
    db.eval(scriptupdateclient, 12, client.clientid, client.brokerid, client.name, client.email, client.mobile, client.address, client.ifaid, client.type, client.insttypes, client.hedge, client.brokerclientcode, client.commissionpercent, function(err, ret) {
      if (err) throw err;

      if (ret != 0) {
        console.log("Error in scriptupdateclient:" + common.getReasonDesc(ret));
        return;
      }

      getSendClient(client.clientid, conn);
    });
  }
}

function newIfa(ifa, conn) {
  db.eval(scriptifa, 5, ifa.ifaid, ifa.name, ifa.email, ifa.address, ifa.mobile, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      console.log("Error in scriptifa:" + common.getReasonDesc(ret[0]));
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
  cashtrans.timestamp = common.getUTCTimeStamp(new Date());

  db.eval(common.scriptcashtrans, 11, cashtrans.clientid, cashtrans.currency, cashtrans.transtype, cashtrans.amount, cashtrans.drcr, cashtrans.description, cashtrans.reference, cashtrans.timestamp, cashtrans.settldate, operatortype, userid, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      console.log("Error in scriptcashtrans:" + common.getReasonDesc(ret[0]));
      return;
    }

    getSendCashtrans(ret[1], conn);
  });
}

function instUpdate(inst, userid, conn) {
  db.eval(scriptinstupdate, 4, inst.symbol, inst.marginpercent, inst.hedge, inst.ptmexempt, function(err, ret) {
    if (err) throw err;

    getSendInst(inst.symbol, conn);
  });
}

function hedgebookUpdate(hedgebook, userid, conn) {
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
  db.eval(scriptcost, 12, cost.insttype, cost.currency, cost.side, cost.commissionpercent, cost.commissionmin, cost.ptmlevylimit, cost.ptmlevy, cost.stampdutylimit, cost.stampdutypercent, cost.contractcharge, cost.finance, cost.defaultnosettdays, function(err, ret) {
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

    // send to user, if connected
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

    db.hgetall("order:" + trade.orderid, function(err, order) {
      if (err) {
        console.log(err);
        return;
      }

      trade.orderdivnum = order.orderdivnum;

      if (order.operatorid in connections) {
        sendTrade(trade, connections[order.operatorid]);
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

function orderCancelReject(ocrid) {
  // get the order cancel request
  db.hgetall("ordercancelrequest:" + ocrid, function(err, ordercancelrequest) {
    if (err) {
      console.log(err); // can't get order cancel request, so just log
      return;
    }

    if (ordercancelrequest == null) {
      console.log("Order cancel request not found, id:" + ocrid);
      return;
    }

    ordercancelrequest.reasondesc = common.getPTPOrderCancelRejectReason(ordercancelrequest.reason);
    if ('text' in ordercancelrequest && ordercancelrequest.text != "") {
      ordercancelrequest.reasondesc += ", " + ordercancelrequest.text;
    }

    if (ordercancelrequest.operatorid in connections) {
      connections[ordercancelrequest.operatorid].write("{\"ordercancelreject\":" + JSON.stringify(ordercancelrequest) + "}");
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
          //orderBookOut(orgclientkey, symbol, conn);
          //orderBookRequest...
        //}
      });
    });
  });
}

function quoteRequestHistory(req, conn) {
  db.eval(common.scriptgetquoterequests, 1, req.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"quoterequests\":" + ret + "}");
  });
}

function quoteHistory(req, conn) {
  db.eval(common.scriptgetquotes, 1, req.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"quotes\":" + ret + "}");
  });
}

function sendOrder(order, conn) {
  if (conn != null) {
    conn.write("{\"order\":" + JSON.stringify(order) + "}");
  }
}

function orderHistory(req, conn) {
  db.eval(common.scriptgetorders, 1, req.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"orders\":" + ret + "}");
  });
}

/*function sendOrders(req, conn) {
  var o = {orders: []};
  var count;

  db.smembers(req.clientid + ":orders", function(err, replies) {
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
        o.orders.push(order);

        // send array if we have added the last item
        count--;
        if (count <= 0) {
          conn.write(JSON.stringify(o));
        }
      });
    });
  });
}*/

function sendTrade(trade, conn) {
  if (conn != null) {
    conn.write("{\"trade\":" + JSON.stringify(trade) + "}");
  }
}

function tradeHistory(req, conn) {
  if ("positionkey" in req) {
    db.eval(common.scriptgetpostrades, 2, req.clientid, req.positionkey, function(err, ret) {
      if (err) throw err;
      conn.write("{\"trades\":" + ret + "}");
    });
  } else if (req.clientid == "0") {
    db.eval(scriptgetalltrades, 0, function(err, ret) {
      if (err) throw err;
      conn.write("{\"trades\":" + ret + "}");
    });
  } else {
    db.eval(common.scriptgettrades, 1, req.clientid, function(err, ret) {
      if (err) throw err;
      conn.write("{\"trades\":" + ret + "}");
    });
  }
}

function cashHistory(req, conn) {
  db.eval(common.scriptgetcashhistory, 2, req.clientid, req.currency, function(err, ret) {
    if (err) throw err;
    conn.write("{\"cashhistory\":" + ret + "}");
  });  
}

function chatHistory(req, conn) {
  db.eval(scriptgetchat, 1, req.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"chathistory\":" + ret + "}");
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
        if (count <= 0) {
          conn.write(JSON.stringify(posarray));
        }
      });
    });
  });
}

function positionRequest(posreq, conn) {
  db.eval(common.scriptgetpositions, 1, posreq.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"positions\":" + ret + "}");
  });
}

function cashRequest(cashreq, conn) {
  db.eval(common.scriptgetcash, 1, cashreq.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"cash\":" + ret + "}");
  });  
}

function accountRequest(acctreq, conn) {
  db.eval(common.scriptgetaccount, 1, acctreq.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"account\":" + ret + "}");
  });
}

function marginRequest(marginreq, conn) {
  db.eval(scriptgetmargin, 1, marginreq.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"margins\":" + ret + "}");
  });  
}

function reservesRequest(reservesreq, conn) {
  db.eval(scriptgetreserves, 1, reservesreq.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"reserves\":" + ret + "}");
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
// send all the orderbooks for a single user
//
function sendOrderBooks(userid, conn) {
  // get all the instruments in the order book for this user
  // todo: script?
  db.smembers("user:" + userid + ":orderbooks", function(err, instruments) {
    if (err) {
      console.log("Error in sendOrderBooks:" + err);
      return;
    }

    // send the order book for each instrument
    instruments.forEach(function (symbol, i) {
      orderBookRequest(userid, symbol, conn);
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
  sendMarkets(conn);

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
  clearConnections();
  clearPendingChat();
}

function clearConnections() {
  // clear any connected users
  db.smembers("connections:" + servertype, function(err, connections) {
    if (err) {
      console.log(err);
      return;
    }

    connections.forEach(function(connection, i) {
      unsubscribeConnection(connection);

      db.srem("connections:" + servertype, connection);
    });
  });
}

function clearPendingChat() {
  db.smembers("pendingchatclients", function(err, pendingchat) {
    if (err) {
      console.log(err);
      return;
    }

    pendingchat.forEach(function(clientid, i) {
      db.srem("pendingchatclients", clientid);
    });
  });
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

function sendMarkets(conn) {
  db.eval(scriptgetmarkets, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"markets\":" + ret + "}");
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
    quoteack.quoterejectreasondesc = common.getPTPQuoteRejectReason(quoterequest.quoterejectreason);
    if ('text' in quoterequest) {
      quoteack.text = quoterequest.text;
    }
    quoteack.orderdivnum = quoterequest.orderdivnum;

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

    // send quote to the user who placed the quote request
    db.hgetall("quoterequest:" + quote.quotereqid, function(err, quoterequest) {
      if (err) {
        console.log(err);
        return;
      }

      // add the orderdivnum so the front-end knows which orderdiv
      quote.orderdivnum = quoterequest.orderdivnum;

      if (quoterequest.operatorid in connections) {
        connections[quoterequest.operatorid].write("{\"quote\":" + JSON.stringify(quote) + "}");
      }
    });
  });
}

function sendConnections(connectionreq, conn) {
  db.eval(scriptgetconnections, 0, function(err, ret) {
    if (err) throw err;
    conn.write("{\"connections\":" + ret + "}");
  });      
}

//
// chat from a user
//
function newChat(chat, userid) {
  chat.timestamp = common.getUTCTimeStamp(new Date());

  db.eval(scriptnewchat, 5, chat.clientid, chat.text, chat.timestamp, chat.chatid, userid, function(err, ret) {
    if (err) throw err;

    // add chat id, so we can identify any chat coming back
    chat.chatid = ret[0];

    // send it to the client server to forward to client
    db.publish(clientserverchannel, "{\"chat\":" + JSON.stringify(chat) + "}");
  });      
}

//
// chat from a client
//
function newChatClient(msg) {
  var userid = "0";

  try {    
    var chatobj = JSON.parse(msg);

    chatobj.chat.timestamp = common.getUTCTimeStamp(new Date());

    db.eval(scriptnewchat, 5, chatobj.chat.clientid, chatobj.chat.text, chatobj.chat.timestamp, chatobj.chat.chatid, userid, function(err, ret) {
      if (err) throw err;

      // update chat id as may be new
      chatobj.chat.chatid = ret[0];
      userid = ret[1];
      msg = JSON.stringify(chatobj);

      if (userid == 0) {
        // no specified user, so send to all users
        for (var x in connections) {
          connections[x].write(msg);
        }
      } else if (userid in connections) {
        connections[userid].write(msg);
      }
    });
  } catch (e) {
    console.log(e);
    return;
  }
}

function pendingChatRequest(req, conn) {
  var pendingchat;

  db.eval(scriptgetpendingchat, 1, req.clientid, function(err, ret) {
    if (err) throw err;

    // check for no pending chat
    if (ret[0] == 0) {
      // todo: send a message?
      return;
    }

    sendChat(ret[1], conn);
  });
}

function sendHolidays(req, conn) {
  console.log(req);

  db.eval(common.scriptgetholidays, 1, req.market, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    conn.write("{\"holidays\":" + ret + "}");
  });  
}

function sendChat(chatid, conn) {
  db.hgetall("chat:" + chatid, function(err, chat) {
    if (err) {
      console.log(err);
      return;
    }

    if (chat == null) {
      console.log("Chat not found, id:" + chatid);
      return;
    }

    conn.write("{\"chat\":" + JSON.stringify(chat) + "}");
  });
}

function endOfDay(userid) {
  var eoddate;
  var nexteoddate;

  db.get("eoddate", function(err, eoddatestr) {
    if (err) throw err;
    console.log(eoddatestr);

    eoddate = common.dateFromUTCString(eoddatestr);
    console.log(eoddate);

    //nexteoddate = getUTCDateString(common.getSettDate(eoddate, 1));

    db.smembers("clients", function(err, clients) {
      if (err) throw err;

      clients.forEach(function(clientid, i) {
        db.eval(scriptendofday, 1, clientid, eoddate, userid, function(err, ret) {
          if (err) throw err;
        });
      });
    });
  });
}

function registerScripts() {
  var stringsplit;
  var gettotalpositions = common.gettotalpositions;
  var getcash = common.getcash;
  var getunrealisedpandl = common.getunrealisedpandl;
  var calcfinance = common.calcfinance;
  var gettrades = common.gettrades;

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
  // get alpha sorted list of clients for a specified broker
  //
  scriptgetclients = '\
  local clients = redis.call("sort", "clients", "ALPHA") \
  local fields = {"brokerid", "clientid", "email", "name", "address", "mobile", "ifaid", "type", "hedge", "brokerclientcode", "commissionpercent"} \
  local vals \
  local tblclient = {} \
  local tblinsttype = {} \
  for index = 1, #clients do \
    vals = redis.call("hmget", "client:" .. clients[index], unpack(fields)) \
    if KEYS[1] == vals[1] then \
      tblinsttype = redis.call("smembers", vals[2] .. ":instrumenttypes") \
      table.insert(tblclient, {brokerid = vals[1], clientid = vals[2], email = vals[3], name = vals[4], address = vals[5], mobile = vals[6], ifaid = vals[7], insttypes = tblinsttype, type = vals[8], hedge = vals[9], brokerclientcode = vals[10], commissionpercent = vals[11]}) \
    end \
  end \
  return cjson.encode(tblclient) \
  ';

  scriptnewclient = stringsplit + '\
  local clientid = redis.call("incr", "clientid") \
  if not clientid then return {1005} end \
  --[[ store the client ]] \
  redis.call("hmset", "client:" .. clientid, "clientid", clientid, "brokerid", KEYS[1], "name", KEYS[2], "email", KEYS[3], "password", KEYS[3], "mobile", KEYS[4], "address", KEYS[5], "ifaid", KEYS[6], "type", KEYS[7], "hedge", KEYS[9], "brokerclientcode", KEYS[10], "marketext", "D", "commissionpercent", KEYS[11]) \
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
  redis.call("hmset", "client:" .. KEYS[1], "clientid", KEYS[1], "brokerid", KEYS[2], "name", KEYS[3], "email", KEYS[4], "mobile", KEYS[5], "address", KEYS[6], "ifaid", KEYS[7], "type", KEYS[8], "hedge", KEYS[10], "brokerclientcode", KEYS[11], "commissionpercent", KEYS[12]) \
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
  local fields = {"instrumenttype", "description", "currency", "marginpercent", "market", "isin", "sedol", "sector", "hedge", "ptmexempt"} \
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
      table.insert(inst, {symbol = instruments[index], description = vals[2], currency = vals[3], instrumenttype = vals[1], marginpercent = marginpc, market = vals[5], isin = vals[6], sedol = vals[7], sector = vals[8], hedge = vals[9], ptmexempt=vals[10]}) \
    end \
  end \
  return cjson.encode(inst) \
  ';

  scriptinstupdate = '\
  redis.call("hmset", "symbol:" .. KEYS[1], "marginpercent", KEYS[2], "hedge", KEYS[3], "ptmexempt", KEYS[4]) \
  ';

  scripthedgeupdate = '\
  local hedgebookkey = KEYS[1] .. ":" .. KEYS[2] \
  redis.call("set", "hedgebook:" .. hedgebookkey, KEYS[3]) \
  redis.call("sadd", "hedgebooks", hedgebookkey) \
  ';

  scriptcost = '\
  local costkey = KEYS[1] .. ":" .. KEYS[2] .. ":" .. KEYS[3] \
  redis.call("hmset", "cost:" .. costkey, "costkey", costkey, "commissionpercent", KEYS[4], "commissionmin", KEYS[5], "ptmlevylimit", KEYS[6], "ptmlevy", KEYS[7], "stampdutylimit", KEYS[8], "stampdutypercent", KEYS[9], "contractcharge", KEYS[10], "finance", KEYS[11], "defaultnosettdays", KEYS[12]) \
  redis.call("sadd", "costs", costkey) \
  ';

  // todo: use 'hgetall' & ipair to get all vals
  scriptgetcosts = '\
  local costs = redis.call("smembers", "costs") \
  local fields = {"commissionpercent", "commissionmin", "ptmlevylimit", "ptmlevy", "stampdutylimit", "stampdutypercent", "contractcharge", "finance", "defaultnosettdays"} \
  local vals \
  local cost = {} \
  for index = 1, #costs do \
    vals = redis.call("hmget", "cost:" .. costs[index], unpack(fields)) \
    table.insert(cost, {costkey = costs[index], commissionpercent = vals[1], commissionmin = vals[2], ptmlevylimit = vals[3], ptmlevy = vals[4], stampdutylimit = vals[5], stampdutypercent = vals[6], contractcharge = vals[7], finance = vals[8], defaultnosettdays = vals[9]}) \
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

  //
  // all trades - todo: limit by date...add date index?
  //
  scriptgetalltrades = gettrades + '\
  local trades = redis.call("smembers", "trades") \
  local tblresults = gettrades(trades) \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id
  //
  scriptgetmargin = '\
  local tblresults = {} \
  local margins = redis.call("smembers", KEYS[1] .. ":margins") \
  for index = 1, #margins do \
    local margin = redis.call("get", KEYS[1] .. ":margin:" .. margins[index]) \
    table.insert(tblresults, {currency=margins[index],amount=margin}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id
  //
  scriptgetreserves = '\
  local tblresults = {} \
  local fields = {"symbol", "quantity", "currency", "settldate"} \
  local vals \
  local reserves = redis.call("smembers", KEYS[1] .. ":reserves") \
  for index = 1, #reserves do \
    vals = redis.call("hmget", KEYS[1] .. ":reserve:" .. reserves[index], unpack(fields)) \
    if vals[1] then \
      table.insert(tblresults, {symbol=vals[1], quantity=vals[2], currency=vals[3], settldate=vals[4]}) \
    end \
  end \
  return cjson.encode(tblresults) \
  ';

  scriptgetconnections = '\
  local tblresults = {} \
  local connections = redis.call("smembers", "connections:client") \
  local fields = {"clientid","name"} \
  local vals \
  for index = 1, #connections do \
    vals = redis.call("hmget", "client:" .. connections[index], unpack(fields)) \
    --[[ see if there is pending chat for this client ]] \
    local pendingchat = 0 \
    if redis.call("sismember", "pendingchatclients", connections[index]) == 1 then \
      pendingchat = 1 \
    end \
    table.insert(tblresults, {clientid=vals[1],name=vals[2], pendingchat=pendingchat}) \
  end \
  return cjson.encode(tblresults) \
  ';

  scriptnewchat = '\
  local chatid \
  local userid \
  --[[ see if this chat already exists ]] \
  if redis.call("sismember", KEYS[1] .. ":chat", KEYS[4]) == 1 then \
    chatid = KEYS[4] \
    --[[ add to the chat ]] \
    local key = "chat:" .. chatid \
    local chattext = redis.call("hget", key, "text") \
    chattext = chattext .. string.char(10) .. KEYS[2] \
    redis.call("hset", key, "text", chattext) \
    if KEYS[5] ~= "0" then \
      redis.call("hset", key, "userid", KEYS[5]) \
    end \
    userid = redis.call("hget", key, "userid") \
  else \
    chatid = redis.call("incr", "chatid") \
    --[[ store the chat ]] \
    redis.call("hmset", "chat:" .. chatid, "chatid", chatid, "clientid", KEYS[1], "text", KEYS[2], "timestamp", KEYS[3], "userid", KEYS[5]) \
    --[[ add to set of chat for this client ]] \
    redis.call("sadd", KEYS[1] .. ":chat", chatid) \
    --[[ keep track of pending chat ]] \
    if KEYS[5] == "0" then \
      --[[ add to pending ]] \
      redis.call("sadd", "pendingchatclients", KEYS[1]) \
      --[[ create a way to get from clientid to chatid ]] \
      redis.call("set", "client:" .. KEYS[1] .. "chat", chatid) \
    end \
    userid = KEYS[5] \
  end \
  return {chatid, userid} \
  ';

  scriptgetchat = '\
  local tblresults = {} \
  local chathistory = redis.call("smembers", KEYS[1] .. ":chat") \
  local fields = {"clientid","chatid","text","timestamp","userid"} \
  local vals \
  for index = 1, #chathistory do \
    vals = redis.call("hmget", "chat:" .. chathistory[index], unpack(fields)) \
    local username = redis.call("hget", "user:" .. vals[5], "name") \
    table.insert(tblresults, {clientid=vals[1],chatid=vals[2],text=vals[3],timestamp=vals[4],user=username}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // returns 0 as no pending chat, 1 & chat id if there is pending chat & clears as pending
  //
  scriptgetpendingchat = '\
  if redis.call("sismember", "pendingchatclients", KEYS[1]) ~= 1 then \
    return {0} \
  end \
  --[[ clear item as pending ]] \
  redis.call("srem", "pendingchatclients", KEYS[1]) \
  --[[ get chat id for this client ]] \
  local chatid = redis.call("get", "client:" .. KEYS[1] .. "chat") \
  return {1, chatid} \
  ';

  //
  // pass client id, end of day date, user id
  //
  scriptendofday = '\
    local clientid = KEYS[1] \
    local positions = redis.call("smembers", clientid .. ":positions") \
    local fields = {"symbol","side","quantity","cost","currency"} \
    local vals \
    for index = 1, #positions do \
      local vals = redis.call("hmget", clientid .. ":position:" .. positions[index], unpack(fields)) \
      if vals[1] then \
        local instrumenttype = redis.call("hget", "symbol:" .. vals[1], "instrumenttype") \
        local finance = calcfinance(instrumenttype, vals[4], vals[5], vals[2], 1) \
        if finance ~= 0 then \
          local drcr \
          local desc \
          if tonumber(vals[2]) == 1 then \
            drcr = 1 \
            desc = "finance charge" \
          else \
            drcr = 2 \
            desc = "finance" \
          end \
          updatecash(clientid, vals[5], "FI", finance, drcr, desc, positions[index], KEYS[2], KEYS[2], 2, KEYS[3]) \
        end \
      end \
    end \
  end \
  ';

  scriptgetmarkets = '\
  local tblresults = {} \
  local markets = redis.call("smembers", "markets") \
  for index = 1, #markets do \
    local market = redis.call("get", "market:" .. markets[index]) \
    table.insert(tblresults, {market=markets[index],name=market}) \
  end \
  return cjson.encode(tblresults) \
  ';
}
