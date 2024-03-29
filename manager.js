/****************
* manager.js
* Front-office server for users
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
var commonfo = require('./commonfo.js');
var commonbo = require('./commonbo.js');
var common = require('./common.js');
var sql = require('./sql.js');

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

// globals
var connections = {}; // added to if & when a client logs on
var static_directory = new node_static.Server(__dirname); // static files server
var cw2tport = 8081; // user listen port
var ordertypes = {};
var brokerid = "1"; // todo: via logon
var defaultnosettdays = 3;
var operatortype = 2; // user
var serverid = 11; // must be unique, use .ini file?
var servertype = "user";
var serverstatus = {};
var feedtype = "nbtrader";
var holidays = {};

// redis scripts
var scriptgetclients;
//var scriptnewclient;
var scriptgetbrokers;
var scriptgetifas;
var scriptgetinstrumenttypes;
var scriptgetcashtranstypes;
var scriptgetcurrencies;
//var scriptupdateclient;
var scriptgetinst;
//var scriptgetclienttypes;
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
  console.log("initialise");

  commonfo.registerScripts();
  commonbo.registerScripts();
  common.registerScripts();
  sql.registerScripts();
  registerScripts();
  initDb();
  //clearSubscriptions();
  pubsub();
  listen();
}

// pubsub connection
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

    if (channel.substr(0, 6) == "price:") {
      commonfo.newPrice(channel.substr(6), serverid, message, connections, feedtype);
    } else {
      try {
        var obj = JSON.parse(message);

        if ("quote" in obj) {
          forwardQuote(obj.quote, message);
          placeOrder(obj.quote);
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

      /*if (message.substr(1, 6) == "prices") {
        commonfo.newPrice(channel, servertype, message, connections, feedtype);
      } else if (message.substr(0, 5) == "price") {
        commonfo.newPrice(channel.substr(6), serverid, message, connections, feedtype);
      } else if (message.substr(0, 8) == "quoteack") {
        sendQuoteack(message.substr(9));
      } else if (message.substr(0, 5) == "quote") {
        sendQuote(message.substr(6));
      } else if (message.substr(0, 17) == "ordercancelreject") {
        orderCancelReject(message.substr(18));
      } else if (message.substr(0, 5) == "order") {
        getSendOrder(message.substr(6));
      } else if (message.substr(0, 5) == "trade") {
        getSendTrade(message.substr(6));
      } else if (message.substr(2, 6) == "status") {
        updateStatus(message.substr(0, 2), message.substr(9));
        sendStatus();
      } else {
        console.log("unknown message, channel=" + channel + ", message=" + message);
      }*/
    }
  });

  // listen for user related messages
  dbsub.subscribe(commonbo.userserverchannel);

  // listen for trading messages
  dbsub.subscribe(commonbo.tradechannel);
}

// sockjs server
//var sockjs_opts = {sockjs_url: "http://cdn.sockjs.org/sockjs-0.3.min.js"};
var sockjs_opts = {sockjs_url: "http://cdn.jsdelivr.net/sockjs/0.3.4/sockjs.min.js"};
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

    //applyCorporateAction(1, 1);
    //test();
    //testtrade();
    //testSettle();
    //testSupplierFundsTransfer();
    //testPositionPostings();
    //testStatement();
    //testOrders();
    //testOrder(1);
    //getOrderbookTopAll();
    testValuation();
    //testcollectagginvest();
    //testSql();
    //testQuoteRequest();
    //testQuoteRequests();
    //testWatchlist();
    //testSubscribeSymbol();
    //testUnsubscribeSymbol();
    //testUnwatchlist();

  sockjs_svr.on('connection', function(conn) {
    // this will be overwritten if & when a user logs on
    var userid = "0";
    var brokerid = 1;

    console.log('new connection');

    // data callback
    // todo: multiple messages in one data event
    conn.on('data', function(msg) {
      console.log('recd:' + msg);

      // just forward to trade server
      /*if (msg.substr(2, 13) == "quoterequest\"") {
        db.publish(commonbo.tradeserverchannel, msg);*/
      if (msg.substr(2, 18) == "ordercancelrequest") {
        db.publish(commonbo.tradeserverchannel, msg);
      } else if (msg.substr(2, 16) == "orderfillrequest") {
        db.publish(commonbo.tradeserverchannel, msg);
      /*} else if (msg.substr(2, 6) == "order\"") {
        db.publish(commonbo.tradeserverchannel, msg);*/
      } else {
        // need to parse
        try {
          var obj = JSON.parse(msg);

          if ("quoterequest" in  obj) {
            quoteRequestReceived(obj.quoterequest, userid);
          } else if ("order" in obj) {
            orderReceived(obj.order, userid);
          } else if ("singlesymbolrequest" in obj) {
            singleSymbolRequest(obj.singlesymbolrequest, userid, conn);
          } else if ("singlesymbolremoverequest" in obj) {
            singleSymbolRemoveRequest(obj.singlesymbolremoverequest, userid, conn);
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
          } else if ("accountsummaryrequest" in obj) {
            accountSummaryRequest(obj.accountsummaryrequest, conn);
          } else if ("index" in obj) {
            sendIndex(obj.index, conn);        
          } else if ("newclient" in obj) {
            newClient(obj.newclient, userid, conn);
          } else if ("cashtrans" in obj) {
            cashTrans(obj.cashtrans, userid, conn);
          } else if ("instupdate" in obj) {
            instUpdate(obj.instupdate, userid, conn);
          } else if ("hedgebookupdate" in obj) {
            hedgebookUpdate(obj.hedgebookupdate, userid, conn);
          } else if ("cost" in obj) {
            costUpdate(obj.cost, conn);
          } else if ("ifa" in obj) {
            newIfa(obj.ifa, userid, conn);
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

      db.get("broker:" + brokerid + ":" + servertype + ":" + signin.email, function(err, emailuserid) {
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
        db.hgetall("broker:" + brokerid + ":" + servertype + ":" + emailuserid, function(err, user) {
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

            db.hget("broker:" + user.brokerid, "name", function(err, name) {
              if (err) {
                console.log(err);
                return;
              }

              reply.broker = name;

              replySignIn(reply, conn);

              console.log("user:" + userid + " logged on");

              // send the data
              start(user, conn);
            });
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
  db.eval(commonfo.scriptunsubscribeid, 0, id, serverid, feedtype, function(err, ret) {
    if (err) throw err;

    // unsubscribe returned topics
    for (var i = 0; i < ret.length; i++) {
      dbsub.unsubscribe("price:" + ret[i]);      
    }
  });
}

//
// request for a new symbol subscription
//
function singleSymbolRequest(symbol, userid, conn) {
  console.log("singleSymbolRequest");
  db.eval(commonfo.scriptsubscribesymbol, 0, symbol, userid, serverid, feedtype, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // see if we need to subscribe
    if (ret[0] == 1) {
      console.log("subscribing to " + symbol);
      dbsub.subscribe("price:" + symbol);
    }

    // send the current stored price
    var price = {};
    price.symbolid = symbol;
    price.bid = ret[1];
    price.ask = ret[2];
    price.timestamp = ret[3];
    price.midnetchg = ret[4];
    price.midpctchg = ret[5];
    console.log(price);

    conn.write("{\"price\":" + JSON.stringify(price) + "}");
  });
}

function singleSymbolRemoveRequest(symbol, userid, conn) {
  db.eval(commonfo.scriptunsubscribesymbol, 0, symbol, userid, serverid, feedtype, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // the script will tell us if we need to unsubscribe from the symbol
    if (ret[0] == 1) {
      console.log("unsubscribing from " + symbol);
      dbsub.unsubscribe("price:" + ret[1]);
    }
  });
}

function newClient(client, userid, conn) {
  console.log(client);
  client.active = 1;

  // get the broker for this client
  /*db.hget(servertype + ":" + userid, "brokerid", function(err, brokerid) {
    if (err) {
      console.log(err);
      return;
    }*/

    // maybe a new client or an updated client
    if (client.clientid == "") {
       db.eval(common.scriptnewclient, 1, "broker:" + brokerid, brokerid, client.name, client.email, client.mobile, client.address, client.ifaid, client.type, client.insttypes, client.hedge, client.brokerclientcode, client.commissionpercent, client.active, function(err, ret) {
        if (err) throw err;

        if (ret[0] != 0) {
          console.log("Error in scriptnewclient:" + common.getErrorcode(ret[0]));
          commonfo.sendErrorMsg(ret[0], conn);
          return;
        }

        getSendClient(brokerid, ret[1], conn);
      });
    } else {
      db.eval(common.scriptupdateclient, 0, client.clientid, brokerid, client.name, client.email, client.mobile, client.address, client.ifaid, client.type, client.insttypes, client.hedge, client.brokerclientcode, client.commissionpercent, client.active, function(err, ret) {
        if (err) throw err;

        if (ret != 0) {
          console.log("Error in scriptupdateclient:" + common.getErrorcode(ret));
          return;
        }
        console.log(ret);

        getSendClient(client.clientid, conn);
      });
    }
  //});
}

function newIfa(ifa, userid, conn) {
  db.eval(scriptifa, 6, ifa.ifaid, ifa.name, ifa.email, ifa.address, ifa.mobile, userid, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      console.log("Error in scriptifa:" + commonbo.getReasonDesc(ret[0]));
      commonfo.sendErrorMsg(ret[0], conn);
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
  cashtrans.timestamp = commonbo.getUTCTimeStamp(new Date());

  console.log(cashtrans);

  cashtrans.action = 1;
  cashtrans.bankaccountid = 999988;
  cashtrans.clientaccountid = 1;
  cashtrans.localamount = cashtrans.amount;
  cashtrans.note = cashtrans.description;
  cashtrans.rate = 1;
  cashtrans.reference = cashtrans.reference;
  if (cashtrans.drcr == 1) {
    cashtrans.transactiontypeid = "CC";
  } else {
    cashtrans.transactiontypeid = "CW";    
  }

  console.log(cashtrans);

  timestamp = new Date();

  if (cashtrans.drcr == 1) {
    newClientFundsTransfer(1, 1000, 1, 1, "GBP", 1000, "note1", "DCP", 1, "ref1", timestamp);
  } else {
    newClientFundsTransfer(2, 100, 1, 1, "GBP", 100, "note2", "BAC", 1, "ref2", timestamp);
  }
}

function loadHolidays() {
  db.smembers("holidays", function(err, ret) {
    if (err) throw err;

    for (var i = 0; i < ret.length; ++i) {
      holidays[ret[i]] = ret[i];
    }
  });
}

function newClientFundsTransfer(action, amount, brokerid, clientaccountid, currencyid, localamount, note, paymenttypeid, rate, reference, timestamp) {
  console.log("newClientFundsTransfer");

  // milliseconds since epoch, used for scoring datetime indexes
  var timestampms = timestamp.getTime();

  // get the number of days for the payment to clear
  db.hget("paymenttypes:" + paymenttypeid, "clearancedays", function(err, clearancedays) {
    if (err) {
      console.log(err);
      return;
    }

    if (clearancedays == null) {
      console.log("Clearance days not found for this payment type");
      return;
    }

    // calculate a clearance date from the number of clearance days
    var clearancedate = commonbo.getUTCDateString(commonbo.getSettDate(timestamp, clearancedays, holidays));

    db.eval(commonbo.newclientfundstransfer, 1, "broker:" + brokerid, action, amount, brokerid, clientaccountid, currencyid, localamount, note, paymenttypeid, rate, reference, timestamp, timestampms, clearancedate, function(err, ret) {
      if (err) throw err;
      console.log(ret);
    });
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

function getSendClient(brokerid, clientid, conn) {
  db.hgetall("broker:" + brokerid + ":client:" + clientid, function(err, client) {
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
    db.smembers("broker:" + brokerid + ":client:" + clientid + ":instrumenttypes", function(err, insttypes) {
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

    conn.write("{\"symbol\":" + JSON.stringify(inst) + "}");
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
  quote.reasondesc = commonfo.getReasonDesc(reason);

  conn.write("{\"quote\":" + JSON.stringify(quote) + "}");
}*/

function getSideDesc(side) {
  if (parseInt(side) == 1) {
    return "Buy";
  } else {
    return "Sell";
  }
}

/*function getSendOrder(orderid) {
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
}*/

// forward order to relevant user
function forwardOrder(order, msg) {
  console.log("forwarding order to user");
  console.log(order);

  if (order.operatorid in connections) {
    connections[order.operatorid].write(msg);
  }
}

/*function getSendTrade(tradeid) {
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

      db.hgetall("client:" + trade.clientid, function(err, client) {
        if (err) {
          console.log(err);
          return;
        }

        trade.brokerclientcode = client.brokerclientcode;

        if (order.operatorid in connections) {
          sendTrade(trade, connections[order.operatorid]);
        }
      });
    });
  });
}*/

// forward trade to relevant client
function forwardTrade(trade, msg) {
  console.log("forwardTrade");
  console.log(trade);

  // send trade to all users as we don't know if & which client they may be looking at
  for (var i in connections) {
    connections[i].write(msg);
  }
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

    ordercancelrequest.reasondesc = commonfo.getPTPOrderCancelRejectReason(ordercancelrequest.reason);
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

    replies.forEach(function(symbolid, j) {
      db.hgetall("symbol:" + symbolid, function(err, inst) {
        var instrument = {};
        if (err) {
          console.log(err);
          return;
        }

        if (inst == null) {
          console.log("Symbol " + symbolid + " not found");
          count--;
          return;
        }

        instrument.symbolid = symbolid;

        // add the order to the array
        i.symbols.push(instrument);

        // send array if we have added the last item
        //count--;
        //if (count <= 0) {
          //conn.write("{\"index\":" + JSON.stringify(i) + "}");
        //}
      });
    });
  });
}

function quoteRequestHistory(req, conn) {
  db.eval(commonfo.scriptgetquoterequests, 1, "broker:" + brokerid, req.accountid, brokerid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"quoterequests\":" + ret + "}");
  });
}

function quoteHistory(req, conn) {
  db.eval(commonfo.scriptgetquotes, 1, "broker:" + brokerid, req.accountid, brokerid, function(err, ret) {
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
  db.eval(commonfo.scriptgetorders, 1, "broker:" + brokerid, req.accountid, brokerid, function(err, ret) {
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

function updateStatus(server, status) {
  if (server == "ts") {
    serverstatus.tsstatus = status;
  } else if (server == "ps") {
    serverstatus.psstatus = status;    
  }
}

function sendStatus() {
  var msg = "{\"status\":" + JSON.stringify(serverstatus) + "}";

  for (var i in connections) {
    connections[i].write(msg);
  }
}

/*function sendTrade(trade, conn) {
  if (conn != null) {
    conn.write("{\"trade\":" + JSON.stringify(trade) + "}");
  }
}*/

function tradeHistory(req, conn) {
  var fromdate;
  var todate;

  if ('fromdate' in req) {
    // create date objects from from/to date strings
    fromdate = commonfo.dateFromUTCString(req.fromdate);
    todate = commonfo.dateFromUTCString(req.todate);

    // set todate to end of day
    todate.setHours(23);
    todate.setMinutes(59);
    todate.setSeconds(59);
  }

  if ("positionkey" in req) {
    db.eval(commonfo.scriptgetpostrades, 0, req.clientid, req.positionkey, function(err, ret) {
      if (err) throw err;
      conn.write("{\"trades\":" + ret + "}");
    });
  } else if (req.clientid == "0") {
    db.eval(scriptgetalltrades, 2, fromdate.getTime(), todate.getTime(), function(err, ret) {
      if (err) throw err;
      conn.write("{\"trades\":" + ret + "}");
    });
  } else {
    db.eval(commonfo.scriptgettrades, 0, req.clientid, function(err, ret) {
      if (err) throw err;
      conn.write("{\"trades\":" + ret + "}");
    });
  }
}

function cashHistory(req, conn) {
  db.eval(commonfo.scriptgetcashhistory, 0, req.clientid, req.currency, function(err, ret) {
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
  console.log("positionrequest");

  var brokerid = 1;
  var accountid = 1;

  db.eval(commonbo.scriptgetpositionvalues, 1, "broker:" + brokerid, accountid, brokerid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
    conn.write("{\"positions\":" + ret + "}");
  });
}

function cashRequest(cashreq, conn) {
  db.eval(commonfo.scriptgetcash, 0, cashreq.clientid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"cash\":" + ret + "}");
  });  
}

function accountSummaryRequest(acctreq, conn) {
  console.log("accountSummaryRequest");

  var brokerid = 1;
  var accountid = 1;

  db.eval(commonbo.scriptgetaccountsummary, 1, "broker:" + brokerid, accountid, brokerid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
    conn.write("{\"accountsummary\":" + ret + "}");
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

function getTimeInForceDesc(timeinforce) {
/*0 = Day

1 = Good Till Cancel (GTC)

2 = At the Opening (OPG)

3 = Immediate or Cancel (IOC)

4 = Fill or Kill (FOK)

5 = Good Till Crossing (GTX)

6 = Good Till Date*/
}

function start(user, conn) {
  sendInstruments(conn);
  //sendBrokers(conn);
  sendIfas(conn);
  sendInstrumentTypes(conn);
  sendOrderTypes(conn);
  sendCashTransTypes(conn);
  sendCurrencies(conn);
  sendClientTypes(conn);
  sendHedgebooks(conn);
  sendCosts(conn);
  sendMarkets(conn);
  sendStatus();
  sendUserid(user.userid, conn);
  loadHolidays();

  // make this the last one, as sends ready status to f/e
  sendClients(user.brokerid, conn);
}

function sendBroker(brokerid, conn) {
  db.hget("broker:" + brokerid, "name", function(err, name) {
    if (err) {
      console.log(err);
      return;
    }

    conn.write("{\"broker\":\"" + name + "\"}");
  });
}

function sendUserid(userid, conn) {
  conn.write("{\"userid\":" + userid + "}");
}

function replySignIn(reply, conn) {
  conn.write("{\"signinreply\":" + JSON.stringify(reply) + "}");
}

function initDb() {
  //clearConnections();
  //clearPendingChat();
}

function clearSubscriptions() {
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


/*function clearConnections() {
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
}*/

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

function sendClients(brokerid, conn) {
  // get sorted set of clients for specified broker
  db.eval(scriptgetclients, 0, brokerid, function(err, ret) {
    if (err) throw err;
    conn.write("{\"clients\":" + ret + "}");
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
  db.eval(commonfo.scriptgetclienttypes, 0, servertype, function(err, ret) {
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

/*function sendQuote(quoteid) {
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

    // send quote to the user who placed the quote request
    db.hgetall("quoterequest:" + quote.quotereqid, function(err, quoterequest) {
      if (err) {
        console.log(err);
        return;
      }

      if (quoterequest.operatorid in connections) {
        connections[quoterequest.operatorid].write("{\"quote\":" + JSON.stringify(quote) + "}");
      }
    });
  });
}*/

// forward quoteack to relevant user
function forwardQuoteAck(quoteack, msg) {
  console.log("forwardQuoteAck");
  console.log(quoteack);

  if (quoteack.operatorid in connections) {
    connections[quoteack.operatorid].write(msg);
  }
}

// forward quote to relevant user
function forwardQuote(quote, msg) {
  console.log("forwardQuote");
  console.log(quote);

  if (quote.operatorid in connections) {
    connections[quote.operatorid].write(msg);
  }
}

function placeOrder(quote) {
  console.log("placeOrder");

  var order = {};

  order.brokerid = 1;
  order.operatortype = operatortype;
  order.operatorid = 1;
  order.accountid = 2;
  order.clientid = 2;
  order.quoteid = quote.quoteid;
  order.ordertype = 1;
  order.symbolid = 'BARC.L';

  db.publish(commonbo.tradeserverchannel, "{\"order\":" + JSON.stringify(order) + "}");
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
  chat.timestamp = commonbo.getUTCTimeStamp(new Date());

  db.eval(scriptnewchat, 5, chat.clientid, chat.text, chat.timestamp, chat.chatid, userid, function(err, ret) {
    if (err) throw err;

    // add chat id, so we can identify any chat coming back
    chat.chatid = ret[0];

    // send it to the client server to forward to client
    db.publish(commonbo.clientserverchannel, "{\"chat\":" + JSON.stringify(chat) + "}");
  });      
}

//
// chat from a client
//
function newChatClient(msg) {
  var userid = "0";

  try {    
    var chatobj = JSON.parse(msg);

    chatobj.chat.timestamp = commonbo.getUTCTimeStamp(new Date());

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

  db.eval(commonbo.scriptgetholidays, 0, req.market, function(err, ret) {
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

    eoddate = commonbo.dateFromUTCString(eoddatestr);
    console.log(eoddate);

    //nexteoddate = getUTCDateString(commonfo.getSettDate(eoddate, 1));

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

function test() {
console.log("test");

var message = {
	"versionNumber": "6",
	"mode": "L",
	"functionParticipantId": "789",
	"dataParticipantId": "789",
	"transactionReference": "15AA00016",
	"tradeDate": "20151112",
	"intendedSettlementDate": "20151116",
	"brokerId": null,
	"tradeSystemOfOrigin": "S",
	"brokerReference": null,
	"ncCondition": false,
	"bargainConditions": "NNNNNNNNNN",
	"traditionalOptions": null,
	"sharedReference": null,
	"tradeReference": null,
	"tradeTime": "120100",
	"specialPrice": false,
	"agentIndicator": "A",
	"participantNote": null,
	"transactionStampStatus": null,
	"stampableConsideration": {
		"currency": null,
		"amount": 0
	},
	"priority": "50",
	"newDepositLink": false,
	"stockDepositLinkDropNumber": 0,
	"originTransactionId": null,
	"originTransactionReference": null,
	"marketClientInd": "M",
	"clientMarketLink": null,
	"dealingCapacity": "A",
	"transactionReportMarker": "S",
	"sundryTransactionInformation": 0,
	"venueIdentification": "XLON",
	"regSCertification": null,
	"crestParticipants": [
		{
			"participantId": "789",
			"sharedNote": null,
			"accountId": "TGNOMS",
			"charityId": null,
			"buyingClientIdShared": null,
			"buyingClientIdHidden": "I#0000680820000000001#",
			"sellingClientIdShared": "B#MAGBGB21#",
			"sellingClientIdHidden": null,
			"nationalityDeclaration": null,
			"partyTransactionStatus": null,
			"partyDelayed": false,
			"partyFrozen": false,
			"inputTimestamp": "CCYYMMDDHHMMSSSSSSSS",
			"dealingFirmId": null,
			"tsoParticipantId": null,
			"settlementFirmId": null,
			"tsoDealingCapacity": null,
			"houseClientIndicator": null,
			"clearingMemberId": null
		}
	],
	"crestStockMovements": [
		{
			"debitPartyStockId": "FNMAY",
			"debitAccountId": null,
			"debitBalanceType": null,
			"otherDebitPartyId": null,
			"otherDebitAccountId": null,
			"creditPartyStockId": "789",
			"creditAccountId": null,
			"creditBalanceType": null,
			"otherCreditPartyId": null,
			"otherCreditAccountId": null,
			"isin": {
				"isin": "IE0005042456",
				"type": "ISIN"
			},
			"quantity": "1000",
			"specialCondition": false,
			"badDeliveryReason": null,
			"tradePrice": {
				"currency": "GBP",
				"amount": "6,2717"
			},
			"countryOfRegister": null,
			"group1Units": 0,
			"group2Units": 0
		}
	],
	"crestCashMovements": [
		{
			"debitPartyCashId": "789",
			"creditPartyCashId": "FNMAY",
			"consideration": {
				"currency": "GBP",
				"amount": "6271,7"
			},
			"cashMovementType": "A",
			"paymentType": "C"
		}
	],
	"dependentTrade": false,
	"internationalCsdId": null,
	"internationalPartyReference": null,
	"internationalParticipantId": null,
	"internationalParticipantBic": null,
	"internationalAccountId": null,
	"crestCrossBorderDetails": [
		{
			"buyerOrSellerInd": null,
			"underlyingClient1": null,
			"underlyingClient2": null,
			"underlyingClient3": null,
			"underlyingClient4": null,
			"underlyingClientBic": null,
			"underlyingClientParty": null
		}
	]
};
console.log(message);
console.log(message.functionParticipantId);
var teststr = "abc" + JSON.stringify(message) + "xyz";
console.log(teststr);
}

function testclientfundtransfer() {
  console.log("test");

  var amount = 1000;
  var bankaccountid = 999988;
  var brokerid = 1;
  var clientaccountid = 2;
  var currencyid = "GBP";
  var localamount = 1000;
  var note = "first cash receipt";
  var rate = 1;
  var reference = "ABC123";
  var timestamp = commonbo.getUTCTimeStamp(new Date());
  var transactiontypeid = "CD";

  // note we are passing the key in, so as to facilitate clustering
  db.eval(commonbo.scriptnewclientfundstransfer, 1, "broker:" + brokerid, amount, bankaccountid, brokerid, clientaccountid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function testtrade() {
  console.log("testtrade");

  var consideration = 1234;
  var commission = 12.34;
  var ptmlevy = 10;
  var stampduty = 1.23;
  var brokerid = 1;
  var clientaccountid = 1;
  var currencyid = "GBP";
  var localamount = 1234;
  var note = "Bought 100 BARC.L";
  var rate = 1;
  var timestamp = "";
  var tradeid = 1;
  var side = 1;

  // note we are passing a key
  db.eval(commonbo.scripttesttrade, 1, "broker:" + brokerid, consideration, commission, ptmlevy, stampduty, brokerid, clientaccountid, currencyid, localamount, note, rate, timestamp, tradeid, side, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function testSettle() {
  var tradesettle = {};

  tradesettle.amount = 125;
  tradesettle.brokerid = 1;
  tradesettle.fromaccountid = 1;
  tradesettle.localamount = 125;
  tradesettle.note = "Trade settlement";
  tradesettle.rate = 1;
  tradesettle.timestamp = new Date();
  tradesettle.toaccountid = 2;
  tradesettle.tradeid = 12;
  tradesettle.transactiontypeid = "";

  console.log(tradesettle);

  var milliseconds = tradesettle.timestamp.getTime();

  // note we are passing the key in, so as to facilitate clustering
  db.eval(commonbo.newTradeSettlementTransaction, 1, "broker:" + brokerid, tradesettle.amount, tradesettle.brokerid, tradesettle.fromaccountid, tradesettle.localamount, tradesettle.note, tradesettle.rate, tradesettle.timestamp, tradesettle.toaccountid, tradesettle.tradeid, tradesettle.transactiontypeid, milliseconds, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function testBrokerFundsTransfer() {
  var bft = {};

  bft.amount = 125;
  bft.brokerid = 1;
  bft.currencyid = "GBP";
  bft.brokerbankaccountid = 1;
  bft.localamount = 125;
  bft.note = "broker funds transfer test";
  bft.rate = 1;
  bft.reference = "bft1";
  bft.supplieraccountid = 2;
  bft.timestamp = new Date();
  bft.transactiontypeid = "BP";

  console.log(bft);

  var milliseconds = bft.timestamp.getTime();

  // note we are passing the key in, so as to facilitate clustering
  db.eval(commonbo.newBrokerFundsTransfer, 1, "broker:" + brokerid, bft.amount, bft.brokerid, bft.currencyid, bft.brokerbankaccountid, bft.localamount, bft.note, bft.rate, bft.reference, bft.supplieraccountid, bft.timestamp, bft.transactiontypeid, milliseconds, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function testSupplierFundsTransfer() {
  var sft = {};

  console.log("testSupplierFundsTransfer");

  sft.amount = 125;
  sft.brokerbankaccountid = 1;
  sft.brokerid = 1;
  sft.localamount = 125;
  sft.note = "supplier funds transfer test";
  sft.rate = 1;
  sft.reference = "sft1";
  sft.supplieraccountid = 2;
  sft.timestamp = new Date();
  sft.transactiontypeid = "SP";

  console.log(sft);

  var milliseconds = sft.timestamp.getTime();

  // note we are passing the key in, so as to facilitate clustering
  db.eval(commonbo.newSupplierFundsTransfer, 1, "broker:" + brokerid, sft.amount, sft.brokerbankaccountid, sft.brokerid, sft.localamount, sft.note, sft.rate, sft.reference, sft.supplieraccountid, sft.timestamp, sft.transactiontypeid, milliseconds, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function testPositionPostings() {
  var brokerid = 1;
  var positionid = 1;

  db.eval(commonbo.scriptgetpositionpostings, 1, "broker:" + brokerid, brokerid, positionid, "-inf", "inf", function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function quoteRequestReceived(quoterequest, userid) {
  console.log(quoterequest);
  quoterequest.brokerid = brokerid;
  quoterequest.operatortype = operatortype;
  quoterequest.operatorid = userid;

  //quoterequest.cashorderqty = 1000;
  //quoterequest.quantity = 0;

  db.smembers("broker:" + brokerid + ":client:" + quoterequest.clientid + ":clientaccounts", function(err, clients) {
    if (err) throw err;

    console.log(clients);

    quoterequest.accountid = clients[0];

    db.publish(commonbo.tradeserverchannel, "{\"quoterequest\":" + JSON.stringify(quoterequest) + "}");
  });
}

function testOrder(userid) {
  var order = {};

  order.accountid = 2;
  order.clientid = 2;
  order.side = 1;
  order.symbolid = 'BARC.L';
  order.quantity = 100;
  //order.price = '1.26';
  order.cashorderqty = '';
  order.currencyid = 'GBP';
  order.ordertype = 2;
  order.settlcurrencyid = 'GBP';
  order.futsettdate = "";
  order.timeinforce = "4";
  order.expiredate = "";
  order.expiretime = "";
  order.settlmnttypid = 0;
  order.quoteid = "";
  order.ordertype = 1;

  orderReceived(order, userid);
}

function orderReceived(order, userid) {
  console.log("orderReceived");
  console.log(order);

  order.brokerid = brokerid;
  order.operatortype = operatortype;
  order.operatorid = userid;

  db.publish(commonbo.tradeserverchannel, "{\"order\":" + JSON.stringify(order) + "}");
}

function testOrders() {
  for (var i = 0; i < 100; i++) {
    testOrder(1);
  }
}

function testQuoteRequest() {
  var quoterequest = {};

  quoterequest.brokerid = 1;
  quoterequest.operatortype = operatortype;
  quoterequest.operatorid = 1;
  quoterequest.accountid = 2;
  quoterequest.clientid = 2;
  quoterequest.side = 1;
  quoterequest.symbolid = 'BARC.L';
  quoterequest.quantity = 98;
  quoterequest.cashorderqty = '';
  quoterequest.currencyid = 'GBP';
  quoterequest.settlcurrencyid = 'GBP';
  quoterequest.futsettdate = "";
  quoterequest.timeinforce = "4";
  quoterequest.expiredate = "";
  quoterequest.expiretime = "";
  quoterequest.settlmnttypid = 0;
  quoterequest.quoteid = "";
  quoterequest.ordertype = 'D';

  db.publish(commonbo.tradeserverchannel, "{\"quoterequest\":" + JSON.stringify(quoterequest) + "}");
}

function testQuoteRequests() {
  for (var i = 0; i < 100; i++) {
    testQuoteRequest(1);
  }
}

function testStatement() {
  var brokerid = 1;
  var accountid = 1;

  var startmilli = new Date("September 13, 2015 00:00:00").getTime();
  var endmilli = new Date("September 19, 2015 00:00:00").getTime();
startmilli = '-inf';
endmilli = '+inf';
  console.log(startmilli);
  console.log(endmilli);

  db.eval(commonbo.scriptgetstatement, 1, "broker:" + brokerid, accountid, brokerid, startmilli, endmilli, function(err, ret) {
    if (err) throw err;

    var obj = JSON.parse(ret);
    console.log(obj);
  });
}

function testValuation() {
  var brokerid = 1;
  var accountid = 1;

  var valuedate = "20160924";
  var dtmilli = new Date("September 24, 2016 23:59:59").getTime();

  db.eval(commonbo.scriptvaluation, 1, "broker:" + brokerid, accountid, brokerid, valuedate, dtmilli, function(err, ret) {
    if (err) throw err;

    var obj = JSON.parse(ret);
    console.log(obj);
  });
}

function applyCorporateAction(brokerid, corporateactionid) {
  console.log("applyCorporateAction");

  // this variable determines whether the ex-date or pay-date part of the rights issue process is run, based on a user selection
  var applyEXdate = 2;

  db.hget("corporateaction:" + corporateactionid, "corporateactiontypeid", function(err, corporateactiontypeid) {
    if (err) {
      console.log("Error in applycorporateaction:" + err);
      return;
    }

    console.log(corporateactiontypeid);

    if (corporateactiontypeid == "DVCA") {
      caCashDividend(brokerid, corporateactionid);
    } else if (corporateactiontypeid == "DVSC") {
      caScripDividend(brokerid, corporateactionid);
    } else if (corporateactiontypeid == "RHTS") {
      if (applyEXdate == 1) {
        caRightsExdate(brokerid, corporateactionid);
      } else {
        caRightsPayDate(brokerid, corporateactionid);
      }
    } else if (corporateactiontypeid == "SPLF" || corporateactiontypeid == "SPLR") {
      caStockSplit(corporateactionid);
    } else if (corporateactiontypeid == "BONU") {
      caScripIssue(brokerid, corporateactionid);
    } else if (corporateactiontypeid == "EXOF") {
      caTakeover(brokerid, corporateactionid);
    } else if (corporateactiontypeid == "CONV") {
      caConversion(brokerid, corporateactionid);
    }
  });
}

function caCashDividend(brokerid, corporateactionid) {
  console.log("caCashDividend");
  var exdatems = new Date("April 25, 2016").getTime();
  var timestamp = new Date();
  var timestampms = timestamp.getTime();
  var mode = 2;

  console.log(exdatems);
  console.log(corporateactionid);
  console.log(timestamp);

  db.eval(commonbo.cacashdividend, 1, "broker:" + brokerid, brokerid, corporateactionid, exdatems, timestamp, timestampms, mode, function(err, ret) {
    if (err) throw err;
    console.log(ret);
    if (ret[0] == 1) {
      console.log("error");
    } else {
      console.log("Total quantity: " + ret[1]);
      console.log("Total unsettled quantity: " + ret[2]);
      console.log("Total dividend: " + ret[3]);
      console.log("Total unsettled dividend: " + ret[4]);
      console.log("Number of accounts: " + ret[5]);
    }
  });
}

function caScripDividend(brokerid, corporateactionid) {
  console.log("applyScripDividend");
  var exdate = new Date("April 25, 2016");
  var exdatems = exdate.getTime();
  var timestamp = new Date();
  var timestampms = timestamp.getTime();
  var mode = 2;

  console.log(exdatems);
  console.log(corporateactionid);
  console.log(timestamp);

  db.eval(commonbo.cadividendscrip, 1, "broker:" + brokerid, brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms, mode, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function caRightsExdate(brokerid, corporateactionid) {
  console.log("caRightsExdate");
  var exdate = new Date("February 12, 2016");

  // millisecond representation of exdate - don't need to subtract a day as this will give us the 00:00:00 time
  var exdatems = exdate.getTime();
  var mode = 1;

  // we need exdate - 1
  exdate.setDate(exdate.getDate() - 1);
  console.log("exdate-1=" + exdate);

  // get a UTC string version in "YYYYMMDD" format
  var exdatestr = commonbo.getUTCDateString(exdate);
  console.log("exdatestr=" + exdatestr);

  // timestamp & millisecond representation
  var timestamp = new Date();
  var timestampms = timestamp.getTime();

  db.eval(commonbo.carightsexdate, 1, "broker:" + brokerid, brokerid, corporateactionid, exdatestr, exdatems, timestamp, timestampms, mode, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    if (ret[0] == 1) {
      console.log("Error in carightsexdate: " + commonbo.getReasonDesc(ret[1]));
      return;      
    }
  });
}

function caRightsPayDate(brokerid, corporateactionid) {
  console.log("caRightsPayDate");
  var operatorid = 1;
  var paydate = new Date("February 16, 2016");
  var mode = 2;

  // millisecond representation of paydate
  var paydatems = paydate.getTime();

  // timestamp & millisecond representation
  var timestamp = new Date();
  var timestampms = timestamp.getTime();

  db.eval(commonbo.carightspaydate, 1, "broker:" + brokerid, brokerid, corporateactionid, paydatems, timestamp, timestampms, operatortype, operatorid, mode, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    if (ret[0] == 1) {
      console.log("Error in carightspaydate: " + commonbo.getReasonDesc(ret[1]));
      return;      
    }
  });
}

function caStockSplit(corporateactionid) {
  console.log("caStockSplit");
  var exdate = new Date("April 25, 2016");
  var mode = 1;

  // millisecond representation of exdate
  var exdatems = exdate.getTime();

  // we need exdate - 1 for eod price
  exdate.setDate(exdate.getDate() - 1);
  var exdatestr = commonbo.getUTCDateString(exdate);

  // timestamp & millisecond representation
  var timestamp = new Date();
  var timestampms = timestamp.getTime();

  db.eval(commonbo.castocksplit, 0, corporateactionid, exdatestr, exdatems, timestamp, timestampms, mode, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    if (ret[0] == 1) {
      console.log("Error in castocksplit: " + commonbo.getReasonDesc(ret[1]));
      return;      
    }
  });
}

function caScripIssue(brokerid, corporateactionid) {
  console.log("caScripIssue");
  var exdate = new Date("April 25, 2016");
  var mode = 3;

  // millisecond representation of exdate - don't need to subtract a day as this will give us the 00:00:00 time
  var exdatems = exdate.getTime();

  // we need exdate - 1 for eod price
  exdate.setDate(exdate.getDate() - 1);
  var exdatestr = commonbo.getUTCDateString(exdate);
console.log(exdatestr);

  // timestamp & millisecond representation
  var timestamp = new Date();
  var timestampms = timestamp.getTime();

  db.eval(commonbo.cascripissue, 1, "broker:" + brokerid, brokerid, corporateactionid, exdatestr, exdatems, timestamp, timestampms, mode, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    if (ret[0] == 1) {
      console.log("Error in cascripissue: " + commonbo.getReasonDesc(ret[1]));
      return;      
    }
  });
}

function caTakeover(brokerid, corporateactionid) {
  console.log("caTakeover");
  var exdate = new Date("April 25, 2016");
  var operatorid = 1;
  var mode = 2;

  // millisecond representation of exdate - don't need to subtract a day as this will give us the 00:00:00 time
  var exdatems = exdate.getTime();

  // timestamp & millisecond representation
  var timestamp = new Date();
  var timestampms = timestamp.getTime();

  db.eval(commonbo.catakeover, 1, "broker:" + brokerid, brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms, operatortype, operatorid, mode, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    if (ret[0] == 1) {
      console.log("Error in catakeover: " + commonbo.getReasonDesc(ret[1]));
      return;  
    }
  });
}

function caConversion(brokerid, corporateactionid) {
  console.log("caConversion");
  var exdate = new Date("December 25, 2016");
  var mode = 2;

  // millisecond representation of exdate - don't need to subtract a day as this will give us the 00:00:00 time
  var exdatems = exdate.getTime();

  // timestamp & millisecond representation
  var timestamp = new Date();
  var timestampms = timestamp.getTime();

  db.eval(commonbo.caconversion, 1, "broker:" + brokerid, brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms, mode, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    if (ret[0] == 1) {
      console.log("Error in catakeover: " + commonbo.getReasonDesc(ret[1]));
      return;
    }
  });
}

function testcollectagginvest() {
  var brokerid = 1;
  var schemeid = 1;
  var cashamount = 10;
  var cashisfixed = 0;
  var fundallocations = "BARC.L,50,AMZN.O,50";
 
  // timestamp & millisecond representation
  var timestamp = new Date();
  var timestampms = timestamp.getTime();

  var operatorid = 1; // user id
  var mode = 2;
 
  db.eval(commonbo.newcollectaggregateinvest, 1, "broker:" + brokerid, brokerid, schemeid, cashamount, cashisfixed, fundallocations, timestamp, timestampms, operatorid, mode, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    if (ret[0] == 1) {
      console.log("Error in newcollectaggregateinvest: " + commonbo.getReasonDesc(ret[1]));
      return;
    }
  });
}

function testSql() {
  var brokerid = 1;
  var s = "select quantity,price from trades where quantity > 10";

  db.eval(sql.scriptretrievebysql, 1, "broker:" + brokerid, brokerid, s, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    if (ret[0] == 1) {
      console.log("Error in scriptretrievebysql: " + ret[1]);
      return;
    }
  }); 
}

function testWatchlist() {
  var brokerid = 1;
  var clientid = 1;

  db.eval(commonfo.scriptgetwatchlist, 0, brokerid, clientid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function testUnwatchlist() {
  var brokerid = 1;
  var clientid = 1;

  db.eval(commonfo.scriptunwatchlist, 0, brokerid, clientid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function testSubscribeSymbol() {
  var brokerid = 1;
  var clientid = 1;
  var symbolid = "BARC.L";

  db.eval(commonfo.scriptsubscribesymbol, 0, brokerid, clientid, symbolid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function testUnsubscribeSymbol() {
  var brokerid = 1;
  var clientid = 1;
  var symbolid = "BARC.L";

  db.eval(commonfo.scriptunsubscribesymbol, 0, brokerid, clientid, symbolid, function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function getOrderBook() {
  var brokerid = 1;

  db.eval(commonbo.scriptgetorderbook, 1, "broker:" + brokerid, brokerid, "BARC.L", "-inf", "inf", function(err, ret) {
    if (err) throw err;
    console.log(ret);
  });
}

function getOrderbookTopAll() {
  var brokerid = 1;

  db.eval(commonbo.scriptgetorderbooktopall, 1, "broker:" + brokerid, brokerid,  function(err, ret) {
    if (err) throw err;
    console.log(ret);
 });
}

function registerScripts() {
  var stringsplit;
  //var getunrealisedpandl = commonfo.getunrealisedpandl;
  //var calcfinance = commonfo.calcfinance;
  //var gettrades = commonfo.gettrades;

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
  local clients = redis.call("sort", "broker:" .. ARGV[1] .. ":clients", "ALPHA") \
  local fields = {"brokerid", "clientid", "email", "name", "address", "mobile", "ifaid", "type", "hedge", "brokerclientcode", "commissionpercent"} \
  local vals \
  local tblclient = {} \
  local tblinsttype = {} \
  for index = 1, #clients do \
    vals = redis.call("hmget", "broker:" .. ARGV[1] .. ":client:" .. clients[index], unpack(fields)) \
    tblinsttype = redis.call("smembers", "broker:" .. ARGV[1] .. "client:" .. vals[2] .. ":instrumenttypes") \
    table.insert(tblclient, {brokerid = vals[1], clientid = vals[2], email = vals[3], name = vals[4], address = vals[5], mobile = vals[6], ifaid = vals[7], insttypes = tblinsttype, type = vals[8], hedge = vals[9], brokerclientcode = vals[10], commissionpercent = vals[11]}) \
  end \
  return cjson.encode(tblclient) \
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
  local symbols = redis.call("sort", "symbols", "ALPHA") \
  local fields = {"instrumenttypeid", "shortname", "currencyid", "exchangeid", "isin", "hedge", "ptmexempt"} \
  local vals \
  local inst = {} \
  local marginpc \
  for index = 1, #symbols do \
    vals = redis.call("hmget", "symbol:" .. symbols[index], unpack(fields)) \
    if redis.call("sismember", "instrumenttypes", vals[1]) == 1 then \
      if vals[4] then \
        marginpc = vals[4] \
      else \
        marginpc = 100 \
      end \
      table.insert(inst, {symbolid = symbols[index], shortname = vals[2], currencyid = vals[3], instrumenttypeid = vals[1], exchangeid = vals[5], isin = vals[6], hedge = vals[7], ptmexempt=vals[8]}) \
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

  //
  // set-up/maintain an ifa
  //
  scriptifa = '\
  local ifaid \
  if KEYS[1] == "" then \
    --[[ get the broker ]] \
    local brokerid = redis.call("hget", "user:" .. KEYS[6], "brokerid") \
    --[[ check email is unique ]] \
    local emailexists = redis.call("get", "ifa:" .. KEYS[3]) \
    if emailexists then return {1023} end \
    ifaid = redis.call("incr", "ifaid") \
    --[[ set the default password to email ]] \
    redis.call("hmset", "ifa:" .. ifaid, "ifaid", ifaid, "name", KEYS[2], "email", KEYS[3], "password", KEYS[3], "address", KEYS[4], "mobile", KEYS[5], "brokerid", brokerid, "marketext", "D") \
    --[[ add route to find ifa from email ]] \
    redis.call("set", "ifa:" .. KEYS[3], ifaid) \
  else \
    ifaid = KEYS[1] \
    --[[ update the email->id link if email has changed ]] \
    local email = redis.call("hget", "ifa:" .. ifaid, "email") \
    if KEYS[3] ~= email then \
      redis.call("del", "ifa:" .. email) \
      redis.call("set", "ifa:" .. KEYS[3], ifaid) \
    end \
    --[[ do not update id/password/brokerid ]] \
    redis.call("hmset", "ifa:" .. ifaid, "name", KEYS[2], "email", KEYS[3], "address", KEYS[4], "mobile", KEYS[5]) \
  end \
  redis.call("sadd", "ifas", ifaid) \
  return {0, ifaid} \
  ';

  //
  // get trades between a start & end date
  //
  scriptgetalltrades = commonbo.gettrades + '\
  local trades = redis.call("zrangebyscore", "tradesbydate", KEYS[1], KEYS[2]) \
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
  local fields = {"symbol", "quantity", "currency", "futsettdate"} \
  local vals \
  local reserves = redis.call("smembers", KEYS[1] .. ":reserves") \
  for index = 1, #reserves do \
    vals = redis.call("hmget", KEYS[1] .. ":reserve:" .. reserves[index], unpack(fields)) \
    if vals[1] then \
      table.insert(tblresults, {symbol=vals[1], quantity=vals[2], currency=vals[3], futsettdate=vals[4]}) \
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
  scriptendofday = commonbo.calcfinance + '\
    local clientid = KEYS[1] \
    local positions = redis.call("smembers", clientid .. ":positions") \
    local fields = {"symbol","side","quantity","cost","currency"} \
    local vals \
    for index = 1, #positions do \
      local vals = redis.call("hmget", clientid .. ":position:" .. positions[index], unpack(fields)) \
      if vals[1] then \
        local instrumenttypeid = redis.call("hget", "symbol:" .. vals[1], "instrumenttypeid") \
        local finance = calcfinance(instrumenttypeid, vals[4], vals[5], vals[2], 1) \
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
