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
var common = require('./common.js');

// globals
var markettype; // comes from database, 0=normal market, 1=out of hours
var tradeserverchannel = 3;
var userserverchannel = 2;
var clientserverchannel = 1;

// redis
var redishost;
var redisport;
var redisauth;
var redispassword;
var redislocal = true; // local or external server
var holidays = {};

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
//var scriptgetholidays;
var scriptorderfillrequest;

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
    try {
      console.log("channel:" + channel + " " + message);
      var obj = JSON.parse(message);

      if ("quoterequest" in obj) {
        quoteRequest(obj.quoterequest);
      } else if ("ordercancelrequest" in obj) {
        orderCancelRequest(obj.ordercancelrequest);
      } else if ("orderfillrequest" in obj) {
        orderFillRequest(obj.orderfillrequest);
      } else if ("order" in obj) {
        newOrder(obj.order);
      }
    } catch(e) {
      console.log(e);
      return;
    }
  });

  dbsub.subscribe(tradeserverchannel);
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
  console.log("connected to Proquote");
});
ptp.on('finished', function(message) {
    console.log(message);
});
ptp.on("initialised", function() {
  connectToTrading();
});

function connectToTrading() {
  // try to connect
  //winner.connect();

  ptp.connect();
}

function quoteRequest(quoterequest) {
  console.log("quoterequest");
  console.log(quoterequest);

  var today = new Date();

  quoterequest.timestamp = common.getUTCTimeStamp(today);

  // get settlement date from T+n no. of days
  quoterequest.futsettdate = common.getUTCDateString(common.getSettDate(today, quoterequest.nosettdays, holidays));

  // store the quote request & get an id
  db.eval(scriptquoterequest, 12, quoterequest.clientid, quoterequest.symbol, quoterequest.quantity, quoterequest.cashorderqty, quoterequest.currency, quoterequest.settlcurrency, quoterequest.nosettdays, quoterequest.futsettdate, quoterequest.timestamp, quoterequest.operatortype, quoterequest.operatorid, quoterequest.orderdivnum, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      // todo: send a quote ack to client

      console.log("Error in scriptquoterequest:" + common.getReasonDesc(ret[0]));
      return;
    }

    // add the quote request id & symbol details required for proquote
    quoterequest.quotereqid = ret[1];
    quoterequest.isin = ret[2];
    quoterequest.proquotesymbol = ret[3];
    quoterequest.exchange = ret[4];

    if (ret[6] == "CFD") {
      // make the quote request to proquote for the default equity settlement date
      // the stored settlement date stays as requested
      // the different settlement will be dealt with using finance
      quoterequest.futsettdate = common.getUTCDateString(common.getSettDate(today, ret[5], holidays));
    }

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

function newOrder(order) {
  var currencyratetoorg = 1; // product currency to org curreny rate
  var currencyindtoorg = 1;
  var settlcurrfxrate = 1; // settlement currency to product currency rate
  var settlcurrfxratecalc = 1;

  console.log(order);

  var today = new Date();

  order.timestamp = common.getUTCTimeStamp(today);
  order.partfill = 1; // accept part-fill

  // default value is in hours
  order.markettype = markettype;

  // always put a price in the order
  if (!("price" in order)) {
    order.price = "";
  }

  // and always have a quote id
  if (!("quoteid" in order)) {
    order.quoteid = "";
  }

  // get settlement date from T+n no. of days
  if (order.futsettdate == "") {
    order.futsettdate = common.getUTCDateString(common.getSettDate(today, order.nosettdays, holidays));
  }

  // store the order, get an id & credit check it
  db.eval(scriptneworder, 24, order.clientid, order.symbol, order.side, order.quantity, order.price, order.ordertype, order.markettype, order.futsettdate, order.partfill, order.quoteid, order.currency, currencyratetoorg, currencyindtoorg, order.timestamp, order.timeinforce, order.expiredate, order.expiretime, order.settlcurrency, settlcurrfxrate, settlcurrfxratecalc, order.nosettdays, order.operatortype, order.operatorid, order.orderdivnum, function(err, ret) {
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

    // set the settlement date to equity default date for cfd orders, in case they are being hedged with the market
    if (order.instrumenttype == "CFD") {
      order.futsettdate = common.getUTCDateString(common.getSettDate(today, ret[11], holidays));

      // update the stored order settlement details if it is a hedge
      if (ret[8] != "") {
        db.hmset("order:" + ret[8], "nosettdays", ret[11], "futsettdate", order.futsettdate);
      }
    }

    processOrder(order, ret[8], ret[9], ret[10]);
  });
}

function matchOrder(order) {
  db.eval(scriptmatchorder, 1, order.orderid, function(err, ret) {
    if (err) throw err;

    // return the active order to the sending operator type
    db.publish(order.operatortype, "order:" + order.orderid);

    // send any trades for active order client
    for (var i = 0; i < ret[1].length; ++i) {
      db.publish(order.operatortype, "trade:" + ret[1][i]);
    }

    // send any matched orders
    for (var i = 0; i < ret[0].length; ++i) {
      db.publish(order.operatortype, "order:" + ret[0][i]);
    }

    // send any matched trades
    for (var i = 0; i < ret[2].length; ++i) {
      db.publish(order.operatortype, "trade:" + ret[2][i]);
    }

    // indicate orderbook may have changed
    db.publish(order.operatortype, "orderbookupdate:" + order.symbol);
  });
}

function orderCancelRequest(ocr) {
  console.log("Order cancel request received for order#" + ocr.orderid);

  ocr.timestamp = common.getUTCTimeStamp(new Date());

  db.eval(scriptordercancelrequest, 5, ocr.clientid, ocr.orderid, ocr.timestamp, ocr.operatortype, ocr.operatorid, function(err, ret) {
    if (err) throw err;

    // error, so send an ordercancelreject message
    if (ret[0] != 0) {
      orderCancelReject(ocr.operatortype, ret[2]);
      return;
    }

    // forward to Proquote & wait for outcome
    if (ret[4] != "") {
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

    // not forwarded, so publish the result
    db.publish(ocr.operatortype, "order:" + ocr.orderid);
  });
}

function orderCancelReject(operatortype, ocrid) {
  db.publish(operatortype, "ordercancelreject:" + ocrid);
}

function orderFillRequest(ofr) {
  console.log("orderfillrequest");
  console.log(ofr);

  var today = new Date();

  ofr.timestamp = common.getUTCTimeStamp(today);

  if (!('price' in ofr)) {
    ofr.price = "";
  }

  // calculate a settlement date from the nosettdays
  ofr.futsettdate = common.getUTCDateString(common.getSettDate(today, ofr.nosettdays, holidays));

  db.eval(scriptorderfillrequest, 7, ofr.clientid, ofr.orderid, ofr.timestamp, ofr.operatortype, ofr.operatorid, ofr.price, ofr.futsettdate, function(err, ret) {
    if (err) throw err;

    // error, so send an orderfillreject message
    if (ret[0] != 0) {
      orderFillReject(ofr.operatortype, ret[2]);
      return;
    }

    // publish the result
    db.publish(ofr.operatortype, "order:" + ofr.orderid);
    db.publish(ofr.operatortype, "trade:" + ret[1]);
  });
}

function processOrder(order, hedgeorderid, tradeid, hedgetradeid) {
  //
  // the order has been credit checked
  // now, either forward to Proquote or attempt to match the order, depending on the type of instrument & whether the market is open
  //
  if (order.markettype == 1) {
    console.log("matching");
    matchOrder(order);
  } else {
    // equity orders
    if (order.instrumenttype == "DE" || order.instrumenttype == "IE") {
      if (tradeid != "") {
        // the order has been executed, so publish the order & trade
        db.publish(order.operatortype, "order:" + order.orderid);
        db.publish(order.operatortype, "trade:" + tradeid);

        // publish the hedge to the client server, so anyone viewing the hedgebook receives it
        db.publish(clientserverchannel, "trade:" + hedgetradeid);
      } else {
        // forward order to the market
        ptp.newOrder(order);
      }
    } else {
      // publish the order to whence it came
      db.publish(order.operatortype, "order:" + order.orderid);

      // publish the trade if there is one
      if (tradeid != "") {
        db.publish(order.operatortype, "trade:" + tradeid);
      }

      // publish any hedge to the client server, so anyone viewing the hedgebook receives it
      if (hedgetradeid != "") {
        db.publish(clientserverchannel, "trade:" + hedgetradeid);
      }

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

function initDb() {
  common.registerCommonScripts();
  registerScripts();
  loadHolidays();
  getMarkettype();
}

function loadHolidays() {
  // we are assuming "L"=London
  db.eval(common.scriptgetholidays, 1, "L", function(err, ret) {
    if (err) throw err;

    for (var i = 0; i < ret.length; ++i) {
      holidays[ret[i]] = ret[i];
    }
  });
}

function getMarkettype() {
  db.get("markettype", function(err, mkttype) {
    if (err) {
      console.log(err);
      return;
    }

    markettype = parseInt(mkttype);
  });
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

    if (ret[0] != 0) {
      // todo: message to operator
      console.log("Error in scriptrejectorder, reason:" + common.getReasonDesc(ret[0]));
      return;
    }

    // send to operator
    db.publish(ret[1], "order:" + exereport.clordid);
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

    // send confirmation to operator type
    db.publish(ret, "order:" + exereport.clordid);
  });
});

ptp.on("orderCancel", function(exereport) {
  console.log("Order cancelled by Proquote, ordercancelrequest id:" + exereport.clordid);

  db.eval(scriptordercancel, 1, exereport.clordid, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      // todo: send to client
      console.log("Error in scriptordercancel, reason:" + common.getReasonDesc(ret[0]));
      return;
    }

    // send confirmation to operator type
    db.publish(ret[2], "order:" + ret[1]);
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

    if (ret[0] != 0) {
      // todo: send to client
      console.log("Error in scriptorderexpire, reason:" + common.getReasonDesc(ret[0]));
      return;
    }

    // send confirmation to operator type
    db.publish(ret[1], "order:" + exereport.clordid);
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

  // milliseconds since epoch, used for scoring trades so they can be retrieved in a range
  var milliseconds = new Date().getTime();

  db.eval(scriptnewtrade, 21, exereport.clordid, exereport.symbol, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currency, currencyratetoorg, currencyindtoorg, exereport.execbroker, exereport.execid, exereport.futsettdate, exereport.transacttime, exereport.ordstatus, exereport.lastmkt, exereport.leavesqty, exereport.orderid, exereport.settlcurrency, exereport.settlcurramt, exereport.settlcurrfxrate, exereport.settlcurrfxratecalc, milliseconds, function(err, ret) {
    if (err) {
      console.log(err);
      return
    }

    // send the order & trade to the returned operator type
    db.publish(ret[1], "order:" + exereport.clordid);
    db.publish(ret[1], "trade:" + ret[0]);

    if (ret[1] == clientserverchannel) {
      // forward to user channel to enable keeping track of all trades
      db.publish(userserverchannel, "trade:" + ret[0]);
    } else if (ret[2] != "") {
      // if we have not sent to the client channel & this fill is for a hedge order, forward to client channel for hedge book monitoring
      db.publish(clientserverchannel, "trade:" + ret[0]);        
    }
  });
});

//
// ordercancelrequest rejected by proquote
//
ptp.on("orderCancelReject", function(ordercancelreject) {
  var text = "";

  console.log("Order cancel reject, order cancel request id:" + ordercancelreject.clordid);
  console.log(ordercancelreject);

  if ('text' in ordercancelreject) {
    text = ordercancelreject.text;
  }

  // update the order cancel request & send on
  db.eval(scriptordercancelreject, 3, ordercancelreject.clordid, ordercancelreject.cxlrejreason, text, function(err, ret) {
    if (err) {
      console.log(err);
      return
    }

    orderCancelReject(ret, ordercancelreject.clordid);
  });
});

ptp.on("quote", function(quote, header) {
  console.log("quote received");
  console.log(quote);

  // check to see if the quote may be a duplicate &, if it is, reject messages older than a limit
  // - todo: inform client & limit as parameter?
  if ('possdupflag' in header) {
    if (header.possdupflag == 'Y') {
      var sendingtime = new Date(common.getDateString(header.sendingtime));
      var validuntiltime = new Date(common.getDateString(quote.validuntiltime));
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
    quote.bidquotedepth = "";
  }
  if (!('offerpx' in quote)) {
    quote.offerpx = '';
    quote.offersize = '';
    quote.offerquoteid = '';
    quote.bidquoteid = quote.quoteid;
    quote.bidqbroker = quote.qbroker;
    quote.offerquotedepth = "";
  }

  // quote script
  // note: not passing securityid & idsource as proquote symbol should be enough
  db.eval(scriptquote, 17, quote.quotereqid, quote.bidquoteid, quote.offerquoteid, quote.symbol, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.transacttime, quote.currency, quote.settlcurrency, quote.bidqbroker, quote.offerqbroker, quote.futsettdate, quote.bidquotedepth, quote.offerquotedepth, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    if (ret[0] != 0) {
      // can't find quote request, so don't know which client to inform
      console.log("Error in scriptquote:" + common.getReasonDesc(ret[0]));
      return;
    }

    // exit if this is the first part of the quote as waiting for a two-way quote
    // todo: do we need to handle event of only getting one leg?
    if (ret[1] == 1) {return};

    db.publish(ret[3], "quote:" + ret[2]);
  });
});

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

    if (ret[0] == 1) {
      console.log("already received rejection for this quote request, ignoring");
      return;
    }

    // publish to server
    db.publish(ret[1], "quoteack:" + quoteack.quotereqid);
  });
});

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

function registerScripts() {
  var updatecash = common.updatecash;
  var getfreemargin = common.getfreemargin;
  var round = common.round;
  var calcfinance = common.calcfinance;
  var updateposition;
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
  var neworder;
  var getreserve;
  var getposition;
  var getrealisedpandl;
  var closeposition;
  var createposition;

  getcosts = round + '\
  local getcosts = function(clientid, symbol, instrumenttype, side, consid, currency) \
    local fields = {"commissionpercent", "commissionmin", "ptmlevylimit", "ptmlevy", "stampdutylimit", "stampdutypercent", "contractcharge"} \
    local vals = redis.call("hmget", "cost:" .. instrumenttype .. ":" .. currency .. ":" .. side, unpack(fields)) \
    local commission = 0 \
    local ptmlevy = 0 \
    local stampduty = 0 \
    local contractcharge = 0 \
    --[[ commission ]] \
    local commpercent = redis.call("hget", "client:" .. clientid, "commissionpercent") \
    if not commpercent or tonumber(commpercent) == nil then \
      if vals[1] and tonumber(vals[1]) ~= nil then \
        commpercent = vals[1] \
      else \
        commpercent = 0 \
      end \
    end \
    if tonumber(commpercent) ~= 0 then \
      commission = round(consid * tonumber(commpercent) / 100, 2) \
    end \
    if vals[2] and tonumber(vals[2]) ~= nil then \
      if commission < tonumber(vals[2]) then \
        commission = tonumber(vals[2]) \
      end \
    end \
    --[[ ptm levy ]] \
    local ptmexempt = redis.call("hget", "symbol:" .. symbol, "ptmexempt") \
    if ptmexempt and tonumber(ptmexempt) == 1 then \
    else \
      --[[ only calculate ptm levy if product is not exempt ]] \
      if vals[3] and tonumber(vals[3]) ~= nil then \
        if consid > tonumber(vals[3]) then \
          if vals[4] and tonumber(vals[4]) ~= nil then \
            ptmlevy = tonumber(vals[4]) \
          end \
        end \
      end \
    end \
    --[[ stamp duty ]] \
    if vals[5] and tonumber(vals[5]) ~= nil then \
      if consid > tonumber(vals[5]) then \
        if vals[6] and tonumber(vals[6]) ~= nil then \
          stampduty = round(consid * tonumber(vals[6]) / 100, 2) \
        end \
      end \
    end \
    --[[ contract charge ]] \
    if vals[7] and tonumber(vals[7]) ~= nil then \
      contractcharge = tonumber(vals[7]) \
    end \
    return {commission, ptmlevy, stampduty, contractcharge} \
  end \
  ';

  gettotalcost = getcosts + '\
  local gettotalcost = function(clientid, symbol, instrumenttype, side, consid, currency) \
    local costs =  getcosts(clientid, symbol, instrumenttype, side, consid, currency) \
    return {costs[1] + costs[2] + costs[3] + costs[4], costs} \
  end \
  ';

  //
  // get initial margin to include costs
  //
  getinitialmargin = '\
  local getinitialmargin = function(symbol, consid) \
    local marginpercent = redis.call("hget", "symbol:" .. symbol, "marginpercent") \
    if not marginpercent then marginpercent = 100 end \
    local initialmargin = consid * tonumber(marginpercent) / 100 \
    return initialmargin \
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

  /*updatetrademargin = '\
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
  ';*/

  updatereserve = '\
  local updatereserve = function(clientid, symbol, currency, settldate, quantity) \
    local poskey = symbol .. ":" .. currency .. ":" .. settldate \
    local reservekey = clientid .. ":reserve:" .. poskey \
    local reserveskey = clientid .. ":reserves" \
    --[[ get existing position, if there is one ]] \
    local reserve = redis.call("hget", reservekey, "quantity") \
    if reserve then \
      local adjquantity = tonumber(reserve) + tonumber(quantity) \
      if adjquantity == 0 then \
        redis.call("hdel", reservekey, "clientid", "symbol", "quantity", "currency", "settldate") \
        redis.call("srem", reserveskey, poskey) \
      else \
        redis.call("hset", reservekey, "quantity", adjquantity) \
      end \
    else \
      redis.call("hmset", reservekey, "clientid", clientid, "symbol", symbol, "quantity", quantity, "currency", currency, "settldate", settldate) \
      redis.call("sadd", reserveskey, poskey) \
    end \
  end \
  ';

  getrealisedpandl = round + '\
  local getrealisedpandl = function(side, tradequantity, tradeprice, avgcost) \
    local realisedpandl = tonumber(tradequantity) * (tonumber(tradeprice) - tonumber(avgcost)) \
    if tonumber(side) == 1 then \
      --[[ we are buying, so reverse p&l ]] \
      realisedpandl = -realisedpandl \
    end \
    realisedpandl = round(realisedpandl, 2) \
    return realisedpandl \
  end \
  ';

  createposition = '\
  local createposition = function(poskey, positionkey, positionskey, postradeskey, clientid, symbol, side, quantity, avgcostpershare, cost, currency, margin, tradeid) \
    local positionid = redis.call("incr", "positionid") \
    redis.call("hmset", positionkey, "clientid", clientid, "symbol", symbol, "side", side, "quantity", quantity, "cost", cost, "currency", currency, "margin", margin, "positionid", positionid, "averagecostpershare", avgcostpershare, "realisedpandl", 0) \
    redis.call("sadd", positionskey, poskey) \
    redis.call("sadd", postradeskey, tradeid) \
    return positionid \
  end \
  ';

  //
  // todo: do something with realisedpandl
  //
  closeposition = '\
  local closeposition = function(poskey, positionkey, positionskey, postradeskey, tradeid, realisedpandl) \
    redis.call("hdel", positionkey, "clientid", "symbol", "side", "quantity", "cost", "currency", "margin", "positionid", "averagecostpershare", "realisedpandl") \
    redis.call("srem", positionskey, poskey) \
    local postrades = redis.call("smembers", postradeskey) \
    for index = 1, #postrades do \
      redis.call("srem", postradeskey, postrades[index]) \
    end \
  end \
  ';

  //
  // positions are keyed on clientid + symbol + currency
  // they are linked to a list of trade ids to provide further information, such as stellement date, if required
  // a position id is allocated against a position and stored against the trade
  //
  updateposition = getrealisedpandl + closeposition + createposition + '\
  local updateposition = function(clientid, symbol, side, tradequantity, tradeprice, tradecost, currency, trademargin, tradeid) \
    local poskey = symbol .. ":" .. currency \
    local positionkey = clientid .. ":position:" .. poskey \
    local positionskey = clientid .. ":positions" \
    local postradeskey = clientid .. ":trades:" .. poskey \
    local posqty = 0 \
    local poscost = 0 \
    local realisedpandl = 0 \
    local posmargin = 0 \
    local avgcostpershare = 0 \
    local positionid = "" \
    local fields = {"quantity", "cost", "margin", "side", "averagecostpershare", "realisedpandl", "positionid"} \
    local vals = redis.call("hmget", positionkey, unpack(fields)) \
    --[[ do we already have a position? ]] \
    if vals[1] then \
      positionid = vals[7] \
      if tonumber(side) == tonumber(vals[4]) then \
        --[[ we are adding to the existing quantity ]] \
        posqty = tonumber(vals[1]) + tonumber(tradequantity) \
        poscost = tonumber(vals[2]) + tonumber(tradecost) \
        posmargin = tonumber(vals[3]) + tonumber(trademargin) \
        avgcostpershare = poscost / posqty \
        avgcostpershare = round(avgcostpershare, 5) \
        realisedpandl = vals[6] \
        --[[ update the position & add the trade to the set ]] \
        redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost, "margin", posmargin, "averagecostpershare", avgcostpershare, "realisedpandl", realisedpandl) \
        redis.call("sadd", postradeskey, tradeid) \
      elseif tonumber(tradequantity) == tonumber(vals[1]) then \
        --[[ just close position ]] \
        realisedpandl = tonumber(vals[6]) + getrealisedpandl(side, tradequantity, tradeprice, vals[5]) \
        closeposition(poskey, positionkey, positionskey, postradeskey, tradeid, realisedpandl) \
      elseif tonumber(tradequantity) > tonumber(vals[1]) then \
        --[[ close position ]] \
        closeposition(poskey, positionkey, positionskey, postradeskey, tradeid) \
        --[[ & open new ]] \
        posqty = tonumber(tradequantity) - tonumber(vals[1]) \
        poscost = posqty * tonumber(tradeprice) \
        poscost = round(poscost, 5) \
        posmargin = posqty / tonumber(tradequantity) * tonumber(trademargin) \
        posmargin = round(posmargin, 5) \
        avgcostpershare = tradeprice \
        realisedpandl = tonumber(vals[6]) + getrealisedpandl(side, tradequantity, tradeprice, vals[5]) \
        createposition(poskey, positionkey, positionskey, postradeskey, clientid, symbol, side, posqty, avgcostpershare, poscost, currency, posmargin, tradeid) \
      else \
        --[[ part-fill ]] \
        posqty = tonumber(vals[1]) - tonumber(tradequantity) \
        poscost = posqty * tonumber(vals[5]) \
        poscost = round(poscost, 5) \
        posmargin = posqty / tonumber(vals[1]) * tonumber(vals[3]) \
        posmargin = round(posmargin, 5) \
        avgcostpershare = vals[5] \
        realisedpandl = tonumber(vals[6]) + getrealisedpandl(side, tradequantity, tradeprice, vals[5]) \
        redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost, "margin", posmargin, "averagecostpershare", avgcostpershare, "realisedpandl", realisedpandl) \
        redis.call("sadd", postradeskey, tradeid) \
      end \
    else \
      --[[ new position ]] \
      createposition(poskey, positionkey, positionskey, postradeskey, clientid, symbol, side, tradequantity, tradeprice, tradecost, currency, trademargin, tradeid) \
    end \
    return positionid \
  end \
  ';

  adjustmarginreserve = getinitialmargin + updateordermargin + updatereserve + '\
  local adjustmarginreserve = function(orderid, clientid, symbol, side, price, ordmargin, currency, remquantity, newremquantity, settldate, nosettdays) \
    local instrumenttype = redis.call("hget", "symbol:" .. symbol, "instrumenttype") \
    if tonumber(side) == 1 then \
      if tonumber(newremquantity) ~= tonumber(remquantity) then \
        local newordmargin = 0 \
        if tonumber(newremquantity) ~= 0 then \
          local consid = tonumber(newremquantity) * tonumber(price) \
          newordmargin = getinitialmargin(symbol, consid) \
        end \
        updateordermargin(orderid, clientid, ordmargin, currency, newordmargin) \
      end \
    else \
      if tonumber(newremquantity) ~= tonumber(remquantity) then \
        updatereserve(clientid, symbol, currency, settldate, -tonumber(remquantity) + tonumber(newremquantity)) \
      end \
    end \
  end \
  ';

  rejectorder = '\
  local rejectorder = function(orderid, reason, text) \
    redis.call("hmset", "order:" .. orderid, "status", "8", "reason", reason, "text", text) \
  end \
  ';

  getposition = '\
  local getposition = function(clientid, symbol, currency) \
    local fields = {"quantity", "cost", "side", "margin"} \
    local position = redis.call("hmget", clientid .. ":position:" .. symbol .. ":" .. currency, unpack(fields)) \
    return position \
  end \
  ';

  getreserve = '\
  local getreserve = function(clientid, symbol, currency, settldate) \
    local reserve = redis.call("hget", clientid .. ":reserve:" .. symbol .. ":" .. currency .. ":" .. settldate, "quantity") \
    if not reserve then \
      return 0 \
    end \
    return reserve \
  end \
  ';

  creditcheck = rejectorder + getinitialmargin + gettotalcost + calcfinance + getposition + getfreemargin + getreserve + updateordermargin + '\
  local creditcheck = function(orderid, clientid, symbol, side, quantity, price, currency, settldate, instrumenttype, nosettdays) \
     --[[ see if client is allowed to trade this product ]] \
    if redis.call("sismember", clientid .. ":instrumenttypes", instrumenttype) == 0 then \
      rejectorder(orderid, 1018, "") \
      return {0} \
    end \
    --[[ calculate initial margin, costs & finance, as a trade may be generated now ]] \
    local consid = tonumber(quantity) * tonumber(price) \
    local initialmargin = getinitialmargin(symbol, consid) \
    local totalcost = gettotalcost(clientid, symbol, instrumenttype, side, consid, currency) \
    local finance = calcfinance(instrumenttype, consid, currency, side, nosettdays) \
    local position = getposition(clientid, symbol, currency) \
    --[[ always allow closing trades ]] \
    if position[1] then \
      if tonumber(side) ~= tonumber(position[3]) then \
        if tonumber(quantity) <= tonumber(position[1]) then \
          --[[ closing trade, so ok ]] \
          return {1, initialmargin, totalcost[2], finance} \
        end \
        if instrumenttype == "DE" and tonumber(side) == 2 then \
            --[[ equity, so cannot sell more than we have ]] \
            rejectorder(orderid, 1019, "") \
            return {0} \
        end \
        --[[ we are trying to close a quantity greater than current position, so need to check we can open a new position ]] \
        local freemargin = getfreemargin(clientid, currency) \
        --[[ add the margin returned by closing the position ]] \
        freemargin = freemargin + position[4] \
        --[[ check part of initial margin that would result from opening new position ]] \
        local newinitialmargin = (tonumber(quantity) - tonumber(position[1])) / tonumber(quantity) * initialmargin \
        if newinitialmargin + totalcost[1] + finance > freemargin then \
          rejectorder(orderid, 1020, "") \
          return {0} \
        end \
        --[[ closing trade or enough margin to close & open a new position, so ok ]] \
        return {1, initialmargin, totalcost[2], finance} \
      end \
    end \
    --[[ check free margin for all derivative trades & equity buys ]] \
    if instrumenttype == "CFD" or instrumenttype == "SPB" or instrumenttype == "CCFD" or tonumber(side) == 1 then \
      local freemargin = getfreemargin(clientid, currency) \
      if initialmargin + totalcost[1] + finance > freemargin then \
        rejectorder(orderid, 1020, "") \
        return {0} \
      end \
      updateordermargin(orderid, clientid, 0, currency, initialmargin) \
    else \
      --[[ allow ifa certificated equity sells ]] \
      if instrumenttype == "DE" then \
        if redis.call("hget", "client:" .. clientid, "type") == "3" then \
          return {1, initialmargin, totalcost[2], finance} \
        end \
      end \
      --[[ check there is a position ]] \
      if not position[1] then \
        rejectorder(orderid, 1003, "") \
        return {0} \
      end \
      --[[ check the position is long ]] \
      if tonumber(position[3]) == 2 then \
        rejectorder(orderid, 1004, "") \
        return {0} \
      end \
      --[[ check there is a large enough position ]] \
      local netpos = tonumber(position[1]) \
      local reserve = getreserve(clientid, symbol, currency, settldate) \
      if reserve then \
        netpos = netpos - tonumber(reserve) \
      end \
      if tonumber(quantity) > netpos then \
        rejectorder(orderid, 1004, "") \
        return {0} \
      end \
      --[[ todo: need for limit orders ]] \
      --[[updatereserve(clientid, symbol, currency, settldate, quantity)]] \
    end \
    return {1, initialmargin, totalcost[2], finance} \
  end \
  ';

  cancelorder = adjustmarginreserve + '\
  local cancelorder = function(orderid, status) \
    local orderkey = "order:" .. orderid \
    redis.call("hset", orderkey, "status", status) \
    local fields = {"clientid", "symbol", "side", "price", "settlcurrency", "margin", "remquantity", "futsettdate", "nosettdays"} \
    local vals = redis.call("hmget", orderkey, unpack(fields)) \
    if not vals[1] then \
      return 1009 \
    end \
    adjustmarginreserve(orderid, vals[1], vals[2], vals[3], vals[4], vals[6], vals[5], vals[7], 0, vals[8], vals[9]) \
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
      --[[ todo: dont think so ]] \
      redis.call("srem", "orderbooks", symbol) \
    end \
  end \
  ';

  newtrade = updateposition + updatecash + '\
  local newtrade = function(clientid, orderid, symbol, side, quantity, price, currency, currencyratetoorg, currencyindtoorg, costs, counterpartyid, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrency, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, nosettdays, initialmargin, operatortype, operatorid, finance, milliseconds) \
    local tradeid = redis.call("incr", "tradeid") \
    if not tradeid then return 0 end \
    local tradekey = "trade:" .. tradeid \
    redis.call("hmset", tradekey, "clientid", clientid, "orderid", orderid, "symbol", symbol, "side", side, "quantity", quantity, "price", price, "currency", currency, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "commission", costs[1], "ptmlevy", costs[2], "stampduty", costs[3], "contractcharge", costs[4], "counterpartyid", counterpartyid, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", tradeid, "settlcurrency", settlcurrency, "settlcurramt", settlcurramt, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "nosettdays", nosettdays, "finance", finance, "margin", initialmargin) \
    redis.call("sadd", "trades", tradeid) \
    redis.call("sadd", clientid .. ":trades", tradeid) \
    redis.call("sadd", "order:" .. orderid .. ":trades", tradeid) \
    redis.call("zadd", "tradesbydate", milliseconds, tradeid) \
    local totalcost = costs[1] + costs[2] + costs[3] + costs[4] \
    if totalcost > 0 then \
      updatecash(clientid, settlcurrency, "TC", totalcost, 1, "trade costs", "trade id:" .. tradeid, timestamp, "", operatortype, operatorid) \
    end \
    if tonumber(finance) > 0 then \
      updatecash(clientid, settlcurrency, "FI", finance, 1, "finance", "trade id:" .. tradeid, timestamp, "", operatortype, operatorid) \
    end \
    local positionid = updateposition(clientid, symbol, side, quantity, price, settlcurramt, settlcurrency, initialmargin, tradeid) \
    redis.call("hset", tradekey, "positionid", positionid) \
    return tradeid \
  end \
  ';

  neworder = '\
  local neworder = function(clientid, symbol, side, quantity, price, ordertype, remquantity, status, markettype, futsettdate, partfill, quoteid, currency, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforce, expiredate, expiretime, settlcurrency, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, nosettdays, operatortype, operatorid, hedgeorderid, orderdivnum) \
    --[[ get a new orderid & store the order ]] \
    local orderid = redis.call("incr", "orderid") \
    redis.call("hmset", "order:" .. orderid, "clientid", clientid, "symbol", symbol, "side", side, "quantity", quantity, "price", price, "ordertype", ordertype, "remquantity", remquantity, "status", status, "markettype", markettype, "futsettdate", futsettdate, "partfill", partfill, "quoteid", quoteid, "currency", currency, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "timestamp", timestamp, "margin", margin, "timeinforce", timeinforce, "expiredate", expiredate, "expiretime", expiretime, "settlcurrency", settlcurrency, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "orderid", orderid, "externalorderid", externalorderid, "execid", execid, "nosettdays", nosettdays, "operatortype", operatortype, "operatorid", operatorid, "hedgeorderid", hedgeorderid, "orderdivnum", orderdivnum) \
    --[[ add to set of orders for this client ]] \
    redis.call("sadd", clientid .. ":orders", orderid) \
    --[[ add order id to associated quote, if there is one ]] \
    if quoteid ~= "" then \
      redis.call("hset", "quote:" .. quoteid, "orderid", orderid) \
    end \
    return orderid \
  end \
  ';

  scriptrejectorder = rejectorder + adjustmarginreserve + '\
  rejectorder(KEYS[1], KEYS[2], KEYS[3]) \
  local fields = {"clientid", "symbol", "side", "price", "margin", "settlcurrency", "remquantity", "futsettdate", "nosettdays", "operatortype"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  if not vals[1] then \
    return 1009 \
  end \
  adjustmarginreserve(KEYS[1], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7], 0, vals[8], vals[9]) \
  return {0, vals[10]} \
  ';

  scriptneworder = neworder + creditcheck + newtrade + getproquotesymbol + getproquotequote + '\
  local orderid = neworder(KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[4], "0", KEYS[7], KEYS[8], KEYS[9], KEYS[10], KEYS[11], KEYS[12], KEYS[13], KEYS[14], 0, KEYS[15], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], "", "", KEYS[21], KEYS[22], KEYS[23], "", KEYS[24]) \
  local side = tonumber(KEYS[3]) \
  local markettype = tonumber(KEYS[7]) \
  --[[ calculate consideration in settlement currency, costs will be added later ]] \
  local settlcurramt = tonumber(KEYS[4]) * tonumber(KEYS[5]) \
  local instrumenttype = redis.call("hget", "symbol:" .. KEYS[2], "instrumenttype") \
  local cc = creditcheck(orderid, KEYS[1], KEYS[2], side, KEYS[4], KEYS[5], KEYS[18], KEYS[8], instrumenttype, KEYS[21]) \
  --[[ cc[1] = ok/fail, cc[2] = margin, cc[3] = costs array, cc[4] = finance ]] \
  if cc[1] == 0 then \
    --[[ return order id for lookup, if credit check has failed, error message will be in order ]] \
    return {cc[1], orderid} \
  end \
  local proquotesymbol = {"", "", "", ""} \
  local proquotequote = {"", ""} \
  local hedgebookid = "" \
  local tradeid = "" \
  local hedgeorderid = "" \
  local hedgetradeid = "" \
  local defaultnosettdays = 0 \
  if instrumenttype == "CFD" or instrumenttype == "SPB" or instrumenttype == "CCFD" then \
    --[[ ignore limit orders for derivatives as they will be handled manually, at least for the time being ]] \
    if KEYS[6] ~= "2" then \
      --[[ create trades for client & hedge book for off-exchange products ]] \
      hedgebookid = redis.call("get", "hedgebook:" .. instrumenttype .. ":" .. KEYS[18]) \
      if not hedgebookid then hedgebookid = 999999 end \
      local reverseside \
      if side == 1 then \
        reverseside = 2 \
      else \
        reverseside = 1 \
      end \
      local hedgecosts = {0,0,0,0} \
      tradeid = newtrade(KEYS[1], orderid, KEYS[2], side, KEYS[4], KEYS[5], KEYS[11], 1, 1, cc[3], hedgebookid, KEYS[7], "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], cc[2], KEYS[22], KEYS[23], cc[4]) \
      hedgetradeid = newtrade(hedgebookid, orderid, KEYS[2], reverseside, KEYS[4], KEYS[5], KEYS[11], 1, 1, hedgecosts, KEYS[1], KEYS[7], "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], 0, KEYS[22], KEYS[23], 0) \
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
          hedgeorderid = neworder(hedgebookid, proquotesymbol[4], KEYS[3], KEYS[4], KEYS[5], "X", KEYS[4], 0, KEYS[7], KEYS[8], KEYS[9], "", KEYS[11], KEYS[12], KEYS[13], KEYS[14], 0, KEYS[15], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], "", "", KEYS[21], KEYS[22], KEYS[23], orderid, "") \
          --[[ get proquote values ]] \
          proquotequote = getproquotequote(KEYS[10], side) \
          --[[ assume uk equity as underlying for default settl days ]] \
          defaultnosettdays = redis.call("hget", "cost:" .. "DE" .. ":" .. KEYS[18] .. ":" .. side, "defaultnosettdays") \
        end \
      end \
    end \
  else \
    --[[ this is an equity - just consider external orders for the time being - todo: internal ]] \
    --[[ todo: consider equity limit orders ]] \
    if markettype == 0 then \
      --[[ see if we need to send this trade to the market - if either product or client hedge, then send ]] \
      local hedgeclient = tonumber(redis.call("hget", "client:" .. KEYS[1], "hedge")) \
      local hedgeinst = tonumber(redis.call("hget", "symbol:" .. KEYS[2], "hedge")) \
      if hedgeclient == 0 and hedgeinst == 0 then \
        --[[ we are taking on the trade, so create trades for client & hedge book ]] \
        hedgebookid = redis.call("get", "hedgebook:" .. instrumenttype .. ":" .. KEYS[18]) \
        if not hedgebookid then hedgebookid = 999999 end \
        local reverseside \
        if side == 1 then \
          reverseside = 2 \
        else \
          reverseside = 1 \
        end \
        local hedgecosts = {0,0,0,0} \
        tradeid = newtrade(KEYS[1], orderid, KEYS[2], side, KEYS[4], KEYS[5], KEYS[11], 1, 1, cc[3], hedgebookid, KEYS[7], "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], cc[2], KEYS[22], KEYS[23], cc[4]) \
        hedgetradeid = newtrade(hedgebookid, orderid, KEYS[2], reverseside, KEYS[4], KEYS[5], KEYS[11], 1, 1, hedgecosts, KEYS[1], KEYS[7], "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], 0, KEYS[22], KEYS[23], 0) \
      else \
        proquotesymbol = getproquotesymbol(KEYS[2]) \
        proquotequote = getproquotequote(KEYS[10], side) \
        defaultnosettdays = redis.call("hget", "cost:" .. instrumenttype .. ":" .. KEYS[18] .. ":" .. side, "defaultnosettdays") \
      end \
    end \
  end \
  return {cc[1], orderid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], proquotequote[1], proquotequote[2], instrumenttype, hedgeorderid, tradeid, hedgetradeid, defaultnosettdays} \
  ';

  // todo: add lastmkt
  // quantity required as number of shares
  // todo: settlcurrency
  scriptmatchorder = addtoorderbook + removefromorderbook + adjustmarginreserve + newtrade + getcosts + '\
  local fields = {"clientid", "symbol", "side", "quantity", "price", "currency", "margin", "remquantity", "futsettdate", "timestamp", "nosettdays"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  local clientid = vals[1] \
  local remquantity = tonumber(vals[8]) \
  if remquantity <= 0 then return "1010" end \
  local instrumenttype = redis.call("hget", "symbol:" .. vals[2], "instrumenttype") \
  local lowerbound \
  local upperbound \
  local matchside \
  if tonumber(vals[3]) == 1 then \
    lowerbound = 0 \
    upperbound = vals[5] \
    matchside = 2 \
  else \
    lowerbound = "-inf" \
    upperbound = "-" .. vals[5] \
    matchside = 1 \
  end \
  local mo = {} \
  local t = {} \
  local mt = {} \
  local mc = {} \
  local j = 1 \
  local matchorders = redis.call("zrangebyscore", vals[2], lowerbound, upperbound) \
  for i = 1, #matchorders do \
    local matchvals = redis.call("hmget", "order:" .. matchorders[i], unpack(fields)) \
    local matchclientid = matchvals[1] \
    local matchremquantity = tonumber(matchvals[8]) \
    if matchremquantity > 0 and matchclientid ~= clientid then \
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
      --[[ adjust order book & update passive order remaining quantity & status ]] \
      local matchorderstatus \
      if matchremquantity == 0 then \
        removefromorderbook(matchvals[2], matchorders[i]) \
        matchorderstatus = 2 \
      else \
        matchorderstatus = 1 \
      end \
      redis.call("hmset", "order:" .. matchorders[i], "remquantity", matchremquantity, "status", matchorderstatus) \
      --[[ adjust margin/reserve for passive order ]] \
      adjustmarginreserve(matchorders[i], matchclientid, matchvals[2], matchvals[3], matchvals[5], matchvals[7], matchvals[6], matchvals[8], matchremquantity, matchvals[9], matchvals[11]) \
      --[[ trade gets done at passive order price ]] \
      local tradeprice = tonumber(matchvals[5]) \
      local consid = tradequantity * tradeprice \
      --[[ create trades for active & passive orders ]] \
      local costs = getcosts(vals[1], vals[2], instrumenttype, vals[3], consid, vals[6]) \
      local matchcosts = getcosts(matchvals[1], vals[2], instrumenttype, matchside, consid, matchvals[6]) \
      local finance = 0 \
      --[[ todo: adjust margin ]] \
      local margin = consid \
      local operatortype = 1 \
      local operatorid = 1 \
      local tradeid = newtrade(clientid, KEYS[1], vals[2], vals[3], tradequantity, tradeprice, vals[6], 1, 1, costs, matchclientid, "1", "", vals[9], vals[10], "", "", vals[6], consid, "", "", vals[11], margin, operatortype, operatorid, finance) \
      local matchtradeid = newtrade(matchclientid, matchorders[i], matchvals[2], matchside, tradequantity, tradeprice, matchvals[6], 1, 1, matchcosts, clientid, "1", "", matchvals[9], vals[10], "", "", vals[6], consid, "", "", matchvals[11], margin, operatortype, operatorid, finance) \
      --[[ update return values ]] \
      mo[j] = matchorders[i] \
      t[j] = tradeid \
      mt[j] = matchtradeid \
      mc[j] = matchclientid \
      j = j + 1 \
    end \
    if remquantity == 0 then break end \
  end \
  local orderstatus = "0" \
  if remquantity > 0 then \
    addtoorderbook(vals[2], KEYS[1], vals[3], vals[5]) \
  else \
    orderstatus = "2" \
  end \
  if remquantity < tonumber(vals[4]) then \
    --[[ reduce margin/reserve that has been added in the credit check ]] \
    adjustmarginreserve(KEYS[1], clientid, vals[2], vals[3], vals[5], vals[7], vals[6], vals[8], remquantity, vals[9], vals[11]) \
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
  //updatetrademargin
  scriptnewtrade = newtrade + getinitialmargin + getcosts + calcfinance + adjustmarginreserve + '\
  local fields = {"clientid", "symbol", "side", "quantity", "price", "margin", "remquantity", "nosettdays", "operatortype", "hedgeorderid", "futsettdate", "operatorid"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  local quantity = tonumber(KEYS[4]) \
  local price = tonumber(KEYS[5]) \
  local consid = quantity * price \
  local instrumenttype = redis.call("hget", "symbol:" .. vals[2], "instrumenttype") \
  local initialmargin = getinitialmargin(vals[2], consid) \
  local costs = getcosts(vals[1], vals[2], instrumenttype, vals[3], consid, KEYS[17]) \
  local finance = calcfinance(instrumenttype, consid, KEYS[17], vals[3], vals[8]) \
  local tradeid = newtrade(vals[1], KEYS[1], vals[2], KEYS[3], quantity, price, KEYS[6], KEYS[7], KEYS[8], costs, KEYS[9], "0", KEYS[10], vals[11], KEYS[12], KEYS[14], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], vals[8], initialmargin, vals[9], vals[12], finance, KEYS[21]) \
  --[[ adjust order related margin/reserve ]] \
  adjustmarginreserve(KEYS[1], vals[1], vals[2], vals[3], vals[5], vals[6], KEYS[17], vals[7], KEYS[15], KEYS[11], vals[8]) \
  --[[ adjust order ]] \
  redis.call("hmset", "order:" .. KEYS[1], "remquantity", KEYS[15], "status", KEYS[13]) \
  --[[ todo: adjust trade related margin ]] \
  --[[updatetrademargin(tradeid, vals[1], KEYS[17], initialmargin[1])]] \
  return {tradeid, vals[9], vals[10]} \
  ';

  scriptordercancelrequest = removefromorderbook + cancelorder + getproquotesymbol + '\
  local errorcode = 0 \
  local orderid = KEYS[2] \
  local ordercancelreqid = redis.call("incr", "ordercancelreqid") \
  --[[ store the order cancel request ]] \
  redis.call("hmset", "ordercancelrequest:" .. ordercancelreqid, "clientid", KEYS[1], "orderid", orderid, "timestamp", KEYS[3], "operatortype", KEYS[4], "operatorid", KEYS[5]) \
  local fields = {"status", "markettype", "symbol", "side", "quantity", "externalorderid"} \
  local vals = redis.call("hmget", "order:" .. orderid, unpack(fields)) \
  local markettype = "" \
  local symbol = "" \
  local side \
  local quantity = "" \
  local externalorderid = "" \
  if vals == nil then \
    --[[ order not found ]] \
    errorcode = 1009 \
  else \
    local status = vals[1] \
    markettype = vals[2] \
    symbol = vals[3] \
    side = vals[4] \
    quantity = vals[5] \
    externalorderid = vals[6] \
    if status == "2" then \
      --[[ already filled ]] \
      errorcode = 1010 \
    elseif status == "4" then \
      --[[ already cancelled ]] \
      errorcode = 1008 \
    elseif status == "8" then \
      --[[ already rejected ]] \
      errorcode = 1012 \
    end \
  end \
  --[[ process according to market type ]] \
  local proquotesymbol = {"", "", ""} \
  if markettype == "1" then \
    if errorcode ~= 0 then \
      redis.call("hset", "ordercancelrequest:" .. ordercancelreqid, "reason", errorcode) \
    else \
      removefromorderbook(symbol, orderid) \
      cancelorder(orderid, "4") \
    end \
  else \
    --[[ if the order is with proquote, it will be forwarded, else if request ok, we can go ahead and cancel the order ]] \
    if externalorderid == "" then \
      if errorcode == 0 then \
        cancelorder(orderid, "4") \
      end \
    else \
      --[[ get required instrument values for proquote ]] \
      if errorcode == 0 then \
        proquotesymbol = getproquotesymbol(symbol) \
      end \
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
  --[[ get the operatortype to enable on-sending ]] \
  local operatortype = redis.call("hget", "order:" .. orderid, "operatortype") \
  return {errorcode, orderid, operatortype} \
  ';

  scriptorderfillrequest = '\
  local errorcode = 0 \
  local orderid = KEYS[2] \
  local fields = {"clientid", "symbol", "side", "quantity", "price", "margin", "remquantity", "nosettdays", "operatortype", "hedgeorderid", "futsettdate", "operatorid", "status", "externalorderid", "settlcurrency", "markettype"} \
  local vals = redis.call("hmget", "order:" .. orderid, unpack(fields)) \
  if vals == nil then \
    --[[ order not found ]] \
    errorcode = 1009 \
  else \
    local status = vals[13] \
    if status == "2" then \
      --[[ filled ]] \
      errorcode = 1010 \
    elseif status == "4" then \
      --[[ cancelled ]] \
      errorcode = 1008 \
    elseif status == "8" then \
      --[[ rejected ]] \
      errorcode = 1012 \
    elseif status == "C" then \
      --[[ expired ]] \
      errorcode = 1022 \
    end \
    local externalorderid = vals[14] \
    if externalorderid == "" then \
      --[[ order held externally, so cannot fill ]] \
      errorcode = 1021 \
    end \
  end \
  local tradeid = 0 \
  if errorcode == 0 then \
    --[[ can only fill the remaining quantity ]] \
    local remquantity = tonumber(vals[7]) \
    --[[ fill at the passed price, may be different from the order price ]] \
    local symbol = vals[2] \
    local price = tonumber(KEYS[6]) \
    --[[ todo: round? ]] \
    local consid = remquantity * price \
    local side = vals[3] \
    local settlcurrency = vals[15] \
    local instrumenttype = redis.call("hget", "symbol:" .. symbol, "instrumenttype") \
    local initialmargin = getinitialmargin(symbol, consid) \
    local costs = getcosts(vals[1], symbol, instrumenttype, side, consid, settlcurrency) \
    local finance = calcfinance(instrumenttype, consid, settlcurrency, side, vals[8]) \
    local hedgebookid = redis.call("get", "hedgebook:" .. instrumenttype .. ":" .. settlcurrency) \
    if not hedgebookid then hedgebookid = 999999 end \
    tradeid = newtrade(vals[1], orderid, symbol, side, quantity, price, settlcurrency, 1, 1, costs, hedgebookid, vals[16], "", KEYS[7], KEYS[3], "", "", settlcurrency, consid, 1, 1, vals[8], initialmargin, vals[9], vals[12], finance) \
  end \
  return {errorcode, tradeid} \
  ';

  scriptorderack = '\
  --[[ update external limit reference ]] \
  redis.call("hmset", "order:" .. KEYS[1], "externalorderid", KEYS[2], "status", KEYS[3], "execid", KEYS[4], "text", KEYS[5]) \
  local operatortype = redis.call("hget", "order:" .. KEYS[1], "operatortype") \
  return operatortype \
  ';

  scriptorderexpire = cancelorder + '\
  local ret = cancelorder(KEYS[1], "C") \
  --[[ get the operatortype to enable on-sending ]] \
  local operatortype = redis.call("hget", "order:" .. KEYS[1], "operatortype") \
  return {ret, operatortype} \
  ';

  scriptquoterequest = getproquotesymbol + '\
  local quotereqid = redis.call("incr", "quotereqid") \
  if not quotereqid then return 1005 end \
  --[[ store the quote request ]] \
  redis.call("hmset", "quoterequest:" .. quotereqid, "clientid", KEYS[1], "symbol", KEYS[2], "quantity", KEYS[3], "cashorderqty", KEYS[4], "currency", KEYS[5], "settlcurrency", KEYS[6], "nosettdays", KEYS[7], "futsettdate", KEYS[8], "quotestatus", "", "timestamp", KEYS[9], "quoteid", "", "quoterejectreason", "", "quotereqid", quotereqid, "operatortype", KEYS[10], "operatorid", KEYS[11], "orderdivnum", KEYS[12]) \
  --[[ add to set of quoterequests for this client ]] \
  redis.call("sadd", KEYS[1] .. ":quoterequests", quotereqid) \
  --[[ get required instrument values for proquote ]] \
  local proquotesymbol = getproquotesymbol(KEYS[2]) \
  local instrumenttype = redis.call("hget", "symbol:" .. KEYS[2], "instrumenttype") \
  --[[ assuming equity buy to get default settlement days ]] \
  local defaultnosettdays = redis.call("hget", "cost:" .. "DE" .. ":" .. KEYS[6] .. ":" .. "1", "defaultnosettdays") \
  return {0, quotereqid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], defaultnosettdays, instrumenttype} \
  ';

  // todo: check proquotsymbol against quote request symbol?
  scriptquote = calcfinance + '\
  local errorcode = 0 \
  local sides = 0 \
  local quoteid = "" \
  --[[ get the quote request ]] \
  local fields = {"clientid", "quoteid", "symbol", "quantity", "cashorderqty", "nosettdays", "settlcurrency", "operatortype", "futsettdate", "orderdivnum"} \
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
    offerfinance = calcfinance(instrumenttype, offerquantity * offerprice, vals[7], 1, vals[6]) \
  else \
    local bidprice = tonumber(KEYS[5]) \
    if vals[4] == "" then \
      bidquantity = round(tonumber(vals[5]) / bidprice, 0) \
    else \
      bidquantity = tonumber(vals[4]) \
    end \
    bidfinance = calcfinance(instrumenttype, bidquantity * bidprice, vals[7], 2, vals[6]) \
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
    redis.call("hmset", "quote:" .. quoteid, "quotereqid", KEYS[1], "clientid", vals[1], "quoteid", quoteid, "bidquoteid", KEYS[2], "offerquoteid", KEYS[3], "symbol", vals[3], "bestbid", bestbid, "bestoffer", bestoffer, "bidpx", KEYS[5], "offerpx", KEYS[6], "bidquantity", bidquantity, "offerquantity", offerquantity, "bidsize", KEYS[7], "offersize", KEYS[8], "validuntiltime", KEYS[9], "transacttime", KEYS[10], "currency", KEYS[11], "settlcurrency", KEYS[12], "bidqbroker", KEYS[13], "offerqbroker", KEYS[14], "nosettdays", vals[6], "futsettdate", vals[9], "bidfinance", bidfinance, "offerfinance", offerfinance, "orderid", "", "bidquotedepth", KEYS[16], "offerquotedepth", KEYS[17]) \
    --[[ keep a list ]] \
    redis.call("sadd", vals[1] .. ":quotes", quoteid) \
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
      redis.call("hmset", "quote:" .. quoteid, "offerquoteid", KEYS[3], "offerpx", KEYS[6], "offerquantity", offerquantity, "offersize", KEYS[8], "offerqbroker", KEYS[14], "offerfinance", offerfinance, "offerquotedepth", KEYS[17])\
    else \
      redis.call("hmset", "quote:" .. quoteid, "bidquoteid", KEYS[2], "bidpx", KEYS[5], "bidquantity", bidquantity, "bidsize", KEYS[7], "bidqbroker", KEYS[13], "bidfinance", bidfinance, "bidquotedepth", KEYS[16]) \
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

  scriptordercancelreject = '\
  --[[ update the order cancel request ]] \
  redis.call("hmset", "ordercancelrequest:" .. KEYS[1], "reason", KEYS[2], "text", KEYS[3]) \
  local operatortype = redis.call("hget", "ordercancelrequest:" .. KEYS[1], "operatortype") \
  return operatortype \
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