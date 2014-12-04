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
//var ptpclient = require('./ptpclient.js'); // Proquote API connection
var externalconn = "NBTrader";
var nbtrader = require('./nbtrader.js'); // NBTrader API connection 
var common = require('./common.js');

// redis
var redishost;
var redisport;
var redisauth;
var redispassword;
var redislocal = true; // local or external server

// globals
//var markettype; // comes from database, 0=normal market, 1=out of hours
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
      } else if ("order" in obj) {
        newOrder(obj.order);
      } else if ("quote" in obj) {
        newQuote(obj.quote);
      } else if ("ordercancelrequest" in obj) {
        orderCancelRequest(obj.ordercancelrequest);
      } else if ("orderfillrequest" in obj) {
        orderFillRequest(obj.orderfillrequest);
      }
    } catch(e) {
      console.log(e);
      return;
    }
  });

  // listen for trade related messages
  dbsub.subscribe(common.tradeserverchannel);
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

// connection to NBTrader
var nbt = new nbtrader.Nbt();
nbt.on("connected", function() {
  console.log("connected to " + externalconn);
});
nbt.on('finished', function(message) {
    console.log(message);
});
nbt.on("initialised", function() {
  connectToTrading();
});

function connectToTrading() {
  // try to connect
  //winner.connect();

  nbt.connect();
}

function quoteRequest(quoterequest) {
  console.log("quoterequest");
  console.log(quoterequest);

  var today = new Date();

  quoterequest.timestamp = common.getUTCTimeStamp(today);

  // get hour & minute for comparison with timezone to determine in/out of hours
  var hour = today.getHours();
  var minute = today.getMinutes();
  var day = today.getDay();

  //todo - remove
  //hour = hour + 4;
  //day = 0;
  //

  // get settlement date from T+n no. of days
  quoterequest.futsettdate = common.getUTCDateString(common.getSettDate(today, quoterequest.nosettdays, holidays));

  // store the quote request & get an id
  db.eval(scriptquoterequest, 14, quoterequest.clientid, quoterequest.symbol, quoterequest.quantity, quoterequest.cashorderqty, quoterequest.currency, quoterequest.settlcurrency, quoterequest.nosettdays, quoterequest.futsettdate, quoterequest.timestamp, quoterequest.operatortype, quoterequest.operatorid, hour, minute, day, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      // todo: send a quote ack to client
      console.log("Error in scriptquoterequest:" + common.getReasonDesc(ret[0]));
      return;
    }
    console.log(ret);

    // add the quote request id & symbol details required for fix connection
    quoterequest.quotereqid = ret[1];
    quoterequest.isin = ret[2];
    quoterequest.proquotesymbol = ret[3];
    quoterequest.exchange = ret[4];

    // match the number of settlement days requested to the default number for this instrument type
    if (quoterequest.nosettdays == ret[5]) {
      quoterequest.settlmnttyp = 0;
    } else if (quoterequest.nosettdays == 2) {
      quoterequest.settlmnttyp = 3;      
    } else if (quoterequest.nosettdays == 3) {
      quoterequest.settlmnttyp = 4;      
    } else if (quoterequest.nosettdays == 4) {
      quoterequest.settlmnttyp = 5;
    } else {
      quoterequest.settlmnttyp = 6; // future, in which case, settlement date is required
    }

    // adjust the quote request to the default equity settlement date
    // the stored settlement date stays as requested
    // the different settlement will be dealt with using finance
    if (ret[6] == "CFD") {
      quoterequest.futsettdate = common.getUTCDateString(common.getSettDate(today, ret[5], holidays));
    }

    quoterequest.markettype = ret[7];

    // see if we are in-hours
    if (quoterequest.markettype == 0) {
      // forward the request
      nbt.quoteRequest(quoterequest);
    } else {
      // publish it
      db.publish(common.quoterequestchannel, "{\"quoterequest\":" + JSON.stringify(quoterequest) + "}");
      //db.publish("quoterequest", "{\"quoterequest\":" + JSON.stringify(quoterequest) + "}");
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

function newOrder(order) {
  var currencyratetoorg = 1; // product currency to org curreny rate
  var currencyindtoorg = 1;
  var settlcurrfxrate = 1; // settlement currency to product currency rate
  var settlcurrfxratecalc = 1;

  console.log("newOrder");
  console.log(order);

  var today = new Date();

  order.timestamp = common.getUTCTimeStamp(today);
  order.partfill = 1; // accept part-fill

  // get hour & minute for comparison with timezone to determine in/out of hours
  var hour = today.getHours();
  var minute = today.getMinutes();
  var day = today.getDay();

  // always put a price in the order
  if (!("price" in order)) {
    order.price = "0";
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
  // note: param #7 not used
  db.eval(scriptneworder, 26, order.clientid, order.symbol, order.side, order.quantity, order.price, order.ordertype, 0, order.futsettdate, order.partfill, order.quoteid, order.currency, currencyratetoorg, currencyindtoorg, order.timestamp, order.timeinforce, order.expiredate, order.expiretime, order.settlcurrency, settlcurrfxrate, settlcurrfxratecalc, order.nosettdays, order.operatortype, order.operatorid, hour, minute, day, function(err, ret) {
    if (err) throw err;

    // credit check failed
    if (ret[0] == 0) {
      console.log("credit check failed, order #" + ret[1]);

      // script will publish the order back to the operator type, with the error
      return;
    }
    console.log(ret);

    // see if the order was client quoted, if so, we are done
    if (ret[13] != "") {
      return;
    }

    // update order details
    order.orderid = ret[1];
    order.instrumenttype = ret[7];
    order.markettype = ret[12];

    // use the returned instrument values required by fix connection
    if (order.markettype == 0) {
      order.isin = ret[2];
      order.proquotesymbol = ret[3];
      order.exchange = ret[4];
    }

    // use the returned quote values required by fix connection
    if (order.ordertype == "D") {
      order.externalquoteid = ret[5];
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

    // todo: sort out publishing
    console.log(ret);
    displayOrderBook(order.symbol, "-inf", "+inf");


    // return the active order to the sending operator type
    //db.publish(order.operatortype, "order:" + order.orderid);

    // send any trades for active order client
    /*for (var i = 0; i < ret[1].length; ++i) {
      db.publish(tradechannel, "trade:" + ret[1][i]);
    }*/

    // send any matched orders - todo: review
    /*for (var i = 0; i < ret[0].length; ++i) {
      db.publish(order.operatortype, "order:" + ret[0][i]);
    }*/

    // send any matched trades
    /*for (var i = 0; i < ret[2].length; ++i) {
      db.publish(tradechannel, "trade:" + ret[2][i]);
    }*/

    // indicate orderbook may have changed - todo: review, maybe send updated orderbook?
    //db.publish(order.operatortype, "orderbookupdate:" + order.symbol);
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

    // forward & wait for outcome
    if (ret[4] != "") {
      ocr.ordercancelreqid = ret[2];
      ocr.symbol = ret[3];
      ocr.isin = ret[4];
      ocr.proquotesymbol = ret[5];
      ocr.exchange = ret[6];
      ocr.side = ret[7];
      ocr.quantity = ret[8];

      nbt.orderCancelRequest(ocr);
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

    // publish the order to the sending server type
    db.publish(ofr.operatortype, "order:" + ofr.orderid);

    // publish the trade
    db.publish(common.tradechannel, "trade:" + ret[1]);
  });
}

function processOrder(order, hedgeorderid, tradeid, hedgetradeid) {
  //
  // the order has been credit checked
  // now, either forward or attempt to match the order, depending on the type of instrument & whether the market is open
  //

  if (order.markettype == 1) {
    console.log("matching");
    matchOrder(order);
  } else {
    console.log("not matching");
    // equity orders
    if (order.instrumenttype == "DE" || order.instrumenttype == "IE") {
      if (tradeid != "") {
        // the order has been executed, so publish the order to the sending server type
        //db.publish(order.operatortype, "order:" + order.orderid);

        // publish the trade
        //db.publish(common.tradechannel, "trade:" + tradeid);

        // publish the hedge
        //db.publish(common.tradechannel, "trade:" + hedgetradeid);
      } else {
        // forward order to the market
        nbt.newOrder(order);
      }
    } else {
      // publish the order to whence it came
      //db.publish(order.operatortype, "order:" + order.orderid);

      // publish the trade if there is one
      /*if (tradeid != "") {
        db.publish(common.tradechannel, "trade:" + tradeid);
      }*/

      // publish any hedge to the client server, so anyone viewing the hedgebook receives it
      /*if (hedgetradeid != "") {
        db.publish(common.tradechannel, "trade:" + hedgetradeid);
      }*/

      // if we are hedging, change the order id to that of the hedge & forward
      if (hedgeorderid != "") {
        order.orderid = hedgeorderid;
        nbt.newOrder(order);
      }
    }
  }
}

function displayOrderBook(symbol, lowerbound, upperbound) {
  db.zrangebyscore("orderbook:" + symbol, lowerbound, upperbound, function(err, matchorders) {
    console.log("order book for instrument " + symbol + " has " + matchorders.length + " order(s)");

    matchorders.forEach(function (matchorderid, i) {
      db.hgetall("order:" + matchorderid, function(err, matchorder) {
        console.log("orderid="+matchorder.orderid+", clientid="+matchorder.clientid+", price="+matchorder.price+", side="+matchorder.side+", remquantity="+matchorder.remquantity);
      });
    });
  });
}

function initDb() {
  common.registerCommonScripts();
  registerScripts();
  loadHolidays();
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

// markettype determined by timezone per instrument
/*function getMarkettype(symbol) {
  db.eval(scriptmarkettype, 1, symbol, function(err, ret) {
    if (err) throw err;

  });

  db.get("markettype", function(err, mkttype) {
    if (err) {
      console.log(err);
      return;
    }

    markettype = parseInt(mkttype);
  });
}*/

// quote received
function newQuote(quote) {
  console.log("newquote");

  if (!('bidpx' in quote)) {
    quote.bidpx = "";
    quote.bidsize = "";
  }
  if (!('offerpx' in quote)) {
    quote.offerpx = "";
    quote.offersize = "";
  }

  if (!('bidquotedepth' in quote)) {
    quote.bidquotedepth = "";
  }
  if (!('offerquotedepth' in quote)) {
    quote.offerquotedepth = "";
  }

  if (!('externalquoteid' in quote)) {
    quote.externalquoteid = "";
  }

  if (!('transacttime' in quote)) {
    var today = new Date();
    quote.transacttime = common.getUTCTimeStamp(today);

    if (!('validuntiltime' in quote)) {
      var validuntiltime = today;
      validuntiltime.setSeconds(today.getSeconds() + quote.noseconds);
      quote.validuntiltime = common.getUTCTimeStamp(validuntiltime);
    }
  }

  if (!('qclientid' in quote)) {
    quote.qclientid = "";
  }

  // quote script
  // note: not passing securityid & idsource as proquote symbol should be enough
  db.eval(scriptquote, 17, quote.quotereqid, quote.symbol, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.transacttime, quote.currency, quote.settlcurrency, quote.qbroker, quote.futsettdate, quote.bidquotedepth, quote.offerquotedepth, quote.externalquoteid, quote.qclientid, quote.cashorderqty, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    if (ret != 0) {
      // can't find quote request, so don't know which client to inform
      console.log("Error in scriptquote:" + common.getReasonDesc(ret[0]));
      return;
    }

    // script publishes quote to the operator type that made the request
  });
}

nbt.on("orderReject", function(exereport) {
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
      // todo: message to operator
      console.log("Error in scriptrejectorder, reason:" + common.getReasonDesc(ret));
      return;
    }

    // order published to operator by script
  });
});

//
// Limit order acknowledgement
//
nbt.on("orderAck", function(exereport) {
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

nbt.on("orderCancel", function(exereport) {
  console.log("Order cancelled externally, ordercancelrequest id:" + exereport.clordid);

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

nbt.on("orderExpired", function(exereport) {
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

nbt.on("orderFill", function(exereport) {
  var currencyratetoorg = 1; // product currency rate back to org 
  var currencyindtoorg = 1;

  console.log(exereport);

  if (!('settlcurrfxrate' in exereport)) {
    exereport.settlcurrfxrate = 1;
  }

  // we don't get this externally - todo: set based on currency pair
  exereport.settlcurrfxratecalc = 1;

  // milliseconds since epoch, used for scoring trades so they can be retrieved in a range
  var milliseconds = new Date().getTime();

  db.eval(scriptnewtrade, 21, exereport.clordid, exereport.symbol, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currency, currencyratetoorg, currencyindtoorg, exereport.execbroker, exereport.execid, exereport.futsettdate, exereport.transacttime, exereport.ordstatus, exereport.lastmkt, exereport.leavesqty, exereport.orderid, exereport.settlcurrency, exereport.settlcurramt, exereport.settlcurrfxrate, exereport.settlcurrfxratecalc, milliseconds, function(err, ret) {
    if (err) {
      console.log(err);
      return
    }

    // script will publish the order & trade
    // order will be published to the operator type
    // trade will be published to the trade channel
  });
});

//
// ordercancelrequest rejected
//
nbt.on("orderCancelReject", function(ordercancelreject) {
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

nbt.on("quote", function(quote, header) {
  console.log("quote received from market");
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
  /*if (!('bidpx' in quote)) {
    quote.bidpx = '';
    quote.bidsize = '';
  }
  if (!('offerpx' in quote)) {
    quote.offerpx = '';
    quote.offersize = '';
  }*/

  newQuote(quote);
});

//
// quote rejection
//
nbt.on("quoteack", function(quoteack) {
  console.log("quote ack, request id " + quoteack.quotereqid);
  console.log(quoteack);

  db.eval(scriptquoteack, 4, quoteack.quotereqid, quoteack.quoteackstatus, quoteack.quoterejectreason, quoteack.text, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    /*if (ret[0] == 1) {
      console.log("already received rejection for this quote request, ignoring");
      return;
    }*/

    // script will publish
    //db.publish(ret[1], "{\"quoteack\":" + JSON.stringify(quoteack) + "}");
  });
});

//
// message error
// todo: get fix message & inform client
//
nbt.on("reject", function(reject) {
  console.log(reject);
  console.log("Error: reject received, fixseqnum:" + reject.refseqnum);
  console.log("Text:" + reject.text);
  console.log("Tag id:" + reject.reftagid);
  console.log("Msg type:" + reject.refmsgtype);
  if ('sessionrejectreason' in reject) {
    var reasondesc = nbt.getSessionRejectReason(reject.sessionrejectreason);
    console.log("Reason:" + reasondesc);
  }
});

nbt.on("businessReject", function(businessreject) {
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

  createposition = '\
  local createposition = function(positionkey, positionskey, postradeskey, clientid, symbol, quantity, costpershare, cost, currency, margin, tradeid) \
    local positionid = redis.call("incr", "positionid") \
    redis.call("hmset", positionkey, "clientid", clientid, "symbol", symbol, "quantity", quantity, "cost", cost, "currency", currency, "margin", margin, "positionid", positionid, "costpershare", costpershare) \
    redis.call("sadd", positionskey, symbol) \
    redis.call("sadd", "position:" .. symbol .. ":clients", clientid) \
    redis.call("sadd", postradeskey, tradeid) \
    return positionid \
  end \
  ';

  // remove position
  closeposition = '\
  local closeposition = function(positionkey, positionskey, postradeskey, clientid, symbol, tradeid) \
    redis.call("hdel", positionkey, "clientid", "symbol", "side", "quantity", "cost", "currency", "margin", "positionid", "costpershare") \
    redis.call("srem", positionskey, symbol) \
    redis.call("srem", "position:" .. symbol .. ":clients", clientid) \
    local postrades = redis.call("smembers", postradeskey) \
    for index = 1, #postrades do \
      redis.call("srem", postradeskey, postrades[index]) \
    end \
  end \
  ';

  publishposition = common.getunrealisedpandl + '\
  local publishposition = function(clientid, symbol, channel) \
    local fields = {"quantity", "cost", "currency", "margin", "positionid", "costpershare"} \
    local vals = redis.call("hmget", clientid .. ":position:" .. symbol, unpack(fields)) \
    local pos = {} \
    if vals[1] then \
      --[[ value the position ]] \
      local unrealisedpandl = getunrealisedpandl(symbol, vals[1], vals[2]) \
      pos = {clientid=clientid,symbol=symbol,quantity=vals[1],cost=vals[2],currency=vals[3],margin=vals[4],positionid=vals[5],costpershare=vals[6],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]} \
    else \
      pos = {clientid=clientid,symbol=symbol,quantity=0} \
    end \
    redis.call("publish", channel, "{" .. cjson.encode("position") .. ":" .. cjson.encode(pos) .. "}") \
  end \
  ';

  //
  // positions are keyed on clientid + symbol - quantity is stored as a +ve/-ve value
  // they are linked to a list of trade ids to provide further information, such as settlement date, if required
  // a position id is allocated against a position and stored against the trade
  //
  updateposition = round + closeposition + createposition + publishposition + '\
  local updateposition = function(clientid, symbol, side, tradequantity, tradeprice, tradecost, currency, trademargin, tradeid) \
    local positionkey = clientid .. ":position:" .. symbol \
    local positionskey = clientid .. ":positions" \
    local postradeskey = clientid .. ":trades:" .. symbol \
    local posqty = 0 \
    local poscost = 0 \
    local posmargin = 0 \
    local costpershare = 0 \
    local positionid = "" \
    side = tonumber(side) \
    local fields = {"quantity", "cost", "margin", "costpershare", "positionid"} \
    local vals = redis.call("hmget", positionkey, unpack(fields)) \
    --[[ do we already have a position? ]] \
    if vals[1] then \
      positionid = vals[5] \
      posqty = tonumber(vals[1]) \
      tradequantity = tonumber(tradequantity) \
      if side == 1 then \
        if posqty >= 0 then \
          --[[ we are adding to an existing long position ]] \
          posqty = posqty + tradequantity \
          poscost = tonumber(vals[2]) + tonumber(tradecost) \
          posmargin = tonumber(vals[3]) + tonumber(trademargin) \
          costpershare = poscost / posqty \
          costpershare = round(costpershare, 5) \
          --[[ update the position & add the trade to the set ]] \
          redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost, "margin", posmargin, "costpershare", costpershare) \
          redis.call("sadd", postradeskey, tradeid) \
        elseif tradequantity == math.abs(posqty) then \
          --[[ just close position ]] \
          closeposition(positionkey, positionskey, postradeskey, clientid, symbol, tradeid) \
        elseif tradequantity > math.abs(posqty) then \
          --[[ close position ]] \
          closeposition(positionkey, positionskey, postradeskey, clientid, symbol, tradeid) \
          --[[ & open new ]] \
          posqty = tradequantity + posqty \
          poscost = posqty * tonumber(tradeprice) \
          poscost = round(poscost, 5) \
          posmargin = posqty / tradequantity * tonumber(trademargin) \
          posmargin = round(posmargin, 5) \
          costpershare = tradeprice \
          positionid = createposition(positionkey, positionskey, postradeskey, clientid, symbol, posqty, costpershare, poscost, currency, posmargin, tradeid) \
        else \
          --[[ part-fill ]] \
          posqty = posqty + tradequantity \
          poscost = posqty * tonumber(vals[4]) \
          poscost = round(poscost, 5) \
          posmargin = posqty / tonumber(vals[1]) * tonumber(vals[3]) \
          posmargin = round(posmargin, 5) \
          costpershare = vals[4] \
          redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost, "margin", posmargin, "costpershare", costpershare) \
          redis.call("sadd", postradeskey, tradeid) \
        end \
      else \
        if posqty <= 0 then \
          --[[ we are adding to an existing short quantity ]] \
          posqty = posqty - tradequantity \
          poscost = tonumber(vals[2]) + tonumber(tradecost) \
          posmargin = tonumber(vals[3]) + tonumber(trademargin) \
          costpershare = poscost / posqty \
          costpershare = round(costpershare, 5) \
          --[[ update the position & add the trade to the set ]] \
          redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost, "margin", posmargin, "costpershare", costpershare) \
          redis.call("sadd", postradeskey, tradeid) \
        elseif tradequantity == posqty then \
          --[[ just close position ]] \
          closeposition(positionkey, positionskey, postradeskey, clientid, symbol, tradeid) \
        elseif tradequantity > posqty then \
          --[[ close position ]] \
          closeposition(positionkey, positionskey, postradeskey, clientid, symbol, tradeid) \
          --[[ & open new ]] \
          posqty = tradequantity - posqty \
          poscost = posqty * tonumber(tradeprice) \
          poscost = round(poscost, 5) \
          posmargin = posqty / tonumber(tradequantity) * tonumber(trademargin) \
          posmargin = round(posmargin, 5) \
          costpershare = tradeprice \
          positionid = createposition(positionkey, positionskey, postradeskey, clientid, symbol, posqty, costpershare, poscost, currency, posmargin, tradeid) \
        else \
          --[[ part-fill ]] \
          posqty = posqty - tradequantity \
          poscost = posqty * tonumber(vals[4]) \
          poscost = round(poscost, 5) \
          posmargin = posqty / tonumber(vals[1]) * tonumber(vals[3]) \
          posmargin = round(posmargin, 5) \
          costpershare = vals[4] \
          redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost, "margin", posmargin, "costpershare", costpershare) \
          redis.call("sadd", postradeskey, tradeid) \
        end \
      end \
    else \
      --[[ new position ]] \
      if side == 1 then \
        posqty = tradequantity \
      else \
        posqty = -tradequantity \
      end \
      positionid = createposition(positionkey, positionskey, postradeskey, clientid, symbol, posqty, tradeprice, tradecost, currency, trademargin, tradeid) \
    end \
    publishposition(clientid, symbol, 10) \
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
  // returns: isin, proquote symbol, market, hedge symbol & market type
  //
  getproquotesymbol = '\
  local getproquotesymbol = function(symbol) \
    local symbolkey = "symbol:" .. symbol \
    local fields = {"isin", "proquotesymbol", "market", "hedgesymbol", "instrumenttype", "timezone"} \
    local vals = redis.call("hmget", symbolkey, unpack(fields)) \
    return {vals[1], vals[2], vals[3], vals[4], vals[5], vals[6]} \
  end \
  ';

  //
  // get proquote quote id & quoting rsp
  //
  getproquotequote = '\
    local getproquotequote = function(quoteid) \
      local quotekey = "quote:" .. quoteid \
      local pqquoteid = redis.call("hget", quotekey, "externalquoteid") \
      local qbroker = redis.call("hget", quotekey, "qbroker") \
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
    redis.call("zadd", "orderbook:" .. symbol, price, orderid) \
    redis.call("sadd", "orderbooks", symbol) \
  end \
  ';

  removefromorderbook = '\
  local removefromorderbook = function(symbol, orderid) \
    redis.call("zrem", "orderbook:" .. symbol, orderid) \
    if (redis.call("zcount", symbol, "-inf", "+inf") == 0) then \
      --[[ todo: dont think so ]] \
      redis.call("srem", "orderbooks", symbol) \
    end \
  end \
  ';

  publishquoteack = '\
  local publishquoteack = function(quotereqid) \
    local fields = {"clientid", "symbol", "operatortype", "quotestatus", "quoterejectreason", "text"} \
    local vals = redis.call("hmget", "quoterequest:" .. quotereqid, unpack(fields)) \
    local quoteack = {quotereqid=quotereqid, clientid=vals[1], symbol=vals[2], quotestatus=vals[4], quoterejectreason=vals[5], text=vals[6]} \
    redis.call("publish", vals[3], "{" .. cjson.encode("quoteack") .. ":" .. cjson.encode(quoteack) .. "}") \
    end \
  ';

  publishquote = '\
  local publishquote = function(quoteid, channel) \
    local fields = {"quotereqid","clientid","quoteid","symbol","bidpx","offerpx","bidquantity","offerquantity","validuntiltime","transacttime","settlcurrency","nosettdays","futsettdate","bidsize","offersize","qclientid"} \
    local vals = redis.call("hmget", "quote:" .. quoteid, unpack(fields)) \
    local quote = {quotereqid=vals[1],clientid=vals[2],quoteid=vals[3],symbol=vals[4],bidpx=vals[5],offerpx=vals[6],bidquantity=vals[7],offerquantity=vals[8],validuntiltime=vals[9],transacttime=vals[10],settlcurrency=vals[11],nosettdays=vals[12],futsettdate=vals[13],bidsize=vals[14],offersize=vals[15],qclientid=vals[16]} \
    redis.call("publish", channel, "{" .. cjson.encode("quote") .. ":" .. cjson.encode(quote) .. "}") \
  end \
  ';

  publishorder = '\
  local publishorder = function(orderid, channel) \
    local fields = {"clientid","symbol","side","quantity","price","ordertype","remquantity","status","markettype","futsettdate","partfill","quoteid","currency","currencyratetoorg","currencyindtoorg","timestamp","margin","timeinforce","expiredate","expiretime","settlcurrency","settlcurrfxrate","settlcurrfxratecalc","orderid","externalorderid","execid","nosettdays","operatortype","operatorid","hedgeorderid","orderdivnum","text"} \
    local vals = redis.call("hmget", "order:" .. orderid, unpack(fields)) \
    local order = {clientid=vals[1],symbol=vals[2],side=vals[3],quantity=vals[4],price=vals[5],ordertype=vals[6],remquantity=vals[7],status=vals[8],markettype=vals[9],futsettdate=vals[10],partfill=vals[11],quoteid=vals[12],currency=vals[13],currencyratetoorg=vals[14],currencyindtoorg=vals[15],timestamp=vals[16],margin=vals[17],timeinforce=vals[18],expiredate=vals[19],expiretime=vals[20],settlcurrency=vals[21],settlcurrfxrate=vals[22],settlcurrfxratecalc=vals[23],orderid=vals[24],externalorderid=vals[25],execid=vals[26],nosettdays=vals[27],operatortype=vals[28],operatorid=vals[29],hedgeorderid=vals[30],orderdivnum=vals[31],text=vals[32]} \
    redis.call("publish", channel, "{" .. cjson.encode("order") .. ":" .. cjson.encode(order) .. "}") \
  end \
  ';

  publishtrade = '\
  local publishtrade = function(tradeid, channel) \
    local fields = {"clientid","orderid","symbol","side","quantity","price","currency","currencyratetoorg","currencyindtoorg","commission","ptmlevy","stampduty","contractcharge","counterpartyid","markettype","externaltradeid","futsettdate","timestamp","lastmkt","externalorderid","tradeid","settlcurrency","settlcurramt","settlcurrfxrate","settlcurrfxratecalc","nosettdays","finance","margin","positionid"} \
    local vals = redis.call("hmget", "trade:" .. tradeid, unpack(fields)) \
    local quoteid = redis.call("hget", "order:" .. vals[2], "quoteid") \
    local trade = {clientid=vals[1],orderid=vals[2],symbol=vals[3],side=vals[4],quantity=vals[5],price=vals[6],currency=vals[7],currencyratetoorg=vals[8],currencyindtoorg=vals[9],commission=vals[10],ptmlevy=vals[11],stampduty=vals[12],contractcharge=vals[13],counterpartyid=vals[14],markettype=vals[15],externaltradeid=vals[16],futsettdate=vals[17],timestamp=vals[18],lastmkt=vals[19],externalorderid=vals[20],tradeid=vals[21],settlcurrency=vals[22],settlcurramt=vals[23],settlcurrfxrate=vals[24],settlcurrfxratecalc=vals[25],nosettdays=vals[26],finance=vals[27],margin=vals[28],positionid=vals[29],quoteid=quoteid} \
    redis.call("publish", channel, "{" .. cjson.encode("trade") .. ":" .. cjson.encode(trade) .. "}") \
  end \
  ';

  newtrade = updateposition + updatecash + publishtrade + '\
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
    publishtrade(tradeid, 6) \
    return tradeid \
  end \
  ';

  neworder = '\
  local neworder = function(clientid, symbol, side, quantity, price, ordertype, remquantity, status, markettype, futsettdate, partfill, quoteid, currency, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforce, expiredate, expiretime, settlcurrency, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, nosettdays, operatortype, operatorid, hedgeorderid) \
    --[[ get a new orderid & store the order ]] \
    local orderid = redis.call("incr", "orderid") \
    redis.call("hmset", "order:" .. orderid, "clientid", clientid, "symbol", symbol, "side", side, "quantity", quantity, "price", price, "ordertype", ordertype, "remquantity", remquantity, "status", status, "markettype", markettype, "futsettdate", futsettdate, "partfill", partfill, "quoteid", quoteid, "currency", currency, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "timestamp", timestamp, "margin", margin, "timeinforce", timeinforce, "expiredate", expiredate, "expiretime", expiretime, "settlcurrency", settlcurrency, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "orderid", orderid, "externalorderid", externalorderid, "execid", execid, "nosettdays", nosettdays, "operatortype", operatortype, "operatorid", operatorid, "hedgeorderid", hedgeorderid) \
    --[[ add to set of orders for this client ]] \
    redis.call("sadd", clientid .. ":orders", orderid) \
    --[[ add order id to associated quote, if there is one ]] \
    if quoteid ~= "" then \
      redis.call("hset", "quote:" .. quoteid, "orderid", orderid) \
    end \
    return orderid \
  end \
  ';

  reverseside = '\
  local reverseside = function(side) \
    local rside \
    if side == 1 then \
      rside = 2 \
    else \
      rside = 1 \
    end \
    return rside \
  end \
  ';

  // compare hour & minute with timezone open/close times to determine in/out of hours - 0=in hours, 1=ooh
  /*getmarkettype = '\
  local getmarkettype = function(timezone, hour, minute) \
    local markettype = 0 \
    local fields = {"openhour","openminute","closehour","closeminute"} \
    local vals = redis.call("hmget", "timezone:" .. timezone, unpack(fields)) \
    if tonumber(hour) < tonumber(vals[1]) or tonumber(hour) > tonumber(vals[3]) then \
      markettype = 1 \
    elseif tonumber(hour) == tonumber(vals[1]) and tonumber(minute) < tonumber(vals[2]) then \
      markettype = 1 \
    elseif tonumber(hour) == tonumber(vals[3]) and tonumber(minute) > tonumber(vals[4]) then \
      markettype = 1 \
    end \
    return markettype \
  end \
  ';*/

  //
  // order rejected externally
  // params: order id, reason, text
  //
  scriptrejectorder = rejectorder + adjustmarginreserve + publishorder + '\
  rejectorder(KEYS[1], KEYS[2], KEYS[3]) \
  local fields = {"clientid", "symbol", "side", "price", "margin", "settlcurrency", "remquantity", "futsettdate", "nosettdays", "operatortype"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  if not vals[1] then \
    return 1009 \
  end \
  adjustmarginreserve(KEYS[1], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7], 0, vals[8], vals[9]) \
  publishorder(KEYS[1], vals[10]) \
  return 0 \
  ';

  // note: param #7, markettype, not used - getmarkettype() used instead
  scriptneworder = neworder + creditcheck + newtrade + getproquotesymbol + getproquotequote + publishorder + getmarkettype + reverseside + '\
  local orderid = neworder(KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[4], "0", KEYS[7], KEYS[8], KEYS[9], KEYS[10], KEYS[11], KEYS[12], KEYS[13], KEYS[14], 0, KEYS[15], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], "", "", KEYS[21], KEYS[22], KEYS[23], "") \
  local symbol = KEYS[2] \
  local side = tonumber(KEYS[3]) \
  --[[ calculate consideration in settlement currency, costs will be added later ]] \
  local settlcurramt = tonumber(KEYS[4]) * tonumber(KEYS[5]) \
  --[[ get instrument values ]] \
  local fields = {"instrumenttype", "timezone", "hedge"} \
  local vals = redis.call("hmget", "symbol:" .. symbol, unpack(fields)) \
  if not vals[2] then \
    rejectorder(orderid, 1007, "") \
    publishorder(orderid, KEYS[22]) \
    return {0} \
  end \
  local instrumenttype = vals[1] \
  local hedge = vals[2] \
  local markettype = getmarkettype(symbol, KEYS[24], KEYS[25], KEYS[26]) \
  --[[ do the credit check ]] \
  local cc = creditcheck(orderid, KEYS[1], symbol, side, KEYS[4], KEYS[5], KEYS[18], KEYS[8], instrumenttype, KEYS[21]) \
  --[[ cc[1] = ok/fail, cc[2] = margin, cc[3] = costs array, cc[4] = finance ]] \
  if cc[1] == 0 then \
    --[[ publish the order back to the operatortype - the order contains the error ]] \
    publishorder(orderid, KEYS[22]) \
    return {cc[1], orderid} \
  end \
  local proquotesymbol = {"", "", "", ""} \
  local proquotequote = {"", ""} \
  local hedgebookid = "" \
  local tradeid = "" \
  local hedgeorderid = "" \
  local hedgetradeid = "" \
  local defaultnosettdays = 0 \
  local qclientid = "" \
  if KEYS[6] == "D" then \
  --[[ see if order was quoted by a client ]] \
    qclientid = redis.call("hget", "quote:" .. KEYS[10], "qclientid") \
    if qclientid and qclientid ~= "" then \
      local hedgecosts = {0,0,0,0} \
      local rside = reverseside(side) \
      --[[ create trades for both clients ]] \
      tradeid = newtrade(KEYS[1], orderid, symbol, side, KEYS[4], KEYS[5], KEYS[11], 1, 1, cc[3], qclientid, markettype, "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], cc[2], KEYS[22], KEYS[23], cc[4]) \
      hedgetradeid = newtrade(qclientid, orderid, symbol, rside, KEYS[4], KEYS[5], KEYS[11], 1, 1, hedgecosts, KEYS[1], markettype, "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], 0, KEYS[22], KEYS[23], 0) \
      --[[ adjust order as filled ]] \
      redis.call("hmset", "order:" .. orderid, "remquantity", 0, "status", 2) \
      return {cc[1], orderid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], proquotequote[1], proquotequote[2], instrumenttype, hedgeorderid, tradeid, hedgetradeid, defaultnosettdays, markettype, qclientid} \
    end \
  end \
  if instrumenttype == "CFD" or instrumenttype == "SPB" or instrumenttype == "CCFD" then \
    --[[ ignore limit orders for derivatives as they will be handled manually, at least for the time being ]] \
    if KEYS[6] ~= "2" then \
      --[[ create trades for client & hedge book for off-exchange products ]] \
      hedgebookid = redis.call("get", "hedgebook:" .. instrumenttype .. ":" .. KEYS[18]) \
      if not hedgebookid then hedgebookid = 999999 end \
      local rside = reverseside(side) \
      local hedgecosts = {0,0,0,0} \
      tradeid = newtrade(KEYS[1], orderid, symbol, side, KEYS[4], KEYS[5], KEYS[11], 1, 1, cc[3], hedgebookid, markettype, "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], cc[2], KEYS[22], KEYS[23], cc[4]) \
      hedgetradeid = newtrade(hedgebookid, orderid, symbol, rside, KEYS[4], KEYS[5], KEYS[11], 1, 1, hedgecosts, KEYS[1], markettype, "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], 0, KEYS[22], KEYS[23], 0) \
      --[[ adjust order as filled ]] \
      redis.call("hmset", "order:" .. orderid, "remquantity", 0, "status", 2) \
      --[[ todo: may need to adjust margin here ]] \
      --[[ see if we need to hedge this trade in the market ]] \
      local hedgeclient = tonumber(redis.call("hget", "client:" .. KEYS[1], "hedge")) \
      local hedgeinst = tonumber(redis.call("hget", "symbol:" .. symbol, "hedge")) \
      if hedgeclient == 1 or hedgeinst == 1 then \
        --[[ create a hedge order in the underlying product ]] \
        proquotesymbol = getproquotesymbol(symbol) \
        if proquotesymbol[4] then \
          hedgeorderid = neworder(hedgebookid, proquotesymbol[4], KEYS[3], KEYS[4], KEYS[5], "X", KEYS[4], 0, markettype, KEYS[8], KEYS[9], "", KEYS[11], KEYS[12], KEYS[13], KEYS[14], 0, KEYS[15], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], "", "", KEYS[21], KEYS[22], KEYS[23], orderid, "") \
          --[[ get quote broker ]] \
          proquotequote = getproquotequote(KEYS[10]) \
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
        local rside = reverseside(side) \
        local hedgecosts = {0,0,0,0} \
        tradeid = newtrade(KEYS[1], orderid, KEYS[2], side, KEYS[4], KEYS[5], KEYS[11], 1, 1, cc[3], hedgebookid, markettype, "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], cc[2], KEYS[22], KEYS[23], cc[4]) \
        hedgetradeid = newtrade(hedgebookid, orderid, KEYS[2], rside, KEYS[4], KEYS[5], KEYS[11], 1, 1, hedgecosts, KEYS[1], markettype, "", KEYS[8], KEYS[14], "", "", KEYS[18], settlcurramt, KEYS[19], KEYS[20], KEYS[21], 0, KEYS[22], KEYS[23], 0) \
      else \
        proquotesymbol = getproquotesymbol(KEYS[2]) \
        proquotequote = getproquotequote(KEYS[10]) \
        defaultnosettdays = redis.call("hget", "cost:" .. instrumenttype .. ":" .. KEYS[18] .. ":" .. side, "defaultnosettdays") \
      end \
    end \
  end \
  return {cc[1], orderid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], proquotequote[1], proquotequote[2], instrumenttype, hedgeorderid, tradeid, hedgetradeid, defaultnosettdays, markettype, qclientid} \
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
  local matchorders = redis.call("zrangebyscore", "orderbook:" .. vals[2], lowerbound, upperbound) \
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
  // fill from the market
  //
  scriptnewtrade = newtrade + getinitialmargin + getcosts + calcfinance + adjustmarginreserve + publishorder + '\
  local orderid = KEYS[1] \
  local fields = {"clientid", "symbol", "side", "quantity", "price", "margin", "remquantity", "nosettdays", "operatortype", "hedgeorderid", "futsettdate", "operatorid"} \
  local vals = redis.call("hmget", "order:" .. orderid, unpack(fields)) \
  local quantity = tonumber(KEYS[4]) \
  local price = tonumber(KEYS[5]) \
  local consid = quantity * price \
  local instrumenttype = redis.call("hget", "symbol:" .. vals[2], "instrumenttype") \
  local initialmargin = getinitialmargin(vals[2], consid) \
  local costs = getcosts(vals[1], vals[2], instrumenttype, vals[3], consid, KEYS[17]) \
  local finance = calcfinance(instrumenttype, consid, KEYS[17], vals[3], vals[8]) \
  local tradeid = newtrade(vals[1], orderid, vals[2], KEYS[3], quantity, price, KEYS[6], KEYS[7], KEYS[8], costs, KEYS[9], "0", KEYS[10], vals[11], KEYS[12], KEYS[14], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], vals[8], initialmargin, vals[9], vals[12], finance, KEYS[21]) \
  --[[ adjust order related margin/reserve ]] \
  adjustmarginreserve(orderid, vals[1], vals[2], vals[3], vals[5], vals[6], KEYS[17], vals[7], KEYS[15], KEYS[11], vals[8]) \
  --[[ adjust order ]] \
  redis.call("hmset", "order:" .. orderid, "remquantity", KEYS[15], "status", KEYS[13]) \
  --[[ todo: adjust trade related margin ]] \
  --[[updatetrademargin(tradeid, vals[1], KEYS[17], initialmargin[1])]] \
  publishorder(orderid, vals[9]) \
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

  scriptquoterequest = getproquotesymbol + getmarkettype + '\
  local quotereqid = redis.call("incr", "quotereqid") \
  if not quotereqid then return 1005 end \
  --[[ store the quote request ]] \
  redis.call("hmset", "quoterequest:" .. quotereqid, "clientid", KEYS[1], "symbol", KEYS[2], "quantity", KEYS[3], "cashorderqty", KEYS[4], "currency", KEYS[5], "settlcurrency", KEYS[6], "nosettdays", KEYS[7], "futsettdate", KEYS[8], "quotestatus", "", "timestamp", KEYS[9], "quoterejectreason", "", "quotereqid", quotereqid, "operatortype", KEYS[10], "operatorid", KEYS[11]) \
  --[[ add to set of quoterequests for this client & open ]] \
  redis.call("sadd", KEYS[1] .. ":quoterequests", quotereqid) \
  redis.call("sadd", "openquoterequests", quotereqid) \
  --[[ get required instrument values for proquote ]] \
  local proquotesymbol = getproquotesymbol(KEYS[2]) \
  local markettype = getmarkettype(KEYS[2], KEYS[12], KEYS[13], KEYS[14]) \
  --[[ assuming equity buy to get default settlement days ]] \
  local defaultnosettdays = redis.call("hget", "cost:" .. "DE" .. ":" .. KEYS[6] .. ":" .. "1", "defaultnosettdays") \
  return {0, quotereqid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], defaultnosettdays, proquotesymbol[5], markettype} \
  ';

  scriptquote = calcfinance + publishquote + '\
  local errorcode = 0 \
  local quoteid = "" \
  --[[ get the quote request ]] \
  local fields = {"clientid", "quoteid", "symbol", "quantity", "cashorderqty", "nosettdays", "settlcurrency", "operatortype", "futsettdate", "orderdivnum"} \
  local vals = redis.call("hmget", "quoterequest:" .. KEYS[1], unpack(fields)) \
  if not vals[1] then \
    errorcode = 1014 \
    return errorcode \
  end \
  local symbol = vals[3] \
  local instrumenttype = redis.call("hget", "symbol:" .. symbol, "instrumenttype") \
  local bidquantity = "" \
  local offerquantity = "" \
  local bidfinance = 0 \
  local offerfinance = 0 \
  --[[ calculate the quantity from the cashorderqty, if necessary, & calculate any finance ]] \
  if KEYS[3] == "" then \
    local offerprice = tonumber(KEYS[4]) \
    if vals[4] == "" then \
      offerquantity = tonumber(KEYS[6]) \
      --[[ offerquantity = round(tonumber(cashorderqty) / offerprice, 0) ]] \
    else \
      offerquantity = tonumber(vals[4]) \
    end \
    offerfinance = calcfinance(instrumenttype, offerquantity * offerprice, vals[7], 1, vals[6]) \
  else \
    local bidprice = tonumber(KEYS[3]) \
    if vals[4] == "" then \
      bidquantity = tonumber(KEYS[5]) \
      --[[ bidquantity = round(tonumber(cashorderqty) / bidprice, 0) ]] \
    else \
      bidquantity = tonumber(vals[4]) \
    end \
    bidfinance = calcfinance(instrumenttype, bidquantity * bidprice, vals[7], 2, vals[6]) \
  end \
  --[[ get touch prices - using delayed - todo: may need to look up delayed/live ]] \
  local bestbid = "" \
  local bestoffer = "" \
  local pricefields = {"bid", "ask"} \
  local pricevals = redis.call("hmget", "price:" .. symbol, unpack(pricefields)) \
  if pricevals[1] then \
    bestbid = pricevals[1] \
    bestoffer = pricevals[2] \
  end \
  --[[ create a quote id as different from external quote ids (one for bid, one for offer)]] \
  quoteid = redis.call("incr", "quoteid") \
  --[[ store the quote ]] \
  redis.call("hmset", "quote:" .. quoteid, "quotereqid", KEYS[1], "clientid", vals[1], "quoteid", quoteid, "symbol", symbol, "bestbid", bestbid, "bestoffer", bestoffer, "bidpx", KEYS[3], "offerpx", KEYS[4], "bidquantity", bidquantity, "offerquantity", offerquantity, "bidsize", KEYS[5], "offersize", KEYS[6], "validuntiltime", KEYS[7], "transacttime", KEYS[8], "currency", KEYS[9], "settlcurrency", KEYS[10], "qbroker", KEYS[11], "nosettdays", vals[6], "futsettdate", vals[9], "bidfinance", bidfinance, "offerfinance", offerfinance, "orderid", "", "bidquotedepth", KEYS[13], "offerquotedepth", KEYS[14], "externalquoteid", KEYS[15], "qclientid", KEYS[16], "cashorderqty", KEYS[17]) \
  --[[ keep a list of quotes for the quoterequest ]] \
  redis.call("sadd", "quoterequest:" .. KEYS[1] .. ":quotes", quoteid) \
  --[[ quoterequest status - 0=new, 1=quoted, 2=rejected ]] \
  local status \
  --[[ bid or offer size needs to be non-zero ]] \
  if KEYS[5] == 0 and KEYS[6] == 0 then \
    status = "5" \
  else \
    status = "0" \
  end \
  --[[ add status to stored quoterequest ]] \
  redis.call("hmset", "quoterequest:" .. KEYS[1], "quotestatus", status) \
  --[[ publish quote to operator type ]] \
  publishquote(quoteid, vals[8]) \
  return errorcode \
  ';

  scriptquoteack = publishquoteack + '\
  --[[ update quote request ]] \
  redis.call("hmset", "quoterequest:" .. KEYS[1], "quotestatus", KEYS[2], "quoterejectreason", KEYS[3], "text", KEYS[4]) \
  --[[ publish to operator type ]] \
  publishquoteack(KEYS[1]) \
  return \
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