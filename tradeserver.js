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
var common = require('./commonfo.js');

// redis
var redishost;
var redisport;
var redisauth;
var redispassword;
var redislocal = true; // local or external server

// globals
var testmode; // 0 = off, 1 = on
var quoteinterval = null;

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

function startQuoteInterval() {
  quoteinterval = setInterval(quoteInterval, 1000);
}

function stopQuoteInterval() {
  clearInterval(quoteinterval);
  quoteinterval = null;
}

function quoteInterval() {
  db.eval(scriptquotemonitor, 0, function(err, ret) {
    if (err) throw err;

    if (ret == 0 && quoteinterval != null) {
      stopQuoteInterval();
    }
  });
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

  // use settlement date, if specified
  if (quoterequest.futsettdate != "") {
    quoterequest.settlmnttyp = 6; // future date
  } else if (quoterequest.nosettdays == 2) {
    quoterequest.settlmnttyp = 3;      
  } else if (quoterequest.nosettdays == 3) {
    quoterequest.settlmnttyp = 4;      
  } else if (quoterequest.nosettdays == 4) {
    quoterequest.settlmnttyp = 5;
  } else if (quoterequest.nosettdays > 5) {
    // get settlement date from T+n no. of days
    quoterequest.futsettdate = common.getUTCDateString(common.getSettDate(today, quoterequest.nosettdays, holidays));
    quoterequest.settlmnttyp = 6;
  } else {
    // default
    quoterequest.settlmnttyp = 0;
  }

  // store the quote request & get an id
  db.eval(scriptquoterequest, 4, "quotereqid", "quoterequest:", ":quoterequests", "openquoterequests", quoterequest.clientid, quoterequest.symbolid, quoterequest.quantity, quoterequest.cashorderqty, quoterequest.currencyid, quoterequest.settlcurrencyid, quoterequest.nosettdays, quoterequest.futsettdate, quoterequest.timestamp, quoterequest.operatortype, quoterequest.operatorid, hour, minute, day, quoterequest.side, function(err, ret) {
    if (err) throw err;

    if (ret[0] != 0) {
      // todo: send a quote ack to client
      console.log("Error in scriptquoterequest:" + common.getReasonDesc(ret[0]));
      return;
    }

    // add the quote request id & symbol details required for fix connection
    quoterequest.quotereqid = ret[1];
    quoterequest.isin = ret[2];
    quoterequest.mnemonic = ret[3];
    quoterequest.exchangeid = ret[4];
    quoterequest.markettype = ret[7];

    if (testmode == "1") {
      console.log("test response");
      testQuoteResponse(quoterequest);
    } else {
      // see if we are in-hours
      if (quoterequest.markettype == 0) {
        console.log("forwarding to nbt");

        // forward the request
        nbt.quoteRequest(quoterequest);
      } else {
        console.log("publishing");
        // publish it to other clients
        db.publish(common.quoterequestchannel, "{\"quoterequest\":" + JSON.stringify(quoterequest) + "}");
      }
    }
  });
}

//
// just testing
//
function testQuoteResponse(quoterequest) {
  if (quoterequest.quantity == 99) {
    testQuoteAck(quoterequest);
  } else {
    if (quoterequest.side == 1 || quoterequest.side == 2) {
      // one side
      testQuote(quoterequest, quoterequest.side);
    } else {
      // bid & offer
      testQuote(quoterequest, 1);
      testQuote(quoterequest, 2);
    }
  }
}

//
// publish a test quote rejection
//
function testQuoteAck(quoterequest) {
  var quoteack = {};

  quoteack.quotereqid = quoterequest.quotereqid;
  quoteack.quoteackstatus = "";
  quoteack.quoterejectreason = "";
  quoteack.text = "too much toblerone";

  db.eval(scriptquoteack, 4, quoteack.quotereqid, quoteack.quoteackstatus, quoteack.quoterejectreason, quoteack.text, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }
  });
}

function testQuote(quoterequest, side) {
  var quote = {};

  if (side == 2) {
    quote.bidpx = "1.23";
    quote.bidsize = quoterequest.quantity;
    quote.bidquotedepth = "1";
    quote.offerpx = "";
    quote.offersize = "";
    quote.offerquotedepth = "";
  } else {
    quote.bidpx = "";
    quote.bidsize = "";
    quote.bidquotedepth = "";
    quote.offerpx = "1.25";
    quote.offersize = quoterequest.quantity;
    quote.offerquotedepth = "1";
  }

  quote.cashorderqty = quoterequest.cashorderqty;
  quote.quotereqid = quoterequest.quotereqid;
  quote.qclientid = "";
  quote.symbolid = quoterequest.symbolid;
  quote.currencyid = "GBP";
  quote.settlcurrencyid = "GBP";
  quote.qbroker = "ABC";
  quote.futsettdate = quoterequest.futsettdate;
  quote.externalquoteid = "";
  quote.settledays = 2;

  var today = new Date();
  quote.transacttime = common.getUTCTimeStamp(today);

  var validuntiltime = today;
  quote.noseconds = 30;
  validuntiltime.setSeconds(today.getSeconds() + quote.noseconds);
  quote.validuntiltime = common.getUTCTimeStamp(validuntiltime);

  // quote script
  // note: not passing securityid & idsource as proquote symbol should be enough
  db.eval(scriptquote, 0, quote.quotereqid, quote.symbolid, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.transacttime, quote.currencyid, quote.settlcurrencyid, quote.qbroker, quote.futsettdate, quote.bidquotedepth, quote.offerquotedepth, quote.externalquoteid, quote.qclientid, quote.cashorderqty, quote.settledays, quote.noseconds, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    if (ret != 0) {
      // can't find quote request, so don't know which client to inform
      console.log("Error in scriptquote:" + common.getReasonDesc(ret[0]));
      return;
    }

    // start the quote monitor if necessary
    if (quoteinterval == null) {
      startQuoteInterval();
    }

    // script publishes quote to the operator type that made the request
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
  var currencyratetoorg = 1; // product currency to org currency rate
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
  db.eval(scriptneworder, 0, order.clientid, order.symbolid, order.side, order.quantity, order.price, order.ordertype, 0, order.futsettdate, order.partfill, order.quoteid, order.currencyid, currencyratetoorg, currencyindtoorg, order.timestamp, order.timeinforce, order.expiredate, order.expiretime, order.settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, order.nosettdays, order.operatortype, order.operatorid, hour, minute, day, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // credit check failed
    if (ret[0] == 0) {
      console.log("credit check failed, order #" + ret[1]);

      // script will publish the order back to the operator type
      return;
    }

    // see if the order was client quoted, if so, we are done
    if (ret[13] != "") {
      return;
    }

    // update order details
    order.orderid = ret[1];
    order.isin = ret[2];
    order.mnemonic = ret[3];
    order.exchangeid = ret[4];
    order.instrumenttypeid = ret[7];
    var hedgeorderid = ret[8];
    order.markettype = ret[12];
    var hedgebookid = ret[14];
    var hedgesymbolid = ret[15];

    // use the returned quote values required by fix connection
    if (order.ordertype == "D") {
      order.externalquoteid = ret[5];
      order.qbroker = ret[6];
    }

    // set the settlement date to equity default date for cfd orders, in case they are being hedged with the market
    if (order.instrumenttypeid == "CFD" || order.instrumenttypeid == "SPB") {
      // commented out as only offering default settlement for the time being
      //order.futsettdate = common.getUTCDateString(common.getSettDate(today, ret[11], holidays));

      // update the stored order settlement details if it is a hedge - todo: req'd?
      if (hedgeorderid != "") {
        db.hmset("order:" + hedgeorderid, "nosettdays", ret[11], "futsettdate", order.futsettdate);
      }
    }

    processOrder(order, hedgeorderid, ret[9], ret[10], hedgebookid, hedgesymbolid);
  });
}

function matchOrder(order) {
  db.eval(scriptmatchorder, 1, order.orderid, function(err, ret) {
    if (err) throw err;

    // todo: sort out publishing
    console.log(ret);
    displayOrderBook(order.symbolid, "-inf", "+inf");


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
    //db.publish(order.operatortype, "orderbookupdate:" + order.symbolid);
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
      ocr.symbolid = ret[3];
      ocr.isin = ret[4];
      ocr.mnemonic = ret[5];
      ocr.exchangeid = ret[6];
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

function processOrder(order, hedgeorderid, tradeid, hedgetradeid, hedgebookid, hedgesymbolid) {
  console.log("processOrder");
  //
  // the order has been credit checked
  // now, either forward or attempt to match the order, depending on the type of instrument & whether the market is open
  //

  // not matching, for now at least

  /*if (order.markettype == 1) {
    console.log("matching");
    matchOrder(order);
  } else {*/

  // equity orders
  if (order.instrumenttypeid == "DE" || order.instrumenttypeid == "IE") {
    if (testmode == "1") {
      // test only
      testTradeResponse(order);
    } else {
      console.log("forwarding to nbt");
      // forward order to the market
      nbt.newOrder(order);
    }
  } else {
    // if we are hedging, change the order id to that of the hedge & forward
    if (hedgeorderid != "") {
      console.log("forwarding hedge order:" + hedgeorderid + " to nbt");
      order.orderid = hedgeorderid;
      order.clientid = hedgebookid;
      order.symbolid = hedgesymbolid;

      if (testmode == "1") {
        // test only
        testTradeResponse(order);
      } else {
        nbt.newOrder(order);
      }
    }
  }
}

function testTradeResponse(order) {
  var exereport = {};
  var currencyratetoorg = 1; // product currency rate back to org 
  var currencyindtoorg = 1;

  console.log("test fill");

  exereport.clordid = order.orderid;
  exereport.symbolid = order.symbolid;
  exereport.side = order.side;
  exereport.lastshares = order.quantity;
  exereport.lastpx = order.price;
  exereport.currencyid = "GBP";
  exereport.execbroker = order.qbroker;
  exereport.execid = 1;
  exereport.futsettdate = order.futsettdate;
  exereport.transacttime = order.timestamp;
  exereport.ordstatus = 2;
  exereport.lastmkt = "";
  exereport.leavesqty = 0;
  exereport.orderid = "";
  exereport.settlcurrencyid = "GBP";
  exereport.settlcurramt = parseFloat(order.price) * parseInt(order.quantity);
  exereport.settlcurrfxrate = 1;
  exereport.settlcurrfxratecalc = 1;
  var milliseconds = new Date().getTime();

  db.eval(scriptnewtrade, 21, exereport.clordid, exereport.symbolid, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currencyid, currencyratetoorg, currencyindtoorg, exereport.execbroker, exereport.execid, exereport.futsettdate, exereport.transacttime, exereport.ordstatus, exereport.lastmkt, exereport.leavesqty, exereport.orderid, exereport.settlcurrencyid, exereport.settlcurramt, exereport.settlcurrfxrate, exereport.settlcurrfxratecalc, milliseconds, function(err, ret) {
    if (err) {
      console.log(err);
      return
    }

    console.log(ret);
  });
}

function displayOrderBook(symbolid, lowerbound, upperbound) {
  db.zrangebyscore("orderbook:" + symbolid, lowerbound, upperbound, function(err, matchorders) {
    console.log("order book for instrument " + symbolid + " has " + matchorders.length + " order(s)");

    matchorders.forEach(function (matchorderid, i) {
      db.hgetall("order:" + matchorderid, function(err, matchorder) {
        console.log("orderid="+matchorder.orderid+", clientid="+matchorder.clientid+", price="+matchorder.price+", side="+matchorder.side+", remquantity="+matchorder.remquantity);
      });
    });
  });
}

function initDb() {
  common.registerScripts();
  registerScripts();
  loadHolidays();
  getTestmode();
}

function loadHolidays() {
  // we are assuming "L"=London
  db.eval(common.scriptgetholidays, 0, "L", function(err, ret) {
    if (err) throw err;

    for (var i = 0; i < ret.length; ++i) {
      holidays[ret[i]] = ret[i];
    }
  });
}

function getTestmode() {
  db.get("testmode", function(err, tm) {
    if (err) {
      console.log(err);
      return;
    }

    testmode = tm;
  });
}

// markettype determined by timezone per instrument
/*function getMarkettype(symbolid) {
  db.eval(scriptmarkettype, 1, symbolid, function(err, ret) {
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
  } else {
    quote.noseconds = common.getSeconds(quote.transacttime, quote.validuntiltime);
  }

  if (!('qclientid' in quote)) {
    quote.qclientid = "";
  }

  if (!('settledays' in quote)) {
    quote.settledays = "";
  }

  // quote script
  // note: not passing securityid & idsource as proquote symbol should be enough
  db.eval(scriptquote, 0, quote.quotereqid, quote.symbolid, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.transacttime, quote.currencyid, quote.settlcurrencyid, quote.qbroker, quote.futsettdate, quote.bidquotedepth, quote.offerquotedepth, quote.externalquoteid, quote.qclientid, quote.cashorderqty, quote.settledays, quote.noseconds, function(err, ret) {
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
  console.log("order rejected");
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

// fill received from market
nbt.on("orderFill", function(exereport) {
  var currencyratetoorg = 1; // product currency rate back to org 
  var currencyindtoorg = 1;

  console.log("fill received");
  console.log(exereport);

  if (!('settlcurrfxrate' in exereport)) {
    exereport.settlcurrfxrate = 1;
  }

  // we don't get this externally - todo: set based on currency pair
  exereport.settlcurrfxratecalc = 1;

  // milliseconds since epoch, used for scoring trades so they can be retrieved in a range
  var milliseconds = new Date().getTime();

  db.eval(scriptnewtrade, 21, exereport.clordid, exereport.symbolid, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currencyid, currencyratetoorg, currencyindtoorg, exereport.execbroker, exereport.execid, exereport.futsettdate, exereport.transacttime, exereport.ordstatus, exereport.lastmkt, exereport.leavesqty, exereport.orderid, exereport.settlcurrencyid, exereport.settlcurramt, exereport.settlcurrfxrate, exereport.settlcurrfxratecalc, milliseconds, function(err, ret) {
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

  db.eval(scriptquoteack, 0, quoteack.quotereqid, quoteack.quoteackstatus, quoteack.quoterejectreason, quoteack.text, function(err, ret) {
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
  var rejectorder;
  var adjustmarginreserve;
  var creditcheck;
  var getproquotesymbol;
  var getinitialmargin;
  //var updatetrademargin;
  var neworder;
  var getreserve;
  var getposition;
  var closeposition;
  var createposition;

  getcosts = round + '\
  local getcosts = function(clientid, symbolid, instrumenttypeid, side, consid, currencyid) \
    local fields = {"commissionpercent", "commissionmin", "ptmlevylimit", "ptmlevy", "stampdutylimit", "stampdutypercent", "contractcharge"} \
    local vals = redis.call("hmget", "cost:" .. instrumenttypeid .. ":" .. currencyid .. ":" .. side, unpack(fields)) \
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
    local ptmexempt = redis.call("hget", "symbol:" .. symbolid, "ptmexempt") \
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

  //
  // get initial margin to include costs
  //
  getinitialmargin = '\
  local getinitialmargin = function(symbolid, consid) \
    local marginpercent = redis.call("hget", "symbol:" .. symbolid, "marginpercent") \
    if not marginpercent then marginpercent = 100 end \
    local initialmargin = tonumber(consid) * tonumber(marginpercent) / 100 \
    return initialmargin \
  end \
  ';

  updateordermargin = '\
  local updateordermargin = function(orderid, clientid, ordmargin, currencyid, newordmargin) \
    if newordmargin == ordmargin then return end \
    local marginkey = clientid .. ":margin:" .. currencyid \
    local marginskey = clientid .. ":margins" \
    local margin = redis.call("get", marginkey) \
    if not margin then \
      redis.call("set", marginkey, newordmargin) \
      redis.call("sadd", marginskey, currencyid) \
    else \
      local adjmargin = tonumber(margin) - tonumber(ordmargin) + tonumber(newordmargin) \
      if adjmargin == 0 then \
        redis.call("del", marginkey) \
        redis.call("srem", marginskey, currencyid) \
      else \
        redis.call("set", marginkey, adjmargin) \
      end \
    end \
    redis.call("hset", "order:" .. orderid, "margin", newordmargin) \
  end \
  ';

  /*updatetrademargin = '\
  local updatetrademargin = function(tradeid, clientid, currencyid, initialmargin) \
    local marginkey = clientid .. ":margin:" .. currencyid \
    local marginskey = clientid .. ":margins" \
    local margin = redis.call("get", marginkey) \
    if not margin then \
      redis.call("set", marginkey, margin) \
      redis.call("sadd", marginskey, currencyid) \
    else \
      local adjmargin = tonumber(margin) + tonumber(initialmargin) \
      if adjmargin == 0 then \
        redis.call("del", marginkey) \
        redis.call("srem", marginskey, currencyid) \
      else \
        redis.call("set", marginkey, adjmargin) \
      end \
    end \
    redis.call("hset", "trade:" .. tradeid, "margin", initialmargin) \
  end \
  ';*/

  updatereserve = '\
  local updatereserve = function(clientid, symbolid, currencyid, settldate, quantity) \
    local poskey = symbolid .. ":" .. currencyid .. ":" .. settldate \
    local reservekey = clientid .. ":reserve:" .. poskey \
    local reserveskey = clientid .. ":reserves" \
    --[[ get existing position, if there is one ]] \
    local reserve = redis.call("hget", reservekey, "quantity") \
    if reserve then \
      local adjquantity = tonumber(reserve) + tonumber(quantity) \
      if adjquantity == 0 then \
        redis.call("hdel", reservekey, "clientid", "symbolid", "quantity", "currencyid", "settldate") \
        redis.call("srem", reserveskey, poskey) \
      else \
        redis.call("hset", reservekey, "quantity", adjquantity) \
      end \
    else \
      redis.call("hmset", reservekey, "clientid", clientid, "symbolid", symbolid, "quantity", quantity, "currencyid", currencyid, "settldate", settldate) \
      redis.call("sadd", reserveskey, poskey) \
    end \
  end \
  ';

  //
  // create a new position
  // positionskeysettdate allows for an additional link between symbol & positions where settlement date is part of the position key
  //
  createposition = '\
  local createposition = function(positionkey, positionskey, postradeskey, clientid, symbolid, quantity, cost, currencyid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
    local positionid = redis.call("incr", "positionid") \
    redis.call("hmset", positionkey, "clientid", clientid, "symbolid", symbolid, "quantity", quantity, "cost", cost, "currencyid", currencyid, "positionid", positionid, "futsettdate", futsettdate) \
    redis.call("sadd", positionskey, symbolsettdatekey) \
    if positionskeysettdate ~= "" then \
      redis.call("sadd", positionskeysettdate, futsettdate) \
    end \
    redis.call("sadd", "position:" .. symbolsettdatekey .. ":clients", clientid) \
    redis.call("sadd", postradeskey, tradeid) \
    return positionid \
  end \
  ';

  //
  // close a position
  //
  closeposition = '\
  local closeposition = function(positionkey, positionskey, postradeskey, clientid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
    redis.call("hdel", positionkey, "clientid", "symbolid", "side", "quantity", "cost", "currencyid", "margin", "positionid", "futsettdate") \
    redis.call("srem", positionskey, symbolsettdatekey) \
    if positionskeysettdate ~= "" then \
      redis.call("srem", positionskeysettdate, futsettdate) \
    end \
    redis.call("srem", "position:" .. symbolsettdatekey .. ":clients", clientid) \
    local postrades = redis.call("smembers", postradeskey) \
    for index = 1, #postrades do \
      redis.call("srem", postradeskey, postrades[index]) \
    end \
  end \
  ';

  //
  // publish a position
  // key may be just a symbol or symbol + settlement date
  //
  publishposition = common.getunrealisedpandl + common.getmargin + '\
  local publishposition = function(clientid, positionkey, symbolid, futsettdate, channel) \
    local fields = {"quantity", "cost", "currencyid", "positionid", "futsettdate", "symbolid"} \
    local vals = redis.call("hmget", positionkey, unpack(fields)) \
    local pos = {} \
    if vals[1] then \
      local margin = getmargin(vals[6], vals[1]) \
      --[[ value the position ]] \
      local unrealisedpandl = getunrealisedpandl(vals[6], vals[1], vals[2]) \
      pos = {clientid=clientid,symbolid=vals[6],quantity=vals[1],cost=vals[2],currencyid=vals[3],margin=margin,positionid=vals[4],futsettdate=vals[5],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]} \
    else \
      pos = {clientid=clientid,symbolid=symbolid,quantity=0,futsettdate=futsettdate} \
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
  local updateposition = function(clientid, symbolid, side, tradequantity, tradeprice, tradecost, currencyid, tradeid, futsettdate) \
    local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
    local positionkey = clientid .. ":position:" .. symbolid \
    local postradeskey = clientid .. ":trades:" .. symbolid \
    local positionskey = clientid .. ":positions" \
    local positionskeysettdate = "" \
    local symbolsettdatekey = symbolid \
    --[[ add settlement date to key for devivs ]] \
    if instrumenttypeid == "CFD" or instrumenttypeid == "SPD" then \
      positionkey = positionkey .. ":" .. futsettdate \
      postradeskey = postradeskey .. ":" .. futsettdate \
      symbolsettdatekey = symbolsettdatekey .. ":" .. futsettdate \
      positionskeysettdate = positionskey .. ":" .. symbolid \
    end \
    local posqty = 0 \
    local poscost = 0 \
    local positionid = "" \
    side = tonumber(side) \
    local fields = {"quantity", "cost", "positionid"} \
    local vals = redis.call("hmget", positionkey, unpack(fields)) \
    --[[ do we already have a position? ]] \
    if vals[1] then \
      positionid = vals[3] \
      posqty = tonumber(vals[1]) \
      tradequantity = tonumber(tradequantity) \
      if side == 1 then \
        if posqty >= 0 then \
          --[[ we are adding to an existing long position ]] \
          posqty = posqty + tradequantity \
          poscost = tonumber(vals[2]) + tonumber(tradecost) \
          --[[ update the position & add the trade to the set ]] \
          redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost) \
          redis.call("sadd", postradeskey, tradeid) \
        elseif tradequantity == math.abs(posqty) then \
          --[[ just close position ]] \
          closeposition(positionkey, positionskey, postradeskey, clientid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
        elseif tradequantity > math.abs(posqty) then \
          --[[ close position ]] \
          closeposition(positionkey, positionskey, postradeskey, clientid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
          --[[ & open new ]] \
          posqty = posqty + tradequantity \
          poscost = round(posqty * tonumber(tradeprice), 5) \
          positionid = createposition(positionkey, positionskey, postradeskey, clientid, symbolid, posqty, poscost, currencyid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
        else \
          --[[ part-fill ]] \
          posqty = posqty + tradequantity \
          poscost = round(posqty / tonumber(vals[1]) * tonumber(vals[2]), 5) \
          redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost) \
          redis.call("sadd", postradeskey, tradeid) \
        end \
      else \
        if posqty <= 0 then \
          --[[ we are adding to an existing short quantity ]] \
          posqty = posqty - tradequantity \
          poscost = tonumber(vals[2]) + tonumber(tradecost) \
          --[[ update the position & add the trade to the set ]] \
          redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost) \
          redis.call("sadd", postradeskey, tradeid) \
        elseif tradequantity == posqty then \
          --[[ just close position ]] \
          closeposition(positionkey, positionskey, postradeskey, clientid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
        elseif tradequantity > posqty then \
          --[[ close position ]] \
          closeposition(positionkey, positionskey, postradeskey, clientid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
          --[[ & open new ]] \
          posqty = posqty - tradequantity \
          poscost = round(posqty * tonumber(tradeprice), 5) \
          positionid = createposition(positionkey, positionskey, postradeskey, clientid, symbolid, posqty, poscost, currencyid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
        else \
          --[[ part-fill ]] \
          posqty = posqty - tradequantity \
          poscost = round(posqty / tonumber(vals[1]) * tonumber(vals[2]), 5) \
          redis.call("hmset", positionkey, "quantity", posqty, "cost", poscost) \
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
      positionid = createposition(positionkey, positionskey, postradeskey, clientid, symbolid, posqty, tradecost, currencyid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
    end \
    publishposition(clientid, positionkey, symbolid, futsettdate, 10) \
    return positionid \
  end \
  ';

  adjustmarginreserve = getinitialmargin + updateordermargin + updatereserve + '\
  local adjustmarginreserve = function(orderid, clientid, symbolid, side, price, ordmargin, currencyid, remquantity, newremquantity, settldate, nosettdays) \
    local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
    remquantity = tonumber(remquantity) \
    newremquantity = tonumber(newremquantity) \
    if tonumber(side) == 1 then \
      if newremquantity ~= remquantity then \
        local newordmargin = 0 \
        if newremquantity ~= 0 then \
          newordmargin = getinitialmargin(symbolid, newremquantity * tonumber(price)) \
        end \
        updateordermargin(orderid, clientid, ordmargin, currencyid, newordmargin) \
      end \
    else \
      if newremquantity ~= remquantity then \
        updatereserve(clientid, symbolid, currencyid, settldate, -remquantity + newremquantity) \
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
  local getposition = function(clientid, symbolsettdatekey) \
    local fields = {"quantity", "cost"} \
    local position = redis.call("hmget", clientid .. ":position:" .. symbolsettdatekey, unpack(fields)) \
    return position \
  end \
  ';

  getreserve = '\
  local getreserve = function(clientid, symbolid, currencyid, settldate) \
    local reserve = redis.call("hget", clientid .. ":reserve:" .. symbolid .. ":" .. currencyid .. ":" .. settldate, "quantity") \
    if not reserve then \
      return 0 \
    end \
    return reserve \
  end \
  ';

  creditcheck = rejectorder + getinitialmargin + getcosts + calcfinance + getposition + getfreemargin + getreserve + updateordermargin + '\
  local creditcheck = function(orderid, clientid, symbolid, side, quantity, price, currencyid, settldate, instrumenttypeid, nosettdays) \
     --[[ see if client is allowed to trade this product ]] \
    if redis.call("sismember", "client:" .. clientid .. ":instrumenttypes", instrumenttypeid) == 0 then \
      rejectorder(orderid, 1018, "") \
      return {0} \
    end \
    --[[ calculate initial margin, costs & finance ]] \
    side = tonumber(side) \
    quantity = tonumber(quantity) \
    price = tonumber(price) \
    local consid = quantity * price \
    local initialmargin = getinitialmargin(symbolid, consid) \
    local costs =  getcosts(clientid, symbolid, instrumenttypeid, side, consid, currencyid) \
    local totalcost = costs[1] + costs[2] + costs[3] + costs[4] \
    local finance = 0 \
    --[[ todo: needs considering ]] \
    --[[local finance = calcfinance(instrumenttypeid, consid, currencyid, side, nosettdays) ]]\
    local symbolsettdatekey = symbolid \
    if instrumenttypeid == "CFD" or instrumenttypeid == "SPB" then \
      symbolsettdatekey = symbolsettdatekey .. ":" .. settldate \
    end \
    local position = getposition(clientid, symbolsettdatekey) \
    if position[1] then \
      --[[ we have a position - always allow closing trades ]] \
      local posqty = tonumber(position[1]) \
      local poscost = tonumber(position[2]) \
      if (side == 1 and posqty < 0) or (side == 2 and posqty > 0) then \
        if quantity <= math.abs(posqty) then \
          --[[ closing trade, so ok ]] \
          return {1, initialmargin, costs, finance} \
        end \
        if side == 2 and (instrumenttypeid == "DE" or instrumenttypeid == "IE") then \
            --[[ equity, so cannot sell more than we have ]] \
            rejectorder(orderid, 1019, "") \
            return {0} \
        end \
        --[[ we are trying to close a quantity greater than current position, so need to check we can open a new position ]] \
        local freemargin = getfreemargin(clientid, currencyid) \
        --[[ add the margin returned by closing the position ]] \
        local margin = getmargin(symbolid, math.abs(posqty)) \
        --[[ get initial margin for remaining quantity after closing position ]] \
        initialmargin = getinitialmargin(symbolid, (quantity - math.abs(posqty)) * price) \
        if initialmargin + totalcost + finance > freemargin + margin then \
          rejectorder(orderid, 1020, "") \
          return {0} \
        end \
        --[[ closing trade or enough margin to close & open a new position, so ok ]] \
        return {1, initialmargin, costs, finance} \
      end \
    end \
    --[[ check free margin for all derivative trades & equity buys ]] \
    if instrumenttypeid == "CFD" or instrumenttypeid == "SPB" or instrumenttypeid == "CCFD" or side == 1 then \
      local freemargin = getfreemargin(clientid, currencyid) \
      if initialmargin + totalcost + finance > freemargin then \
        rejectorder(orderid, 1020, "") \
        return {0} \
      end \
      updateordermargin(orderid, clientid, 0, currencyid, initialmargin) \
    else \
      --[[ allow ifa certificated equity sells ]] \
      if instrumenttypeid == "DE" then \
        if redis.call("hget", "client:" .. clientid, "type") == "3" then \
          return {1, initialmargin, costs, finance} \
        end \
      end \
      --[[ check there is a position ]] \
      if not position[1] then \
        rejectorder(orderid, 1003, "") \
        return {0} \
      end \
      local posqty = tonumber(position[1]) \
      --[[ check the position is long ]] \
      if posqty < 0 then \
        rejectorder(orderid, 1004, "") \
        return {0} \
      end \
      --[[ check there is a large enough position ]] \
      local reserve = getreserve(clientid, symbolid, currencyid, settldate) \
      if reserve then \
        posqty = posqty - tonumber(reserve) \
      end \
      if quantity > posqty then \
        rejectorder(orderid, 1004, "") \
        return {0} \
      end \
      --[[ todo: need for limit orders ]] \
      --[[updatereserve(clientid, symbolid, currencyid, settldate, quantity)]] \
    end \
    return {1, initialmargin, costs, finance} \
  end \
  ';

  cancelorder = adjustmarginreserve + '\
  local cancelorder = function(orderid, status) \
    local orderkey = "order:" .. orderid \
    redis.call("hset", orderkey, "status", status) \
    local fields = {"clientid", "symbolid", "side", "price", "settlcurrencyid", "margin", "remquantity", "futsettdate", "nosettdays"} \
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
  // returns: isin, mnemonic, exchange, hedge symbol, instrumenttypeid & market type
  //
  getproquotesymbol = '\
  local getproquotesymbol = function(symbolid) \
    local symbolkey = "symbol:" .. symbolid \
    local fields = {"isin", "mnemonic", "exchangeid", "hedgesymbolid", "instrumenttypeid", "timezoneid"} \
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
  local addtoorderbook = function(symbolid, orderid, side, price) \
    --[[ buy orders need a negative price ]] \
    if tonumber(side) == 1 then \
      price = "-" .. price \
    end \
    --[[ add order to order book ]] \
    redis.call("zadd", "orderbook:" .. symbolid, price, orderid) \
    redis.call("sadd", "orderbooks", symbolid) \
  end \
  ';

  removefromorderbook = '\
  local removefromorderbook = function(symbolid, orderid) \
    redis.call("zrem", "orderbook:" .. symbolid, orderid) \
    if (redis.call("zcount", symbolid, "-inf", "+inf") == 0) then \
      --[[ todo: dont think so ]] \
      redis.call("srem", "orderbooks", symbolid) \
    end \
  end \
  ';

  publishquoteack = '\
  local publishquoteack = function(quotereqid) \
    local fields = {"clientid", "symbolid", "operatortype", "quotestatus", "quoterejectreason", "text", "operatorid"} \
    local vals = redis.call("hmget", "quoterequest:" .. quotereqid, unpack(fields)) \
    local quoteack = {quotereqid=quotereqid, clientid=vals[1], symbolid=vals[2], quotestatus=vals[4], quoterejectreason=vals[5], text=vals[6], operatorid=vals[7]} \
    redis.call("publish", vals[3], "{" .. cjson.encode("quoteack") .. ":" .. cjson.encode(quoteack) .. "}") \
    end \
  ';

  publishquote = '\
  local publishquote = function(quoteid, channel, operatorid) \
    local fields = {"quotereqid","clientid","quoteid","symbolid","bidpx","offerpx","bidquantity","offerquantity","validuntiltime","transacttime","settlcurrencyid","nosettdays","futsettdate","bidsize","offersize","qclientid","noseconds"} \
    local vals = redis.call("hmget", "quote:" .. quoteid, unpack(fields)) \
    local quote = {quotereqid=vals[1],clientid=vals[2],quoteid=vals[3],symbolid=vals[4],bidpx=vals[5],offerpx=vals[6],bidquantity=vals[7],offerquantity=vals[8],validuntiltime=vals[9],transacttime=vals[10],settlcurrencyid=vals[11],nosettdays=vals[12],futsettdate=vals[13],bidsize=vals[14],offersize=vals[15],qclientid=vals[16],operatorid=operatorid,noseconds=vals[17]} \
    redis.call("publish", channel, "{" .. cjson.encode("quote") .. ":" .. cjson.encode(quote) .. "}") \
  end \
  ';

  publishorder = '\
  local publishorder = function(orderid, channel) \
    local fields = {"clientid","symbolid","side","quantity","price","ordertype","remquantity","status","markettype","futsettdate","partfill","quoteid","currencyid","currencyratetoorg","currencyindtoorg","timestamp","margin","timeinforce","expiredate","expiretime","settlcurrencyid","settlcurrfxrate","settlcurrfxratecalc","orderid","externalorderid","execid","nosettdays","operatortype","operatorid","hedgeorderid","text","reason"} \
    local vals = redis.call("hmget", "order:" .. orderid, unpack(fields)) \
    local order = {clientid=vals[1],symbolid=vals[2],side=vals[3],quantity=vals[4],price=vals[5],ordertype=vals[6],remquantity=vals[7],status=vals[8],markettype=vals[9],futsettdate=vals[10],partfill=vals[11],quoteid=vals[12],currencyid=vals[13],currencyratetoorg=vals[14],currencyindtoorg=vals[15],timestamp=vals[16],margin=vals[17],timeinforce=vals[18],expiredate=vals[19],expiretime=vals[20],settlcurrencyid=vals[21],settlcurrfxrate=vals[22],settlcurrfxratecalc=vals[23],orderid=vals[24],externalorderid=vals[25],execid=vals[26],nosettdays=vals[27],operatortype=vals[28],operatorid=vals[29],hedgeorderid=vals[30],text=vals[31],reason=vals[32]} \
    redis.call("publish", channel, "{" .. cjson.encode("order") .. ":" .. cjson.encode(order) .. "}") \
  end \
  ';

  publishtrade = '\
  local publishtrade = function(tradeid, channel) \
    local fields = {"clientid","orderid","symbolid","side","quantity","price","currencyid","currencyratetoorg","currencyindtoorg","commission","ptmlevy","stampduty","contractcharge","counterpartyid","markettype","externaltradeid","futsettdate","timestamp","lastmkt","externalorderid","tradeid","settlcurrencyid","settlcurramt","settlcurrfxrate","settlcurrfxratecalc","nosettdays","finance","margin","positionid"} \
    local vals = redis.call("hmget", "trade:" .. tradeid, unpack(fields)) \
    local quoteid = redis.call("hget", "order:" .. vals[2], "quoteid") \
    local trade = {clientid=vals[1],orderid=vals[2],symbolid=vals[3],side=vals[4],quantity=vals[5],price=vals[6],currencyid=vals[7],currencyratetoorg=vals[8],currencyindtoorg=vals[9],commission=vals[10],ptmlevy=vals[11],stampduty=vals[12],contractcharge=vals[13],counterpartyid=vals[14],markettype=vals[15],externaltradeid=vals[16],futsettdate=vals[17],timestamp=vals[18],lastmkt=vals[19],externalorderid=vals[20],tradeid=vals[21],settlcurrencyid=vals[22],settlcurramt=vals[23],settlcurrfxrate=vals[24],settlcurrfxratecalc=vals[25],nosettdays=vals[26],finance=vals[27],margin=vals[28],positionid=vals[29],quoteid=quoteid} \
    redis.call("publish", channel, "{" .. cjson.encode("trade") .. ":" .. cjson.encode(trade) .. "}") \
  end \
  ';

  newtrade = updateposition + updatecash + publishtrade + '\
  local newtrade = function(clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, costs, counterpartyid, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, nosettdays, initialmargin, operatortype, operatorid, finance, milliseconds) \
    local tradeid = redis.call("incr", "tradeid") \
    if not tradeid then return 0 end \
    local tradekey = "trade:" .. tradeid \
    redis.call("hmset", tradekey, "clientid", clientid, "orderid", orderid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "commission", costs[1], "ptmlevy", costs[2], "stampduty", costs[3], "contractcharge", costs[4], "counterpartyid", counterpartyid, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", tradeid, "settlcurrencyid", settlcurrencyid, "settlcurramt", settlcurramt, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "nosettdays", nosettdays, "finance", finance, "margin", initialmargin) \
    redis.call("sadd", "trades", tradeid) \
    redis.call("sadd", "client:" .. clientid .. ":trades", tradeid) \
    redis.call("sadd", "order:" .. orderid .. ":trades", tradeid) \
    redis.call("zadd", "tradesbydate", milliseconds, tradeid) \
    local transtype \
    local drcr \
    local desc \
    if tonumber(side) == 1 then \
      transtype = "BT" \
      drcr = 2 \
      desc = "Bought " .. quantity .. " " .. symbolid .. " @ " .. price \
    else \
      transtype = "ST" \
      drcr = 1 \
      desc = "Sold " .. quantity .. " " .. symbolid .. " @ " .. price \
    end \
    --[[ cash transaction for the trade consideration ]] \
    updatecash(clientid, settlcurrencyid, transtype, settlcurramt, drcr, desc, "trade id: " .. tradeid, timestamp, "", operatortype, operatorid) \
    --[[ cash transactions for any costs ]] \
    if costs[1] > 0 then \
      updatecash(clientid, settlcurrencyid, "CO", costs[1], 2, "commission", "trade id: " .. tradeid, timestamp, "", operatortype, operatorid) \
    end \
    if costs[2] > 0 then \
      updatecash(clientid, settlcurrencyid, "PL", costs[2], 2, "ptm levy", "trade id: " .. tradeid, timestamp, "", operatortype, operatorid) \
    end \
    if costs[3] > 0 then \
      updatecash(clientid, settlcurrencyid, "SD", costs[3], 2, "stamp duty", "trade id: " .. tradeid, timestamp, "", operatortype, operatorid) \
    end \
    if costs[4] > 0 then \
      updatecash(clientid, settlcurrencyid, "CC", costs[4], 2, "contract charge", "trade id: " .. tradeid, timestamp, "", operatortype, operatorid) \
    end \
    --[[ cash transaction for any finance ]] \
    if tonumber(finance) > 0 then \
      updatecash(clientid, settlcurrencyid, "FI", finance, 2, "trade finance", "trade id: " .. tradeid, timestamp, "", operatortype, operatorid) \
    end \
    local positionid = updateposition(clientid, symbolid, side, quantity, price, settlcurramt, settlcurrencyid, tradeid, futsettdate) \
    redis.call("hset", tradekey, "positionid", positionid) \
    publishtrade(tradeid, 6) \
    return tradeid \
  end \
  ';

  neworder = '\
  local neworder = function(clientid, symbolid, side, quantity, price, ordertype, remquantity, status, markettype, futsettdate, partfill, quoteid, currencyid, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforce, expiredate, expiretime, settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, nosettdays, operatortype, operatorid, hedgeorderid) \
    --[[ get a new orderid & store the order ]] \
    local orderid = redis.call("incr", "orderid") \
    redis.call("hmset", "order:" .. orderid, "clientid", clientid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "ordertype", ordertype, "remquantity", remquantity, "status", status, "markettype", markettype, "futsettdate", futsettdate, "partfill", partfill, "quoteid", quoteid, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "timestamp", timestamp, "margin", margin, "timeinforce", timeinforce, "expiredate", expiredate, "expiretime", expiretime, "settlcurrencyid", settlcurrencyid, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "orderid", orderid, "externalorderid", externalorderid, "execid", execid, "nosettdays", nosettdays, "operatortype", operatortype, "operatorid", operatorid, "hedgeorderid", hedgeorderid) \
    --[[ add to set of orders for this client ]] \
    redis.call("sadd", "client:" .. clientid .. ":orders", orderid) \
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
    if tonumber(side) == 1 then \
      rside = 2 \
    else \
      rside = 1 \
    end \
    return rside \
  end \
  ';

  // compare hour & minute with timezone open/close times to determine in/out of hours - 0=in hours, 1=ooh
  /*getmarkettype = '\
  local getmarkettype = function(timezoneid, hour, minute) \
    local markettype = 0 \
    local fields = {"openhour","openminute","closehour","closeminute"} \
    local vals = redis.call("hmget", "timezone:" .. timezoneid, unpack(fields)) \
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
  local fields = {"clientid", "symbolid", "side", "price", "margin", "settlcurrencyid", "remquantity", "futsettdate", "nosettdays", "operatortype"} \
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
  local orderid = neworder(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[4], "0", ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13], ARGV[14], 0, ARGV[15], ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20], "", "", ARGV[21], ARGV[22], ARGV[23], "") \
  local symbolid = ARGV[2] \
  local side = tonumber(ARGV[3]) \
  --[[ calculate consideration in settlement currency, costs will be added later ]] \
  local settlcurramt = tonumber(ARGV[4]) * tonumber(ARGV[5]) \
  --[[ get instrument values ]] \
  local fields = {"instrumenttypeid", "timezoneid"} \
  local vals = redis.call("hmget", "symbol:" .. symbolid, unpack(fields)) \
  if not vals[2] then \
    rejectorder(orderid, 1007, "") \
    publishorder(orderid, ARGV[22]) \
    return {0} \
  end \
  local instrumenttypeid = vals[1] \
  local markettype = getmarkettype(symbolid, ARGV[24], ARGV[25], ARGV[26]) \
  --[[ do the credit check ]] \
  local cc = creditcheck(orderid, ARGV[1], symbolid, side, ARGV[4], ARGV[5], ARGV[18], ARGV[8], instrumenttypeid, ARGV[21]) \
  --[[ cc[1] = ok/fail, cc[2] = margin, cc[3] = costs array, cc[4] = finance ]] \
  if cc[1] == 0 then \
    --[[ publish the order back to the operatortype - the order contains the error ]] \
    publishorder(orderid, ARGV[22]) \
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
  if ARGV[6] == "D" then \
  --[[ see if order was quoted by a client ]] \
    qclientid = redis.call("hget", "quote:" .. ARGV[10], "qclientid") \
    if qclientid and qclientid ~= "" then \
      local hedgecosts = {0,0,0,0} \
      local rside = reverseside(side) \
      --[[ create trades for both clients ]] \
      tradeid = newtrade(ARGV[1], orderid, symbolid, side, ARGV[4], ARGV[5], ARGV[11], 1, 1, cc[3], qclientid, markettype, "", ARGV[8], ARGV[14], "", "", ARGV[18], settlcurramt, ARGV[19], ARGV[20], ARGV[21], cc[2], ARGV[22], ARGV[23], cc[4]) \
      hedgetradeid = newtrade(qclientid, orderid, symbolid, rside, ARGV[4], ARGV[5], ARGV[11], 1, 1, hedgecosts, ARGV[1], markettype, "", ARGV[8], ARGV[14], "", "", ARGV[18], settlcurramt, ARGV[19], ARGV[20], ARGV[21], 0, ARGV[22], ARGV[23], 0) \
      --[[ adjust order as filled ]] \
      redis.call("hmset", "order:" .. orderid, "remquantity", 0, "status", 2) \
      return {cc[1], orderid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], proquotequote[1], proquotequote[2], instrumenttypeid, hedgeorderid, tradeid, hedgetradeid, defaultnosettdays, markettype, qclientid} \
    end \
  end \
  if instrumenttypeid == "CFD" or instrumenttypeid == "SPB" or instrumenttypeid == "CCFD" then \
    --[[ ignore limit orders for derivatives as they will be handled manually, at least for the time being ]] \
    if ARGV[6] ~= "2" then \
      --[[ create trades for client & hedge book for off-exchange products ]] \
      hedgebookid = redis.call("get", "hedgebook:" .. instrumenttypeid .. ":" .. ARGV[18]) \
      if not hedgebookid then hedgebookid = 999999 end \
      local rside = reverseside(side) \
      local hedgecosts = {0,0,0,0} \
      tradeid = newtrade(ARGV[1], orderid, symbolid, side, ARGV[4], ARGV[5], ARGV[11], 1, 1, cc[3], hedgebookid, markettype, "", ARGV[8], ARGV[14], "", "", ARGV[18], settlcurramt, ARGV[19], ARGV[20], ARGV[21], cc[2], ARGV[22], ARGV[23], cc[4]) \
      hedgetradeid = newtrade(hedgebookid, orderid, symbolid, rside, ARGV[4], ARGV[5], ARGV[11], 1, 1, hedgecosts, ARGV[1], markettype, "", ARGV[8], ARGV[14], "", "", ARGV[18], settlcurramt, ARGV[19], ARGV[20], ARGV[21], 0, ARGV[22], ARGV[23], 0) \
      --[[ adjust order as filled ]] \
      redis.call("hmset", "order:" .. orderid, "remquantity", 0, "status", 2) \
      --[[ todo: may need to adjust margin here ]] \
      --[[ see if we need to hedge this trade in the market ]] \
      local hedgeclient = tonumber(redis.call("hget", "client:" .. ARGV[1], "hedge")) \
      local hedgeinst = tonumber(redis.call("hget", "symbol:" .. symbolid, "hedge")) \
      if hedgeclient == 1 or hedgeinst == 1 then \
        --[[ create a hedge order in the underlying product ]] \
        proquotesymbol = getproquotesymbol(symbolid) \
        if proquotesymbol[4] then \
          hedgeorderid = neworder(hedgebookid, proquotesymbol[4], ARGV[3], ARGV[4], ARGV[5], "X", ARGV[4], 0, markettype, ARGV[8], ARGV[9], "", ARGV[11], ARGV[12], ARGV[13], ARGV[14], 0, ARGV[15], ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20], "", "", ARGV[21], ARGV[22], ARGV[23], orderid, "") \
          --[[ get quote broker ]] \
          proquotequote = getproquotequote(ARGV[10]) \
          --[[ assume uk equity as underlying for default settl days ]] \
          defaultnosettdays = redis.call("hget", "cost:" .. "DE" .. ":" .. ARGV[18] .. ":" .. side, "defaultnosettdays") \
        end \
      end \
    end \
  else \
    --[[ this is an equity - just consider external orders for the time being - todo: internal ]] \
    --[[ todo: consider equity limit orders ]] \
    if markettype == 0 then \
      --[[ see if we need to send this trade to the market - if either product or client hedge, then send ]] \
      local hedgeclient = tonumber(redis.call("hget", "client:" .. ARGV[1], "hedge")) \
      local hedgeinst = tonumber(redis.call("hget", "symbol:" .. ARGV[2], "hedge")) \
      if hedgeclient == 0 and hedgeinst == 0 then \
        --[[ we are taking on the trade, so create trades for client & hedge book ]] \
        hedgebookid = redis.call("get", "hedgebook:" .. instrumenttypeid .. ":" .. ARGV[18]) \
        if not hedgebookid then hedgebookid = 999999 end \
        local rside = reverseside(side) \
        local hedgecosts = {0,0,0,0} \
        tradeid = newtrade(ARGV[1], orderid, ARGV[2], side, ARGV[4], ARGV[5], ARGV[11], 1, 1, cc[3], hedgebookid, markettype, "", ARGV[8], ARGV[14], "", "", ARGV[18], settlcurramt, ARGV[19], ARGV[20], ARGV[21], cc[2], ARGV[22], ARGV[23], cc[4]) \
        hedgetradeid = newtrade(hedgebookid, orderid, ARGV[2], rside, ARGV[4], ARGV[5], ARGV[11], 1, 1, hedgecosts, ARGV[1], markettype, "", ARGV[8], ARGV[14], "", "", ARGV[18], settlcurramt, ARGV[19], ARGV[20], ARGV[21], 0, ARGV[22], ARGV[23], 0) \
      else \
        proquotesymbol = getproquotesymbol(ARGV[2]) \
        proquotequote = getproquotequote(ARGV[10]) \
        defaultnosettdays = redis.call("hget", "cost:" .. instrumenttypeid .. ":" .. ARGV[18] .. ":" .. side, "defaultnosettdays") \
      end \
    end \
  end \
  return {cc[1], orderid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], proquotequote[1], proquotequote[2], instrumenttypeid, hedgeorderid, tradeid, hedgetradeid, defaultnosettdays, markettype, qclientid, hedgebookid, proquotesymbol[4]} \
  ';

  // todo: add lastmkt
  // quantity required as number of shares
  // todo: settlcurrency
  scriptmatchorder = addtoorderbook + removefromorderbook + adjustmarginreserve + newtrade + getcosts + '\
  local fields = {"clientid", "symbolid", "side", "quantity", "price", "currencyid", "margin", "remquantity", "futsettdate", "timestamp", "nosettdays"} \
  local vals = redis.call("hmget", "order:" .. KEYS[1], unpack(fields)) \
  local clientid = vals[1] \
  local remquantity = tonumber(vals[8]) \
  if remquantity <= 0 then return "1010" end \
  local instrumenttypeid = redis.call("hget", "symbol:" .. vals[2], "instrumenttypeid") \
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
      local costs = getcosts(vals[1], vals[2], instrumenttypeid, vals[3], consid, vals[6]) \
      local matchcosts = getcosts(matchvals[1], vals[2], instrumenttypeid, matchside, consid, matchvals[6]) \
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
  scriptnewtrade = newtrade + getinitialmargin + getcosts + calcfinance + adjustmarginreserve + publishorder + reverseside + '\
  local orderid = KEYS[1] \
  local fields = {"clientid", "symbolid", "side", "quantity", "price", "margin", "remquantity", "nosettdays", "operatortype", "hedgeorderid", "futsettdate", "operatorid"} \
  local vals = redis.call("hmget", "order:" .. orderid, unpack(fields)) \
  local quantity = tonumber(KEYS[4]) \
  local price = tonumber(KEYS[5]) \
  local consid = quantity * price \
  local instrumenttypeid = redis.call("hget", "symbol:" .. vals[2], "instrumenttypeid") \
  local initialmargin = getinitialmargin(vals[2], consid) \
  local costs = getcosts(vals[1], vals[2], instrumenttypeid, vals[3], consid, KEYS[17]) \
  local finance = 0 \
  --[[ todo: needs considering ]] \
  --[[ local finance = calcfinance(instrumenttypeid, consid, KEYS[17], vals[3], vals[8]) ]]\
  --[[ treat the excuting broker as a client ]] \
  local cptyid = redis.call("hget", "broker:" .. KEYS[9], "clientid") \
  if not cptyid then \
    cptyid = 999998 \
  end \
  local rside = reverseside(KEYS[3]) \
  local tradeid = newtrade(vals[1], orderid, vals[2], KEYS[3], quantity, price, KEYS[6], KEYS[7], KEYS[8], costs, cptyid, "0", KEYS[10], KEYS[11], KEYS[12], KEYS[14], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], vals[8], initialmargin, vals[9], vals[12], finance, KEYS[21]) \
  local cptytradeid = newtrade(cptyid, orderid, vals[2], rside, quantity, price, KEYS[6], KEYS[7], KEYS[8], costs, vals[1], "0", KEYS[10], KEYS[11], KEYS[12], KEYS[14], KEYS[16], KEYS[17], KEYS[18], KEYS[19], KEYS[20], vals[8], initialmargin, vals[9], vals[12], finance, KEYS[21]) \
  --[[ adjust order related margin/reserve ]] \
  adjustmarginreserve(orderid, vals[1], vals[2], vals[3], vals[5], vals[6], KEYS[17], vals[7], KEYS[15], KEYS[11], vals[8]) \
  --[[ adjust order ]] \
  redis.call("hmset", "order:" .. orderid, "remquantity", KEYS[15], "status", KEYS[13]) \
  --[[ todo: adjust trade related margin ]] \
  --[[updatetrademargin(tradeid, vals[1], KEYS[17], initialmargin[1])]] \
  --[[publishorder(orderid, vals[9]) ]]\
  return cptyid \
  ';

  scriptordercancelrequest = removefromorderbook + cancelorder + getproquotesymbol + '\
  local errorcode = 0 \
  local orderid = KEYS[2] \
  local ordercancelreqid = redis.call("incr", "ordercancelreqid") \
  --[[ store the order cancel request ]] \
  redis.call("hmset", "ordercancelrequest:" .. ordercancelreqid, "clientid", KEYS[1], "orderid", orderid, "timestamp", KEYS[3], "operatortype", KEYS[4], "operatorid", KEYS[5]) \
  local fields = {"status", "markettype", "symbolid", "side", "quantity", "externalorderid"} \
  local vals = redis.call("hmget", "order:" .. orderid, unpack(fields)) \
  local markettype = "" \
  local symbolid = "" \
  local side \
  local quantity = "" \
  local externalorderid = "" \
  if vals == nil then \
    --[[ order not found ]] \
    errorcode = 1009 \
  else \
    local status = vals[1] \
    markettype = vals[2] \
    symbolid = vals[3] \
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
      removefromorderbook(symbolid, orderid) \
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
        proquotesymbol = getproquotesymbol(symbolid) \
      end \
    end \
  end \
  return {errorcode, markettype, ordercancelreqid, symbolid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], side, quantity} \
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
  local fields = {"clientid", "symbolid", "side", "quantity", "price", "margin", "remquantity", "nosettdays", "operatortype", "hedgeorderid", "futsettdate", "operatorid", "status", "externalorderid", "settlcurrencyid", "markettype"} \
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
    local symbolid = vals[2] \
    local price = tonumber(KEYS[6]) \
    --[[ todo: round? ]] \
    local consid = remquantity * price \
    local side = vals[3] \
    local settlcurrencyid = vals[15] \
    local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
    local initialmargin = getinitialmargin(symbolid, consid) \
    local costs = getcosts(vals[1], symbolid, instrumenttypeid, side, consid, settlcurrencyid) \
    local finance = calcfinance(instrumenttypeid, consid, settlcurrencyid, side, vals[8]) \
    local hedgebookid = redis.call("get", "hedgebook:" .. instrumenttypeid .. ":" .. settlcurrencyid) \
    if not hedgebookid then hedgebookid = 999999 end \
    tradeid = newtrade(vals[1], orderid, symbolid, side, quantity, price, settlcurrencyid, 1, 1, costs, hedgebookid, vals[16], "", KEYS[7], KEYS[3], "", "", settlcurrencyid, consid, 1, 1, vals[8], initialmargin, vals[9], vals[12], finance) \
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
  local quotereqid = redis.call("incr", KEYS[1]) \
  if not quotereqid then return 1005 end \
  --[[ store the quote request ]] \
  redis.call("hmset", KEYS[2] .. quotereqid, "clientid", ARGV[1], "symbolid", ARGV[2], "quantity", ARGV[3], "cashorderqty", ARGV[4], "currencyid", ARGV[5], "settlcurrencyid", ARGV[6], "nosettdays", ARGV[7], "futsettdate", ARGV[8], "quotestatus", "", "timestamp", ARGV[9], "quoterejectreason", "", "quotereqid", quotereqid, "operatortype", ARGV[10], "operatorid", ARGV[11], "side", ARGV[15]) \
  --[[ add to set of quoterequests for this client ]] \
  redis.call("sadd", "client:" .. ARGV[1] .. KEYS[3], quotereqid) \
  --[[ open quote requests ]] \
  redis.call("sadd", KEYS[4], quotereqid) \
  --[[ get required instrument values for external feed ]] \
  local proquotesymbol = getproquotesymbol(ARGV[2]) \
  --[[ get in/out of hours - assume in-hours for now]] \
  local markettype = 0 \
  --[[ local markettype = getmarkettype(ARGV[2], ARGV[12], ARGV[13], ARGV[14]) ]] \
  --[[ assuming equity buy to get default settlement days ]] \
  local defaultnosettdays = redis.call("hget", "cost:" .. "DE" .. ":" .. ARGV[6] .. ":" .. "1", "defaultnosettdays") \
  return {0, quotereqid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], defaultnosettdays, proquotesymbol[5], markettype} \
  ';

  scriptquote = calcfinance + publishquote + '\
  local errorcode = 0 \
  local quoteid = "" \
  --[[ get the quote request ]] \
  local fields = {"clientid", "quoteid", "symbolid", "quantity", "cashorderqty", "nosettdays", "settlcurrencyid", "operatortype", "futsettdate", "operatorid"} \
  local vals = redis.call("hmget", "quoterequest:" .. ARGV[1], unpack(fields)) \
  if not vals[1] then \
    errorcode = 1014 \
    return errorcode \
  end \
  --[[ get touch prices - using delayed - todo: may need to look up delayed/live ]] \
  local symbolid = vals[3] \
  local symbolfields = {"bid", "ask", "instrumenttypeid"} \
  local symbolvals = redis.call("hmget", "symbol:" .. symbolid, unpack(symbolfields)) \
  local bestbid = symbolvals[1] \
  local bestoffer = symbolvals[2] \
  local instrumenttypeid = symbolvals[3] \
  local bidquantity = "" \
  local offerquantity = "" \
  local bidfinance = 0 \
  local offerfinance = 0 \
  --[[ calculate the quantity from the cashorderqty, if necessary, & calculate any finance ]] \
  if ARGV[3] == "" then \
    local offerprice = tonumber(ARGV[4]) \
    if vals[4] == "" then \
      offerquantity = tonumber(ARGV[6]) \
      --[[ offerquantity = round(tonumber(cashorderqty) / offerprice, 0) ]] \
    else \
      offerquantity = tonumber(vals[4]) \
    end \
    --[[ this needs revisiting ]]\
    --[[offerfinance = calcfinance(instrumenttypeid, offerquantity * offerprice, vals[7], 1, vals[6]) ]]\
  else \
    local bidprice = tonumber(ARGV[3]) \
    if vals[4] == "" then \
      bidquantity = tonumber(ARGV[5]) \
      --[[ bidquantity = round(tonumber(cashorderqty) / bidprice, 0) ]] \
    else \
      bidquantity = tonumber(vals[4]) \
    end \
    --[[ this needs revisiting ]]\
    --[[bidfinance = calcfinance(instrumenttypeid, bidquantity * bidprice, vals[7], 2, vals[6]) ]]\
  end \
  --[[ create a quote id as different from external quote ids (one for bid, one for offer)]] \
  quoteid = redis.call("incr", "quoteid") \
  --[[ store the quote ]] \
  redis.call("hmset", "quote:" .. quoteid, "quotereqid", ARGV[1], "clientid", vals[1], "quoteid", quoteid, "symbolid", symbolid, "bestbid", bestbid, "bestoffer", bestoffer, "bidpx", ARGV[3], "offerpx", ARGV[4], "bidquantity", bidquantity, "offerquantity", offerquantity, "bidsize", ARGV[5], "offersize", ARGV[6], "validuntiltime", ARGV[7], "transacttime", ARGV[8], "currencyid", ARGV[9], "settlcurrencyid", ARGV[10], "qbroker", ARGV[11], "nosettdays", vals[6], "futsettdate", vals[9], "bidfinance", bidfinance, "offerfinance", offerfinance, "orderid", "", "bidquotedepth", ARGV[13], "offerquotedepth", ARGV[14], "externalquoteid", ARGV[15], "qclientid", ARGV[16], "cashorderqty", ARGV[17], "settledays", ARGV[18], "noseconds", ARGV[19]) \
  --[[ set of open quotes ]] \
  redis.call("sadd", "openquotes", quoteid) \
  --[[ keep a list of quotes for the quoterequest ]] \
  redis.call("sadd", "quoterequest:" .. ARGV[1] .. ":quotes", quoteid) \
  --[[ quoterequest status - 0=new, 1=quoted, 2=rejected ]] \
  local status \
  --[[ bid or offer size needs to be non-zero ]] \
  if ARGV[5] == 0 and ARGV[6] == 0 then \
    status = "5" \
  else \
    status = "0" \
  end \
  --[[ add status to stored quoterequest ]] \
  redis.call("hmset", "quoterequest:" .. ARGV[1], "quotestatus", status) \
  --[[ publish quote to operator type, with operator id, so can be forwarded as appropriate ]] \
  publishquote(quoteid, vals[8], vals[10]) \
  return errorcode \
  ';

  scriptquoteack = publishquoteack + '\
  --[[ update quote request ]] \
  redis.call("hmset", "quoterequest:" .. ARGV[1], "quotestatus", ARGV[2], "quoterejectreason", ARGV[3], "text", ARGV[4]) \
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
  local symbols = redis.call("sort", "symbols", "ALPHA") \
  local fields = {"instrumenttypeid", "shortname", "currencyid", "marginpercent"} \
  local vals \
  local inst = {} \
  local marginpc \
  for index = 1, #symbols do \
    vals = redis.call("hmget", "symbol:" .. symbols[index], unpack(fields)) \
    if redis.call("sismember", "client:" .. KEYS[1] .. ":instrumenttypes", vals[1]) == 1 then \
      if vals[4] then \
        marginpc = vals[4] \
      else \
        marginpc = 100 \
      end \
      table.insert(inst, {symbolid = symbols[index], shortname = vals[2], currencyid = vals[3], instrumenttypeid = vals[1], marginpercent = marginpc}) \
    end \
  end \
  return cjson.encode(inst) \
  ';

  //
  // countdown timer on open quote requests
  // & remove when counted down
  //
  scriptquotemonitor = '\
  local openquotes = redis.call("smembers", "openquotes") \
  local numopenquotes = 0 \
  for index = 1, #openquotes do \
    local noseconds = tonumber(redis.call("hget", "quote:" .. openquotes[index], "noseconds")) \
    noseconds = noseconds - 1 \
    if noseconds == 0 then \
      redis.call("srem", "openquotes", openquotes[index]) \
    end \
    redis.call("hset", "quote:" .. openquotes[index], "noseconds", noseconds) \
    numopenquotes = redis.call("scard", "openquotes") \
  end \
  return numopenquotes \
  ';
}