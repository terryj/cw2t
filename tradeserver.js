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
var commonbo = require('./commonbo.js');

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
var scriptneworder;
var scriptmatchorder;
var scriptordercancelrequest;
var scriptordercancel;
var scriptorderack;
var scriptnewtrade;
//var scriptQuote;
//var scriptquoteack;
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
  dbsub.subscribe(commonbo.tradeserverchannel);
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

  // create timestamp
  var today = new Date();
  quoterequest.timestamp = commonbo.getUTCTimeStamp(today);

  // get hour & minute for comparison with timezone to determine in/out of hours
  //var hour = today.getHours();
  //var minute = today.getMinutes();
  //var day = today.getDay();

  if (!("accountid" in quoterequest)) {
    quoterequest.accountid = "";
  }

  // if there is no settlement type, set to standard
  if (!("settlmnttypid" in quoterequest)) {
    quoterequest.settlmnttypid = 0;
  }

  if (!("futsettdate" in quoterequest)) {
    quoterequest.futsettdate = "";
  }

  if (!("side" in quoterequest)) {
    quoterequest.side = "";
  }


  // use settlement date, if specified
  /*if (quoterequest.futsettdate != "") {
    quoterequest.settlmnttyp = 6; // future date
  } else if (quoterequest.nosettdays == 2) {
    quoterequest.settlmnttyp = 3;      
  } else if (quoterequest.nosettdays == 3) {
    quoterequest.settlmnttyp = 4;      
  } else if (quoterequest.nosettdays == 4) {
    quoterequest.settlmnttyp = 5;
  } else if (quoterequest.nosettdays > 5) {
    // get settlement date from T+n no. of days
    quoterequest.futsettdate = commonbo.getUTCDateString(commonbo.getSettDate(today, quoterequest.nosettdays, holidays));
    quoterequest.settlmnttyp = 6;
  } else {
    // default
    quoterequest.settlmnttyp = 0;
  }*/

  // store the quote request & get an id
  db.eval(scriptQuoteRequest, 1, "broker:" + quoterequest.brokerid, quoterequest.accountid, quoterequest.brokerid, quoterequest.cashorderqty, quoterequest.clientid, quoterequest.currencyid, quoterequest.futsettdate, quoterequest.operatorid, quoterequest.operatortype, quoterequest.quantity, quoterequest.settlmnttypid, quoterequest.side, quoterequest.symbolid, quoterequest.timestamp, function(err, ret) {
    if (err) throw err;

    // todo:sort out
    if (ret[0] != 0) {
      // todo: send a quote ack to client
      console.log("Error in scriptQuoteRequest:" + commonbo.getReasonDesc(ret[0]));
      return;
    }

    console.log(ret);

    // add the quote request id & symbol details required for fix connection
    quoterequest.quoterequestid = ret[1];
    quoterequest.isin = ret[2][2];
    quoterequest.mnemonic = ret[2][3];
    quoterequest.exchangeid = ret[2][4];

    if (testmode == "1") {
      console.log("test response");
      testQuoteResponse(quoterequest);
    } else {
      console.log("forwarding to nbt");

      // forward the request
      nbt.quoteRequest(quoterequest);
    }
  });
}

    /*quoterequest.markettype = ret[7];
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
        db.publish(commonbo.quoterequestchannel, "{\"quoterequest\":" + JSON.stringify(quoterequest) + "}");
      }
    }*/

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
  console.log("testQuoteAck");
  var quoteack = {};

  console.log(quoterequest);

  quoteack.quoterequestid = quoterequest.quoterequestid;
  quoteack.quoteackstatusid = "";
  quoteack.quoterejectreasonid = "";
  quoteack.text = "too much toblerone";

  db.eval(scriptQuoteAck, 0, quoterequest.brokerid, quoteack.quoterequestid, quoteack.quoteackstatusid, quoteack.quoterejectreasonid, quoteack.text, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }
  });
}

function testQuote(quoterequest, side) {
  var quote = {};

  console.log("sending test quote");

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
  quote.quoterequestid = quoterequest.quoterequestid;
  quote.symbolid = quoterequest.symbolid;
  quote.currencyid = "GBP";
  quote.settlcurrencyid = "GBP";
  quote.quoterid = "ABC";
  quote.quotertype = 1;
  quote.futsettdate = quoterequest.futsettdate;
  quote.externalquoteid = "";
  quote.settledays = 2;

  var today = new Date();
  quote.transacttime = commonbo.getUTCTimeStamp(today);

  var validuntiltime = today;
  quote.noseconds = 30;
  validuntiltime.setSeconds(today.getSeconds() + quote.noseconds);
  quote.validuntiltime = commonbo.getUTCTimeStamp(validuntiltime);

  console.log(quote);

  // quote script
  // note: not passing securityid & idsource as proquote symbol should be enough
  db.eval(scriptQuote, 0, quote.quoterequestid, quote.symbolid, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.transacttime, quote.currencyid, quote.settlcurrencyid, quote.quoterid, quote.quotertype, quote.futsettdate, quote.bidquotedepth, quote.offerquotedepth, quote.externalquoteid, quote.cashorderqty, quote.settledays, quote.noseconds, quoterequest.brokerid, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }
    console.log(ret);

    //todo:sortout
    if (ret != 0) {
      // can't find quote request, so don't know which client to inform
      console.log("Error in scriptquote:" + commonbo.getReasonDesc(ret[0]));
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

  var today = new Date();

  order.timestamp = commonbo.getUTCTimeStamp(today);
  order.partfill = 1; // accept part-fill

  // get hour & minute for comparison with timezone to determine in/out of hours
  //var hour = today.getHours();
  //var minute = today.getMinutes();
  //var day = today.getDay();

  // assume in-hours
  order.markettype = 0;

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
    order.futsettdate = commonbo.getUTCDateString(commonbo.getSettDate(today, order.nosettdays, holidays));
  }

  // todo
  order.nosettdays = 2;

  console.log(order);

  // store the order, get an id & credit check it
  db.eval(scriptneworder, 0, order.accountid, order.brokerid, order.clientid, order.symbolid, order.side, order.quantity, order.price, order.ordertype, order.markettype, order.futsettdate, order.partfill, order.quoteid, order.currencyid, currencyratetoorg, currencyindtoorg, order.timestamp, order.timeinforce, order.expiredate, order.expiretime, order.settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, order.nosettdays, order.operatortype, order.operatorid, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // credit check failed
    if (ret[0] == 0) {
      console.log("credit check failed, order #" + ret[1]);

      // script will publish the order back to the operator type
      return;
    }

    // update order details
    order.orderid = ret[1];
    order.isin = ret[2][0];
    order.mnemonic = ret[2][1];
    order.exchangeid = ret[2][2];
    order.instrumenttypeid = ret[2][4];

    // use the returned quote values required by fix connection
    if (order.ordertype == "D") {
      order.externalquoteid = ret[3][0];
      order.quoterid = ret[3][1];
    }

    // set the settlement date to equity default date for cfd orders, in case they are being hedged with the market
    if (order.instrumenttypeid == "CFD" || order.instrumenttypeid == "SPB") {
      // commented out as only offering default settlement for the time being
      //order.futsettdate = commonbo.getUTCDateString(commonbo.getSettDate(today, ret[11], holidays));

      order.hedgesymbolid = ret[2][3];
      order.hedgeorderid = ret[4];

      // update the stored order settlement details if it is a hedge - todo: req'd?
      /*if (hedgeorderid != "") {
        db.hmset("order:" + hedgeorderid, "nosettdays", ret[11], "futsettdate", order.futsettdate);
      }*/
    }

    processOrder(order);
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

  ocr.timestamp = commonbo.getUTCTimeStamp(new Date());

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

  ofr.timestamp = commonbo.getUTCTimeStamp(today);

  if (!('price' in ofr)) {
    ofr.price = "";
  }

  // calculate a settlement date from the nosettdays
  ofr.futsettdate = commonbo.getUTCDateString(commonbo.getSettDate(today, ofr.nosettdays, holidays));

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
    db.publish(commonbo.tradechannel, "trade:" + ret[1]);
  });
}

function processOrder(order) {
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
      console.log("forwarding hedge order:" + order.hedgeorderid + " to nbt");

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

  exereport.accountid = order.accountid;
  exereport.brokerid = order.brokerid;
  exereport.clientid = order.clientid;
  exereport.clordid = order.orderid;
  exereport.symbolid = order.symbolid;
  exereport.side = order.side;
  exereport.lastshares = order.quantity;
  exereport.lastpx = order.price;
  exereport.currencyid = "GBP";
  exereport.execbroker = order.quoterid;
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

  db.eval(scriptnewtrade, 1, "broker:" + exereport.brokerid, exereport.accountid, exereport.brokerid, exereport.clientid, exereport.clordid, exereport.symbolid, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currencyid, currencyratetoorg, currencyindtoorg, exereport.execbroker, 1, exereport.execid, exereport.futsettdate, exereport.transacttime, exereport.ordstatus, exereport.lastmkt, exereport.leavesqty, exereport.orderid, exereport.settlcurrencyid, exereport.settlcurramt, exereport.settlcurrfxrate, exereport.settlcurrfxratecalc, function(err, ret) {
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
  commonbo.registerScripts();
  registerScripts();
  loadHolidays();
  getTestmode();
}

function loadHolidays() {
  // we are assuming "L"=London
  db.eval(commonbo.scriptgetholidays, 0, "L", function(err, ret) {
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
    quote.transacttime = commonbo.getUTCTimeStamp(today);

    if (!('validuntiltime' in quote)) {
      var validuntiltime = today;
      validuntiltime.setSeconds(today.getSeconds() + quote.noseconds);
      quote.validuntiltime = commonbo.getUTCTimeStamp(validuntiltime);
    }
  } else {
    quote.noseconds = commonbo.getSeconds(quote.transacttime, quote.validuntiltime);
  }

  if (!('settledays' in quote)) {
    quote.settledays = "";
  }

  if ('quoterid' in quote) {
    quote.quotertype = 1;
    quote.quoterid = quote.quoterid;
  }

  // quote script
  db.eval(scriptQuote, 0, quote.quoterequestid, quote.symbolid, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.transacttime, quote.currencyid, quote.settlcurrencyid, quote.quoterid, quote.quotertype, quote.futsettdate, quote.bidquotedepth, quote.offerquotedepth, quote.externalquoteid, quote.cashorderqty, quote.settledays, quote.noseconds, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    //todo:sortout
    if (ret != 0) {
      // can't find quote request, so don't know which client to inform
      console.log("Error in scriptquote:" + commonbo.getReasonDesc(ret[0]));
      return;
    }

    // script publishes quote to the operator type that made the request
  });
}

nbt.on("orderReject", function(exereport) {
  var text = "";
  var orderrejectreasonid = "";
  console.log("order rejected");
  console.log(exereport);

  console.log("order rejected, id:" + exereport.clordid);

  // execution reports vary as to whether they contain a reject reason &/or text
  if ('ordrejreason' in exereport) {
    orderrejectreasonid = exereport.ordrejreason;
  }
  if ('text' in exereport) {
    text = exereport.text;
  }

  db.eval(scriptrejectorder, 3, exereport.clordid, orderrejectreasonid, text, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    //todo:sortout
    if (ret != 0) {
      // todo: message to operator
      console.log("Error in scriptrejectorder, reason:" + commonbo.getReasonDesc(ret));
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
      // todo: send to client & sort out error
      console.log("Error in scriptordercancel, reason:" + commonbo.getReasonDesc(ret[0]));
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
      // todo: send to client & sort out error
      console.log("Error in scriptorderexpire, reason:" + commonbo.getReasonDesc(ret[0]));
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

  db.eval(scriptnewtrade, 0, exereport.clordid, exereport.symbolid, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currencyid, currencyratetoorg, currencyindtoorg, exereport.execbroker, exereport.execid, exereport.futsettdate, exereport.transacttime, exereport.ordstatus, exereport.lastmkt, exereport.leavesqty, exereport.orderid, exereport.settlcurrencyid, exereport.settlcurramt, exereport.settlcurrfxrate, exereport.settlcurrfxratecalc, function(err, ret) {
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
      var sendingtime = new Date(commonbo.getDateString(header.sendingtime));
      var validuntiltime = new Date(commonbo.getDateString(quote.validuntiltime));
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
  console.log("quote ack, request id " + quoteack.quoterequestid);
  console.log(quoteack);

  db.eval(scriptQuoteAck, 0, quoteack.quoterequestid, quoteack.quoteackstatus, quoteack.orderrejectreasonid, quoteack.text, function(err, ret) {
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
  var updateposition;
  var updateordermargin;
  var updatereserve;
  var removefromorderbook;
  var cancelorder;
  var newtrade;
  var getcosts;
  var rejectorder;
  var adjustmarginreserve;
  //var creditcheck;
  var getproquotesymbol;
  //var getinitialmargin;
  //var updatetrademargin;
  var neworder;
  //var getreserve;
  //var getposition;
  var closeposition;
  var createposition;

  /*
  * getcosts()
  * calculates costs for an order/trades
  * params: brokerid, clientid, symbolid, side, consideration, currencyid
  * returns: commission, ptmlevy, stampduty, contractlevy as an array
  */
  getcosts = commonbo.round + '\
  local getcosts = function(brokerid, clientid, symbolid, side, consid, currencyid) \
    local brokerkey = "broker:" .. brokerid \
    local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
    --[[ get costs for this instrument type & currency ]] \
    local costid = redis.call("get", brokerkey .. ":cost:" .. instrumenttypeid .. ":" .. currencyid) \
    local fields = {"commissionpercent", "commissionmax", "commissionmin", "ptmlevylimit", "ptmlevy", "stampdutypercent", "contractlevy"} \
    local vals = redis.call("hmget", brokerkey .. ":cost:" .. costid, unpack(fields)) \
    --[[ set default costs ]] \
    local commission = 0 \
    local ptmlevy = 0 \
    local stampduty = 0 \
    local contractlevy = 0 \
    --[[ commission ]] \
    local commpercent = redis.call("hget", brokerkey .. ":client:" .. clientid, "commissionpercent") \
    if not commpercent or commpercent == "" then \
      commpercent = 0 \
    else \
      commpercent = tonumber(commpercent) \
    end \
    --[[ use client commission rate, if there is one ]] \
    if commpercent == 0 then \
      --[[ otherwise use standard commission rate ]] \
      if vals[1] then commpercent = tonumber(vals[1]) end \
    end \
    commission = round(consid * commpercent / 100, 2) \
    --[[ check commission against min commission ]] \
    if vals[3] and vals[3] ~= "" then \
      local mincommission = tonumber(vals[3]) \
      if commission < mincommission then \
        commission = mincommission \
      end \
    end \
    --[[ check commission against max commission ]] \
    if vals[2] and vals[2] ~= "" then \
      local maxcommission = tonumber(vals[2]) \
      if commission < maxcommission then \
        commission = maxcommission \
      end \
    end \
    --[[ ptm levy ]] \
    local ptmexempt = redis.call("hget", "symbol:" .. symbolid, "ptmexempt") \
    if ptmexempt and tonumber(ptmexempt) == 1 then \
    else \
      --[[ only calculate ptm levy if product is not exempt ]] \
      if vals[4] and vals[4] ~= "" then \
        local ptmlevylimit = tonumber(vals[4]) \
        if consid > ptmlevylimit then \
          if vals[5] and vals[5] ~= "" then \
            ptmlevy = tonumber(vals[5]) \
          end \
        end \
      end \
    end \
    --[[ stamp duty ]] \
    if vals[6] and vals[6] ~= "" then \
      local stampdutypercent = tonumber(vals[6]) \
      stampduty = round(consid * stampdutypercent / 100, 2) \
    end \
    --[[ contract levy ]] \
    if vals[7] and vals[7] ~= "" then \
      contractlevy = tonumber(vals[7]) \
    end \
    return {commission, ptmlevy, stampduty, contractlevy} \
  end \
  ';

  /*
  * getinitialmargin()
  * calcualtes initial margin required for an order, including costs
  * params: brokerid, symbolid, consid, totalcosts
  * returns: initialmargin
  */
  getinitialmargin = '\
  local getinitialmargin = function(brokerid, symbolid, consid, totalcost) \
    local marginpercent = redis.call("hget", "broker:" .. brokerid .. "brokersymbol:" .. symbolid, "marginpercent") \
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
  /*createposition = '\
  local createposition = function(positionkey, positionskey, postradeskey, brokerid, accountid, symbolid, quantity, cost, currencyid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
    local positionid = redis.call("incr", "positionid") \
    redis.call("hmset", positionkey, "brokerid", brokerid, "accountid", accountid, "symbolid", symbolid, "quantity", quantity, "cost", cost, "currencyid", currencyid, "positionid", positionid, "futsettdate", futsettdate) \
    redis.call("sadd", positionskey, symbolsettdatekey) \
    if positionskeysettdate ~= "" then \
      redis.call("sadd", positionskeysettdate, futsettdate) \
    end \
    --[[redis.call("sadd", "position:" .. symbolsettdatekey .. ":clients", clientid) ]]\
    redis.call("sadd", postradeskey, tradeid) \
    return positionid \
  end \
  ';*/

  //
  // close a position
  //
  /*closeposition = '\
  local closeposition = function(positionkey, positionskey, postradeskey, brokerid, accountid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
    redis.call("hdel", positionkey, "brokerid", "accountid", "symbolid", "side", "quantity", "cost", "currencyid", "margin", "positionid", "futsettdate") \
    redis.call("srem", positionskey, symbolsettdatekey) \
    if positionskeysettdate ~= "" then \
      redis.call("srem", positionskeysettdate, futsettdate) \
    end \
    --[[redis.call("srem", "position:" .. symbolsettdatekey .. ":clients", clientid) ]]\
    local postrades = redis.call("smembers", postradeskey) \
    for index = 1, #postrades do \
      redis.call("srem", postradeskey, postrades[index]) \
    end \
  end \
  ';*/

  //
  // publish a position
  // key may be just a symbol or symbol + settlement date
  //
  /*publishposition = commonbo.getunrealisedpandl + commonbo.getmargin + '\
  local publishposition = function(brokerid, accountid, symbolid, futsettdate, channel) \
    local fields = {"quantity", "cost", "currencyid", "positionid", "futsettdate", "symbolid"} \
    local vals = redis.call("hmget", "broker:" .. brokerid .. ":account" .. accountid .. ":position:" .. symbolid, unpack(fields)) \
    local pos = {} \
    if vals[1] then \
      local margin = getmargin(vals[6], vals[1]) \
      --[[ value the position ]] \
      local unrealisedpandl = getunrealisedpandl(vals[6], vals[1], vals[2]) \
      pos = {brokerid=brokerid,accountid=accountid,symbolid=vals[6],quantity=vals[1],cost=vals[2],currencyid=vals[3],margin=margin,positionid=vals[4],futsettdate=vals[5],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]} \
    else \
      pos = {brokerid=brokerid,accountid=accountid,symbolid=symbolid,quantity=0,futsettdate=futsettdate} \
    end \
    redis.call("publish", channel, "{" .. cjson.encode("position") .. ":" .. cjson.encode(pos) .. "}") \
  end \
  ';*/

  //
  // positions are keyed on accountid + symbol - quantity is stored as a +ve/-ve value
  // they are linked to a list of trade ids to provide further information, such as settlement date, if required
  // a position id is allocated against a position and stored against the trade
  //
  /*updateposition = commonbo.round + closeposition + createposition + publishposition + '\
  local updateposition = function(brokerid, accountid, symbolid, side, tradequantity, tradeprice, tradecost, currencyid, tradeid, futsettdate) \
    redis.log(redis.LOG_DEBUG, "updateposition") \
    local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
    local brokeraccountkey = "broker:" .. brokerid .. ":account:" .. accountid \
    local positionkey = brokeraccountkey .. ":position:" .. symbolid \
    local postradeskey = brokeraccountkey .. ":trades:" .. symbolid \
    local positionskey = brokeraccountkey .. ":positions" \
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
          closeposition(positionkey, positionskey, postradeskey, brokerid, accountid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
        elseif tradequantity > math.abs(posqty) then \
          --[[ close position ]] \
          closeposition(positionkey, positionskey, postradeskey, brokerid, accountid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
          --[[ & open new ]] \
          posqty = posqty + tradequantity \
          poscost = round(posqty * tonumber(tradeprice), 5) \
          positionid = createposition(positionkey, positionskey, postradeskey, brokerid, accountid, symbolid, posqty, poscost, currencyid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
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
          closeposition(positionkey, positionskey, postradeskey, brokerid, accountid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
        elseif tradequantity > posqty then \
          --[[ close position ]] \
          closeposition(positionkey, positionskey, postradeskey, brokerid, accountid, symbolid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
          --[[ & open new ]] \
          posqty = posqty - tradequantity \
          poscost = round(posqty * tonumber(tradeprice), 5) \
          positionid = createposition(positionkey, positionskey, postradeskey, brokerid, accountid, symbolid, posqty, poscost, currencyid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
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
      positionid = createposition(positionkey, positionskey, postradeskey, brokerid, accountid, symbolid, posqty, tradecost, currencyid, tradeid, futsettdate, symbolsettdatekey, positionskeysettdate) \
    end \
    publishposition(brokerid, accountid, symbolid, futsettdate, 10) \
    return positionid \
  end \
  ';*/

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

  /*
  * rejectorder()
  * rejects an order with a reason & any additional text
  */
  rejectorder = '\
  local rejectorder = function(brokerid, orderid, orderrejectreasonid, text) \
    redis.log(redis.LOG_DEBUG, "rejectorder") \
    redis.call("hmset", "broker:" .. brokerid .. ":order:" .. orderid, "orderstatusid", "8", "orderrejectreasonid", orderrejectreasonid, "text", text) \
  end \
  ';

  /*getreserve = '\
  local getreserve = function(clientid, symbolid, currencyid, settldate) \
    local reserve = redis.call("hget", clientid .. ":reserve:" .. symbolid .. ":" .. currencyid .. ":" .. settldate, "quantity") \
    if not reserve then \
      return 0 \
    end \
    return reserve \
  end \
  ';*/

  /*
  * creditcheck()
  * credit checks an order
  * params: accountid, brokerid, orderid, clientid, symbolid, side, quantity, price, currencyid, settldate, nosettdays
  * returns: 0=fail/1=succeed, inialmargin, costs as a table
  */
  creditcheck = rejectorder + getinitialmargin + getcosts + commonbo.getposition + commonbo.getfreemargin + '\
  local creditcheck = function(accountid, brokerid, orderid, clientid, symbolid, side, quantity, price, currencyid, settldate, nosettdays) \
    --[[ see if client is allowed to trade this product ]] \
    local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
    if not instrumenttypeid then \
      rejectorder(brokerid, orderid, 0, "Symbol not found") \
      return {0} \
    end \
    --[[ todo: reinstate - commented out until instrument types added to client setup ]]\
    --[[if redis.call("sismember", "broker:" .. brokerid .. ":client:" .. clientid .. ":instrumenttypes", instrumenttypeid) == 0 then ]]\
      --[[rejectorder(brokerid, orderid, 0, "Client not authorised to trade this type of product") ]]\
      --[[return {0} ]]\
    --[[end ]]\
    side = tonumber(side) \
    quantity = tonumber(quantity) \
    local consid = tonumber(quantity) * tonumber(price) \
    --[[ calculate costs ]] \
    local costs =  getcosts(brokerid, clientid, symbolid, side, consid, currencyid) \
    local totalcost = costs[1] + costs[2] + costs[3] + costs[4] \
    redis.log(redis.LOG_DEBUG, "costs") \
    redis.log(redis.LOG_DEBUG, costs[1]) \
    redis.log(redis.LOG_DEBUG, costs[2]) \
    redis.log(redis.LOG_DEBUG, costs[3]) \
    redis.log(redis.LOG_DEBUG, costs[4]) \
    --[[ calculate margin required for order ]] \
    local initialmargin = getinitialmargin(brokerid, symbolid, consid, totalcost) \
    --[[ position key varies by instrument type ]] \
    local symbolsettdatekey = symbolid \
    if instrumenttypeid == "CFD" or instrumenttypeid == "SPB" then \
      symbolsettdatekey = symbolsettdatekey .. ":" .. settldate \
    end \
    --[[ get position, if there is one, as may be a closing buy or sell ]] \
    local position = getposition(accountid, brokerid, symbolsettdatekey) \
    if position[1] then \
      --[[ we have a position - always allow closing trades ]] \
      local posqty = tonumber(position[1]) \
      local poscost = tonumber(position[2]) \
      if (side == 1 and posqty < 0) or (side == 2 and posqty > 0) then \
        if quantity <= math.abs(posqty) then \
          --[[ closing trade, so ok ]] \
          return {1, initialmargin, costs} \
        end \
        if side == 2 and (instrumenttypeid == "DE" or instrumenttypeid == "IE") then \
            --[[ equity, so cannot sell more than we have ]] \
            rejectorder(brokerid, orderid, 0, "Quantity greater than position quantity") \
            return {0} \
        end \
        --[[ we are trying to close a quantity greater than current position, so need to check we can open a new position ]] \
        local freemargin = getfreemargin(accountid, brokerid) \
        --[[ add the margin returned by closing the position ]] \
        local margin = getmargin(symbolid, math.abs(posqty)) \
        --[[ get initial margin for remaining quantity after closing position ]] \
        initialmargin = getinitialmargin(brokerid, symbolid, (quantity - math.abs(posqty)) * price, totalcost) \
        if initialmargin + totalcost > freemargin + margin then \
          rejectorder(brokerid, orderid, 0, "Insufficient free margin") \
          return {0} \
        end \
        --[[ closing trade or enough margin to close & open a new position, so ok ]] \
        return {1, initialmargin, costs} \
      end \
    end \
    --[[ check free margin for all derivative trades & equity buys ]] \
    if instrumenttypeid == "CFD" or instrumenttypeid == "SPB" or instrumenttypeid == "CCFD" or side == 1 then \
      local freemargin = getfreemargin(accountid, brokerid) \
      redis.log(redis.LOG_DEBUG, "initialmargin") \
      redis.log(redis.LOG_DEBUG, initialmargin) \
      redis.log(redis.LOG_DEBUG, "freemargin") \
      redis.log(redis.LOG_DEBUG, freemargin) \
      if initialmargin + totalcost > freemargin then \
        rejectorder(brokerid, orderid, 0, "Insufficient free margin") \
        return {0} \
      end \
      --[[updateordermargin(orderid, clientid, 0, currencyid, initialmargin) ]]\
    else \
      --[[ allow ifa certificated equity sells ]] \
      if instrumenttypeid == "DE" then \
        if redis.call("hget", "client:" .. clientid, "type") == "3" then \
          return {1, initialmargin, costs} \
        end \
      end \
      --[[ check there is a position ]] \
      if not position[1] then \
        rejectorder(brokerid, orderid, 0, "No position held in this instrument") \
        return {0} \
      end \
      local posqty = tonumber(position[1]) \
      --[[ check the position is long ]] \
      if posqty < 0 then \
        rejectorder(brokerid, orderid, 0, "Insufficient position size in this instrument") \
        return {0} \
      end \
      if quantity > posqty then \
        rejectorder(brokerid, orderid, 1004, "Insufficient position size in this instrument") \
        return {0} \
      end \
    end \
    return {1, initialmargin, costs} \
  end \
  ';

  cancelorder = adjustmarginreserve + '\
  local cancelorder = function(brokerid, orderid, orderstatusid) \
    local orderkey = "broker:" .. brokerid .. "order:" .. orderid \
    redis.call("hset", orderkey, "orderstatusid", orderstatusid) \
    local fields = {"clientid", "symbolid", "side", "price", "settlcurrencyid", "margin", "remquantity", "futsettdate", "nosettdays"} \
    local vals = redis.call("hmget", orderkey, unpack(fields)) \
    if not vals[1] then \
      return 1009 \
    end \
    adjustmarginreserve(orderid, vals[1], vals[2], vals[3], vals[4], vals[6], vals[5], vals[7], 0, vals[8], vals[9]) \
    return 0 \
  end \
  ';

  /*
  * getproquotesymbol()
  * get symbol details for external feed
  * params: symbol
  * returns: isin, mnemonic, exchangeid, hedgesymbolid, instrumenttypeid & timezoneid as a table
  */
  getproquotesymbol = '\
  local getproquotesymbol = function(symbolid) \
    redis.log(redis.LOG_DEBUG, "getproquotesymbol") \
    if symbolid == nil then \
      return {"","","","","",""} \
    end \
    local fields = {"isin", "mnemonic", "exchangeid", "hedgesymbolid", "instrumenttypeid", "timezoneid"} \
    local vals = redis.call("hmget", "symbol:" .. symbolid, unpack(fields)) \
    return vals \
  end \
  ';

  /*
  * getproquotequote()
  * get quote details
  * params: brokerid, quoteid
  * returns: externalquoteid, quoterid as a table
  */
  getproquotequote = '\
  local getproquotequote = function(brokerid, quoteid) \
    redis.log(redis.LOG_DEBUG, "getproquotequote") \
    if quoteid == nil then \
      return {"",""} \
    end \
    local fields = {"externalquoteid", "quoterid"} \
    local vals = redis.call("hmget", "broker:" .. brokerid .. ":quote:" .. quoteid, unpack(fields)) \
    return vals \
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
  local publishquoteack = function(brokerid, quoterequestid) \
    local fields = {"accountid", "clientid", "symbolid", "operatortype", "quotestatusid", "quoterejectreasonid", "text", "operatorid"} \
    local vals = redis.call("hmget", "broker:" .. brokerid .. ":quoterequest:" .. quoterequestid, unpack(fields)) \
    local quoteack = {brokerid=brokerid, quoterequestid=quoterequestid, accountid=vals[1], clientid=vals[2], symbolid=vals[3], quotestatusid=vals[5], quoterejectreasonid=vals[6], text=vals[7], operatorid=vals[8]} \
    redis.call("publish", vals[4], "{" .. cjson.encode("quoteack") .. ":" .. cjson.encode(quoteack) .. "}") \
    end \
  ';

  publishquote = '\
  local publishquote = function(brokerid, quoteid, channel, operatorid) \
    local fields = {"quoterequestid","accountid","clientid","quoteid","symbolid","bidpx","offerpx","bidquantity","offerquantity","validuntiltime","transacttime","settlcurrencyid","futsettdate","bidsize","offersize","quoterid","quotertype","noseconds"} \
    local vals = redis.call("hmget", "broker:" .. brokerid .. ":quote:" .. quoteid, unpack(fields)) \
    local quote = {brokerid=brokerid,quoterequestid=vals[1],accountid=vals[2],clientid=vals[3],quoteid=vals[4],symbolid=vals[5],bidpx=vals[6],offerpx=vals[7],bidquantity=vals[8],offerquantity=vals[9],validuntiltime=vals[10],transacttime=vals[11],settlcurrencyid=vals[12],futsettdate=vals[13],bidsize=vals[14],offersize=vals[15],quoterid=vals[16],quotertype=vals[17],operatorid=operatorid,noseconds=vals[18]} \
    redis.call("publish", channel, "{" .. cjson.encode("quote") .. ":" .. cjson.encode(quote) .. "}") \
  end \
  ';

  publishorder = '\
  local publishorder = function(brokerid, orderid, channel) \
    redis.log(redis.LOG_DEBUG, "publishorder") \
    local fields = {"accountid","clientid","symbolid","side","quantity","price","ordertype","remquantity","orderstatusid","markettype","futsettdate","partfill","quoteid","currencyid","currencyratetoorg","currencyindtoorg","timestamp","margin","timeinforce","expiredate","expiretime","settlcurrencyid","settlcurrfxrate","settlcurrfxratecalc","orderid","externalorderid","execid","nosettdays","operatortype","operatorid","hedgeorderid","text","orderrejectreasonid"} \
    local vals = redis.call("hmget", "broker:" .. brokerid .. ":order:" .. orderid, unpack(fields)) \
    local order = {accountid=vals[1],brokerid=brokerid,clientid=vals[2],symbolid=vals[3],side=vals[4],quantity=vals[5],price=vals[6],ordertype=vals[7],remquantity=vals[8],orderstatusid=vals[9],markettype=vals[10],futsettdate=vals[11],partfill=vals[12],quoteid=vals[13],currencyid=vals[14],currencyratetoorg=vals[15],currencyindtoorg=vals[16],timestamp=vals[17],margin=vals[18],timeinforce=vals[19],expiredate=vals[20],expiretime=vals[21],settlcurrencyid=vals[22],settlcurrfxrate=vals[23],settlcurrfxratecalc=vals[24],orderid=vals[25],externalorderid=vals[26],execid=vals[27],nosettdays=vals[28],operatortype=vals[29],operatorid=vals[30],hedgeorderid=vals[31],text=vals[32],orderrejectreasonid=vals[33]} \
    redis.call("publish", channel, "{" .. cjson.encode("order") .. ":" .. cjson.encode(order) .. "}") \
  end \
  ';

  publishtrade = '\
  local publishtrade = function(brokerid, tradeid, channel) \
    local brokerkey = "broker:" .. brokerid \
    local fields = {"accountid","brokerid","clientid","orderid","symbolid","side","quantity","price","currencyid","currencyratetoorg","currencyindtoorg","commission","ptmlevy","stampduty","contractcharge","counterpartyid","markettype","externaltradeid","futsettdate","timestamp","lastmkt","externalorderid","tradeid","settlcurrencyid","settlcurramt","settlcurrfxrate","settlcurrfxratecalc","nosettdays","finance","margin","positionid"} \
    local vals = redis.call("hmget", brokerkey .. ":trade:" .. tradeid, unpack(fields)) \
    local trade = {accountid=vals[1],brokerid=vals[2],clientid=vals[3],orderid=vals[4],symbolid=vals[5],side=vals[6],quantity=vals[7],price=vals[8],currencyid=vals[9],currencyratetoorg=vals[10],currencyindtoorg=vals[11],commission=vals[12],ptmlevy=vals[13],stampduty=vals[14],contractcharge=vals[15],counterpartyid=vals[16],markettype=vals[17],externaltradeid=vals[18],futsettdate=vals[19],timestamp=vals[20],lastmkt=vals[21],externalorderid=vals[22],tradeid=vals[23],settlcurrencyid=vals[24],settlcurramt=vals[25],settlcurrfxrate=vals[26],settlcurrfxratecalc=vals[27],nosettdays=vals[28],finance=vals[29],margin=vals[30],positionid=vals[31]} \
    redis.call("publish", channel, "{" .. cjson.encode("trade") .. ":" .. cjson.encode(trade) .. "}") \
  end \
  ';

  newtrade = updateposition + commonbo.newtradeaccounttransactions + publishtrade + '\
  local newtrade = function(accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, costs, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, nosettdays, margin, operatortype, operatorid, finance) \
    redis.log(redis.LOG_DEBUG, "newtrade") \
    local brokerkey = "broker:" .. brokerid \
    local tradeid = redis.call("hincrby", brokerkey, "lasttradeid", 1) \
    if not tradeid then return 0 end \
    local tradekey = brokerkey .. ":trade:" .. tradeid \
    redis.call("hmset", tradekey, "accountid", accountid, "brokerid", brokerid, "clientid", clientid, "orderid", orderid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "commission", costs[1], "ptmlevy", costs[2], "stampduty", costs[3], "contractcharge", costs[4], "counterpartyid", counterpartyid, "counterpartytype", counterpartytype, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", tradeid, "settlcurrencyid", settlcurrencyid, "settlcurramt", settlcurramt, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "nosettdays", nosettdays, "margin", margin, "finance", finance) \
    redis.call("sadd", brokerkey .. ":trades", tradeid) \
    redis.call("sadd", brokerkey .. ":order:" .. orderid .. ":trades", tradeid) \
    local note \
    if tonumber(side) == 1 then \
      note = "Bought " .. quantity .. " " .. symbolid .. " @ " .. price \
    else \
      note = "Sold " .. quantity .. " " .. symbolid .. " @ " .. price \
    end \
    local retval = newtradeaccounttransactions(settlcurramt, costs[1], costs[2], costs[3], brokerid, accountid, settlcurrencyid, settlcurramt, note, 1, timestamp, tradeid, side) \
    local positionid = updateposition(brokerid, accountid, symbolid, side, quantity, price, settlcurramt, settlcurrencyid, tradeid, futsettdate) \
    redis.call("hset", tradekey, "positionid", positionid) \
    publishtrade(brokerid, tradeid, 6) \
    return tradeid \
  end \
  ';

  //    redis.call("zadd", brokerkey .. ":account:" .. accountid .. ":tradesbydate", milliseconds, tradeid) \

/*
    local transtype \
    local drcr \
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
*/

  /*
  * neworder()
  * gets an orderid & saves the order
  */
  neworder = '\
  local neworder = function(accountid, brokerid, clientid, symbolid, side, quantity, price, ordertype, remquantity, orderstatusid, markettype, futsettdate, partfill, quoteid, currencyid, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforce, expiredate, expiretime, settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, nosettdays, operatortype, operatorid, hedgeorderid) \
    local brokerkey = "broker:" .. brokerid \
    --[[ get a new orderid & store the order ]] \
    local orderid = redis.call("hincrby", brokerkey, "lastorderid", 1) \
    redis.call("hmset", brokerkey .. ":order:" .. orderid, "accountid", accountid, "brokerid", brokerid, "clientid", clientid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "ordertype", ordertype, "remquantity", remquantity, "orderstatusid", orderstatusid, "markettype", markettype, "futsettdate", futsettdate, "partfill", partfill, "quoteid", quoteid, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "timestamp", timestamp, "margin", margin, "timeinforce", timeinforce, "expiredate", expiredate, "expiretime", expiretime, "settlcurrencyid", settlcurrencyid, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "orderid", orderid, "externalorderid", externalorderid, "execid", execid, "nosettdays", nosettdays, "operatortype", operatortype, "operatorid", operatorid, "hedgeorderid", hedgeorderid, "orderrejectreasonid", "", "text", "") \
    --[[ add to set of orders ]] \
    redis.call("sadd", brokerkey .. ":orders", orderid) \
    --[[ add to set of orders for this client ]] \
    redis.call("sadd", brokerkey .. ":account:" .. accountid .. ":orders", orderid) \
    --[[ add order id to associated quote, if there is one ]] \
    if quoteid ~= "" then \
      redis.call("hset", brokerkey .. ":quote:" .. quoteid, "orderid", orderid) \
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
  // params: order id, orderrejectreasonid, text
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

  //  redis.log(redis.LOG_DEBUG, cc[1]) \

  /*
  * scriptneworder
  * params: 1=accountid, 2=brokerid, 3=clientid, 4=symbolid, 5=side, 6=quantity, 7=price, 8=ordertype, 9=markettype, 10=futsettdate, 11=partfill, 12=quoteid, 13=currencyid, 14=currencyratetoorg, 15=currencyindtoorg, 16=timestamp, 17=timeinforce, 18=expiredate, 19=expiretime, 20=settlcurrencyid, 21=settlcurrfxrate, 22=settlcurrfxratecalc, 23=nosettdays, 24=operatortype, 25=operatorid
  * returns: orderid
  */
  scriptneworder = neworder + rejectorder + creditcheck + publishorder + getproquotesymbol + getproquotequote + '\
  local orderid = neworder(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[6], 0, ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13], ARGV[14], ARGV[15], ARGV[16], 0, ARGV[17], ARGV[18], ARGV[19], ARGV[20], ARGV[21], ARGV[22], "", "", ARGV[23], ARGV[24], ARGV[25], "") \
  local brokerid = ARGV[2] \
  local brokerkey = "broker:" .. brokerid \
  local symbolid = ARGV[4] \
  local side = tonumber(ARGV[5]) \
  local settlcurramt = tonumber(ARGV[6]) * tonumber(ARGV[7]) \
  local cc = creditcheck(ARGV[1], brokerid, orderid, ARGV[3], symbolid, side, ARGV[6], ARGV[7], ARGV[20], ARGV[10], ARGV[23]) \
   if cc[1] == 0 then \
    --[[ publish the order back to the operatortype - the order contains the error ]] \
    publishorder(brokerid, orderid, ARGV[24]) \
    return {cc[1], orderid} \
  end \
  local hedgebookid = "" \
  local tradeid = "" \
  local hedgeorderid = "" \
  local hedgetradeid = "" \
  local proquotesymbol = getproquotesymbol(symbolid) \
  local proquotequote = getproquotequote(brokerid, ARGV[12]) \
  local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
  if instrumenttypeid == "CFD" or instrumenttypeid == "SPB" or instrumenttypeid == "CCFD" then \
    --[[ ignore limit orders for derivatives as they will be handled manually, at least for the time being ]] \
    if ARGV[8] ~= "2" then \
      --[[ create trades for client & hedge book for off-exchange products ]] \
      hedgebookid = redis.call("get", brokerkey .. ":hedgebook:" .. instrumenttypeid .. ":" .. ARGV[20]) \
      if not hedgebookid then hedgebookid = 999999 end \
      local rside = reverseside(side) \
      local hedgecosts = {0,0,0,0} \
      tradeid = newtrade(ARGV[1], ARGV[2], ARGV[3], orderid, symbolid, side, ARGV[6], ARGV[7], ARGV[13], 1, 1, cc[3], hedgebookid, ARGV[11], "", ARGV[10], ARGV[16], "", "", ARGV[20], settlcurramt, ARGV[21], ARGV[22], ARGV[23], cc[2], ARGV[24], ARGV[25], cc[4]) \
      hedgetradeid = newtrade(hedgebookid, orderid, symbolid, rside, ARGV[6], ARGV[7], ARGV[13], 1, 1, hedgecosts, ARGV[3], ARGV[11], "", ARGV[10], ARGV[16], "", "", ARGV[20], settlcurramt, ARGV[21], ARGV[22], ARGV[23], 0, ARGV[24], ARGV[25], 0) \
      --[[ adjust order as filled ]] \
      redis.call("hmset", brokerkey .. ":order:" .. orderid, "remquantity", 0, "orderstatusid", 2) \
      --[[ todo: may need to adjust margin here ]] \
      --[[ see if we need to hedge this trade in the market ]] \
      local hedgeclient = tonumber(redis.call("hget", brokerkey .. ":client:" .. ARGV[3], "hedge")) \
      local hedgeinst = tonumber(redis.call("hget", brokerkey ":brokersymbol:" .. symbolid, "hedge")) \
      if hedgeclient == 1 or hedgeinst == 1 then \
        --[[ create a hedge order in the underlying product ]] \
        if proquotesymbol[4] then \
          hedgeorderid = neworder(hedgebookid, proquotesymbol[4], ARGV[5], ARGV[6], ARGV[7], "X", ARGV[6], 0, ARGV[11], ARGV[10], ARGV[11], "", ARGV[13], ARGV[14], ARGV[15], ARGV[14], 0, ARGV[17], ARGV[18], ARGV[19], ARGV[20], ARGV[21], ARGV[22], "", "", ARGV[23], ARGV[24], ARGV[25], orderid, "") \
        end \
      end \
    end \
  else \
    --[[ this is an equity - just consider external orders for the time being - todo: internal ]] \
    --[[ todo: consider equity limit orders ]] \
  end \
  return {cc[1], orderid, proquotesymbol, proquotequote} \
  ';

  //local defaultnosettdays = redis.call("hget", "cost:" .. instrumenttypeid .. ":" .. ARGV[20] .. ":" .. side, "defaultnosettdays") \

  //local markettype = getmarkettype(symbolid, ARGV[24], ARGV[25], ARGV[26]) \

    /*if ARGV[9] == 0 then \
      --[[ see if we need to send this trade to the market - if either product or client hedge, then send ]] \
      local hedgeclient = tonumber(redis.call("hget", brokerkey .. ":client:" .. ARGV[3], "hedge")) \
      local hedgeinst = tonumber(redis.call("hget", brokerkey .. ":brokersymbol:" .. symbolid, "hedge")) \
      if hedgeclient == 0 and hedgeinst == 0 then \
        --[[ we are taking on the trade, so create trades for client & hedge book ]] \
        hedgebookid = redis.call("get", brokerkey .. ":hedgebook:" .. instrumenttypeid .. ":" .. ARGV[20]) \
        if not hedgebookid then hedgebookid = 999999 end \
        local rside = reverseside(side) \
        local hedgecosts = {0,0,0,0} \
        tradeid = newtrade(ARGV[1], ARGV[2], ARGV[3], orderid, symbolid, side, ARGV[6], ARGV[7], ARGV[13], 1, 1, cc[3], hedgebookid, ARGV[11], "", ARGV[10], ARGV[16], "", "", ARGV[20], settlcurramt, ARGV[21], ARGV[22], ARGV[23], cc[2], ARGV[24], ARGV[25], cc[4]) \
        hedgetradeid = newtrade(ARGV[1], ARGV[2], hedgebookid, orderid, symbolid, rside, ARGV[6], ARGV[7], ARGV[13], 1, 1, hedgecosts, ARGV[3], ARGV[11], "", ARGV[10], ARGV[16], "", "", ARGV[20], settlcurramt, ARGV[21], ARGV[22], ARGV[23], 0, ARGV[24], ARGV[25], 0) \
      else \
        proquotesymbol = getproquotesymbol(symbolid) \
        proquotequote = getproquotequote(ARGV[12]) \
        defaultnosettdays = redis.call("hget", "cost:" .. instrumenttypeid .. ":" .. ARGV[20] .. ":" .. side, "defaultnosettdays") \
      end \
    end \*/

  /*if ARGV[8] == "D" then \
  --[[ see if order was quoted by a client ]] \
    qclientid = redis.call("hget", "quote:" .. ARGV[10], "qclientid") \
    if qclientid and qclientid ~= "" then \
      local hedgecosts = {0,0,0,0} \
      local rside = reverseside(side) \
      --[[ create trades for both clients ]] \
      tradeid = newtrade(ARGV[1], orderid, symbolid, side, ARGV[4], ARGV[5], ARGV[11], 1, 1, cc[3], qclientid, ARGV[9], "", ARGV[8], ARGV[14], "", "", ARGV[18], settlcurramt, ARGV[19], ARGV[20], ARGV[21], cc[2], ARGV[22], ARGV[23], cc[4]) \
      hedgetradeid = newtrade(qclientid, orderid, symbolid, rside, ARGV[4], ARGV[5], ARGV[11], 1, 1, hedgecosts, ARGV[1], ARGV[9], "", ARGV[8], ARGV[14], "", "", ARGV[18], settlcurramt, ARGV[19], ARGV[20], ARGV[21], 0, ARGV[22], ARGV[23], 0) \
      --[[ adjust order as filled ]] \
      redis.call("hmset", "order:" .. orderid, "remquantity", 0, "status", 2) \
      return {cc[1], orderid, proquotesymbol[1], proquotesymbol[2], proquotesymbol[3], proquotequote[1], proquotequote[2], instrumenttypeid, hedgeorderid, tradeid, hedgetradeid, defaultnosettdays, markettype, qclientid} \
    end \
  end \*/

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
      local matchorderstatusid \
      if matchremquantity == 0 then \
        removefromorderbook(matchvals[2], matchorders[i]) \
        matchorderstatusid = 2 \
      else \
        matchorderstatusid = 1 \
      end \
      redis.call("hmset", "order:" .. matchorders[i], "remquantity", matchremquantity, "orderstatusid", matchorderstatusid) \
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
  local orderstatusid = "0" \
  if remquantity > 0 then \
    addtoorderbook(vals[2], KEYS[1], vals[3], vals[5]) \
  else \
    orderstatusid = "2" \
  end \
  if remquantity < tonumber(vals[4]) then \
    --[[ reduce margin/reserve that has been added in the credit check ]] \
    adjustmarginreserve(KEYS[1], clientid, vals[2], vals[3], vals[5], vals[7], vals[6], vals[8], remquantity, vals[9], vals[11]) \
    if remquantity ~= 0 then \
      orderstatusid = "1" \
    end \
  end \
  --[[ update active order ]] \
  redis.call("hmset", "broker:" .. brokerid .. "order:" .. KEYS[1], "remquantity", remquantity, "orderstatusid", orderstatusid) \
  return {mo, t, mt, mc} \
  ';

  //
  // fill from the market
  //
  scriptnewtrade = newtrade + getcosts + '\
  --[[ get order ]] \
  local orderid = ARGV[4] \
  local fields = {"clientid", "symbolid", "side", "quantity", "price", "margin", "remquantity", "nosettdays", "operatortype", "hedgeorderid", "futsettdate", "operatorid", "accountid", "brokerid"} \
  local vals = redis.call("hmget", KEYS[1] .. ":order:" .. orderid, unpack(fields)) \
  --[[ get costs ]] \
  local consid = tonumber(ARGV[7]) * tonumber(ARGV[8]) \
  local costs = getcosts(ARGV[2], ARGV[3], ARGV[5], ARGV[6], consid, ARGV[20]) \
  --[[ finance/margin may be needed for derivs ]] \
  local finance = 0 \
  local margin = 0 \
  --[[ create trade ]] \
  local tradeid = newtrade(ARGV[1], ARGV[2], ARGV[3], ARGV[4], vals[2], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], costs, ARGV[12], ARGV[13], 0, ARGV[14], ARGV[15], ARGV[16], ARGV[18], ARGV[20], ARGV[21], ARGV[22], ARGV[23], ARGV[24], vals[8], margin, vals[9], vals[12], finance) \
  --[[ adjust order ]] \
  redis.call("hmset", KEYS[1] .. ":order:" .. orderid, "remquantity", ARGV[18], "orderstatusid", ARGV[16]) \
  return tradeid \
  ';

    //--[[local initialmargin = getinitialmargin(vals[2], consid) ]]\
  //local instrumenttypeid = redis.call("hget", "symbol:" .. ARGV[4], "instrumenttypeid") \
  /*--[[local cptytradeid = newtrade(cptyid, orderid, vals[2], rside, quantity, price, ARGV[6], ARGV[7], ARGV[8], costs, vals[1], "0", ARGV[10], ARGV[11], ARGV[12], ARGV[14], ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20], vals[8], initialmargin, vals[9], vals[12], finance, ARGV[21]) ]]\
  --[[ adjust order related margin/reserve ]] \
  --[[ adjustmarginreserve(orderid, vals[1], vals[2], vals[3], vals[5], vals[6], ARGV[17], vals[7], ARGV[15], ARGV[11], vals[8]) ]]\*/
  /*--[[ todo: adjust trade related margin ]] \
  --[[updatetrademargin(tradeid, vals[1], ARGV[17], initialmargin[1])]] \
  --[[publishorder(orderid, vals[9]) ]]\*/
  /*--[[ todo: needs considering ]] \
  --[[ local finance = calcfinance(instrumenttypeid, consid, ARGV[17], vals[3], vals[8]) ]]\
  --[[ treat the excuting broker as a client ]] \
  local cptyid = redis.call("hget", "counterparty:" .. ARGV[9], "clientid") \
  if not cptyid then \
    cptyid = 999998 \
  end \
  --[[ local rside = reverseside(ARGV[3]) ]]\*/

  scriptordercancelrequest = removefromorderbook + cancelorder + getproquotesymbol + '\
  local errorcode = 0 \
  local orderid = KEYS[2] \
  local ordercancelreqid = redis.call("incr", "ordercancelreqid") \
  --[[ store the order cancel request ]] \
  redis.call("hmset", "ordercancelrequest:" .. ordercancelreqid, "clientid", KEYS[1], "orderid", orderid, "timestamp", KEYS[3], "operatortype", KEYS[4], "operatorid", KEYS[5]) \
  local fields = {"orderstatusid", "markettype", "symbolid", "side", "quantity", "externalorderid"} \
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
    local orderstatusid = vals[1] \
    markettype = vals[2] \
    symbolid = vals[3] \
    side = vals[4] \
    quantity = vals[5] \
    externalorderid = vals[6] \
    if orderstatusid == "2" then \
      --[[ already filled ]] \
      errorcode = 1010 \
    elseif orderstatusid == "4" then \
      --[[ already cancelled ]] \
      errorcode = 1008 \
    elseif orderstatusid == "8" then \
      --[[ already rejected ]] \
      errorcode = 1012 \
    end \
  end \
  --[[ process according to market type ]] \
  local proquotesymbol = {"", "", ""} \
  if markettype == "1" then \
    if errorcode ~= 0 then \
      redis.call("hset", "ordercancelrequest:" .. ordercancelreqid, "orderrejectreasonid", errorcode) \
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
  local fields = {"clientid", "symbolid", "side", "quantity", "price", "margin", "remquantity", "nosettdays", "operatortype", "hedgeorderid", "futsettdate", "operatorid", "orderstatusid", "externalorderid", "settlcurrencyid", "markettype"} \
  local vals = redis.call("hmget", "order:" .. orderid, unpack(fields)) \
  if vals == nil then \
    --[[ order not found ]] \
    errorcode = 1009 \
  else \
    local orderstatusid = vals[13] \
    if orderstatusid == "2" then \
      --[[ filled ]] \
      errorcode = 1010 \
    elseif orderstatusid == "4" then \
      --[[ cancelled ]] \
      errorcode = 1008 \
    elseif orderstatusid == "8" then \
      --[[ rejected ]] \
      errorcode = 1012 \
    elseif orderstatusid == "C" then \
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
  redis.call("hmset", "broker:" .. brokerid .. "order:" .. KEYS[1], "externalorderid", KEYS[2], "orderstatusid", KEYS[3], "execid", KEYS[4], "text", KEYS[5]) \
  local operatortype = redis.call("hget", "order:" .. KEYS[1], "operatortype") \
  return operatortype \
  ';

  scriptorderexpire = cancelorder + '\
  local ret = cancelorder(KEYS[1], "C") \
  --[[ get the operatortype to enable on-sending ]] \
  local operatortype = redis.call("hget", "order:" .. KEYS[1], "operatortype") \
  return {ret, operatortype} \
  ';

  scriptQuoteRequest = getproquotesymbol + commonbo.getclientaccountid + '\
  local quoterequestid = redis.call("hincrby", KEYS[1], "lastquoterequestid", 1) \
  if not quoterequestid then return 1005 end \
  --[[ get trading accountid if not specified ]] \
  local accountid = 0 \
  if ARGV[1] == "" then \
    accountid = getclientaccountid(ARGV[2], ARGV[4], 1) \
  else \
    accountid = ARGV[1] \
  end \
  --[[ store the quote request ]] \
  redis.call("hmset", KEYS[1] .. ":quoterequest:" .. quoterequestid, "accountid", accountid, "brokerid", ARGV[2], "cashorderqty", ARGV[3], "clientid", ARGV[4], "currencyid", ARGV[5], "futsettdate", ARGV[6], "operatorid", ARGV[7], "operatortype", ARGV[8], "quantity", ARGV[9], "quoterejectreasonid", "", "quoterequestid", quoterequestid, "quotestatusid", 0, "settlmnttypid", ARGV[10], "side", ARGV[11], "symbolid", ARGV[12], "timestamp", ARGV[13], "text", "") \
  --[[ add to set of quoterequests ]] \
  redis.call("sadd", KEYS[1] .. ":quoterequests", quoterequestid) \
  --[[ add to set of quoterequests for this account ]] \
  redis.call("sadd", KEYS[1] .. ":accountid:" .. accountid .. ":quoterequests", quoterequestid) \
  --[[ get required instrument values for external feed ]] \
  local proquotesymbol = getproquotesymbol(ARGV[12]) \
  return {0, quoterequestid, proquotesymbol} \
  ';

  /*
  local markettype = 0 \
  --[[ local markettype = getmarkettype(ARGV[2], ARGV[12], ARGV[13], ARGV[14]) ]] \
  --[[ assuming equity buy to get default settlement days ]] \
  local defaultnosettdays = redis.call("hget", "cost:" .. "DE" .. ":" .. ARGV[6] .. ":" .. "1", "defaultnosettdays") \
  --[[ get in/out of hours - assume in-hours for now ]]\
  */
  //db.eval(scriptQuote, 0, quote.quoterequestid, quote.symbolid, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.transacttime, quote.currencyid, quote.settlcurrencyid, quote.quoterid, quote.futsettdate, quote.bidquotedepth, quote.offerquotedepth, quote.externalquoteid, quote.qclientid, quote.cashorderqty, quote.settledays, quote.noseconds, function(err, ret) {

  scriptQuote = publishquote + commonbo.round + '\
  local errorcode = 0 \
  local brokerkey = "broker:" .. ARGV[20] \
  --[[ get the quote request ]] \
  local fields = {"accountid", "clientid", "symbolid", "quantity", "cashorderqty", "operatortype", "operatorid"} \
  local vals = redis.call("hmget", brokerkey .. ":quoterequest:" .. ARGV[1], unpack(fields)) \
  if not vals[1] then \
    errorcode = 1014 \
    return errorcode \
  end \
  --[[ get touch prices - using delayed - todo: may need to look up delayed/live ]] \
  local bestbid = 0 \
  local bestoffer = 0 \
  local symbolfields = {"bid", "ask"} \
  local symbolvals = redis.call("hmget", "symbol:" .. vals[3], unpack(symbolfields)) \
  if symbolvals[1] then \
    bestbid = symbolvals[1] \
    bestoffer = symbolvals[2] \
  end \
  local bidquantity = "" \
  local offerquantity = "" \
  local bidfinance = 0 \
  local offerfinance = 0 \
  --[[ calculate the quantity from the cashorderqty, if necessary ]] \
  if ARGV[3] == "" then \
    local offerprice = tonumber(ARGV[4]) \
    if vals[4] == "" then \
      offerquantity = round(tonumber(vals[5]) / offerprice, 0) \
    else \
      offerquantity = tonumber(vals[4]) \
    end \
  else \
    local bidprice = tonumber(ARGV[3]) \
    if vals[4] == "" then \
      bidquantity = round(tonumber(vals[5]) / bidprice, 0) \
    else \
      bidquantity = tonumber(vals[4]) \
    end \
  end \
  --[[ create a quote id as different from external quote ids (one for bid, one for offer)]] \
  local quoteid = redis.call("hincrby", brokerkey, "lastquoteid", 1) \
  --[[ store the quote ]] \
  redis.call("hmset", brokerkey .. ":quote:" .. quoteid, "quoterequestid", ARGV[1], "brokerid", ARGV[20], "accountid", vals[1], "clientid", vals[2], "quoteid", quoteid, "symbolid", vals[3], "bestbid", bestbid, "bestoffer", bestoffer, "bidpx", ARGV[3], "offerpx", ARGV[4], "bidquantity", bidquantity, "offerquantity", offerquantity, "bidsize", ARGV[5], "offersize", ARGV[6], "validuntiltime", ARGV[7], "transacttime", ARGV[8], "currencyid", ARGV[9], "settlcurrencyid", ARGV[10], "quoterid", ARGV[11], "quotertype", ARGV[12], "futsettdate", ARGV[13], "bidfinance", bidfinance, "offerfinance", offerfinance, "orderid", "", "bidquotedepth", ARGV[14], "offerquotedepth", ARGV[15], "externalquoteid", ARGV[16], "cashorderqty", ARGV[17], "settledays", ARGV[18], "noseconds", ARGV[19]) \
  --[[ add to set of quotes ]] \
  redis.call("sadd", brokerkey .. ":quotes", quoteid) \
  --[[ keep a list of quotes for the quoterequest ]] \
  redis.call("sadd", brokerkey .. ":quoterequest:" .. ARGV[1] .. ":quotes", quoteid) \
  --[[ quote status - 0=new, 1=quoted, 2=rejected ]] \
  local quotestatusid \
  --[[ bid or offer size needs to be non-zero ]] \
  if ARGV[5] == 0 and ARGV[6] == 0 then \
    quotestatusid = "5" \
  else \
    quotestatusid = "0" \
  end \
  --[[ add status to stored quoterequest ]] \
  redis.call("hmset", brokerkey .. ":quoterequest:" .. ARGV[1], "quotestatusid", quotestatusid) \
  --[[ publish quote to operator type, with operator id, so can be forwarded as appropriate ]] \
  publishquote(ARGV[20], quoteid, vals[6], vals[7]) \
  return errorcode \
  ';

  /* finance
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
    --[[ this needs revisiting ]]\
    --[[offerfinance = calcfinance(instrumenttypeid, offerquantity * offerprice, vals[7], 1, vals[6]) ]]\
    --[[ this needs revisiting ]]\
    --[[bidfinance = calcfinance(instrumenttypeid, bidquantity * bidprice, vals[7], 2, vals[6]) ]]\
  */

  scriptQuoteAck = publishquoteack + '\
  --[[ update quote request ]] \
  redis.call("hmset", "broker:" .. ARGV[1] .. ":quoterequest:" .. ARGV[2], "quotestatusid", ARGV[3], "quoterejectreasonid", ARGV[4], "text", ARGV[5]) \
  --[[ publish to operator type ]] \
  publishquoteack(ARGV[1], ARGV[2]) \
  return \
  ';

  scriptordercancelreject = '\
  --[[ update the order cancel request ]] \
  redis.call("hmset", "ordercancelrequest:" .. KEYS[1], "orderrejectreasonid", KEYS[2], "text", KEYS[3]) \
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