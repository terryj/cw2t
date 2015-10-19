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
var testmode = 1; // 0 = off, 1 = on
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

  if (!('currencyid' in quoterequest)) {
    quoterequest.currencyid = "GBP";
  }

  // todo - match settle currency with currency?
  if (!('settlcurrencyid' in quoterequest)) {
    quoterequest.settlcurrencyid = "GBP";
  }

  // store the quote request & get an id
  db.eval(scriptQuoteRequest, 1, "broker:" + quoterequest.brokerid, quoterequest.accountid, quoterequest.brokerid, quoterequest.cashorderqty, quoterequest.clientid, quoterequest.currencyid, quoterequest.futsettdate, quoterequest.operatorid, quoterequest.operatortype, quoterequest.quantity, quoterequest.settlmnttypid, quoterequest.side, quoterequest.symbolid, quoterequest.timestamp, quoterequest.settlcurrencyid, function(err, ret) {
    if (err) throw err;

    console.log(ret);

    // todo:sort out
    if (ret[0] != 0) {
      // todo: send a quote ack to client
      console.log("Error in scriptQuoteRequest: " + commonbo.getReasonDesc(ret[0]));
      return;
    }
    console.log("ere");

    // add the quote request id & symbol details required for the fix connection
    quoterequest.quoterequestid = ret[1];
    quoterequest.isin = ret[2];
    quoterequest.mnemonic = ret[3];
    quoterequest.exchangeid = ret[4];


    if (testmode == 1) {
      console.log("test response");
      testQuoteResponse(quoterequest);
    } else {
      console.log("forwarding to nbt");

      // forward the request
      nbt.quoteRequest(quoterequest);
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
  console.log("testQuoteAck");
  var quoteack = {};

  console.log(quoterequest);

  quoteack.quoterequestid = quoterequest.quoterequestid;
  quoteack.quotestatusid = 5;
  quoteack.quoterejectreasonid = "";
  quoteack.text = "test rejection message";

  db.eval(scriptQuoteAck, 1, "broker:" + quoterequest.brokerid, quoterequest.brokerid, quoteack.quoterequestid, quoteack.quotestatusid, quoteack.quoterejectreasonid, quoteack.text, function(err, ret) {
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

  quote.brokerid = quoterequest.brokerid;
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
  quote.settlmnttypid = 0;

  var today = new Date();
  quote.transacttime = commonbo.getUTCTimeStamp(today);

  var validuntiltime = today;
  quote.noseconds = 30;
  validuntiltime.setSeconds(today.getSeconds() + quote.noseconds);
  quote.validuntiltime = commonbo.getUTCTimeStamp(validuntiltime);

  console.log(quote);

  newQuote(quote);
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
  console.log("newOrder");
  console.log(order);

  var currencyratetoorg = 1; // product currency to org currency rate
  var currencyindtoorg = 1;
  var settlcurrfxrate = 1; // settlement currency to product currency rate
  var settlcurrfxratecalc = 1;

  var today = new Date();

  order.timestamp = commonbo.getUTCTimeStamp(today);
  order.markettype = 0;

  if (!("accountid" in order)) {
    order.accountid = "";
  }

  // always put a price in the order
  if (!("price" in order)) {
    order.price = "";
  }

  // always have a quote id
  // todo: not if not quoted order
  if (!("quoteid" in order)) {
    order.quoteid = "";
  }

  // get settlement date from T+n no. of days if not specified
  if (!('futsettdate' in order) || order.futsettdate == "") {
    order.futsettdate = commonbo.getUTCDateString(commonbo.getSettDate(today, 2, holidays));
  }

  if (order.ordertype == "D") {
    order.settlmnttypid = 6;
  } else {
    // if there is no settlement type, set to standard
    if (!("settlmnttypid" in order)) {
      order.settlmnttypid = 0;
    }
  }

  // store the order, get an id & credit check it
  db.eval(scriptneworder, 1, "broker:" + order.brokerid, order.accountid, order.brokerid, order.clientid, order.symbolid, order.side, order.quantity, order.price, order.ordertype, order.markettype, order.futsettdate, order.quoteid, order.currencyid, currencyratetoorg, currencyindtoorg, order.timestamp, order.timeinforce, order.expiredate, order.expiretime, order.settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, order.operatortype, order.operatorid, function(err, ret) {
    if (err) throw err;
    console.log(ret);

    // error check
    if (ret[0] == 0) {
      console.log("Error in scriptneworder, order #" + ret[2] + " - " + commonbo.getReasonDesc(ret[1]));

      // script will publish the order back to the operator type if an order was created
      return;
    }

    // update order details
    order.orderid = ret[1];
    order.isin = ret[2];
    order.mnemonic = ret[3];
    order.exchangeid = ret[4];
    order.instrumenttypeid = ret[5];
    order.hedgesymbolid = ret[6];
    order.hedgeorderid = ret[7];

    // use the returned quote values required by fix connection
    if (order.ordertype == "D") {
      order.externalquoteid = ret[8][0];
      order.quoterid = ret[8][1];
    }

    // set the settlement date to equity default date for cfd orders, in case they are being hedged with the market
    //if (order.instrumenttypeid == "CFD" || order.instrumenttypeid == "SPB") {
      // commented out as only offering default settlement for the time being
      //order.futsettdate = commonbo.getUTCDateString(commonbo.getSettDate(today, ret[11], holidays));

      //order.hedgesymbolid = ret[6];
      //order.hedgeorderid = ret[7];

      // update the stored order settlement details if it is a hedge - todo: req'd?
      /*if (hedgeorderid != "") {
        db.hmset("order:" + hedgeorderid, "nosettdays", ret[11], "futsettdate", order.futsettdate);
      }*/
    //}

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
    if (testmode == 1) {
      // test only
      testTradeResponse(order);
    } else {
      console.log("forwarding to nbt");
      // forward order to the market
      nbt.newOrder(order);
    }
  } else if (order.instrumenttypeid == "CFD" || order.instrumenttypeid == "SPB" || order.instrumenttypeid == "CCFD") {
    if (order.hedgeorderid != "") {
      if (testmode == 1) {
        // test only
        testTradeResponse(order);
      } else {
        // todo: may need to change order id to hedge order id
        console.log("forwarding hedge order:" + order.hedgeorderid + " to nbt");

        nbt.newOrder(order);
      }
    }
  } else if (testmode == 1) {
    // test only
    testTradeResponse(order);
  }
}

function testTradeResponse(order) {
  var exereport = {};

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
  // todo: sort out
  exereport.futsettdate = order.futsettdate;
  exereport.transacttime = order.timestamp;
  exereport.lastmkt = "";
  exereport.orderid = "";
  exereport.settlcurrencyid = "GBP";
  exereport.settlcurramt = parseFloat(order.price) * parseInt(order.quantity);
  exereport.settlcurrfxrate = 1;
  exereport.settlcurrfxratecalc = 1;

  processTrade(exereport);
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

    if (tm) {
      testmode = parseInt(tm);
    }
  });
}

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

  if (!('settlmnttypid' in quote)) {
    quote.settlmnttypid = 0;
  }

  if (!('futsettdate' in quote)) {
    quote.futsettdate = "";    
  }

  // do we need?
  if (!('settledays' in quote)) {
    quote.settledays = "";
  }

  if ('quoterid' in quote) {
    quote.quotertype = 1;
  }

  // mnemonic is specified, so no symbolid
  if (!('symbolid' in  quote)) {
    quote.symbolid = "";
  }

  // quote script
  db.eval(scriptQuote, 1, "broker:" + quote.brokerid, quote.quoterequestid, quote.symbolid, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.transacttime, quote.currencyid, quote.settlcurrencyid, quote.quoterid, quote.quotertype, quote.futsettdate, quote.bidquotedepth, quote.offerquotedepth, quote.externalquoteid, quote.cashorderqty, quote.settledays, quote.noseconds, quote.brokerid, quote.settlmnttypid, function(err, ret) {
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
    /*if (quoteinterval == null) {
      startQuoteInterval();
    }*/

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

  db.eval(scriptrejectorder, 1, "broker:" + exereport.brokerid, exereport.brokerid, exereport.clordid, orderrejectreasonid, text, function(err, ret) {
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
  console.log("fill received");
  console.log(exereport);

  processTrade(exereport);
});

function processTrade(exereport) {
  var currencyratetoorg = 1; // product currency rate back to org 
  var currencyindtoorg = 1;
  var markettype = 0;
  var counterpartytype = 1;

  if (!('accountid' in exereport)) {
    exereport.accountid = "";
  }

  if (!('clientid' in exereport)) {
    exereport.clientid = "";
  }

  if (!('settlcurrfxrate' in exereport)) {
    exereport.settlcurrfxrate = 1;
  }

  if (!('settlcurrfxratecalc' in exereport)) {
    exereport.settlcurrfxratecalc = 1;
  }

  // will have a mnemonic & isin from market
  if (!('symbolid') in exereport) {
    exereport.symbolid = "";
  }

  if (!('operatortype' in exereport)) {
    exereport.operatortype = "";
  }

  if (!('operatorid' in exereport)) {
    exereport.operatorid = "";
  }

  // milliseconds since epoch, used for scoring datetime indexes
  var milliseconds = new Date().getTime();

  db.eval(scriptnewtrade, 1, "broker:" + exereport.brokerid, exereport.accountid, exereport.brokerid, exereport.clientid, exereport.clordid, exereport.symbolid, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currencyid, currencyratetoorg, currencyindtoorg, exereport.execbroker, counterpartytype, markettype, exereport.execid, exereport.futsettdate, exereport.transacttime, exereport.lastmkt, exereport.orderid, exereport.settlcurrencyid, exereport.settlcurramt, exereport.settlcurrfxrate, exereport.settlcurrfxratecalc, milliseconds, exereport.operatortype, exereport.operatorid, function(err, ret) {
    if (err) {
      console.log(err);
      return
    }

    if (ret[0] != 0) {
      console.log("Error in scriptnewtrade: " + commonbo.getReasonDesc(ret[0]));
      return;
    }
  });
}

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

  newQuote(quote);
});

//
// quote rejection
//
nbt.on("quoteack", function(quoteack) {
  console.log("quote ack, request id " + quoteack.quoterequestid);
  console.log(quoteack);

  db.eval(scriptQuoteAck, 1, "broker:" + quoteack.brokerid, quoteack.brokerid, quoteack.quoterequestid, quoteack.quoteackstatus, quoteack.quoterejectreasonid, quoteack.text, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }
  });
});

//
// message error
// todo: get fix message & inform client
//
nbt.on("reject", function(reject) {
  console.log("Error: reject received");
  console.log(reject);

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
  var updateordermargin;
  var updatereserve;
  var removefromorderbook;
  var cancelorder;
  var getcosts;
  var rejectorder;
  var adjustmarginreserve;
  //var getinitialmargin;
  //var updatetrademargin;
  var neworder;
  //var getreserve;

  /*
  * getcosts()
  * calculates costs for an order/trades
  * params: brokerid, clientid, symbolid, side, consideration, currencyid
  * returns: commission, ptmlevy, stampduty, contractcharge as an array
  */
  getcosts = commonbo.round + commonbo.gethashvalues + '\
  local getcosts = function(brokerid, clientid, symbolid, side, consid, currencyid) \
    redis.log(redis.LOG_WARNING, "getcosts") \
    --[[ set default costs ]] \
    local commission = 0 \
    local ptmlevy = 0 \
    local stampduty = 0 \
    local contractcharge = 0 \
    local brokerkey = "broker:" .. brokerid \
    local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
    --[[ get costs for this instrument type & currency - will be set to zero if not found ]] \
    local costid = redis.call("get", brokerkey .. ":cost:" .. instrumenttypeid .. ":" .. currencyid) \
    if costid then \
      local costs = gethashvalues(brokerkey .. ":cost:" .. costid) \
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
        if costs["commissionpercent"] then commpercent = tonumber(costs["commissionpercent"]) end \
      end \
      commission = round(consid * commpercent / 100, 2) \
      --[[ check commission against min commission ]] \
      if costs["commissionmin"] and costs["commissionmin"] ~= "" then \
        local mincommission = tonumber(costs["commissionmin"]) \
        if commission < mincommission then \
          commission = mincommission \
        end \
      end \
      --[[ check commission against max commission ]] \
      if costs["commissionmax"] and costs["commissionmax"] ~= "" then \
        local maxcommission = tonumber(costs["commissionmax"]) \
        if commission < maxcommission then \
          commission = maxcommission \
        end \
      end \
      --[[ ptm levy ]] \
      local ptmexempt = redis.call("hget", "symbol:" .. symbolid, "ptmexempt") \
      if ptmexempt and tonumber(ptmexempt) == 1 then \
      else \
        --[[ only calculate ptm levy if product is not exempt ]] \
        if costs["ptmlevylimit"] and costs["ptmlevylimit"] ~= "" then \
          local ptmlevylimit = tonumber(costs["ptmlevylimit"]) \
          if consid > ptmlevylimit then \
            if costs["ptmlevy"] and costs["ptmlevy"] ~= "" then \
              ptmlevy = tonumber(costs["ptmlevy"]) \
            end \
          end \
        end \
      end \
      --[[ stamp duty ]] \
      if costs["stampdutypercent"] and costs["stampdutypercent"] ~= "" then \
        local stampdutypercent = tonumber(costs["stampdutypercent"]) \
        stampduty = round(consid * stampdutypercent / 100, 2) \
      end \
      --[[ contract charge ]] \
      if costs["contractcharge"] and costs["contractcharge"] ~= "" then \
        contractcharge = tonumber(costs["contractcharge"]) \
      end \
    end \
    return {commission, ptmlevy, stampduty, contractcharge} \
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
  local updatereserve = function(clientid, symbolid, currencyid, futsettdate, quantity) \
    local poskey = symbolid .. ":" .. currencyid .. ":" .. futsettdate \
    local reservekey = clientid .. ":reserve:" .. poskey \
    local reserveskey = clientid .. ":reserves" \
    --[[ get existing position, if there is one ]] \
    local reserve = redis.call("hget", reservekey, "quantity") \
    if reserve then \
      local adjquantity = tonumber(reserve) + tonumber(quantity) \
      if adjquantity == 0 then \
        redis.call("hdel", reservekey, "clientid", "symbolid", "quantity", "currencyid", "futsettdate") \
        redis.call("srem", reserveskey, poskey) \
      else \
        redis.call("hset", reservekey, "quantity", adjquantity) \
      end \
    else \
      redis.call("hmset", reservekey, "clientid", clientid, "symbolid", symbolid, "quantity", quantity, "currencyid", currencyid, "futsettdate", futsettdate) \
      redis.call("sadd", reserveskey, poskey) \
    end \
  end \
  ';

  adjustmarginreserve = getinitialmargin + updateordermargin + updatereserve + '\
  local adjustmarginreserve = function(orderid, clientid, symbolid, side, price, ordmargin, currencyid, remquantity, newremquantity, futsettdate, nosettdays) \
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
        updatereserve(clientid, symbolid, currencyid, futsettdate, -remquantity + newremquantity) \
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
    redis.log(redis.LOG_WARNING, "rejectorder") \
    redis.call("hmset", "broker:" .. brokerid .. ":order:" .. orderid, "orderstatusid", "8", "orderrejectreasonid", orderrejectreasonid, "text", text) \
  end \
  ';

  /*getreserve = '\
  local getreserve = function(clientid, symbolid, currencyid, futsettdate) \
    local reserve = redis.call("hget", clientid .. ":reserve:" .. symbolid .. ":" .. currencyid .. ":" .. futsettdate, "quantity") \
    if not reserve then \
      return 0 \
    end \
    return reserve \
  end \
  ';*/

  /*
  * creditcheck()
  * credit checks an order
  * params: accountid, brokerid, orderid, clientid, symbolid, side, quantity, price, currencyid, futsettdate
  * returns: 0=fail/1=succeed, inialmargin, costs as a table
  */
  creditcheck = rejectorder + getinitialmargin + getcosts + commonbo.getpositionbysymbol + commonbo.getfreemargin + '\
  local creditcheck = function(accountid, brokerid, orderid, clientid, symbolid, side, quantity, price, currencyid, futsettdate) \
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
    redis.log(redis.LOG_WARNING, "costs") \
    redis.log(redis.LOG_WARNING, costs[1]) \
    redis.log(redis.LOG_WARNING, costs[2]) \
    redis.log(redis.LOG_WARNING, costs[3]) \
    redis.log(redis.LOG_WARNING, costs[4]) \
    --[[ calculate margin required for order ]] \
    local initialmargin = getinitialmargin(brokerid, symbolid, consid, totalcost) \
    --[[ get position, if there is one, as may be a closing buy or sell ]] \
    local position = getpositionbysymbol(accountid, brokerid, symbolid, futsettdate) \
    if position then \
      --[[ we have a position - always allow closing trades ]] \
      local posqty = tonumber(position["quantity"]) \
      redis.log(redis.LOG_WARNING, "posqty") \
      redis.log(redis.LOG_WARNING, posqty) \
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
      redis.log(redis.LOG_WARNING, "initialmargin") \
      redis.log(redis.LOG_WARNING, initialmargin) \
      redis.log(redis.LOG_WARNING, "freemargin") \
      redis.log(redis.LOG_WARNING, freemargin) \
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
      if not position then \
        rejectorder(brokerid, orderid, 0, "No position held in this instrument") \
        return {0} \
      end \
      local posqty = tonumber(position["quantity"]) \
      --[[ check the position is of sufficient size ]] \
      if posqty < 0 or quantity > posqty then \
        rejectorder(brokerid, orderid, 0, "Insufficient position size in this instrument") \
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
  * getproquotequote()
  * get quote details
  * params: brokerid, quoteid
  * returns: externalquoteid, quoterid as a table
  */
  getproquotequote = '\
  local getproquotequote = function(brokerid, quoteid) \
    redis.log(redis.LOG_WARNING, "getproquotequote") \
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

  publishquoteack = commonbo.gethashvalues + '\
  local publishquoteack = function(brokerid, quoterequestid) \
    redis.log(redis.LOG_WARNING, "publishquoteack") \
    local quoterequest = gethashvalues("broker:" .. brokerid .. ":quoterequest:" .. quoterequestid) \
    local quoteack = {brokerid=brokerid, quoterequestid=quoterequestid, accountid=quoterequest["accountid"], clientid=quoterequest["clientid"], symbolid=quoterequest["symbolid"], quotestatusid=quoterequest["quotestatusid"], quoterejectreasonid=quoterequest["quoterejectreasonid"], text=quoterequest["text"], operatorid=quoterequest["operatorid"]} \
    redis.call("publish", quoterequest["operatortype"], "{" .. cjson.encode("quoteack") .. ":" .. cjson.encode(quoteack) .. "}") \
    end \
  ';

  publishquote = commonbo.gethashvalues + '\
  local publishquote = function(brokerid, quoteid, channel, operatorid) \
    redis.log(redis.LOG_WARNING, "publishquote") \
    local quote = gethashvalues("broker:" .. brokerid .. ":quote:" .. quoteid) \
    quote["operatorid"] = operatorid \
    redis.call("publish", channel, "{" .. cjson.encode("quote") .. ":" .. cjson.encode(quote) .. "}") \
  end \
  ';

  publishorder = commonbo.gethashvalues + '\
  local publishorder = function(brokerid, orderid, channel) \
    redis.log(redis.LOG_WARNING, "publishorder") \
    local order = gethashvalues("broker:" .. brokerid .. ":order:" .. orderid) \
    redis.call("publish", channel, "{" .. cjson.encode("order") .. ":" .. cjson.encode(order) .. "}") \
  end \
  ';

  quoteack = publishquoteack + '\
  local quoteack = function(brokerid, quoterequestid, quotestatusid, quoterejectreasonid, text) \
    --[[ update quote request ]] \
    redis.call("hmset", "broker:" .. brokerid .. ":quoterequest:" .. quoterequestid, "quotestatusid", quotestatusid, "quoterejectreasonid", quoterejectreasonid, "text", text) \
    --[[ publish to operator type ]] \
    publishquoteack(brokerid, quoterequestid) \
  end \
  ';

  /*
  * neworder()
  * gets an orderid & saves the order
  */
  neworder = '\
  local neworder = function(accountid, brokerid, clientid, symbolid, side, quantity, price, ordertype, markettype, futsettdate, quoteid, currencyid, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforce, expiredate, expiretime, settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, operatortype, operatorid, hedgeorderid) \
    local brokerkey = "broker:" .. brokerid \
    --[[ get a new orderid & store the order ]] \
    local orderid = redis.call("hincrby", brokerkey, "lastorderid", 1) \
    redis.call("hmset", brokerkey .. ":order:" .. orderid, "accountid", accountid, "brokerid", brokerid, "clientid", clientid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "ordertype", ordertype, "remquantity", quantity, "orderstatusid", 0, "markettype", markettype, "futsettdate", futsettdate, "quoteid", quoteid, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "timestamp", timestamp, "margin", margin, "timeinforce", timeinforce, "expiredate", expiredate, "expiretime", expiretime, "settlcurrencyid", settlcurrencyid, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "orderid", orderid, "externalorderid", externalorderid, "execid", execid, "operatortype", operatortype, "operatorid", operatorid, "hedgeorderid", hedgeorderid, "orderrejectreasonid", "", "text", "") \
    --[[ add to set of orders ]] \
    redis.call("sadd", brokerkey .. ":orders", orderid) \
    redis.call("sadd", brokerkey .. ":orderid", "order:" .. orderid) \
    --[[ add to set of orders for this account ]] \
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

  /*
  * scriptrejectorder
  * order rejected externally
  * params: brokerid, orderid, orderrejectreasonid, text
  */
  scriptrejectorder = rejectorder + publishorder + '\
  rejectorder(ARGV[1], ARGV[2], ARGV[3], ARGV[4]) \
  local operatortype = redis.call("hget", "broker:" .. ARGV[1] .. ":order:" .. ARGV[2], "operatortype") \
  publishorder(ARGV[1], ARGV[2], operatortype) \
  return 0 \
  ';

  /*
  * scriptneworder
  * params: 1=accountid, 2=brokerid, 3=clientid, 4=symbolid, 5=side, 6=quantity, 7=price, 8=ordertype, 9=markettype, 10=futsettdate, 11=quoteid, 12=currencyid, 13=currencyratetoorg, 14=currencyindtoorg, 15=timestamp, 16=timeinforce, 17=expiredate, 18=expiretime, 19=settlcurrencyid, 20=settlcurrfxrate, 21=settlcurrfxratecalc, 22=operatortype, 23=operatorid
  * returns: orderid
  */
  scriptneworder = getclientaccountid + neworder + creditcheck + commonbo.newtrade + publishorder + getproquotequote + '\
  redis.log(redis.LOG_WARNING, "scriptneworder") \
  --[[ get trading accountid if not specified ]] \
  local accountid \
  if ARGV[1] == "" then \
    accountid = getclientaccountid(ARGV[2], ARGV[3], 1) \
  else \
    accountid = ARGV[1] \
  end \
  if not accountid then \
    return {0, 1025, "n/a"} \
  end \
  local orderid = neworder(accountid, ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13], ARGV[14], ARGV[15], 0, ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20], ARGV[21], "", "", ARGV[22], ARGV[23], "") \
  local brokerid = ARGV[2] \
  local brokerkey = "broker:" .. brokerid \
  local symbolid = ARGV[4] \
  local side = tonumber(ARGV[5]) \
  local settlcurramt = tonumber(ARGV[6]) * tonumber(ARGV[7]) \
  local cc = creditcheck(accountid, brokerid, orderid, ARGV[3], symbolid, side, ARGV[6], ARGV[7], ARGV[19], ARGV[10]) \
  if cc[1] == 0 then \
    --[[ publish the order back to the operatortype - the order contains the error ]] \
    publishorder(brokerid, orderid, ARGV[22]) \
    return {cc[1], 1026, orderid} \
  end \
  redis.log(redis.LOG_WARNING, "credit check ok") \
  local hedgebookid = "" \
  local tradeid = "" \
  local hedgeorderid = "" \
  local hedgetradeid = "" \
  local proquotesymbol = gethashvalues("symbol:" .. symbolid) \
  local proquotequote = getproquotequote(brokerid, ARGV[11]) \
  local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
  if instrumenttypeid == "CFD" or instrumenttypeid == "SPB" or instrumenttypeid == "CCFD" then \
    --[[ ignore limit orders for derivatives as they will be handled manually, at least for the time being ]] \
    if ARGV[8] ~= "2" then \
      --[[ create trades for client & hedge book for off-exchange products ]] \
      hedgebookid = redis.call("get", brokerkey .. ":hedgebook:" .. instrumenttypeid .. ":" .. ARGV[20]) \
      if not hedgebookid then hedgebookid = 999999 end \
      local rside = reverseside(side) \
      local finance = 0 \
      local hedgecosts = {0,0,0,0} \
      tradeid = newtrade(accountid, brokerid, ARGV[3], orderid, symbolid, side, ARGV[6], ARGV[7], ARGV[12], ARGV[13], ARGV[14], cc[3], hedgebookid, counterpartytype, ARGV[9], "", ARGV[10], ARGV[15], lastmkt, "", ARGV[19], settlcurramt, ARGV[20], ARGV[21], cc[2], ARGV[22], ARGV[23], finance, milliseconds) \
      hedgetradeid = newtrade(hedgebookid, brokerid, hedgeclientid, orderid, symbolid, side, ARGV[6], ARGV[7], ARGV[12], ARGV[13], ARGV[14], hedgecosts, accountid, counterpartytype, ARGV[9], "", ARGV[10], ARGV[15], lastmkt, "", ARGV[19], settlcurramt, ARGV[20], ARGV[21], cc[2], ARGV[22], ARGV[23], finance, milliseconds) \
      --[[ adjust order as filled ]] \
      redis.call("hmset", brokerkey .. ":order:" .. orderid, "remquantity", 0, "orderstatusid", 2) \
      --[[ todo: may need to adjust margin here ]] \
      --[[ see if we need to hedge this trade in the market ]] \
      local hedgeclient = tonumber(redis.call("hget", brokerkey .. ":client:" .. ARGV[3], "hedge")) \
      local hedgeinst = tonumber(redis.call("hget", brokerkey ":brokersymbol:" .. symbolid, "hedge")) \
      if hedgeclient == 1 or hedgeinst == 1 then \
        --[[ create a hedge order in the underlying product ]] \
        if proquotesymbol["hedgesymbolid"] then \
          --[[ todo: get this right ]] \
          hedgeorderid = neworder(hedgebookid, proquotesymbol["hedgesymbolid"], ARGV[5], ARGV[6], ARGV[7], "X", ARGV[6], 0, ARGV[11], ARGV[10], ARGV[11], "", ARGV[13], ARGV[14], ARGV[15], ARGV[14], 0, ARGV[17], ARGV[18], ARGV[19], ARGV[20], ARGV[21], ARGV[22], "", "", ARGV[23], ARGV[24], ARGV[25], orderid, "") \
        end \
      end \
    end \
  else \
    --[[ this is an equity - just consider external orders for the time being - todo: internal ]] \
    --[[ todo: consider equity limit orders ]] \
  end \
  return {cc[1], orderid, proquotesymbol["isin"], proquotesymbol["mnemonic"], proquotesymbol["exchangeid"], proquotesymbol["instrumenttypeid"], proquotesymbol["hedgesymbolid"], hedgeorderid, proquotequote} \
  ';

  // todo: add lastmkt
  // quantity required as number of shares
  // todo: settlcurrency
  scriptmatchorder = addtoorderbook + removefromorderbook + adjustmarginreserve + commonbo.newtrade + getcosts + '\
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

  /*
  * scriptnewtrade()
  * process a fill from the market
  * params: 1=accountid, 2=brokerid, 3=clientid, 4=clordid, 5=symbolid, 6=side, 7=lastshares, 8=lastpx, 9=currencyid, 10=currencyratetoorg, 11=currencyindtoorg, 12=execbroker, 13=counterpartytype, 14=markettype, 15=execid, 16=futsettdate, 17=transacttime, 18=lastmkt, 19=orderid, 20=settlcurrencyid, 21=settlcurramt, 22=settlcurrfxrate, 23=settlcurrfxratecalc, 24=milliseconds, 25=operatortype, 26=operatorid
  */
  scriptnewtrade = getcosts + commonbo.newtrade + '\
  redis.log(redis.LOG_WARNING, "scriptnewtrade") \
  local accountid = ARGV[1] \
  local clientid = ARGV[3] \
  local orderid = ARGV[4] \
  local symbolid = ARGV[5] \
  local operatortype = ARGV[25] \
  local operatorid = ARGV[26] \
  local order \
  --[[ if there is an orderid, get the order & update unknown values for the fill ]] \
  if orderid ~= "" then \
    order = gethashvalues(KEYS[1] .. ":order:" .. orderid) \
    if not order["orderid"] then \
      return {1009} \
    end \
    accountid = order["accountid"] \
    clientid = order["clientid"] \
    symbolid = order["symbolid"] \
    operatortype = order["operatortype"] \
    operatorid = order["operatorid"] \
  end \
  --[[ get costs ]] \
  local consid = tonumber(ARGV[7]) * tonumber(ARGV[8]) \
  local costs = getcosts(ARGV[2], clientid, symbolid, ARGV[6], consid, ARGV[20]) \
  --[[ finance/margin may be needed for derivs ]] \
  local finance = 0 \
  local margin = 0 \
  --[[ create trade ]] \
  local tradeid = newtrade(accountid, ARGV[2], clientid, orderid, symbolid, ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], costs, ARGV[12], ARGV[13], ARGV[14], ARGV[15], ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20], ARGV[21], ARGV[22], ARGV[23], margin, operatortype, operatorid, finance, ARGV[24]) \
  --[[ adjust order remaining quantity & status ]] \
  if orderid ~= "" then \
    local remquantity = tonumber(order["remquantity"]) - tonumber(ARGV[7]) \
    local orderstatusid \
    if remquantity > 0 then \
      orderstatusid = 1 \
    else \
      orderstatusid = 2 \
    end \
    redis.call("hmset", KEYS[1] .. ":order:" .. orderid, "remquantity", remquantity, "orderstatusid", orderstatusid) \
  end \
  return {0, tradeid} \
  ';

  scriptordercancelrequest = removefromorderbook + cancelorder + commonbo.gethashvalues + '\
  local errorcode = 0 \
  local orderid = KEYS[2] \
  local ordercancelreqid = redis.call("incr", "ordercancelreqid") \
  --[[ store the order cancel request ]] \
  redis.call("hmset", "ordercancelrequest:" .. ordercancelreqid, "clientid", KEYS[1], "orderid", orderid, "timestamp", KEYS[3], "operatortype", KEYS[4], "operatorid", KEYS[5]) \
  local order = gethashvalues("broker:" .. brokerid .. ":order:" .. orderid) \
  if order["orderid"] == nil then \
    --[[ order not found ]] \
    errorcode = 1009 \
  else \
    if order["orderstatusid"] == "2" then \
      --[[ already filled ]] \
      errorcode = 1010 \
    elseif order["orderstatusid"] == "4" then \
      --[[ already cancelled ]] \
      errorcode = 1008 \
    elseif order["orderstatusid"] == "8" then \
      --[[ already rejected ]] \
      errorcode = 1012 \
    end \
  end \
  --[[ process according to market type ]] \
  local proquotesymbol = gethashvalues("symbol:" .. order["symbolid"]) \
  if order["markettype"] == "1" then \
    if errorcode ~= 0 then \
      redis.call("hset", "ordercancelrequest:" .. ordercancelreqid, "orderrejectreasonid", errorcode) \
    else \
      removefromorderbook(order["symbolid"], orderid) \
      cancelorder(orderid, "4") \
    end \
  else \
    --[[ if the order is with proquote, it will be forwarded, else if request ok, we can go ahead and cancel the order ]] \
    if order["externalorderid"] == "" then \
      if errorcode == 0 then \
        cancelorder(orderid, "4") \
      end \
    end \
  end \
  return {errorcode, order["markettype"], ordercancelreqid, order["symbolid"], proquotesymbol["isin"], proquotesymbol["mnemonic"], proquotesymbol["exchangeid"], order["side"], order["quantity"]} \
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

  /*
  * scriptQuoteRequest
  * store a quote request
  * params: 1=accountid, 2=brokerid, 3=cashorderqty, 4=clientid, 5=currencyid, 6=futsettdate, 7=operatorid, 8=operatortype, 9=quantity, 10=settlmnttypid, 11=side, 12=symbolid, 13=timestamp, 14=settlcurrencyid
  */
  scriptQuoteRequest = commonbo.gethashvalues + commonbo.getclientaccountid + quoteack + '\
  redis.log(redis.LOG_WARNING, "scriptQuoteRequest") \
  local quoterequestid = redis.call("hincrby", KEYS[1], "lastquoterequestid", 1) \
  if not quoterequestid then return {1005} end \
  --[[ get trading accountid if not specified ]] \
  local accountid \
  if ARGV[1] == "" then \
    accountid = getclientaccountid(ARGV[2], ARGV[4], 1) \
  else \
    accountid = ARGV[1] \
  end \
  if not accountid then \
    return {1025} \
  end \
  --[[ store the quote request ]] \
  redis.call("hmset", KEYS[1] .. ":quoterequest:" .. quoterequestid, "accountid", accountid, "brokerid", ARGV[2], "cashorderqty", ARGV[3], "clientid", ARGV[4], "currencyid", ARGV[5], "futsettdate", ARGV[6], "operatorid", ARGV[7], "operatortype", ARGV[8], "quantity", ARGV[9], "quoterejectreasonid", "", "quoterequestid", quoterequestid, "quotestatusid", 0, "settlmnttypid", ARGV[10], "side", ARGV[11], "symbolid", ARGV[12], "timestamp", ARGV[13], "text", "", "settlcurrencyid", ARGV[14]) \
  --[[ add to set of quoterequests ]] \
  redis.call("sadd", KEYS[1] .. ":quoterequests", quoterequestid) \
  redis.call("sadd", KEYS[1] .. ":quoterequestid", "quoterequest:" .. quoterequestid) \
  --[[ add to set of quoterequests for this account ]] \
  redis.call("sadd", KEYS[1] .. ":account:" .. accountid .. ":quoterequests", quoterequestid) \
  --[[ get required instrument values for external feed ]] \
  local symbol = gethashvalues("symbol:" .. ARGV[12]) \
  if not symbol["symbolid"] then \
    quoteack(ARGV[2], quoterequestid, 5, 99, "Symbol data not found") \
    return {1016} \
  end \
  return {0, quoterequestid, symbol["isin"], symbol["mnemonic"], symbol["exchangeid"]} \
  ';

  /*
  * scriptQuote
  * script to store and publish a quote
  * params: 1=quoterequestid, 2=symbolid, 3=bidpx, 4=offerpx, 5=bidsize, 6=offersize, 7=validuntiltime, 8=transacttime, 9=currencyid, 10=settlcurrencyid, 11=quoterid, 12=quotertype, 13=futsettdate, 14=bidquotedepth, 15=offerquotedepth, 16=externalquoteid, 17=cashorderqty, 18=settledays, 19=noseconds, 20=brokerid, 21=settlmnttypid
  */
  scriptQuote = publishquote + getcosts + '\
  local errorcode = 0 \
  local brokerid = ARGV[20] \
  local brokerkey = "broker:" .. brokerid \
  --[[ get the quote request ]] \
  local quoterequest = gethashvalues(brokerkey .. ":quoterequest:" .. ARGV[1]) \
  if not quoterequest["quoterequestid"] then \
    return 1014 \
  end \
  --[[ get touch prices - using delayed - todo: may need to look up delayed/live ]] \
  local bestbid = 0 \
  local bestoffer = 0 \
  local symbolfields = {"bid", "ask"} \
  local symbolvals = redis.call("hmget", "symbol:" .. quoterequest["symbolid"], unpack(symbolfields)) \
  if symbolvals[1] then \
    bestbid = symbolvals[1] \
    bestoffer = symbolvals[2] \
  end \
  local bidquantity = "" \
  local offerquantity = "" \
  local bidfinance = 0 \
  local offerfinance = 0 \
  local cashorderqty \
  local side \
  --[[ calculate the quantity from the cashorderqty, if necessary ]] \
  if ARGV[3] == "" then \
    local offerprice = tonumber(ARGV[4]) \
    if quoterequest["quantity"] == "" then \
      offerquantity = round(tonumber(quoterequest["cashorderqty"]) / offerprice, 0) \
    else \
      offerquantity = tonumber(quoterequest["quantity"]) \
    end \
    --[[ set the amount of cash based on the quoted price ]] \
    cashorderqty = offerquantity * offerprice \
    side = 1 \
  else \
    local bidprice = tonumber(ARGV[3]) \
    if quoterequest["quantity"] == "" then \
      bidquantity = round(tonumber(quoterequest["cashorderqty"]) / bidprice, 0) \
    else \
      bidquantity = tonumber(quoterequest["quantity"]) \
    end \
    --[[ set the amount of cash based on the quoted price ]] \
    cashorderqty = bidquantity * bidprice \
    side = 2 \
  end \
  --[[ create a quote id as different from external quote ids (one for bid, one for offer)]] \
  local quoteid = redis.call("hincrby", brokerkey, "lastquoteid", 1) \
  --[[ get the costs ]] \
  local costs = {0,0,0,0} \
  local clientid = redis.call("get", brokerkey .. ":account:" .. quoterequest["accountid"] .. ":client") \
  if clientid then \
    costs = getcosts(brokerid, clientid, quoterequest["symbolid"], side, cashorderqty, ARGV[10]) \
  end \
  --[[ store the quote ]] \
  redis.call("hmset", brokerkey .. ":quote:" .. quoteid, "quoterequestid", ARGV[1], "brokerid", brokerid, "accountid", quoterequest["accountid"], "clientid", quoterequest["clientid"], "quoteid", quoteid, "symbolid", quoterequest["symbolid"], "bestbid", bestbid, "bestoffer", bestoffer, "bidpx", ARGV[3], "offerpx", ARGV[4], "bidquantity", bidquantity, "offerquantity", offerquantity, "bidsize", ARGV[5], "offersize", ARGV[6], "validuntiltime", ARGV[7], "transacttime", ARGV[8], "currencyid", ARGV[9], "settlcurrencyid", ARGV[10], "quoterid", ARGV[11], "quotertype", ARGV[12], "futsettdate", ARGV[13], "bidfinance", bidfinance, "offerfinance", offerfinance, "orderid", "", "bidquotedepth", ARGV[14], "offerquotedepth", ARGV[15], "externalquoteid", ARGV[16], "cashorderqty", cashorderqty, "settledays", ARGV[18], "noseconds", ARGV[19], "settlmnttypid", ARGV[21], "commission", costs[1], "ptmlevy", costs[2], "stampduty", costs[3], "contractcharge", costs[4]) \
  --[[ add to sets of quotes & quotes for this account ]] \
  redis.call("sadd", brokerkey .. ":quotes", quoteid) \
  redis.call("sadd", brokerkey .. ":account:" .. quoterequest["accountid"] .. ":quotes", quoteid) \
  redis.call("sadd", brokerkey .. ":quoteid", "quote:" .. quoteid) \
  --[[ keep a list of quotes for the quoterequest ]] \
  redis.call("sadd", brokerkey .. ":quoterequest:" .. ARGV[1] .. ":quotes", quoteid) \
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
  publishquote(brokerid, quoteid, quoterequest["operatortype"], quoterequest["operatorid"]) \
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

  /*
  * scriptQuoteAck
  * script to handle a quote acknowledgement
  * params: brokerid, quoterequestid, quotestatusid, quoterejectreasonid, text
  */
  scriptQuoteAck = quoteack + '\
  quoteack(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]) \
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