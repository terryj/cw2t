/****************
* tradeserver.js
* Front-office trading server
* Cantwaittotrade Limited
* Terry Johnston
* November 2013
* Modifications
* 25 Aug 2016 - scriptQuoteRequest - used account currency as quote request currency
*             - scriptQuoteRequest - changed return code to 1=error, 0=success
* 21 Oct 2016 - added limit orders to newordersingle() & call to matchordersingle()
*             - updated matchorder(), addtoorderbook() & removefromorderbook()
* 23 Oct 2016 - added matchordersingle() & publishorderbook()
* 27 Oct 2016 - added ordertype validation to validorder()
* 30 Oct 2016 - added brokerid, orderid & ordertype to getsettlcurramt()
*             - updated newordersingle() & testTradeResponse()
*             - updated error handling of scriptneworder() in newOrderSingle()
* 10 Dec 2016 - added fixseqnum storage to quote request process
* 12 Dec 2016 - added separate quoteacks - quoteack(), publishquoteack()
* 20 Dec 2016 - modified scriptQuoteRequest(), scriptQuote(), scriptQuoteAck()
* 21 Dec 2016 - modified scriptreject(), added scriptbusinessreject()
* 14 Mar 2017 - modified processTrade()
* 10 Apr 2017 - added limit order support out of hours to validorder()
* 13 Apr 2017 - added check for markettype in newOrder() to allow for out of hours orders
*             - removed broker from orderbook key in matchordersingle(), addtoorderbook(), removefromorderbook()
*             - commented out matchorder() - only handling whole orders for now
*             - changed stored value in orderbook set to contain brokerid and orderid (i.e. 1:1) in matchordersingle(), addtoorderbook(), removefromorderbook()
*             - removed displayOrderBook(), matchorder()
*             - updated publishorderbook() & added publishorderbooktop()
*             - added publishorderbook() to addtoorderbook() & removefromorderbook()
* 23 Jun 2017 - operatortimestamp removed - scriptQuoteRequest(), scriptQuote(), scriptdealatquote(), newordersingle(), neworder(), matchordersingle(), scriptneworder() and scriptnewtrade()
* 29 Aug 2017 - updated scriptneworder() and scriptQuoteRequest() currencyid passed to getaccountsbyclient()
* 29 Aug 2017 - update processTrade()
* 16 Sep 2017 - removed dependency on nbtrader.js to support use of separate comms server
* 02 Oct 2017 - added channelid to publishquoterequest
*             - added getchannel()
*             - added getchannel() to scriptdealatquote() and scriptneworder()
* 06 Oct 2017 - updated scriptQuoteRequest(), scriptQuote()
* 07 Oct 2017 - updated function return values to be consistent as 0=ok, 1=error
*             - added isin and channel lookup to publishorder()
* 08 Oct 2017 - updated scriptordercancelrequest() to include broker
*             - update scriptQuoteRequest() to handle isin or symbolid
* 09 Oct 2017 - updated scriptQuoteAck(), quoteack() and publishquoteack() to handle channel lookup
*             - updated scriptQuote() so that publishquote() handles channel lookup
* 11 Oct 2017 - updated newordersingle(), scriptdealatquote(), scriptneworder(), publishorder() to handle channel lookup
*             - added rejectorder() to validorder()
*             - moved getchannel() to commonbo.js
* 13 Oct 2017 - updated validorder() to remove market hour limit check and return error codes
*             - updated getsettlcurramt() to return error codes
*             - updated scriptordercancelrequest() & added publishordercancelrequest()
*             - updated scriptordercancel()
* 20 Oct 2017 - added check for settlement currency matching account currency to validorder() 
* 22 Oct 2017 - added check for limit order type in order matching
*             - added validmatchorder() 
*             - removed credit check from and added costs parameter to matchordersingle()
* 27 Oct 2017 - added ordercancelreject() and publishordercancelreject() and updated scriptordercancelreject() and scriptordercancelrequest()
*             - added orderCancelReject() to handle order cancel reject messages
*             - added operatorid to publishquoteack()
* 28 Oct 2017 - fixed error in cancelorder()
* 29 Oct 2017 - simplified cancelorder()
*             - added key for account orderbook orders to addtoorderbook() & removefromorderbook()
*             - added clearorderbooks() & clearorderbook()
********************/

// external libraries
var redis = require('redis');

// cw2t libraries
var commonbo = require('./commonbo.js');
var utils = require('./utils.js');

//get the database connection configuration based on deployment mode 'development' or 'production'
//from environment variable
var redisConfig = require('../config/auth.js').auth.redis[process.env.NODE_ENV];

//redis
var redishost = redisConfig.host,
  redishost = redisConfig.host,
  redisport = redisConfig.port,
  redisauth = redisConfig.auth,
  redispassword = redisConfig.password;


// globals
var testmode; // comes from database,  0 = off, 1 = on
var quoteinterval = null;

// redis scripts
var scriptneworder;
var scriptordercancelrequest;
var scriptordercancel;
var scriptorderack;
var scriptnewtrade;
var scriptrejectorder;
var scriptgetinst;
var scriptorderfillrequest;

// set-up a redis client
if (process.env.NODE_ENV == 'test')
  db = redis.createClient('redis://' + redisConfig.host + ':' + redisConfig.port + '/' + redisConfig.database);
else
  db = redis.createClient(redisport, redishost, {no_ready_check: true});

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

function isEmpty(data) {
  return (data === null || data === undefined || data === '' || typeof data === 'undefined');
}

function sendToMonitor(brokerid, clientid, accountid, message) {
  var prefix = '';
  if (!isEmpty(brokerid)) {
    prefix = prefix + 'broker: ' + brokerid + ' ';
  }
  if (!isEmpty(clientid)) {
    prefix = prefix + 'client: ' + clientid + ' ';
  }
  if (!isEmpty(accountid)) {
    prefix = prefix + 'account: ' + accountid + ' ';
  }
  prefix = prefix + '. ' + message;
  db.eval(commonbo.publishsystemmonitorlog, 0, prefix, function(error) {
    if(error) {
      console.log(error);
    }
  });
}

// log errors
function errorLog(brokerid, businessobjectid, businessobjecttypeid, errortypeid, messageid, messagetypeid, module, rejectreasonid, text) {
  var timestamp = commonbo.getUTCTimeStamp(new Date());

  db.eval(commonbo.scripterrorlog, 0, brokerid, businessobjectid, businessobjecttypeid, errortypeid, messageid, messagetypeid, module, rejectreasonid, text, timestamp, function(err, ret) {
    if (err) {
      // do not call error log here
      console.log(err);
      return;
    }
  });
}

function initialise() {
  initDb();
  pubsub();
}

// pubsub connections
function pubsub() {
  if (process.env.NODE_ENV == 'test')
    dbsub = redis.createClient('redis://' + redisConfig.host + ':' + redisConfig.port + '/' + redisConfig.database);
  else
    dbsub = redis.createClient(redisport, redishost, {no_ready_check: true});

  if (redisauth) {
    dbsub.auth(redispassword, function(err) {
      if (err) {
        console.log(err);
        return;
      }
    });
  }
  dbsub.on("subscribe", function(channel, count) {
    console.log("Subscribed to channel: " + channel + ", num. channels:" + count);
  });

  dbsub.on("unsubscribe", function(channel, count) {
    console.log("Unsubscribed from channel: " + channel + ", num. channels:" + count);
  });

  dbsub.on("message", function(channel, message) {
    try {
      var obj = JSON.parse(message);
      if ("quoterequest" in obj) {
        quoteRequest(obj.quoterequest);
      } else if ("order" in obj) {
        newOrder(obj.order);
     } else if ("quote" in obj) {
        newQuote(obj.quote);
      } else if ("executionreport" in obj) {
        newExecutionReport(obj.executionreport);
      } else if ("ordercancelrequest" in obj) {
        orderCancelRequest(obj.ordercancelrequest);
      } else if ("orderfillrequest" in obj) {
        orderFillRequest(obj.orderfillrequest);
      } else if ("quoteack" in obj) {
        quoteAck(obj.quoteack);
      } else if ("ordercancelreject" in obj) {
        orderCancelReject(obj.ordercancelreject);
      } else {
        console.log("Unknown message received");
        console.log(message);
      }
    } catch(e) {
      console.log(e);
      return;
    }
  });

  // listen for trade related messages
  dbsub.subscribe(commonbo.tradeserverchannel);
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
    if (err) {
      console.log(err);
      return;
    }

    if (ret == 0 && quoteinterval != null) {
      stopQuoteInterval();
    }
  });
}

/*
* Receive a quote request
*/
function quoteRequest(quoterequest) {
  console.log("Quote request received");
  console.log(quoterequest);

  // create timestamp
  var today = new Date();
  quoterequest.timestamp = commonbo.getUTCTimeStamp(today);
  quoterequest.timestampms = today.getTime();

  if (!("accountid" in quoterequest)) {
    quoterequest.accountid = "";
  }

  // if there is no settlement type, set to standard
  if (!("settlmnttypid" in quoterequest)) {
    quoterequest.settlmnttypid = "";
  }

  if (!("futsettdate" in quoterequest)) {
    quoterequest.futsettdate = "";
  }

  if (!("side" in quoterequest)) {
    quoterequest.side = "";
  }

  if (!("cashorderqty" in quoterequest)) {
    quoterequest.cashorderqty = "";
  }

  if (!("quantity" in quoterequest)) {
    quoterequest.quantity = "";
  }

  // default currency to GBP
  if (!('currencyid' in quoterequest)) {
    quoterequest.currencyid = "GBP";
  }

  // match the settlement currency to the currency requested
  if (!('settlcurrencyid' in quoterequest)) {
    quoterequest.settlcurrencyid = quoterequest.currencyid;
  }

  if (!('symbolid' in quoterequest)) {
    quoterequest.symbolid = "";
  }

  if (!('isin' in quoterequest)) {
    quoterequest.isin = "";
  }

  // store the quote request & get an id
  db.eval(scriptQuoteRequest, 1, "broker:" + quoterequest.brokerid, quoterequest.accountid, quoterequest.brokerid, quoterequest.cashorderqty, quoterequest.clientid, quoterequest.currencyid, quoterequest.futsettdate, quoterequest.operatorid, quoterequest.operatortype, quoterequest.quantity, quoterequest.settlmnttypid, quoterequest.side, quoterequest.symbolid, quoterequest.timestamp, quoterequest.settlcurrencyid, quoterequest.timestampms, quoterequest.isin, function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(quoterequest.brokerid, "", 5, 4, "", "", "tradeserver.scriptQuoteRequest", "", err);
      return;
    }

    if (ret[0] != 0) {
      console.log("Error in scriptQuoteRequest: " + commonbo.getReasonDesc(ret[1]));
      errorLog(quoterequest.brokerid, "", 5, 4, "", "", "tradeserver.scriptQuoteRequest", "", commonbo.getReasonDesc(ret[1]));
      return;
    }

    console.log("Quote request id: " + ret[1] + " forwarded to channel: " + ret[2]);

    // quote request will be forwarded by script
  });
}

/*
* Send a test quote
*/
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

/*
* Publish a test quote rejection
*/
function testQuoteAck(quoterequest) {
  console.log("Publishing a testQuoteAck");
  var quoteack = {};

  console.log(quoterequest);

  quoteack.quoterequestid = quoterequest.quoterequestid;
  quoteack.quotestatusid = 5;
  quoteack.quoterejectreasonid = "";
  quoteack.text = "test rejection message";
  quoteack.timestamp = commonbo.getUTCTimeStamp(new Date());

  db.eval(scriptQuoteAck, 1, "broker:" + quoterequest.brokerid, quoterequest.brokerid, quoteack.quoterequestid, quoteack.quotestatusid, quoteack.quoterejectreasonid, quoteack.text, "", quoteack.timestamp, quoteack.timestamp, function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(quoterequest.brokerid, "", 7, 4, "", "", "tradeserver.testQuoteAck", "", err);
      return;
    }

    if (ret[0] != 0) {
      console.log("Error in scriptQuoteAck: " + commonbo.getReasonDesc(ret[1]));
    }
  });
}

/*
* Send a test quote
*/
function testQuote(quoterequest, side) {
  var quote = {};

  console.log("Sending a test quote");

  if (side == 2) {
    quote.bidpx = "1.23";
    quote.bidsize = quoterequest.quantity;
    quote.bidquotedepth = "1";
    quote.bidspotrate = 0;
    quote.offerpx = "";
    quote.offersize = "";
    quote.offerquotedepth = "";
    quote.offerspotrate = "";
    quote.cashorderqty = quote.bidsize * quote.bidpx;
  } else {
    quote.bidpx = "";
    quote.bidsize = "";
    quote.bidquotedepth = "";
    quote.bidspotrate = "";
    quote.offerpx = "1.25";
    quote.offerspotrate = 0;
    quote.offersize = quoterequest.quantity;
    quote.offerquotedepth = "1";
    quote.cashorderqty = quote.offersize * quote.offerpx;
  }

  quote.brokerid = quoterequest.brokerid;
  quote.quoterequestid = quoterequest.quoterequestid;
  quote.symbolid = quoterequest.symbolid;
  quote.currencyid = quoterequest.currencyid;
  quote.settlcurrencyid = quoterequest.currencyid;
  quote.quoterid = "ABC";
  quote.quotertype = 1;
  quote.externalquoteid = "";
  quote.settlmnttypid = 0;
  quote.noseconds = 30;
  quote.operatortype = quoterequest.operatortype;
  quote.operatorid = quoterequest.operatorid;

  var today = new Date();
  quote.transacttime = commonbo.getUTCTimeStamp(today);
  var validuntiltime = today;
  validuntiltime.setSeconds(today.getSeconds() + quote.noseconds);
  quote.validuntiltime = commonbo.getUTCTimeStamp(validuntiltime);
  quote.fixseqnumid = "";

  if ('futsettdate' in quoterequest && quoterequest.futsettdate != '') {
    quote.futsettdate = quoterequest.futsettdate;
    newQuote(quote);
  } else {
    commonbo.getSettDate(today, quote.symbolid, function(err, settDate) {
      if (err) {
        console.log("Error in getting settlement date. Error:", err);
      }
      quote.futsettdate = commonbo.getUTCDateString(settDate);
      newQuote(quote);
    });
  }

}

/*
 * Receive an order message
 */
function newOrder(order) {
  console.log("newOrder");
  console.log(order);

  order.currencyratetoorg = 1; // product currency to org currency rate
  order.currencyindtoorg = 1;

  var today = new Date();
  order.timestamp = commonbo.getUTCTimeStamp(today);
  order.timestampms = today.getTime();

  // default is market trading
  if (!('markettype' in order)) {
    order.markettype = 0;
  }

  if (!('timeinforceid' in order)) {
    order.timeinforceid = "4";
  }

  if (!('futsettdate' in order)) {
    order.futsettdate = "";
  }

  // handle the order depending on whether it is based on a quote or not
  if (order.ordertypeid == "D") {
    dealAtQuote(order);
  } else {
    if (order.markettype == 1)
newOrderSingle(order);
else
cob();
  }
}
function cob() {
console.log("cob");
    db.eval(scriptclearorderbooks, 0, function(err, ret) {
      if (err) {
        console.log(err);
        errorLog(order.brokerid, "", 2, 4, "", "", "tradeserver.scriptdealatquote", "", err);
        return;
      }
console.log("out");
    });
}

/*
 * Process an order based on a quote
 */
function dealAtQuote(order) {
    db.eval(scriptdealatquote, 1, "broker:" + order.brokerid, order.brokerid, order.ordertypeid, order.markettype, order.quoteid, order.currencyratetoorg, order.currencyindtoorg, order.timestamp, order.timeinforceid, order.operatortype, order.operatorid, order.timestampms, order.futsettdate, function(err, ret) {
      if (err) {
        console.log(err);
        errorLog(order.brokerid, "", 2, 4, "", "", "tradeserver.scriptdealatquote", "", err);
        return;
      }

      // error check
      if (ret[0] != 0) {
        console.log("Error in scriptdealatquote, order #" + ret[2] + " - " + commonbo.getReasonDesc(ret[1]));

        // record the error
        errorLog(order.brokerid, ret[2], 2, 4, "", "", "tradeserver.scriptdealatquote", "", commonbo.getReasonDesc(ret[1]));

        // script will publish the order back to the operator type

        return;
      }

      console.log("Order id: " + ret[1] + " forwarded to channel: " + ret[2]);

      // script will publish the order based on routing rules
    });
}

/*
 * Process a regular order
 */
function newOrderSingle(order) {
  if (!("accountid" in order)) {
    order.accountid = "";
  }

  if (!("cashorderqty" in order)) {
    order.cashorderqty = "";
  }

  if (!("price" in order)) {
    order.price = "";
  }

  order.quoteid = "";

  // if there is no settlement type, set to regular
  if (!("settlmnttypid" in order)) {
    order.settlmnttypid = 0;
  }

  if (!('expiredate' in order)) {
    order.expiredate = "";
  }

  if (!('expiretime' in order)) {
    order.expiretime = "";
  }

  if (!('currencyid' in order)) {
    order.currencyid = "GBP";
  }

  if (!('settlcurrencyid' in order)) {
    order.settlcurrencyid = "GBP";
  }

  // settlement currency to product currency rate
  if (!('settlcurrfxrate' in order)) {
    order.settlcurrfxrate = 1;
    order.settlcurrfxratecalc = 0;
  }

  if (!('symbolid' in order)) {
    order.symbolid = "";
  }

  if (!('isin' in order)) {
    order.isin = "";
  }

  db.eval(scriptneworder, 1, "broker:" + order.brokerid, order.accountid, order.brokerid, order.clientid, order.symbolid, order.side, order.quantity, order.price, order.ordertypeid, order.markettype, order.futsettdate, order.quoteid, order.currencyid, order.currencyratetoorg, order.currencyindtoorg, order.timestamp, order.timeinforceid, order.expiredate, order.expiretime, order.settlcurrencyid, order.settlcurrfxrate, order.settlcurrfxratecalc, order.operatortype, order.operatorid, order.cashorderqty, order.settlmnttypid, order.timestampms, order.isin, function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(order.brokerid, "", 2, 4, "", "", "tradeserver.scriptneworder", "", err);
      return;
    }

    // error check
    if (ret[0] != 0) {
      console.log("Error in scriptneworder, order #" + ret[2] + " - " + commonbo.getReasonDesc(ret[1]));

      // record the error
      errorLog(order.brokerid, ret[2], 2, 4, "", "", "tradeserver.scriptneworder", "", commonbo.getReasonDesc(ret[1]));

      // script will publish the order back to the operator type if an order was created
      return;
    }
    
    console.log("Order id: " + ret[1] + " forwarded to channel: " + ret[2]);

    // script will publish the order based on routing rules
 });
}

/*
 * Send a test response to an order
 */
function testTradeResponse(order) {
  var exereport = {};

  console.log("Sending a test fill");

  exereport.accountid = order.accountid;
  exereport.brokerid = order.brokerid;
  exereport.clientid = order.clientid;
  exereport.clordid = order.orderid;
  exereport.symbolid = order.symbolid;
  exereport.side = order.side;
  exereport.lastshares = order.quantity;
  exereport.currencyid = order.currencyid;
  exereport.execbroker = "ABC";
  exereport.execid = 1;

  // todo: sort out
  exereport.futsettdate = order.futsettdate;
  exereport.transacttime = order.timestamp;
  exereport.markettimestamp = order.timestamp;
  exereport.lastmkt = "XLON";
  exereport.orderid = "";
  exereport.settlcurrencyid = order.currencyid;
  // may not be a price i.e. market order
  if (order.price == "") {
    exereport.lastpx = 1.23;
  } else {
    exereport.lastpx = order.price;
  }
  exereport.settlcurramt = parseFloat(exereport.lastpx) * parseInt(order.quantity);
  exereport.settlcurrfxrate = order.settlcurrfxrate ? order.settlcurrfxrate : 1;
  exereport.settlcurrfxratecalc = order.settlcurrfxratecalc ? order.settlcurrfxratecalc : 0;
  exereport.fixseqnumid = "";

  processTrade(exereport);
}

/*
 * Request to cancel an order received
 */
function orderCancelRequest(ocr) {
  console.log("Order cancel request received for order#" + ocr.orderid);

  ocr.timestamp = commonbo.getUTCTimeStamp(new Date());

  if (!('clientid' in ocr)) {
    ocr.clientid = "";
  }

  if (!('accountid' in ocr)) {
    ocr.accountid = "";
  }

  db.eval(scriptordercancelrequest, 1, "broker:" + ocr.brokerid, ocr.brokerid, ocr.clientid, ocr.accountid, ocr.orderid, ocr.timestamp, ocr.operatortype, ocr.operatorid, function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(ocr.brokerid, "", 6, 4, "", "", "tradeserver.scriptordercancelrequest", "", err);
      return;
    }

    if (ret[0] != 0) {
      console.log("Error in scriptordercancelrequest, order #" + ocr.orderid + " - " + commonbo.getReasonDesc(ret[1]));
      errorLog(ocr.brokerid, "", 6, 4, "", "", "tradeserver.scriptordercancelrequest", "", commonbo.getReasonDesc(ret[1]));
      return;
    }

    console.log("Order cancel request id: " + ret[1] + " processed ok");

    // processing and publishing taken care of by the script
  });
}

/*
 * Order cancel reject message received from Comms Server
 */
function orderCancelReject(ocr) {
  console.log("Order cancel reject received for order#" + ocr.orderid);

  ocr.timestamp = commonbo.getUTCTimeStamp(new Date());

  db.eval(scriptordercancelreject, 1, "broker:" + ocr.brokerid, ocr.brokerid, ocr.ordercancelrequestid, ocr.orderid, ocr.externalorderid, ocr.orderstatusid, ocr.ordercancelrejectreasonid, ocr.text, ocr.timestamp, ocr.markettimestamp, function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(ocr.brokerid, "", 6, 4, "", "", "tradeserver.scriptordercancelreject", "", err);
      return;
    }

    if (ret[0] != 0) {
      console.log("Error in scriptordercancelreject, ordercancelrequestid:" + ocr.ordercancelrequestid + " - " + commonbo.getReasonDesc(ret[1]));
      errorLog(ocr.brokerid, "", 6, 4, "", "", "tradeserver.scriptordercancelreject", "", commonbo.getReasonDesc(ret[1]));
      return;
    }

    console.log("Order cancel request id: " + ret[1] + " processed ok");

    // processing and publishing taken care of by the script
  });
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
  commonbo.getSettDate(today, ocr.symbolid, function(err, settDate) {
    if (err) {
      console.log("Error in getting settlement date. Error:", err);
    }
    ofr.futsettdate = commonbo.getUTCDateString(settDate);

    db.eval(scriptorderfillrequest, 7, ofr.clientid, ofr.orderid, ofr.timestamp, ofr.operatortype, ofr.operatorid, ofr.price, ofr.futsettdate, function(err, ret) {
      if (err) {
        console.log(err);
        return;
      }

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
  });
}

function initDb() {
  commonbo.registerScripts();
  utils.utils();
  registerScripts();
  getTestmode();
}

/*
* Determine whether or not we are in test mode
*/
function getTestmode() {
  db.hget('config', 'testmode', function(err, tm) {
    if (err) {
      console.log(err);
      return;
    }

    if (tm) {
      testmode = parseInt(tm);
    }
  });
}

/*
 * A quote has been received
 */
function newQuote(quote) {
  console.log("newQuote");
  console.log(quote);

  // extract brokerid & quoterequestid
  var quotereqid = quote.quotereqid.split(':');

  if (!('bidpx' in quote)) {
    quote.bidpx = "";
    quote.bidsize = "";
    quote.bidspotrate = "";
  }
  if (!('offerpx' in quote)) {
    quote.offerpx = "";
    quote.offersize = "";
    quote.offerspotrate = "";
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

  var today = new Date();
  quote.timestampms = today.getTime();

  quote.noseconds = commonbo.getSeconds(quote.markettimestamp, quote.validuntiltime);

  if (!('settlmnttypid' in quote)) {
    quote.settlmnttypid = 0;
  }

  // mnemonic is specified, so no symbolid
  if (!('symbolid' in  quote)) {
    quote.symbolid = "";
  }

  db.eval(scriptQuote, 1, "broker:" + quotereqid[0], quotereqid[1], quote.symbolid, quote.bidpx, quote.offerpx, quote.bidsize, quote.offersize, quote.validuntiltime, quote.timestamp, quote.currencyid, quote.settlcurrencyid, quote.quoterid, quote.quotertype, quote.futsettdate, quote.bidquotedepth, quote.offerquotedepth, quote.externalquoteid, quote.cashorderqty, quote.settledays, quote.noseconds, quotereqid[0], quote.settlmnttypid, quote.bidspotrate, quote.offerspotrate, quote.timestampms, quote.fixseqnumid, quote.markettimestamp, quote.isin, function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(quotereqid[0], quotereqid[1], 3, 4, quote.fixseqnumid, "S", "tradeserver.scriptQuote", "", err);
      return;
    }

    if (ret[0] != 0) {
      // can't find quote request, so don't know which client to inform
      console.log("Error in scriptquote: " + commonbo.getReasonDesc(ret[1]));
      errorLog(quotereqid[0], quotereqid[1], 3, 4, quote.fixseqnumid, "S", "tradeserver.scriptQuote", "", commonbo.getReasonDesc(ret[1]));
      return;
    }

    // start the quote monitor if necessary
    if (quoteinterval == null) {
      startQuoteInterval();
    }

    console.log("Quote id: " + ret[1] + " forwarded ok");

    // script publishes quote to the operator type that made the request
  });
}

/*
 * Order rejection reveived from the market
 */
function orderReject(exereport) {
  var text = "";
  var orderrejectreasonid = "";

  console.log("Order rejected, id: " + exereport.clordid);
  console.log(exereport);

  // extract brokerid & our orderid
  var clordid = exereport.clordid.split(':');

  // execution reports vary as to whether they contain a reject reason &/or text
  if ('ordrejreason' in exereport) {
    orderrejectreasonid = exereport.ordrejreason;
  }
  if ('text' in exereport) {
    text = exereport.text;
  }

  db.eval(scriptrejectorder, 1, "broker:" + clordid[0], clordid[0], clordid[1], orderrejectreasonid, text, function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(clordid[0], clordid[1], 2, 4, exereport.fixseqnumid, "8", "tradeserver.scriptrejectorder", "", err);
      return;
    }

    // raise an error
    errorLog(clordid[0], clordid[1], 2, 4, exereport.fixseqnumid, "8", "tradeserver.scriptrejectorder", orderrejectreasonid, text);

    // order published to operator by script
  });
}

/*
 * A Limit order acknowledgement has been received from the market
 */
function orderAck(exereport) {
  var text = "";
  console.log("Order acknowledged, id: " + exereport.clordid);
  console.log(exereport);

  // extract brokerid & our orderid
  var clordid = exereport.clordid.split(':');

  if ('text' in exereport) {
    text = exereport.text;
  }

  db.eval(scriptorderack, 1, "broker:" + clordid[0], clordid[0], clordid[1], exereport.orderid, exereport.orderstatusid, exereport.execid, text, exereport.fixseqnumid, function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(clordid[0], clordid[1], 8, 4, exereport.fixseqnumid, "b", "tradeserver.scriptorderack", "", err);
      return;
    }

    // script publishes order to operator type
  });
}

/*
 * An order has been cancelled in the market
 */
function orderCancel(exereport) {
  console.log("Order cancelled externally, ordercancelrequest id: " + exereport.clordid);

  // extract brokerid & our orderid
  var clordid = exereport.clordid.split(':');

  db.eval(scriptordercancel, 1, "broker:" + clordid[0], clordid[0], clordid[1], function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(clordid[0], "", 9, 4, exereport.fixseqnumid, "9", "tradeserver.orderCancel", "", err);
      return;
    }

    if (ret[0] != 0) {
      // todo: send to client & sort out error
      console.log("Error in scriptordercancel, reason: " + commonbo.getReasonDesc(ret[1]));
      errorLog(clordid[0], clordid[1], 9, 4, exereport.fixseqnumid, "9", "tradeserver.orderCancel", "", commonbo.getReasonDesc(ret[1]));
      return;
    }

    // script will publish the order
  });
}

/*
 * An order has expired
 */
function orderExpired(exereport) {
  console.log(exereport);
  console.log("Order expired, id: " + exereport.clordid);

  // extract brokerid & our orderid
  var clordid = exereport.clordid.split(':');

  db.eval(scriptorderexpire, 1, "broker:" + clordid[0], clordid[0], clordid[1], function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    if (ret[0] != 0) {
      // todo: send to client & sort out error
      console.log("Error in scriptorderexpire, reason: " + commonbo.getReasonDesc(ret[1]));
      return;
    }

    // script will publish the order
  });
}

/*
 * An execution report has been received from the market
 */
function newExecutionReport(exereport) {
  if (exereport.orderstatusid == '1' || exereport.orderstatusid == '2') {
    processTrade(exereport);
  } else if (exereport.orderstatusid == '8') {
    orderReject(exereport);
  } else if (exereport.orderstatusid == '4') {
    orderCancel(exereport);
  } else if (exereport.orderstatusid == 'C') {
    orderExpired(exereport);
  } else if (exereport.orderstatusid == '0') {
    orderAck(exereport);
  }
}

/*
 * Store & process a trade
 */
function processTrade(exereport) {
  console.log("processTrade");
  console.log(exereport);

  var currencyratetoorg = 1; // product currency rate back to org
  var currencyindtoorg = 1;
  var markettype = 0;
  var counterpartytype = 1;

  // extract brokerid & our orderid
  var clordid = exereport.clordid.split(':');

  exereport.accountid = "";
  exereport.clientid = "";
  exereport.operatortype = "";
  exereport.operatorid = "";
  exereport.symbolid = "";

  if (!('settlcurramt' in exereport)) {
    exereport.settlcurramt = exereport.lastshares * exereport.lastpx;
  }

  if (!('settlcurrfxrate' in exereport)) {
    exereport.settlcurrfxrate = 1;
  }

  if (!('settlcurrfxratecalc' in exereport)) {
    exereport.settlcurrfxratecalc = 0;
  }

  if (!('leavesqty' in exereport)) {
    exereport.leavesqty = 0;
  }

  if (!('markettimestamp' in exereport)) {
    exereport.markettimestamp = exereport.transacttime;
  }

  // if (!('currencyid' in exereport)) {
  //   exereport.currencyid = "";
  // }

  if (!('settlcurrencyid' in exereport)) {
    exereport.settlcurrencyid = "GBP";
  }

  if (!('currencyid' in exereport)) {
    if (exereport.settlcurrencyid) {
      exereport.currencyid = exereport.settlcurrencyid;
    } else {
      exereport.currencyid = "";
    }
  }

  if (!('exchangeid' in exereport)) {
    exereport.exchangeid = "";
  }

  if (!('lastmkt' in exereport)) {
    exereport.lastmkt = "";
  }

  // tradesettlestatustime will be empty on create process
  exereport.tradesettlestatustime = "";

  // milliseconds since epoch, used for scoring datetime indexes
  var tradedate = new Date();
  var milliseconds = tradedate.getTime();

  var promise = new Promise(function(resolve, reject) {
    // if no settlement date present, use the default
    if (!('futsettdate' in exereport)) {
      commonbo.getSettDate(tradedate, exereport.symbolid, function(err, settDate) {
        if (err) {
          console.log('processTrade/getSettDate: ', err);
          reject(err);
        } else {
          exereport.futsettdate = commonbo.getUTCDateString(settDate);
          resolve();
        }
      });
    } else {
      resolve();
    }
  });
  promise.then(function() {
    db.eval(scriptnewtrade, 1, "broker:" + clordid[0], exereport.accountid, clordid[0], exereport.clientid, clordid[1], exereport.symbolid, exereport.side, exereport.lastshares, exereport.lastpx, exereport.currencyid, currencyratetoorg, currencyindtoorg, exereport.onbehalfofcompid, counterpartytype, markettype, exereport.execid, exereport.futsettdate, exereport.timestamp, exereport.lastmkt, exereport.orderid, exereport.settlcurrencyid, exereport.settlcurramt, exereport.settlcurrfxrate, exereport.settlcurrfxratecalc, milliseconds, exereport.operatortype, exereport.operatorid, exereport.leavesqty, exereport.fixseqnumid, exereport.markettimestamp, exereport.tradesettlestatustime, exereport.exchangeid, exereport.securityid, function(err, ret) {
      if (err) {
        console.log(err);
        errorLog(clordid[0], clordid[1], 4, 4, exereport.fixseqnumid, "8", "tradeserver.scriptnewtrade", "", err);
        return;
      }

      if (ret[0] != 0) {
        console.log("Error in scriptnewtrade: " + commonbo.getReasonDesc(ret[1]));
        errorLog(clordid[0], clordid[1], 4, 4, exereport.fixseqnumid, "8", "tradeserver.scriptnewtrade", "", commonbo.getReasonDesc(ret[1]));
        return;
      }
    
      console.log("Trade id: " + ret[1] + " forwarded ok");

      // trade published as part of script
    });
  }).catch(function(err) {
    console.log('Trade rejected');
    errorLog(clordid[0], clordid[1], 4, 4, exereport.fixseqnumid, "8", "tradeserver.scriptnewtrade", "", err);
    return;
  });
}

/*
 * A quote request has been rejected
 */
function quoteAck(quoteack) {
  console.log("Quote ack received");
  console.log(quoteack);

  // extract brokerid & our quoterequestid
  var quotereqid = quoteack.quotereqid.split(':');

  //var timestamp = commonbo.getUTCTimeStamp(new Date());

  db.eval(scriptQuoteAck, 1, "broker:" + quotereqid[0], quotereqid[0], quotereqid[1], quoteack.quotestatusid, quoteack.quoterejectreasonid, quoteack.text, quoteack.fixseqnumid, quoteack.timestamp, quoteack.markettimestamp, function(err, ret) {
    if (err) {
      console.log(err);
      errorLog(quotereqid[0], quotereqid[1], 7, 4, quoteack.fixseqnumid, "b", "tradeserver.scriptQuoteAck", "", err);
      return;
    }

    if (ret[0] != 0) {
      console.log("Error in scriptQuoteAck: " + commonbo.getReasonDesc(ret[1]));
      return;
    }

    console.log("Quoteack id: " + ret[1] + " processed ok");

    // quoteack will be published by the script
  });
}

function registerScripts() {
  var updateordermargin;
  var updatereserve;
  var removefromorderbook;
  var cancelorder;
  var getcosts;
  //var rejectorder;
  var adjustmarginreserve;
  //var getinitialmargin;
  //var updatetrademargin;
  var neworder;
  //var getreserve;

 /*
  * publishquoterequest()
  * publish a quote request
  * params: brokerid, quoterequestid
  * returns: 0, channelid if ok, else 1, error code
  */
  publishquoterequest = commonbo.gethashvalues + commonbo.getchannel + '\
  local publishquoterequest = function(brokerid, quoterequestid) \
    redis.log(redis.LOG_NOTICE, "publishquoterequest") \
    local quoterequest = gethashvalues("broker:" .. brokerid .. ":quoterequest:" .. quoterequestid) \
    quoterequest.isin = redis.call("hget", "symbol:" .. quoterequest.symbolid, "isin") \
    local channel = getchannel(brokerid, "quoterequest", quoterequestid) \
    if channel[1] ~= 0 then \
      return channel \
    end \
    redis.call("publish", channel[2], "{" .. cjson.encode("quoterequest") .. ":" .. cjson.encode(quoterequest) .. "}") \
    return {0, channel[2]} \
  end \
  ';

  /*
  * publishquoteack()
  * publish a quote acknowledgement
  * params: brokerid, quoteackid
  * returns: 0 if ok, else 1, error code
  */
  publishquoteack = commonbo.gethashvalues + commonbo.getchannel + '\
  local publishquoteack = function(brokerid, quoteackid) \
    redis.log(redis.LOG_NOTICE, "publishquoteack") \
    local quoteack = gethashvalues("broker:" .. brokerid .. ":quoteack:" .. quoteackid) \
    local quoterequest = gethashvalues("broker:" .. brokerid .. ":quoterequest:" .. quoteack.quoterequestid) \
    quoteack.clientid = quoterequest.clientid \
    quoteack.symbolid = quoterequest.symbolid \
    quoteack.accountid = quoterequest.accountid \
    quoteack.operatorid = quoterequest.operatorid \
    local channel = getchannel(brokerid, "quoteack", quoteackid) \
    if channel[1] ~= 0 then \
      return channel \
    end \
    redis.call("publish", channel[2], "{" .. cjson.encode("quoteack") .. ":" .. cjson.encode(quoteack) .. "}") \
    return {0} \
  end \
  ';

  /*
  * publishquote()
  * publish a quote
  * params: brokerid, quoteid
  * returns: 0 if ok, else 1, error code
  */
  publishquote = commonbo.gethashvalues + commonbo.getchannel + '\
  local publishquote = function(brokerid, quoteid) \
    redis.log(redis.LOG_NOTICE, "publishquote") \
    local quote = gethashvalues("broker:" .. brokerid .. ":quote:" .. quoteid) \
    local channel = getchannel(brokerid, "quote", quoteid) \
    if channel[1] ~= 0 then \
      return channel \
    end \
    redis.call("publish", channel[2], "{" .. cjson.encode("quote") .. ":" .. cjson.encode(quote) .. "}") \
    return {0} \
  end \
  ';

 /*
  * publishorder()
  * publish an order
  * params: brokerid, orderid
  * returns: 0, channelid if ok, else 1, error code
  */
  publishorder = commonbo.gethashvalues + commonbo.getchannel + '\
  local publishorder = function(brokerid, orderid) \
    redis.log(redis.LOG_NOTICE, "publishorder") \
    local order = gethashvalues("broker:" .. brokerid .. ":order:" .. orderid) \
    --[[ add required values for external feed ]] \
    order.isin = redis.call("hget", "symbol:" .. order.symbolid, "isin") \
    if order.ordertypeid == "D" then \
      order.externalquoteid = redis.call("hget", "broker:" .. brokerid .. ":quote:" .. order.quoteid, "externalquoteid") \
      order.quoterid = redis.call("hget", "broker:" .. brokerid .. ":quote:" .. order.quoteid, "quoterid") \
    end \
    --[[ get the channel to forward to ]] \
    local channel = getchannel(brokerid, "order", orderid) \
    if channel[1] ~= 0 then \
      return channel \
    end \
    redis.call("publish", channel[2], "{" .. cjson.encode("order") .. ":" .. cjson.encode(order) .. "}") \
    return {0, channel[2]} \
  end \
  ';

  /*
  * publishordercancelrequest()
  * publish an ordercancelrequest
  * params: brokerid, ordercancelrequestid
  * returns: 0, channelid if ok, else 1, error code
  */
  publishordercancelrequest = commonbo.gethashvalues + commonbo.getchannel + '\
  local publishordercancelrequest = function(brokerid, ordercancelrequestid) \
    redis.log(redis.LOG_NOTICE, "publishordercancelrequest") \
    local ordercancelrequest = gethashvalues("broker:" .. brokerid .. ":ordercancelrequest:" .. ordercancelrequestid) \
    --[[ add required values for external feed ]] \
    ordercancelrequest.isin = redis.call("hget", "symbol:" .. ordercancelrequest.symbolid, "isin") \
    ordercancelrequest.externalorderid = redis.call("hget", "broker:" .. brokerid .. ":order:" .. ordercancelrequest.orderid, "externalorderid") \
    --[[ get the channel to forward to ]] \
    local channel = getchannel(brokerid, "ordercancelrequest", ordercancelrequestid) \
    if channel[1] ~= 0 then \
      return channel \
    end \
    redis.call("publish", channel[2], "{" .. cjson.encode("ordercancelrequest") .. ":" .. cjson.encode(ordercancelrequest) .. "}") \
    return {0, channel[2]} \
  end \
  ';

  /*
  * publishordercancelreject()
  * publish an order cancel reject
  * params: brokerid, ordercancelrejectid
  * returns: 0, channelid if ok, else 1, error code
  */  
  publishordercancelreject = commonbo.gethashvalues + commonbo.getchannel + '\
  local publishordercancelreject = function(brokerid, ordercancelrejectid) \
    redis.log(redis.LOG_NOTICE, "publishordercancelreject") \
    local ordercancelreject = gethashvalues("broker:" .. brokerid .. ":ordercancelreject:" .. ordercancelrejectid) \
    local ordercancelrequest = gethashvalues("broker:" .. brokerid .. ":ordercancelrequest:" .. ordercancelreject.ordercancelrequestid) \
    if not ordercancelrequest.ordercancelrequestid then \
      return {1, 1053} \
    end \
    --[[ add operatorid to assist with forwarding ]] \
    ordercancelreject.operatorid = ordercancelrequest.operatorid \
    --[[ get the channel to forward to ]] \
    local channel = getchannel(brokerid, "ordercancelreject", ordercancelrejectid) \
    if channel[1] ~= 0 then \
      return channel \
    end \
    redis.call("publish", channel[2], "{" .. cjson.encode("ordercancelreject") .. ":" .. cjson.encode(ordercancelreject) .. "}") \
    return {0, channel[2]} \
  end \
  ';

 /*
  * publishorderbook()
  * publish a change on the orderbook
  * params: brokerid, orderid
  */
  publishorderbook = commonbo.getorderbook + '\
  local publishorderbook = function(symbolid, side) \
    redis.log(redis.LOG_NOTICE, "publishorderbook") \
    --[[ unable to access commonbo.orderbookchannel in side the lua script, so hard coding the value (14) here ]] \
    --[[ redis.call("publish", commonbo.orderbookchannel, "{" .. cjson.encode("orderbook") .. ":" .. cjson.encode(getorderbook(symbolid, side)) .. "}") ]] \
    redis.call("publish", 14, "{" .. cjson.encode("orderbook") .. ":" .. cjson.encode(getorderbook(symbolid, side)) .. "}") \
  end \
  ';

  /*
  * publishorderbooktop()
  * publish a change to the orderbooktop
  * params: brokerid, orderid
  */
  publishorderbooktop = commonbo.getorderbooktop + '\
  local publishorderbooktop = function(symbolid, side) \
    redis.log(redis.LOG_NOTICE, "publishorderbooktop") \
    --[[ unable to access commonbo.orderbookchannel in side the lua script, so hard coding the value (14) here ]] \
    --[[ redis.call("publish", commonbo.orderbookchannel, "{" .. cjson.encode("orderbooktop") .. ":" .. cjson.encode(getorderbooktop(symbolid, side)) .. "}") ]] \
    redis.call("publish", 14, "{" .. cjson.encode("orderbooktop") .. ":" .. cjson.encode(getorderbooktop(symbolid, side)) .. "}") \
  end \
  ';

  /*
  * getcosts()
  * calculates costs for an order/trades
  * params: brokerid, clientid, symbolid, side, consideration, currencyid
  * returns: 0, commission, ptmlevy, stampduty, contractcharge if ok, else 1, error message number
  */
  getcosts = utils.getcommission + utils.getstampduty + utils.getptmlevy + '\
    local getcosts = function(brokerid, accountid, symbolid, side, consid, currencyid) \
      redis.log(redis.LOG_NOTICE, "getcosts") \
      --[[ set default costs ]] \
      local costs = {} \
      costs.commission = 0 \
      costs.ptmlevy = 0 \
      costs.stampduty = 0 \
      costs.stampdutyid = 0 \
      costs.contractcharge = 0 \
      costs.brokerkey = "broker:" .. brokerid \
      --[[ commission ]] \
      costs.commission = getcommission(brokerid, accountid, tonumber(consid)) \
      if costs.commission[1] == 1 then \
        return {1, 1044} \
      end \
      costs.commission = tonumber(costs.commission[2]) \
      --[[ ptm levy will calculated in buy ]] \
      costs.ptmlevy = tonumber(getptmlevy(symbolid, tonumber(consid))) \
      --[[ stamp duty ]] \
      costs.stampresult = getstampduty(symbolid, tonumber(consid), tonumber(side), "1") \
      costs.stampduty = tonumber(costs.stampresult[1]) \
      costs.stampdutyid = costs.stampresult[2] \
      --[[ contract charge ]] \
      --[[ if costs["contractcharge"] and costs["contractcharge"] ~= "" then ]] \
        --[[ contractcharge = tonumber(costs["contractcharge"]) ]] \
      --[[ end ]] \
      return {0, costs.commission, costs.ptmlevy, costs.stampduty, costs.contractcharge, costs.stampdutyid} \
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

  adjustmarginreserve = commonbo.getinitialmargin + updateordermargin + updatereserve + '\
  local adjustmarginreserve = function(orderid, clientid, symbolid, side, price, ordmargin, currencyid, leavesqty, newleavesqty, futsettdate, nosettdays) \
    local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
    leavesqty = tonumber(leavesqty) \
    newleavesqty = tonumber(newleavesqty) \
    if tonumber(side) == 1 then \
      if newleavesqty ~= leavesqty then \
        local newordmargin = 0 \
        if newleavesqty ~= 0 then \
          newordmargin = getinitialmargin(symbolid, newleavesqty * tonumber(price)) \
        end \
        updateordermargin(orderid, clientid, ordmargin, currencyid, newordmargin) \
      end \
    else \
      if newleavesqty ~= leavesqty then \
        updatereserve(clientid, symbolid, currencyid, futsettdate, -leavesqty + newleavesqty) \
      end \
    end \
  end \
  ';

  getreserve = '\
  local getreserve = function(clientid, symbolid, currencyid, futsettdate) \
    local reserve = redis.call("hget", clientid .. ":reserve:" .. symbolid .. ":" .. currencyid .. ":" .. futsettdate, "quantity") \
    if not reserve then \
      return 0 \
    end \
    return reserve \
  end \
  ';

  /*cancelorder = adjustmarginreserve + '\
  local cancelorder = function(brokerid, orderid, orderstatusid) \
    local orderkey = "broker:" .. brokerid .. ":order:" .. orderid \
    redis.call("hset", orderkey, "orderstatusid", orderstatusid) \
    local fields = {"clientid", "symbolid", "side", "price", "settlcurrencyid", "margin", "leavesqty", "futsettdate", "nosettdays"} \
    local vals = redis.call("hmget", orderkey, unpack(fields)) \
    if not vals[1] then \
      return 1009 \
    end \
    adjustmarginreserve(orderid, vals[1], vals[2], vals[3], vals[4], vals[6], vals[5], vals[7], 0, vals[8], vals[9]) \
    return 0 \
  end \
  ';*/

  /*
  * set the staus of an order to cancelled
  */
  cancelorder = '\
  local cancelorder = function(brokerid, orderid) \
    redis.call("hset", "broker:" .. brokerid .. ":order:" .. orderid, "orderstatusid", "4") \
  end \
  ';

  /*
  * set the staus of an order to expired
  */
  expireorder = '\
  local expireorder = function(brokerid, orderid) \
    redis.call("hset", "broker:" .. brokerid .. ":order:" .. orderid, "orderstatusid", "C") \
  end \
  ';

  /*
  * add an order to the orderbook
  * params: order
  */
  addtoorderbook =  '\
  local addtoorderbook = function(order) \
    local price \
    --[[ buy orders need a negative price ]] \
    if tonumber(order.side) == 1 then \
      price = "-" .. order.price \
    else \
      price = order.price \
    end \
    --[[ add order to order book ]] \
    redis.call("zadd", "orderbook:" .. order.symbolid, price, order.brokerid .. ":" .. order.orderid) \
    redis.call("sadd", "orderbooks", order.symbolid) \
    publishorderbook(order.symbolid, order.side) \
    publishorderbooktop(order.symbolid, order.side) \
    --[[ open limits for credit checking ]] \
    redis.call("sadd", "broker:" .. order.brokerid .. ":account:" .. order.accountid .. ":openlimits", order.orderid) \
    redis.call("sadd", "broker:" .. order.brokerid .. ":account:" .. order.accountid .. ":orderbook", order.orderid) \
  end \
  ';

  /*
  * remove an order from the orderbook
  * params: order
  */
  removefromorderbook = publishorderbook + publishorderbooktop + '\
  local removefromorderbook = function(order) \
    redis.call("zrem", "orderbook:" .. order.symbolid, order.brokerid .. ":" .. order.orderid) \
    if (redis.call("zcount", "orderbook:" .. order.symbolid, "-inf", "+inf") == 0) then \
      redis.call("srem", "orderbooks", order.symbolid) \
    end \
    publishorderbook(order.symbolid, order.side) \
    publishorderbooktop(order.symbolid, order.side) \
    redis.call("srem", "broker:" .. order.brokerid .. ":account:" .. order.accountid .. ":openlimits", order.orderid) \
    redis.call("srem", "broker:" .. order.brokerid .. ":account:" .. order.accountid .. ":orderbook", order.orderid) \
  end \
  ';

  /*
  * clearorderbook()
  * clear the orderbook for a symbol & expire orders
  * params: symbolid
  * returns: none
  */
  clearorderbook = commonbo.split + commonbo.gethashvalues + removefromorderbook + expireorder + '\
  local clearorderbook = function(symbolid) \
    local orders = redis.call("zrangebyscore", "orderbook:" .. symbolid, "-inf", "+inf") \
    for i = 1, #orders do \
      local brokerorderid = split(orders[i], ":") \
      local order = gethashvalues("broker:" .. brokerorderid[1] .. ":order:" .. brokerorderid[2]) \
      removefromorderbook(order) \
      expireorder(brokerorderid[1], brokerorderid[2]) \
    end \
  end \
  ';

  /*
  * clearorderbooks()
  * clears all orderbooks
  * params: none
  * returns: none
  */
  clearorderbooks = clearorderbook + '\
  local clearorderbooks = function() \
    redis.log(redis.LOG_NOTICE, "clearorderbooks") \
    local orderbooks = redis.call("smembers", "orderbooks") \
    for i = 1, #orderbooks do \
      clearorderbook(orderbooks[i]) \
    end \
  end \
  ';

  /*
  * scriptclearorderbooks()
  * script to clear all orderbooks
  */
  scriptclearorderbooks = clearorderbooks + '\
    redis.log(redis.LOG_NOTICE, "scriptneworder") \
    clearorderbooks() \
  ';

 /*
   * quoteack()
   * process a quote acknowledgement
   * params: brokerid, quoterequestid, quotestatusid, quoterejectreasonid, text, fixseqnumid, timestamp, markettimestamp
   * returns: 0, quoteackid if ok, else 1, error code
   */
  quoteack = publishquoteack + getparentlinkdetails + updateaddtionalindex + '\
  local quoteack = function(brokerid, quoterequestid, quotestatusid, quoterejectreasonid, text, fixseqnumid, timestamp, markettimestamp) \
    redis.log(redis.LOG_NOTICE, "quoteack") \
    --[[ create a quote ack ]] \
    local quoteackid = redis.call("hincrby", "broker:" .. brokerid, "lastquoteackid", 1) \
    local operatortype = redis.call("hget", "broker:" .. brokerid .. ":quoterequest:" .. quoterequestid, "operatortype") \
    redis.call("hmset", "broker:" .. brokerid .. ":quoteack:" .. quoteackid, "broker", brokerid, "quoterequestid", quoterequestid, "quotestatusid", quotestatusid, "quoterejectreasonid", quoterejectreasonid, "text", text, "quoteackid", quoteackid, "fixseqnumid", fixseqnumid, "timestamp", timestamp, "markettimestamp", markettimestamp, "operatortype", operatortype) \
    --[[ update quote request ]] \
    redis.call("hmset", "broker:" .. brokerid .. ":quoterequest:" .. quoterequestid, "quotestatusid", quotestatusid, "quoteackid", quoteackid) \
    --[[ add indices for quoteack ]] \
    local accountid = redis.call("hget", "broker:" .. brokerid .. ":quoterequest:" .. quoterequestid, "accountid") \
    redis.call("sadd", "broker:" .. brokerid .. ":quoteacks", quoteackid) \
    redis.call("sadd", "broker:" .. brokerid .. ":quoteackid", "quoteack:" .. quoteackid) \
    --[[ add parent link details using client account ]] \
    local linkdetails = getparentlinkdetails(brokerid, accountid) \
    --[[ add to set of additional quoteacks indexes ]] \
    updateaddtionalindex(brokerid, "quoteacks", quoteackid, linkdetails[1], linkdetails[2], "", "") \
    local pubsub = publishquoteack(brokerid, quoteackid) \
    if pubsub[1] ~= 0 then \
      return pubsub \
    end \
    return {0, quoteackid} \
  end \
  ';

  /*
  * neworder()
  * gets the next orderid for a broker & saves the order
  */
  neworder = utils.getparentlinkdetails + utils.updateaddtionalindex + utils.lowercase + utils.gettimestampindex + '\
  local neworder = function(accountid, brokerid, clientid, symbolid, side, quantity, price, ordertypeid, markettype, futsettdate, quoteid, currencyid, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforceid, expiredate, expiretime, settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, operatortype, operatorid, hedgeorderid, cashorderqty, settlmnttypid, timestampms) \
    redis.log(redis.LOG_NOTICE, "neworder") \
    local temp = {} \
    temp.brokerkey = "broker:" .. brokerid \
    --[[ get a new orderid & store the order ]] \
    temp.orderid = redis.call("hincrby", temp.brokerkey, "lastorderid", 1) \
    redis.call("hmset", temp.brokerkey .. ":order:" .. temp.orderid, "accountid", accountid, "brokerid", brokerid, "clientid", clientid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "ordertypeid", ordertypeid, "leavesqty", quantity, "orderstatusid", 0, "markettype", markettype, "futsettdate", futsettdate, "quoteid", quoteid, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "timestamp", timestamp, "margin", margin, "timeinforceid", timeinforceid, "expiredate", expiredate, "expiretime", expiretime, "settlcurrencyid", settlcurrencyid, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "orderid", temp.orderid, "externalorderid", externalorderid, "execid", execid, "operatortype", operatortype, "operatorid", operatorid, "hedgeorderid", hedgeorderid, "orderrejectreasonid", "", "text", "", "cashorderqty", cashorderqty, "settlmnttypid", settlmnttypid) \
    --[[ add to set of orders ]] \
    redis.call("sadd", temp.brokerkey .. ":orders", temp.orderid) \
    redis.call("sadd", temp.brokerkey .. ":orderid", "order:" .. temp.orderid) \
    --[[ add to set of orders for this account ]] \
    redis.call("sadd", temp.brokerkey .. ":account:" .. accountid .. ":orders", temp.orderid) \
    --[[ add to order search index. The search will be on orderid, symbolid and isin ]] \
    temp.symbolshortname = redis.call("hget", "symbol:" .. symbolid, "shortname") \
    --[[ trim the symbol shortname ]] \
    temp.symbolshortname = temp.symbolshortname:gsub("%s", "") \
    temp.searchvalue = clientid .. ":" .. accountid .. ":" .. temp.orderid .. ":" .. lowercase(symbolid .. ":" .. temp.symbolshortname .. ":" .. gettimestampindex(timestamp)) \
    redis.call("zadd", temp.brokerkey .. ":order:search_index", temp.orderid, temp.searchvalue) \
    redis.call("zadd", temp.brokerkey .. ":account:" .. accountid .. ":ordersbydate", timestampms, temp.orderid) \
    --[[ add order id to associated quote, if there is one ]] \
    --[[ add parent link details using client account ]] \
    temp.linkdetails = getparentlinkdetails(brokerid, accountid) \
     --[[ add to set of additional quotes indexes ]] \
    updateaddtionalindex(brokerid, "orders", temp.orderid, temp.linkdetails[1], temp.linkdetails[2], "order:search_index", temp.searchvalue) \
    if quoteid ~= "" then \
      redis.call("hset", temp.brokerkey .. ":quote:" .. quoteid, "orderid", temp.orderid) \
    end \
    return temp.orderid \
  end \
  ';

  /*
  * reverseside()
  * function to reverse a side
  */
  reverseside = '\
  local reverseside = function(side) \
    if tonumber(side) == 1 then \
      return 2 \
    else \
      return 1 \
    end \
  end \
  ';

  /*
  * getsettlcurramt()
  * calculate order consideration
  * params: brokerid, orderid, quantity, cashorderqty, price, ordertypeid
  * returns: 1, error code if fail, 0, consideration if ok
  * note: order status & reject reason will be updated if there is an error
  */
  getsettlcurramt = commonbo.rejectorder + '\
    local getsettlcurramt = function(brokerid, orderid, quantity, cashorderqty, price, ordertypeid) \
      if quantity == "" then \
        if cashorderqty == "" then \
          rejectorder(brokerid, orderid, 0, "Either quantity or cashorderqty must be specified") \
          return {1, 1034} \
        else \
          return {0, tonumber(cashorderqty)} \
        end \
      else \
        if price == "" then \
          if tonumber(ordertypeid) == 1 then \
            --[[ no price is correct for a market order ]] \
            return {0, ""} \
          else \
            rejectorder(brokerid, orderid, 0, "Price must be specified for this type of order") \
            return {1, 1049} \
          end \
        else \
          if tonumber(ordertypeid) == 1 then \
            rejectorder(brokerid, orderid, 0, "Price should not be specified in a market order") \
            return {1, 1050} \
          else \
            return {0, round(tonumber(quantity) * tonumber(price), 2)} \
          end \
        end \
      end \
    end \
  ';

  /*
  * validorder()
  * validate an order
  * params: accountid, brokerid, clientid, orderid, symbolid, ordertypeid, markettype, settlcurrencyid
  * returns: 1, error code if fail, 0 if ok together with symbol details
  * note: order status & reject reason will be updated if there is an error
  */
  validorder = commonbo.rejectorder + '\
    local validorder = function(accountid, brokerid, clientid, orderid, symbolid, ordertypeid, markettype, settlcurrencyid) \
      --[[ see if symbol exists ]] \
      local symbol = gethashvalues("symbol:" .. symbolid) \
      if not symbol["symbolid"] then \
        rejectorder(brokerid, orderid, 0, "Symbol not found") \
        return {1, 1015} \
      end \
      --[[ see if client is allowed to trade this type of product ]] \
      if redis.call("sismember", "broker:" .. brokerid .. ":client:" .. clientid .. ":instrumenttypes", symbol["instrumenttypeid"]) == 0 then \
        rejectorder(brokerid, orderid, 0, "Client not authorised to trade this type of product") \
        return {1, 1018} \
      end \
      --[[ see if valid order type ]] \
      if ordertypeid ~= "D" and tonumber(ordertypeid) ~= 1 and tonumber(ordertypeid) ~= 2 then \
        rejectorder(brokerid, orderid, 0, "Order type not supported") \
        return {1, 1048} \
      end \
      local accountcurrencyid = redis.call("hget", "broker:" .. brokerid .. ":account:" .. accountid, "currencyid") \
      if settlcurrencyid ~= accountcurrencyid then \
        rejectorder(brokerid, orderid, 0, "Settlement currency does not match account currency") \
        return {1, 1051} \
      end \
      return {0, symbol} \
    end \
  ';

  /*
  * validmatchorder()
  * validate an order for order matching
  * params: order
  * returns: 1, error code if fail, 0 if ok
  * note: order status & reject reason will be updated if there is an error
  */
  validmatchorder = commonbo.rejectorder + '\
    local validmatchorder = function(order) \
      if tonumber(order.ordertypeid) ~= 2 then \
        rejectorder(order.brokerid, order.orderid, 0, "Order matching only supports limit orders") \
        return {1, 1052} \
      end \
      if tonumber(order.leavesqty) <= 0 then \
        rejectorder(order.brokerid, order.orderid, 0, "Order already filled") \
        return {1, 1010} \
      end \
      return {0} \
    end \
  ';

 /*
  * matchordersingle()
  * tries to match a single order in the orderbook
  * if unable to match, adds order to the orderbook
  * params: brokerid, orderid, timestampms, costs
  * returns: 1, errorcode if error, else ok
  */
  matchordersingle = removefromorderbook + addtoorderbook + commonbo.split + validmatchorder + commonbo.newtrade + '\
  local matchordersingle = function(brokerid, orderid, timestampms, costs) \
    redis.log(redis.LOG_NOTICE, "matchordersingle") \
    local order = gethashvalues("broker:" .. brokerid .. ":order:" .. orderid) \
    local retval = validmatchorder(order) \
    if retval[1] ~= 0 then \
      return {retval} \
    end \
    local temp = {} \
    if tonumber(order.side) == 1 then \
      temp.lowerbound = 0 \
      temp.upperbound = order.price \
      temp.matchside = 2 \
    else \
      temp.lowerbound = "-inf" \
      temp.upperbound = "-" .. order.price \
      temp.matchside = 1 \
    end \
    --[[ get the matchable orders ]] \
    local matchorders = redis.call("zrangebyscore", "orderbook:" .. order.symbolid, temp.lowerbound, temp.upperbound) \
    for i = 1, #matchorders do \
      local brokerorderid = split(matchorders[i], ":") \
      local matchorder = gethashvalues("broker:" .. brokerorderid[1] .. ":order:" .. brokerorderid[2]) \
      if not matchorder["orderid"] then \
        return {1, 1009} \
      end \
      --[[ match on price & quantity ]] \
      if tonumber(matchorder.price) == tonumber(order.price) and tonumber(matchorder.quantity) == tonumber(order.quantity) and matchorder.clientid ~= order.clientid then \
        --[[ validate passive order trade ]] \
        temp.settlcurramt = tonumber(matchorder.quantity) * tonumber(matchorder.price) \
        temp.consid = temp.settlcurramt \
        temp.costs = {0,0,0,0,0} \
        temp.matchcosts = {0,0,0,0,0} \
        temp.finance = 0 \
        temp.margin = 0 \
        temp.counterpartytype = 2 \
        temp.markettype = 1 \
        temp.currencyratetoorg = 1 \
        temp.currencyindtoorg = 0 \
        temp.settlcurrfxrate = 1 \
        temp.settlcurrfxratecalc = 0 \
        temp.lastmkt = "XLON" \
        temp.tradesettlestatustime = "" \
        temp.exchangeid = "L" \
        --[[ active order trade ]] \
        local tradeid = newtrade(order.accountid, order.brokerid, order.clientid, order.orderid, order.symbolid, order.side, order.quantity, order.price, order.currencyid, temp.currencyratetoorg, temp.currencyindtoorg, costs[2], costs[3], costs[4], costs[5], costs[6], matchorder.clientid, temp.counterpartytype, temp.markettype, "", order.futsettdate, order.timestamp, temp.lastmkt, "", order.settlcurrencyid, temp.settlcurramt, temp.settlcurrfxrate, temp.settlcurrfxratecalc, temp.margin, order.operatortype, order.operatorid, temp.finance, timestampms, "000", "", "", "", "000", order.timestamp, temp.tradesettlestatustime, temp.exchangeid) \
        --[[ passive order trade ]] \
        local matchtradeid = newtrade(matchorder.accountid, matchorder.brokerid, matchorder.clientid, matchorder.orderid, matchorder.symbolid, matchorder.side, matchorder.quantity, matchorder.price, matchorder.currencyid, temp.currencyratetoorg, temp.currencyindtoorg, temp.matchcosts[1], temp.matchcosts[2], temp.matchcosts[3], temp.matchcosts[4], temp.matchcosts[5], order.clientid, temp.counterpartytype, temp.markettype, "", order.futsettdate, order.timestamp, temp.lastmkt, "", matchorder.settlcurrencyid, temp.consid, temp.settlcurrfxrate, temp.settlcurrfxratecalc, temp.margin, order.operatortype, order.operatorid, temp.finance, timestampms, "000", "", "", "", "000", order.timestamp, temp.tradesettlestatustime, temp.exchangeid) \
        --[[ update passive order ]] \
        redis.call("hmset", "broker:" .. brokerid .. ":order:" .. matchorder.orderid, "leavesqty", 0, "orderstatusid", 2) \
        removefromorderbook(matchorder) \
        --[[ update active order ]] \
        redis.call("hmset", "broker:" .. brokerid .. ":order:" .. order.orderid, "leavesqty", 0, "orderstatusid", 2) \
        order.leavesqty = 0 \
        break \
      end \
    end \
    --[[ add the order to the orderbook if it has not been matched ]] \
    if tonumber(order.leavesqty) ~= 0 then \
      addtoorderbook(order) \
    end \
    return {0} \
  end \
  ';

 /*
  * newordersingle()
  * validates and stores an order
  * params: accountid, brokerid, clientid, symbolid, side, quantity, price, ordertypeid, markettype, futsettdate, quoteid, currencyid, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforceid, expiredate, expiretime, settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, operatortype, operatorid, hedgeorderid, cashorderqty, settlmnttypid, timestampms
  * returns: {fail 1=fail, errorcode, orderid} or {success 0=ok, orderid} \
  */
  newordersingle = neworder + getcosts + getsettlcurramt + validorder + commonbo.creditcheck + publishorder + matchordersingle + '\
  local newordersingle = function(accountid, brokerid, clientid, symbolid, side, quantity, price, ordertypeid, markettype, futsettdate, quoteid, currencyid, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforceid, expiredate, expiretime, settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, operatortype, operatorid, hedgeorderid, cashorderqty, settlmnttypid, timestampms) \
    redis.log(redis.LOG_NOTICE, "newordersingle") \
    local temp = {} \
    local order = {} \
    temp.brokerkey = "broker:" .. brokerid \
    local orderid = neworder(accountid, brokerid, clientid, symbolid, side, quantity, price, ordertypeid, markettype, futsettdate, quoteid, currencyid, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforceid, expiredate, expiretime, settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, operatortype, operatorid, hedgeorderid, cashorderqty, settlmnttypid, timestampms) \
    side = tonumber(side) \
    order.hedgeorderid = "" \
    --[[ validate the order ]] \
    temp.vo = validorder(accountid, brokerid, clientid, orderid, symbolid, ordertypeid, markettype, settlcurrencyid) \
    if temp.vo[1] ~= 0 then \
      --[[ publish the order back to the operatortype - the order contains the error ]] \
      publishorder(brokerid, orderid) \
      return {1, temp.vo[2], orderid} \
    end \
    temp.symbol = temp.vo[2] \
    --[[ calculate the consideration ]] \
    temp.sc = getsettlcurramt(brokerid, orderid, quantity, cashorderqty, price, ordertypeid) \
    if temp.sc[1] ~= 0 then \
      --[[ publish the order back to the operatortype - the order contains the error ]] \
      publishorder(brokerid, orderid) \
      return {1, temp.sc[2], orderid} \
    end \
    order.settlcurramt = temp.sc[2] \
    --[[ calculate costs ]] \
    order.costs = {1, 0, 0, 0, 0, 0} \
    order.totalcost = 0 \
    --[[ a market order does not have a price or consideration, so we are assuming it has already been credit checked ]] \
    if tonumber(ordertypeid) == 1 then \
      order.costs = getcosts(brokerid, accountid, symbolid, side, 0, currencyid) \
      if order.costs[1] ~= 0 then \
        return {1, order.costs[2], orderid} \
      end \
      order.totalcost = order.costs[2] + order.costs[3] + order.costs[4] + order.costs[5] \
      --[[ credit check the order ]] \
      temp.cc = creditcheck(accountid, brokerid, orderid, clientid, symbolid, side, quantity, price, 0, settlcurrencyid, futsettdate, order.totalcost, temp.symbol["instrumenttypeid"]) \
      if temp.cc[1] ~= 0 then \
        --[[ publish the order back to the operatortype - the order contains the error ]] \
        publishorder(brokerid, orderid) \
        return {1, 1026, orderid} \
      end \
    elseif order.settlcurramt ~= "" then \
      order.costs = getcosts(brokerid, accountid, symbolid, side, order.settlcurramt, currencyid) \
      if order.costs[1] ~= 0 then \
        return {1, order.costs[2], orderid} \
      end \
      order.totalcost = order.costs[2] + order.costs[3] + order.costs[4] + order.costs[5] \
      --[[ credit check the order ]] \
      temp.cc = creditcheck(accountid, brokerid, orderid, clientid, symbolid, side, quantity, price, order.settlcurramt, settlcurrencyid, futsettdate, order.totalcost, temp.symbol["instrumenttypeid"]) \
      if temp.cc[1] ~= 0 then \
        --[[ publish the order back to the operatortype - the order contains the error ]] \
        publishorder(brokerid, orderid) \
        return {1, 1026, orderid} \
      end \
    end \
    local pubsub = publishorder(brokerid, orderid) \
    if pubsub[1] ~= 0 then \
      --[[ change the order status & try to publish again ]] \
      rejectorder(brokerid, orderid, 0, "Channel not found") \
      publishorder(brokerid, orderid) \
      return {pubsub[1], pubsub[2], orderid} \
    end \
    --[[ we are handling channel 17 internally ]] \
    if tonumber(pubsub[2]) == 17 then \
      local retval = matchordersingle(brokerid, orderid, timestampms, order.costs) \
      if retval[1] ~= 0 then \
        publishorder(brokerid, orderid) \
        return {retval[1], retval[2], orderid} \
      end \
    end \
    return {0, orderid, pubsub[2]} \
  end \
  ';

/*    if temp.symbol["instrumenttypeid"] == "CFD" or temp.symbol["instrumenttypeid"] == "SPB" or temp.symbol["instrumenttypeid"] == "CCFD" then \
      --[[ ignore limit orders for derivatives as they will be handled manually, at least for the time being ]] \
      if tonumber(ordertypeid) ~= 2 then \
        --[[ create trades for client & hedge book for off-exchange products ]] \
        order.principleaccountid = redis.call("get", temp.brokerkey .. ":principleaccount") \
        if not order.principleaccountid then \
          rejectorder(brokerid, orderid, 0, "Principle account not found") \
          --[[ publish the order back to the operatortype - the order contains the error ]] \
          publishorder(brokerid, orderid) \
          return {1, 1032, orderid} \
        end \
        order.principleclientid = redis.call("get", temp.brokerkey .. ":account:" .. order.principleaccountid .. ":client") \
        order.rside = reverseside(side) \
        order.finance = 0 \
        order.counterpartytype = 2 \
        order.principletradeid = newtrade(order.principleaccountid, brokerid, order.principleclientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, 0, 0, 0, 0, 0, accountid, order.counterpartytype, markettype, "", futsettdate, timestamp, "", "", settlcurrencyid, order.settlcurramt, settlcurrfxrate, settlcurrfxratecalc, temp.cc[2], operatortype, operatorid, order.finance, timestampms, "000", "", "", "", "000", timestamp, "", "") \
        order.tradeid = newtrade(accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, order.costs[2], order.costs[3], order.costs[4], order.costs[5], order.costs[6], order.principleaccountid, order.counterpartytype, markettype, "", futsettdate, timestamp, "", "", settlcurrencyid, order.settlcurramt, settlcurrfxrate, settlcurrfxratecalc, temp.cc[2], operatortype, operatorid, order.finance, timestampms, "000", "", "", "", "000", timestamp, "", "") \
        --[[ adjust order as filled ]] \
        redis.call("hmset", temp.brokerkey .. ":order:" .. orderid, "leavesqty", 0, "orderstatusid", 2) \
        --[[ todo: may need to adjust margin here ]] \
        --[[ see if we need to hedge this trade in the market ]] \
        temp.hedgeclient = tonumber(redis.call("hget", temp.brokerkey .. ":client:" .. clientid, "hedge")) \
        temp.hedgeinst = tonumber(redis.call("hget", temp.brokerkey .. ":brokersymbol:" .. symbolid, "hedge")) \
        if temp.hedgeclient == 1 or temp.hedgeinst == 1 then \
          --[[ create a hedge order in the underlying product ]] \
          if temp.symbol["hedgesymbolid"] then \
            order.hedgeorderid = neworder(order.principleaccountid, brokerid, order.principleclientid, temp.symbol["hedgesymbolid"], side, quantity, price, ordertypeid, markettype, futsettdate, quoteid, currencyid, currencyratetoorg, currencyindtoorg, timestamp, margin, timeinforceid, expiredate, expiretime, settlcurrencyid, settlcurrfxrate, settlcurrfxratecalc, externalorderid, execid, operatortype, operatorid, order.hedgeorderid, cashorderqty, settlmnttypid, timestampms) \
          end \
        end \
      end \
    elseif temp.symbol["instrumenttypeid"] == "DE" or temp.symbol["instrumenttypeid"] == "IE" then \
      --[[ consider equity limit orders ]] \
      if tonumber(ordertypeid) == 2 then \
        --[[ match the order with the orderbook ]] \
        return matchordersingle(brokerid, orderid, timestampms) \
      end \
    end \
 */

 /*
  * scriptdealatquote
  * place an order based on a quote
  * params: 1=brokerid, 2=ordertypeid, 3=markettype, 4=quoteid, 5=currencyratetoorg, 6=currencyindtoorg, 7=timestamp, 8=timeinforceid, 9=operatortype, 10=operatorid, 11=timestampms, 12=futsettdate
  * returns: 0, orderid, channelid if ok, else 1, error code
  */
  scriptdealatquote = commonbo.gethashvalues + newordersingle + '\
  redis.log(redis.LOG_NOTICE, "scriptdealatquote") \
  local order = {} \
  local quote = gethashvalues("broker:" .. ARGV[1] .. ":quote:" .. ARGV[4]) \
  if not quote["quoteid"] then \
    return {1, 1024, "n/a"} \
  end \
  local settlcurrfxrate \
  local settlcurrfxratecalc = 0 \
  if quote["bidpx"] == "" then \
    order["side"] = 1 \
    order["quantity"] = quote["offerquantity"] \
    order["price"] = quote["offerpx"] \
    if tonumber(quote["offerspotrate"]) ~= 0 then \
      settlcurrfxrate = quote["offerspotrate"] \
    else \
      settlcurrfxrate = 1 \
    end \
  else \
    order["side"] = 2 \
    order["quantity"] = quote["bidquantity"] \
    order["price"] = quote["bidpx"] \
    if tonumber(quote["bidspotrate"]) ~= 0 then \
      settlcurrfxrate = quote["bidspotrate"] \
    else \
      settlcurrfxrate = 1 \
    end \
  end \
  --[[ note: we store & forward settlement values from the quote, which may be different from those of the quote request ]] \
  return newordersingle(quote["accountid"], ARGV[1], quote["clientid"], quote["symbolid"], order["side"], order["quantity"], order["price"], ARGV[2], ARGV[3], ARGV[12], ARGV[4], quote["currencyid"], ARGV[5], ARGV[6], ARGV[7], 0, ARGV[8], "", "", quote["settlcurrencyid"], settlcurrfxrate, settlcurrfxratecalc, "", "", ARGV[9], ARGV[10], 0, quote["cashorderqty"], quote["settlmnttypid"], ARGV[11]) \
  ';

 /*
  * scriptneworder
  * params: 1=accountid, 2=brokerid, 3=clientid, 4=symbolid, 5=side, 6=quantity, 7=price, 8=ordertypeid, 9=markettype, 10=futsettdate, 11=quoteid, 12=currencyid, 13=currencyratetoorg, 14=currencyindtoorg, 15=timestamp, 16=timeinforceid, 17=expiredate, 18=expiretime, 19=settlcurrencyid, 20=settlcurrfxrate, 21=settlcurrfxratecalc, 22=operatortype, 23=operatorid, 24=cashorderqty, 25=settlmnttypid, 26=timestampms, 27=isin
  * returns: 0, orderid, channelid if ok, else 1, error code, orderid
  */
  scriptneworder = commonbo.getclientaccountid + newordersingle + '\
  redis.log(redis.LOG_NOTICE, "scriptneworder") \
  --[[ get trading accountid if not specified ]] \
  local accountid \
  if ARGV[1] == "" then \
    accountid = getclientaccountid(ARGV[2], ARGV[3], 1) \
  else \
    accountid = ARGV[1] \
  end \
  if not accountid then \
    return {1, 1025, "n/a"} \
  end \
  --[[ check there is a symbolid or isin ]] \
  if ARGV[4] == "" and ARGV[27] == "" then \
    return {1, 1046} \
  end \
  --[[ get a symbolid if necessary ]] \
  local symbolid = ARGV[4] \
  if symbolid == "" then \
    symbolid = redis.call("get", "isin:" .. ARGV[27]) \
    if not symbolid then \
      return {1, 1016, "n/a"} \
    end \
  end \
  return newordersingle(accountid, ARGV[2], ARGV[3], symbolid, ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13], ARGV[14], ARGV[15], 0, ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20], ARGV[21], "", "", ARGV[22], ARGV[23], 0, ARGV[24], ARGV[25], ARGV[26]) \
  ';

 /*
  * scriptrejectorder
  * order rejected externally
  * params: brokerid, orderid, orderrejectreasonid, text
  */
  scriptrejectorder = commonbo.rejectorder + publishorder + '\
  rejectorder(ARGV[1], ARGV[2], ARGV[3], ARGV[4]) \
  publishorder(ARGV[1], ARGV[2]) \
  return 0 \
  ';

 /*
  * scriptnewtrade()
  * process a fill from the market
  * params: 1=accountid, 2=brokerid, 3=clientid, 4=clordid, 5=symbolid, 6=side, 7=lastshares, 8=lastpx, 9=currencyid, 10=currencyratetoorg, 11=currencyindtoorg, 12=execbroker, 13=counterpartytype, 14=markettype, 15=execid, 16=futsettdate, 17=transacttime, 18=lastmkt, 19=orderid, 20=settlcurrencyid, 21=settlcurramt, 22=settlcurrfxrate, 23=settlcurrfxratecalc, 24=milliseconds, 25=operatortype, 26=operatorid, 27=leavesqty, 28=fixseqnumid, 29=markettimestamp, 30=tradesettlestatustime, 31=exchangeid, 32=securityid
  */
  scriptnewtrade = getcosts + commonbo.newtrade + '\
  redis.log(redis.LOG_NOTICE, "scriptnewtrade") \
  local trade = {} \
  trade.accountid = ARGV[1] \
  trade.clientid = ARGV[3] \
  trade.orderid = ARGV[4] \
  trade.settlcurrfxrate = ARGV[22] \
  trade.settlcurrfxratecalc = ARGV[23] \
  trade.operatortype = ARGV[25] \
  trade.operatorid = ARGV[26] \
  trade.leavesqty = ARGV[27] \
  trade.fixseqnumid = ARGV[28] \
  trade.settlcurramt = round(tonumber(ARGV[21]), 2) \
  --[[ default value of lastcrestmessagestatus is "000" ]] \
  trade.lastcrestmessagestatus = "000" \
  --[[ get the symbol ]] \
  trade.symbolid = redis.call("get", "isin:" .. ARGV[32]) \
  if not trade.symbolid then \
    return {1, 1016} \
  end \
  --[[ if there is an orderid, get the order & update unknown values for the fill ]] \
  if trade.orderid ~= "" then \
    local order = gethashvalues(KEYS[1] .. ":order:" .. trade.orderid) \
    if not order["orderid"] then \
      return {1, 1009} \
    end \
    trade.accountid = order["accountid"] \
    trade.clientid = order["clientid"] \
    --[[ use fx rate from order, as this is copied from the quote & we do not get one from a fill ]] \
    trade.settlcurrfxrate = order["settlcurrfxrate"] \
    trade.settlcurrfxratecalc = order["settlcurrfxratecalc"] \
    trade.operatortype = order["operatortype"] \
    trade.operatorid = order["operatorid"] \
  end \
  --[[ get costs ]] \
  trade.consid = tonumber(ARGV[7]) * tonumber(ARGV[8]) \
  local costs = getcosts(ARGV[2], trade.accountid, trade.symbolid, ARGV[6], trade.consid, ARGV[20]) \
  --[[ finance/margin may be needed for derivs (set default value for both 0)]] \
  --[[ create trade ]] \
  local retval = newtrade(trade.accountid, ARGV[2], trade.clientid, trade.orderid, trade.symbolid, ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], costs[2], costs[3], costs[4], costs[5], costs[6], ARGV[12], ARGV[13], ARGV[14], ARGV[15], ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20], trade.settlcurramt, trade.settlcurrfxrate, trade.settlcurrfxratecalc, 0, trade.operatortype, trade.operatorid, 0, ARGV[24], "000", "", "", trade.fixseqnumid, trade.lastcrestmessagestatus, ARGV[29], ARGV[30], ARGV[31]) \
  --[[ adjust order remaining quantity & status ]] \
  if trade.orderid ~= "" then \
    if trade.leavesqty ~= "" and tonumber(trade.leavesqty) > 0 then \
      trade.orderstatusid = 1 \
    else \
      trade.orderstatusid = 2 \
    end \
    redis.call("hmset", KEYS[1] .. ":order:" .. trade.orderid, "leavesqty", trade.leavesqty, "orderstatusid", trade.orderstatusid) \
  end \
  return retval \
  ';

  /*
  * ordercancelreject()
  * store and forward an order cancel reject
  * params: brokerid, ordercancelrequestid, orderid, externalorderid, orderstatusid, ordercancelrejectreasonid, text, timestamp, markettimestamp
  * returns: 0, ordercancelrejectid if ok, else 1, error code
  */
  ordercancelreject = publishordercancelreject + '\
  local ordercancelreject = function(brokerid, ordercancelrequestid, orderid, externalorderid, orderstatusid, ordercancelrejectreasonid, text, timestamp, markettimestamp) \
    --[[ get the operator type from the request for forwarding ]] \
    local operatortype = redis.call("hget", "broker:" .. brokerid .. ":ordercancelrequest:" .. ordercancelrequestid, "operatortype") \
    if not operatortype then \
      return {1, 1053} \
    end \
    --[[ store the order cancel reject ]] \
    local ordercancelrejectid = redis.call("hincrby", "broker:" .. brokerid, "lastordercancelrejectid", 1) \
    redis.call("hmset", "broker:" .. brokerid .. ":ordercancelreject:" .. ordercancelrejectid, "brokerid", brokerid, "ordercancelrequestid", ordercancelrequestid, "orderid", orderid, "externalorderid", externalorderid, "orderstatusid", orderstatusid, "ordercancelrejectreasonid", ordercancelrejectreasonid, "text", text, "timestamp", timestamp, "markettimestamp", markettimestamp, "operatortype", operatortype) \
    redis.call("sadd", "broker:" .. brokerid .. ":ordercancelrejects", ordercancelrejectid) \
    redis.call("hmset", "broker:" .. brokerid .. ":ordercancelrequest:" .. ordercancelrequestid, "ordercancelrejectid", ordercancelrejectid) \
    local ret = publishordercancelreject(brokerid, ordercancelrejectid) \
    if ret[1] ~= 0 then \
      return ret \
    end \
    return {0, ordercancelrejectid} \
  end \
  ';

 /*
  * scriptordercancelrequest
  * request to cancel an order
  * params: 1=brokerid, 2=clientid, 3=accountid, 4=orderid, 5=timestamp, 6=operatortype, 7=operatorid
  * returns: 0, ordercancelreqest id if ok, else 1, error code
  */
  scriptordercancelrequest = removefromorderbook + cancelorder + commonbo.gethashvalues + publishorder + publishordercancelrequest + ordercancelreject + '\
  redis.log(redis.LOG_NOTICE, "scriptordercancelrequest") \
  local errorcode = 0 \
  local desc = "" \
  local brokerid = ARGV[1] \
  local orderid = ARGV[4] \
  local order = gethashvalues("broker:" .. brokerid .. ":order:" .. orderid) \
  if not order["orderid"] then \
    --[[ order not found ]] \
    return {1, 1009} \
  end \
  local ordercancelrequestid = redis.call("incr", "broker:" .. brokerid .. ":lastordercancelrequestid") \
  --[[ store the order cancel request ]] \
  redis.call("hmset", "broker:" .. brokerid .. ":ordercancelrequest:" .. ordercancelrequestid, "brokerid", brokerid, "clientid", order.clientid, "accountid", order.accountid, "orderid", orderid, "timestamp", ARGV[5], "operatortype", ARGV[6], "operatorid", ARGV[7], "symbolid", order.symbolid, "ordercancelrequestid", ordercancelrequestid, "markettype", order.markettype) \
  redis.call("sadd", "broker:" .. brokerid .. ":ordercancelrequests", ordercancelrequestid) \
  redis.call("sadd", "broker:" .. brokerid .. ":order:" .. orderid .. ":ordercancelrequests", ordercancelrequestid) \
  if order.orderstatusid == "2" then \
    --[[ already filled ]] \
    desc = "Order already filled" \
    errorcode = 1010 \
  elseif order.orderstatusid == "4" then \
    --[[ already cancelled ]] \
    desc = "Order already cancelled" \
    errorcode = 1008 \
  elseif order.orderstatusid == "8" then \
    --[[ already rejected ]] \
    desc = "Order already rejected" \
    errorcode = 1012 \
  end \
  if errorcode ~= 0 then \
    local ocr = ordercancelreject(brokerid, ordercancelrequestid, orderid, "", order.orderstatusid, 2, desc, ARGV[5], ARGV[5]) \
    if ocr[1] ~= 0 then \
      return ocr \
    end \
    redis.call("hset", "broker:" .. brokerid .. ":ordercancelrequest:" .. ordercancelrequestid, "ordercancelrejectid", ocr[2]) \
    return {1, errorcode} \
  end \
  --[[ forward the request ]] \
  local pubsub = publishordercancelrequest(brokerid, ordercancelrequestid) \
  if pubsub[1] ~= 0 then \
    return pubsub \
  end \
  --[[ handle channel 17 locally ]] \
  if tonumber(pubsub[2]) == 17 then \
    removefromorderbook(order) \
    cancelorder(brokerid, orderid) \
    publishorder(brokerid, orderid) \
  end \
  return {0, ordercancelrequestid} \
  ';

/*
 * scriptordercancel
 * cancel an order
 * params: brokerid, orderid
 * returns: 1, error code if error, else 0
 */
  scriptordercancel = cancelorder + publishorder + '\
  local orderid = redis.call("hget", "broker:" .. ARGV[1] .. ":order:" .. ARGV[2], "orderid") \
  if not orderid then \
    return {1, 1013} \
  end \
  cancelorder(ARGV[1], orderid) \
  publishorder(ARGV[1], orderid) \
  return {0} \
  ';

  // todo: add timems, fixseqnum to newtrade()
  scriptorderfillrequest = commonbo.getinitialmargin + '\
  local errorcode = 0 \
  local orderid = KEYS[2] \
  local fields = {"clientid", "symbolid", "side", "quantity", "price", "margin", "leavesqty", "nosettdays", "operatortype", "hedgeorderid", "futsettdate", "operatorid", "orderstatusid", "externalorderid", "settlcurrencyid", "markettype"} \
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
  local symbolid = vals[2] \
  local consid = leavesqty * price \
  local side = vals[3] \
  local settlcurrencyid = vals[15] \
  local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
  local costs = getcosts(vals[1], symbolid, instrumenttypeid, side, consid, settlcurrencyid) \
  if costs[1] ~= 0 then \
    errorcode = costs[2] \
  end \
  local tradeid = 0 \
  if errorcode == 0 then \
    --[[ can only fill the remaining quantity ]] \
    local leavesqty = tonumber(vals[7]) \
    --[[ fill at the passed price, may be different from the order price ]] \
    local price = tonumber(KEYS[6]) \
    --[[ todo: round? ]] \
    local initialmargin = getinitialmargin(symbolid, consid) \
    local finance = calcfinance(instrumenttypeid, consid, settlcurrencyid, side, vals[8]) \
    local hedgebookid = redis.call("get", "hedgebook:" .. instrumenttypeid .. ":" .. settlcurrencyid) \
    if not hedgebookid then hedgebookid = 999999 end \
    tradeid = newtrade(vals[1], orderid, symbolid, side, quantity, price, settlcurrencyid, 1, 1, costs[1], costs[2], costs[3], costs[4], costs[5], hedgebookid, vals[16], "", KEYS[7], KEYS[3], "", "", settlcurrencyid, consid, 1, 1, vals[8], initialmargin, vals[9], vals[12], finance, "", "000", "", "", "", "000", "", "") \
  end \
  return {errorcode, tradeid} \
  ';

  scriptorderack = publishorder + '\
  --[[ update external limit reference ]] \
  redis.call("hmset", "broker:" .. ARGV[1] .. ":order:" .. ARGV[2], "externalorderid", ARGV[3], "orderstatusid", ARGV[4], "execid", ARGV[5], "text", ARGV[6], "fixseqnumid", ARGV[7]) \
  publishorder(ARGV[1], ARGV[2]) \
  return \
  ';

 /*
  * called if an order has expired
  * params: brokerid, orderid
  * returns: 1, error code if error, else 0 
  */
  scriptorderexpire = expireorder + publishorder + '\
  local ret = expireorder(ARGV[1], ARGV[2], "C") \
  publishorder(ARGV[1], ARGV[2]) \
  return ret \
  ';

  /*
  * scriptQuoteRequest
  * store a quote request
  * params: 1=accountid, 2=brokerid, 3=cashorderqty, 4=clientid, 5=currencyid, 6=futsettdate, 7=operatorid, 8=operatortype, 9=quantity, 10=settlmnttypid, 11=side, 12=symbolid, 13=timestamp, 14=settlcurrencyid, 15=timestampms, 16=isin
  * returns: 1, error message if error, else 0
  */
  scriptQuoteRequest = commonbo.gethashvalues + commonbo.getclientaccountid + commonbo.getaccount + utils.getparentlinkdetails + utils.updateaddtionalindex + utils.lowercase + utils.trim + utils.gettimestampindex + quoteack + publishquoterequest + '\
  redis.log(redis.LOG_NOTICE, "scriptQuoteRequest") \
  local quoterequestid = redis.call("hincrby", KEYS[1], "lastquoterequestid", 1) \
  if not quoterequestid then return {1, 1005} end \
  --[[ get trading accountid if not specified ]] \
  local accountid \
  if ARGV[1] == "" then \
    accountid = getclientaccountid(ARGV[2], ARGV[4], 1, ARGV[14]) \
  else \
    accountid = ARGV[1] \
  end \
  if not accountid or accountid == 0 then \
    return {1, 1025} \
  end \
  --[[ get the account & use the account currency as the request currency ]] \
  local account = getaccount(accountid, ARGV[2]) \
  --[[ check there is a symbolid or isin ]] \
  if ARGV[12] == "" and ARGV[16] == "" then \
    return {1, 1046} \
  end \
  --[[ get a symbolid if necessary ]] \
  local symbolid = ARGV[12] \
  if symbolid == "" then \
    symbolid = redis.call("get", "isin:" .. ARGV[16]) \
    if not symbolid then \
      return {1, 1016} \
    end \
  end \
  --[[ store the quote request ]] \
  redis.call("hmset", KEYS[1] .. ":quoterequest:" .. quoterequestid, "accountid", accountid, "brokerid", ARGV[2], "cashorderqty", ARGV[3], "clientid", ARGV[4], "currencyid", account["currencyid"], "futsettdate", ARGV[6], "operatorid", ARGV[7], "operatortype", ARGV[8], "quantity", ARGV[9], "quoterequestid", quoterequestid, "quotestatusid", 0, "settlmnttypid", ARGV[10], "side", ARGV[11], "symbolid", symbolid, "timestamp", ARGV[13], "settlcurrencyid", account["currencyid"], "quoteackid", "", "fixseqnumid", "") \
  --[[ add to the set of quoterequests ]] \
  redis.call("sadd", KEYS[1] .. ":quoterequests", quoterequestid) \
  redis.call("sadd", KEYS[1] .. ":quoterequestid", "quoterequest:" .. quoterequestid) \
  --[[ add to set of quoterequests for this account ]] \
  redis.call("sadd", KEYS[1] .. ":account:" .. accountid .. ":quoterequests", quoterequestid) \
  redis.call("zadd", KEYS[1] .. ":account:" .. accountid .. ":quoterequestsbydate", ARGV[15], quoterequestid) \
  --[[ add to quote-request search index. The search will be on quoterequestid, symbolid, and isin ]] \
  local shortname = redis.call("hget", "symbol:" .. symbolid, "shortname") \
  redis.call("zadd", KEYS[1] .. ":quoterequest:search_index", quoterequestid, ARGV[4] .. ":" .. accountid .. ":" .. quoterequestid .. ":" .. lowercase(symbolid .. ":" .. trim(shortname) .. ":" .. gettimestampindex(ARGV[13]))) \
  --[[ add parent link details using client account ]] \
  local linkdetails = getparentlinkdetails(ARGV[2], accountid) \
  --[[ add to set of additional quoterequests indexes and pass last two parameter only when search index needed ]] \
  updateaddtionalindex(ARGV[2], "quoterequests", quoterequestid, linkdetails[1], linkdetails[2], "", "") \
  --[[ check there is some kind of quantity ]] \
  if ARGV[3] == "" and ARGV[9] == "" then \
    local ret = quoteack(ARGV[2], quoterequestid, 5, 99, "Either quantity or cashorderqty must be present", "", ARGV[13], ARGV[13]) \
    if ret[1] ~= 0 then \
      return ret \
    end \
    return {1, 1034} \
  end \
  local pubsub = publishquoterequest(ARGV[2], quoterequestid) \
  if pubsub[1] ~= 0 then \
    quoteack(ARGV[2], quoterequestid, 5, 99, "Channel not found", "", ARGV[13], ARGV[13]) \
    return pubsub \
  end \
  return {0, quoterequestid, pubsub[2]} \
  ';

  /*
  * scriptQuote
  * store a quote
  * params: 1=quoterequestid, 2=symbolid, 3=bidpx, 4=offerpx, 5=bidsize, 6=offersize, 7=validuntiltime, 8=transacttime, 9=currencyid, 10=settlcurrencyid, 11=quoterid, 12=quotertype, 13=futsettdate, 14=bidquotedepth, 15=offerquotedepth, 16=externalquoteid, 17=cashorderqty, 18=settledays, 19=noseconds, 20=brokerid, 21=settlmnttypid, 22=bidspotrate, 23=offerspotrate, 24=timestampms, 25=fixseqnumid, 26=markettimestamp, 27=isin
  */
  scriptQuote = publishquote + getcosts + utils.getparentlinkdetails + utils.lowercase + utils.gettimestampindex + utils.createaddtionalindex + utils.createsearchindex + '\
  redis.log(redis.LOG_NOTICE, "scriptQuote") \
  local brokerid = ARGV[20] \
  local brokerkey = "broker:" .. brokerid \
  --[[ get the quote request ]] \
  local quoterequest = gethashvalues(brokerkey .. ":quoterequest:" .. ARGV[1]) \
  if not quoterequest["quoterequestid"] then \
    return {1, 1014} \
  end \
  --[[ get the client ]] \
  local clientid = redis.call("get", brokerkey .. ":account:" .. quoterequest["accountid"] .. ":client") \
  if not clientid then \
    return {1, 1017} \
  end \
  --[[ get the symbol ]] \
  local symbolid = redis.call("get", "isin:" .. ARGV[27]) \
  if not symbolid then \
    return {1, 1016} \
  end \
  --[[ get touch prices - using delayed - todo: may need to look up delayed/live ]] \
  local bestbid = 0 \
  local bestoffer = 0 \
  local symbolfields = {"bid", "ask", "shortname"} \
  local symbolvals = redis.call("hmget", "symbol:" .. symbolid, unpack(symbolfields)) \
  if not symbolvals[1] then \
    return {1, 1015} \
  end \
  local bestbid = symbolvals[1] \
  local bestoffer = symbolvals[2] \
  symbolvals[3] = symbolvals[3]:gsub("%s", "") \
  local bidquantity = "" \
  local offerquantity = "" \
  local bidfinance = 0 \
  local offerfinance = 0 \
  local cashorderqty \
  local side \
  local costs = {1,0,0,0,0,0} \
  local searchvalue \
  --[[ calculate the quantity from the cashorderqty, if necessary ]] \
  --[[ note that the value of the quote may be greater than client cash - any order will be credit checked so does not matter ]] \
  if ARGV[3] == "" then \
    local offerprice = tonumber(ARGV[4]) \
    if quoterequest["quantity"] == "" or tonumber(quoterequest["quantity"]) == 0 then \
      offerquantity = math.floor(tonumber(quoterequest["cashorderqty"]) / offerprice, 0) \
    else \
      offerquantity = tonumber(quoterequest["quantity"]) \
    end \
    --[[ set the amount of cash based on the quoted price ]] \
    cashorderqty = round(offerquantity * offerprice, 2) \
    side = 1 \
  else \
    local bidprice = tonumber(ARGV[3]) \
    if quoterequest["quantity"] == "" or tonumber(quoterequest["quantity"]) == 0 then \
      bidquantity = round(tonumber(quoterequest["cashorderqty"]) / bidprice, 0) \
    else \
      bidquantity = tonumber(quoterequest["quantity"]) \
    end \
    --[[ set the amount of cash based on the quoted price ]] \
    cashorderqty = round(bidquantity * bidprice, 2) \
    side = 2 \
  end \
  --[[ get the costs ]] \
  costs = getcosts(brokerid, quoterequest["accountid"], quoterequest["symbolid"], side, cashorderqty, ARGV[10]) \
  if costs[1] ~= 0 then \
    return costs \
  end \
  --[[ create a quote id as different from external quote ids (one for bid, one for offer)]] \
  local quoteid = redis.call("hincrby", brokerkey, "lastquoteid", 1) \
  --[[ store the quote ]] \
  redis.call("hmset", brokerkey .. ":quote:" .. quoteid, "quoterequestid", ARGV[1], "brokerid", brokerid, "accountid", quoterequest["accountid"], "clientid", quoterequest["clientid"], "quoteid", quoteid, "symbolid", quoterequest["symbolid"], "bestbid", bestbid, "bestoffer", bestoffer, "bidpx", ARGV[3], "offerpx", ARGV[4], "bidquantity", bidquantity, "offerquantity", offerquantity, "bidsize", ARGV[5], "offersize", ARGV[6], "validuntiltime", ARGV[7], "transacttime", ARGV[8], "currencyid", ARGV[9], "settlcurrencyid", ARGV[10], "quoterid", ARGV[11], "quotertype", ARGV[12], "futsettdate", ARGV[13], "bidfinance", bidfinance, "offerfinance", offerfinance, "orderid", "", "bidquotedepth", ARGV[14], "offerquotedepth", ARGV[15], "externalquoteid", ARGV[16], "cashorderqty", tostring(cashorderqty), "settledays", ARGV[18], "noseconds", ARGV[19], "settlmnttypid", ARGV[21], "commission", tostring(costs[2]), "ptmlevy", tostring(costs[3]), "stampduty", tostring(costs[4]), "contractcharge", tostring(costs[5]), "bidspotrate", ARGV[22], "offerspotrate", ARGV[23], "operatortype", quoterequest["operatortype"], "operatorid", quoterequest["operatorid"], "fixseqnumid", ARGV[25], "markettimestamp", ARGV[26]) \
  --[[ add parent link details using client account ]] \
  local linkdetails = getparentlinkdetails(brokerid, quoterequest["accountid"]) \
  --[[ add to sets of quotes & quotes for this account ]] \
  redis.call("sadd", brokerkey .. ":quotes", quoteid) \
  createaddtionalindex(brokerid, "quotes", quoteid, linkdetails[1], linkdetails[2]) \
  redis.call("sadd", brokerkey .. ":account:" .. quoterequest["accountid"] .. ":quotes", quoteid) \
  redis.call("zadd", brokerkey .. ":account:" .. quoterequest["accountid"] .. ":quotesbydate",  ARGV[24], quoteid) \
  redis.call("sadd", brokerkey .. ":quoteid", "quote:" .. quoteid) \
  --[[ keep a list of quotes for the quoterequest ]] \
  redis.call("sadd", brokerkey .. ":quoterequest:" .. ARGV[1] .. ":quotes", quoteid) \
  --[[ add to quote search index. The search will be on quoteid, symbolid, isin and symbol shortname and also add set of additional indexes ]] \
  searchvalue = quoterequest.clientid .. ":" .. quoterequest.accountid .. ":" .. quoteid .. ":" .. lowercase(quoterequest.symbolid .. ":" .. symbolvals[3] .. ":" .. gettimestampindex(ARGV[8])) \
  createsearchindex(brokerid, "quote:search_index", quoteid, searchvalue, linkdetails[1], linkdetails[2]) \
  local quotestatusid \
  --[[ bid or offer size needs to be non-zero ]] \
  if ARGV[5] == 0 and ARGV[6] == 0 then \
    quotestatusid = "5" \
  else \
    quotestatusid = "0" \
  end \
  --[[ update status to stored quoterequest ]] \
  redis.call("hmset", brokerkey .. ":quoterequest:" .. ARGV[1], "quotestatusid", quotestatusid) \
  --[[ publish quote based on routing ]] \
  local pubsub = publishquote(brokerid, quoteid) \
  if pubsub[1] ~= 0 then \
    return pubsub \
  end \
  return {0, quoteid} \
  ';

  /*
  * scriptQuoteAck
  * script to handle a quote acknowledgement
  * params: brokerid, quoterequestid, quotestatusid, quoterejectreasonid, text, fixseqnumid, timestamp, markettimestamp
  */
  scriptQuoteAck = quoteack + '\
  local ret = quoteack(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8]) \
  return ret \
  ';

  /*
   * scriptordercancelreject()
   * script to handle an order cancel reject
   * params: brokerid, ordercancelrequestid, orderid, externalorderid, orderstatusid, ordercancelrejectreasonid, text, timestamp, markettimestamp
   * returns: 0, ordercancelrejectid, else 1, error code
   */
  scriptordercancelreject = ordercancelreject + '\
  return ordercancelreject(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9]) \
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
