/****************
* nbtinterfeed.js
* Market data feed using NBTrader Interfeed
* Cantwaittotrade Limited
* Terry Johnston
* August 2014
*
* This server should be running at all times.
* It makes a connection to the service and then sits waiting for any requests via the price server channel.
* Requests will be forwarded and the results will update Redis directly and may be published,
* so that any listening applications can receive them.
*
* To request symbols from a Redis connection...
* Real-time partial record i.e. "publish 7 rp:BARC.L"
* Real-time full record i.e. "publish 7 rf:BARC.L"
* Snap full record i.e. "publish 7 snap:L.B***"
*
* Changes:
* 3 September 2016 - modified getExchangeId() to split instrument code in a more flexible way to get exchange id
* 6 November 2016 - added default mnemonic in case of no value returned
* 14 December 2016 - added midprice to default symbol record
*****************/

// node libraries
var net = require('net');
var fs = require('fs');

// external libraries
var redis = require('redis');

// cw2t libraries
var commonfo = require('./commonfo.js');
var commonbo = require('./commonbo.js');

// redis
var redishost;
var redisport;
var redisauth;
var redispassword;
var redislocal = true; // local or external server

// globals
var host; // ip address, get from database
var messageport; // port, get from database
var buf = new Buffer(1024 * 2048); // incoming data buffer
var bufbytesread = 0;
var bytestoread = 0;

// regular or authenticated connection to redis
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

// set-up a redis client
db = redis.createClient(redisport, redishost);
if (redisauth) {
  db.auth(redispassword, function(err) {
    if (err) {
      console.log(err);
      return;
    }
    console.log("Redis authenticated at " + redishost + " port " + redisport);
    start();
  });
} else {
  db.on("connect", function(err) {
    if (err) {
      console.log(err);
      return;
    }
    console.log("connected to Redis at " + redishost + " port " + redisport);
    start();
  });
}
db.on("error", function(err) {
  console.log(err);
});

/*
* this is the first routine
*/
function start() {
  initialise();
  startToConnect();
}

/*
* initialisation routines
*/
function initialise() {
  commonfo.registerScripts();
  commonbo.registerScripts();
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
    console.log("received: " + message);

    try {
      var obj = JSON.parse(message);

      if ("pricerequest" in obj) {
        priceRequest(obj.pricerequest);
      }
    } catch (e) {
      console.log(e);
      console.log(message);
      return;
    }
  });

  // listen for anything specifically for a price server
  dbsub.subscribe(commonbo.priceserverchannel);

  // listen for position messages
  dbsub.subscribe(commonbo.positionchannel);
}

/*
* get connection ip address & port & try to connect
*/
function startToConnect() {
  db.get("pricing:ipaddress", function(err, ipaddress) {
    if (err) {
      console.log(err);
      return;
    }

    if (ipaddress == null) {
      console.log("Error: no ip address found - add key 'pricing:ipaddress'");
      return;
    }

    host = ipaddress;

    db.get("pricing:port", function(err, port) {
      if (err) {
        console.log(err);
        return;
      }

      if (port == null) {
        console.log("Error: no port found - add key 'pricing:port'");
        return;
      }

      messageport = port;

      // we have connection details, so we can try to connect
      tryToConnect();
    });
  });
}

/*
* try to connect to external feed
*/
function tryToConnect() {
  console.log("trying to connect to host: " + host + ", port:" + messageport);

  conn = net.connect(messageport, host, function() {
    console.log('connected to: ' + host);

    // we are connected, so can request subscriptions();
    getSubscriptions();
  });

  conn.on('data', function(data) {
    parse(data);
  });

  conn.on('end', function() {
    console.log('disconnected from: ' + host);
    tryToConnect();
  });

  conn.on('error', function(error) {
    console.log(error);
  });
}

/*
* get the symbols we need to subscribe to and subscribe to them
*/
function getSubscriptions() {
}

function priceRequest(pricerequest) {
  console.log("pricerequest");

  console.log(pricerequest);

  requestData(pricerequest);
}

/*
* parse incoming data
* we need to allow for receiving part-messages
* and more than one message in a single receive event
*/
function parse(data) {
  var parsestate = 0;
  var fileseparator;
  var unitseparator;
  var groupseparator;
  var rtlseparator;
  var functioncode;
  var instrumentcode;
  var fid;
  var msglen;
  var instrec = {};

  //console.log('data recd');
  //console.log(data);

  // need something in these fields for script 
  instrec.bid = "";
  instrec.ask = "";
  instrec.midprice = "";
  instrec.midnetchange = "";
  instrec.midpercentchange = "";
  instrec.currencyid = "";
  instrec.isin = "";
  instrec.mnemonic = "";
  instrec.shortname = "";

  //var datalen = data.length;
  //console.log("data length="+data.length);
  //console.log("bytestoread="+bytestoread);

  // copy this data to our global buffer, offset for any messages waiting to be read
  data.copy(buf, bytestoread);
  bytestoread += data.length;

  // go through the buffer one byte at a time, allowing for messages already read
  for (var i = bufbytesread; i < bytestoread; i++) {
    switch (parsestate) {
    case 0:
      if (buf[i] == 28) { // <FS>
        // we need at least 4 bytes
        if (i - bufbytesread - 4 < 0) {
          continue;
        }
        // message length is contained in the first 4 bytes of each message
        msglen = buf.readUInt32BE(i-4);

        if (bytestoread - i < msglen) {
          return;
        }

        fileseparator = i;
        parsestate = 1;
      }
      break;
    case 1:
      if (buf[i] == 31) { // <US>
        functioncode = buf.toString('utf8', fileseparator+1, i);
        //console.log("functioncode="+functioncode);
        unitseparator = i;
        parsestate = 2;
      }
      break;
    case 2:
      if (buf[i] == 29) { // <GS>
        var tag = buf.toString('utf8', unitseparator+1, i);
        groupseparator = i;
        parsestate = 3;
      }
      break;
    case 3:
      if (buf[i] == 31) { // <US>
        instrumentcode = buf.toString('utf8', groupseparator+1, i);
        //console.log("instrumentcode="+instrumentcode);
        unitseparator = i;
        if (functioncode == "340") {
          parsestate = 4;
        } else {
          parsestate = 5;          
        }
      } else if (buf[i] == 30) { // <RS>
        var errorcode = buf.toString('utf8', groupseparator+1, i);
        var err = getError(errorcode);
        console.log("status: " + errorcode + " - " + err);
        // removed error message - todo - ok?
        //console.log("error: " + errorcode + " - " + err);
        rtlseparator = i;
        parsestate = 7;
      }
      break;
    case 4:
      if (buf[i] == 31) { // <US>
        var recordtype = buf.toString('utf8', unitseparator+1, i);
        //console.log("recordtype="+recordtype);
        unitseparator = i;
        parsestate = 5;
      } else if (buf[i] == 28) { // <FS>
        console.log("complete msg");
        var recordtype = buf.toString('utf8', unitseparator+1, i);
        console.log("recordtype="+recordtype);

        // we have finished a message
        bufbytesread = i + 1;
        parsestate = 0;
      }
      break;
    case 5:
      if (buf[i] == 30) { // <RS>
        var rtl = buf.toString('utf8', unitseparator+1, i);
        rtlseparator = i;
        parsestate = 6;
      } else if (buf[i] == 28) { // <FS>
        var rtl = buf.toString('utf8', unitseparator+1, i);

        // we have finished a message
        bufbytesread = i + 1;
        parsestate = 0;
      }
      break;
    case 6:
      if (buf[i] == 31) { // <US>
        fid = buf.toString('utf8', rtlseparator+1, i);
        unitseparator = i;
        parsestate = 7;
      }
      break;
    case 7:
      if (buf[i] == 30) { // <RS>
        var value = buf.toString('utf8', unitseparator+1, i);

        // get a field:value pair
        updateRec(fid, value, instrec);
        rtlseparator = i;
        parsestate = 6;
      } else if (buf[i] == 28) { // <FS>
        if (functioncode == "407") {
          var msg = buf.toString('utf8', rtlseparator+1, i);
          console.log("status msg: " + msg);
        } else {
          var value = buf.toString('utf8', unitseparator+1, i);

          // get the last field:value pair
          updateRec(fid, value, instrec);

          // update the database
          updateDb(functioncode, instrumentcode, instrec);

          // re-initialise prices as may be more than one message
          instrec.bid = "";
          instrec.ask = "";
          instrec.midprice = "";
          instrec.midnetchange = "";
          instrec.midpercentchange = "";
          instrec.currencyid = "";
          instrec.isin = "";
          instrec.mnemonic = "";
          instrec.shortname = "";
        }

        // we have finished a message
        bufbytesread = i + 1;
        parsestate = 0;
      }
      break;
    }
  }

  //console.log("bufbytesread="+bufbytesread);
  //console.log("bytestoread="+bytestoread);

  // see if we have caught up, if so reset
  if (bufbytesread == bytestoread) {
    bufbytesread = 0;
    bytestoread = 0;
  }
}

//
// just bid/offer & change requested as real-time loop
//
function subscribe(symbolid) {
  console.log("subscribe to " + symbolid);

  // get the nbtrader symbol
  db.hget("symbol:" + symbolid, "nbtsymbol", function(err, nbtsymbol) {
    if (err) {
      console.log("Error in subscribe():" + err);
      return;
    }

    console.log("subscribe to " + nbtsymbol);
    var nbtsymbollen = nbtsymbol.length;
    var buf = new Buffer(31+nbtsymbollen);

    buf[0] = 0;
    buf[1] = 0;
    buf[2] = 0;
    buf[3] = 27+nbtsymbollen;
    buf[4] = 28;
    buf.write("332", 5);
    buf[8] = 31;
    buf.write("mtag", 9);
    buf[13] = 29;
    buf.write(nbtsymbol, 14);
    buf[14+nbtsymbollen] = 30;
    buf.write("22", 15+nbtsymbollen); // bid
    buf[17+nbtsymbollen] = 30;
    buf.write("25", 18+nbtsymbollen); // offer
    buf[20+nbtsymbollen] = 30;
    buf.write("-374", 21+nbtsymbollen); // midnetchange
    buf[25+nbtsymbollen] = 30;
    buf.write("-375", 26+nbtsymbollen); // midpercentchange
    buf[30+nbtsymbollen] = 28;
  
    conn.write(buf);
  });
}

//
// subscribe to all subscribed to nbtrader symbols
//
function subscriptions() {
  db.smembers("nbtsymbols", function(err, nbtsymbols) {
    if (err) {
      console.log(err);
      return;
    }

    nbtsymbols.forEach(function(nbtsymbol, i) {
      subscribe(nbtsymbol);
    });
  });
}

function updateRec(fid, value, instrec) {
  var field = getFid(fid);
  if (field != "") {
    instrec[field] = value;
  }
}

function updateDb(functioncode, instrumentcode, instrec) {
  console.log("updateDb: " + instrumentcode);

  // create a unix timestamp
  var now = new Date();
  instrec.timestamp = now; //commonbo.getUTCTimeStamp(now);

  console.log(functioncode);
  console.log(instrec);

  // store a complete record for a symbol
  if (functioncode == "340") {
    // currency
    if (instrec.currencyid == "GBX") {
      instrec.currencyid = "GBP";
    }

    // create symbol & add to lists
    if ("insttype" in instrec) {
      // we only want to store part of the instrument record
      var dbinstrec = getDbInstrec(instrumentcode, instrec);

      // just equities & fx for now, may need to add other instrument types
      if (dbinstrec.instrumenttypeid != "DE" && dbinstrec.instrumenttypeid != "FX") {
        return;
      }

      console.log("creating..." + instrumentcode);

      // create/update the symbol & related sets
      db.hmset("symbol:" + instrumentcode, dbinstrec);
      db.sadd("symbol:symbolid", "symbol:" + instrumentcode);
      db.sadd("nbtsymbol:" + dbinstrec.nbtsymbol + ":symbols", instrumentcode);
      db.set("isin:" + dbinstrec.isin, instrumentcode);

      // create tab delimitted text
      /*var txt = dbinstrec.ask + "\t"
        + dbinstrec.bid + "\t"
        + dbinstrec.currencyid + "\t"
        + dbinstrec.exchangeid + "\t"
        + dbinstrec.hedgesymbolid + "\t"
        + dbinstrec.instrumenttypeid + "\t"
        + dbinstrec.isin + "\t"
        + dbinstrec.longname + "\t"
        + dbinstrec.midnetchange + "\t"
        + dbinstrec.midpercentchange + "\t"
        + dbinstrec.mnemonic + "\t"
        + dbinstrec.nbtsymbol + "\t"
        + dbinstrec.ptmexempt + "\t"
        + dbinstrec.shortname + "\t"
        + dbinstrec.symbolid + "\t"
        + dbinstrec.timestamp + "\t"
        + dbinstrec.timezoneid + "\n";

      // & write to a file
      fs.appendFile('symbols.txt', txt, function (err) {
        if (err) return console.log(err);
      });*/
    }
  }

  // update price
  db.eval(commonfo.scriptpriceupdate, 0, instrumentcode, instrec.timestamp, instrec.bid, instrec.ask, instrec.midprice, instrec.midnetchange, instrec.midpercentchange, function(err, ret) {
    if (err) throw err;
  });
}

function getDbInstrec(instrumentcode, instrec) {
  var dbinstrec = {};

  // may need to adjust prices to £
  if (instrec.currencyid == "GBP") {
    dbinstrec.ask = instrec.ask / 100;
    dbinstrec.bid = instrec.bid / 100;
    dbinstrec.midprice = instrec.midprice / 100;
  } else {
    dbinstrec.ask = instrec.ask;
    dbinstrec.bid = instrec.bid;
    dbinstrec.midprice = instrec.midprice;
  }

  dbinstrec.currencyid = instrec.currencyid;
  dbinstrec.isin = instrec.isin;
  dbinstrec.longname = instrec.longname;
  dbinstrec.midnetchange = instrec.midnetchange;
  dbinstrec.midpercentchange = instrec.midpercentchange;
  //dbinstrec.mnemonic = instrec.mnemonic;
  dbinstrec.timestamp = instrec.timestamp;
  dbinstrec.timezoneid = instrec.timezoneid;

  // we need a mnemonic as this is used in the trade feed
  if (instrec.mnemonic == "") {
    dbinstrec.mnemonic = instrumentcode.split(".")[0];
  } else {
    dbinstrec.mnemonic = instrec.mnemonic;
  }

  // we need a shortname
  if (instrec.shortname == "") {
    dbinstrec.shortname = instrumentcode;
  } else {
    dbinstrec.shortname = instrec.shortname;
  }

  // add additional values we need
  dbinstrec.symbolid = instrumentcode;
  dbinstrec.nbtsymbol = instrumentcode;
  dbinstrec.hedgesymbolid = "";
  dbinstrec.exchangeid = getExchangeId(instrumentcode);
  dbinstrec.ptmexempt = 0;
  dbinstrec.stampexempt = 0;

  // add our own instrument type, as we use text not numeric
  if ("insttype" in instrec) {
    dbinstrec.instrumenttypeid = getInstrumentType(instrec.insttype);
  }

  return dbinstrec;
}

function getExchangeId(instrumentcode) {
  var exchange = "";

  if (instrumentcode.indexOf('.') > -1) {
    var s = instrumentcode.split('.'); 
    exchange = s[s.length-1];
  }

  return exchange;
}

function getInstrumentType(nbinsttype) {
  var insttype;

  nbinsttype = parseInt(nbinsttype);

  switch (nbinsttype) {
    case 1:
      insttype = "DE";
      break;
    case 2:
      insttype = "GT";
      break;
    case 7:
      insttype = "DR";
      break;
    case 9:
      insttype = "IE";
      break;
    case 10:
      insttype = "EW";
      break;
    case 13:
      insttype = "BG";
      break;
    case 14:
      insttype = "FB";
      break;
    case 15:
      insttype = "DB";
      break;
    case 16:
      insttype = "LS";
      break;
    case 17:
      insttype = "CN";
      break;
    case 20:
      insttype = "ML";
      break;
    case 21:
      insttype = "BO";
      break;
    case 22:
      insttype = "PR";
      break;
    case 23:
      insttype = "PU";
      break;
    case 24:
      insttype = "CW";
      break;
    case 28:
      insttype = "FX";
      break;
    default:
      insttype = "77";
      break;
  }

  return insttype;
}

function loginReceived(message) {
  if (message.substr(0, 2) == "OK") {
    console.log("logged on ok");
  } else {
    console.log("login failed");
  }
}

function requestData(msg) {
  if (msg.substr(0, 2) == "rp") {
    // real-time partial record i.e. "rp:BARC.L"

    var instcode = msg.substr(3);
    subscribe(instcode);
    /*var instcodelen = instcode.length;
    var buf = new Buffer(21+instcodelen);

    buf[0] = 0;
    buf[1] = 0;
    buf[2] = 0;
    buf[3] = 17+instcodelen;
    buf[4] = 28;
    buf.write("332", 5);
    buf[8] = 31;
    buf.write("mtag", 9);
    buf[13] = 29;
    buf.write(instcode, 14);
    buf[14+instcodelen] = 30;
    buf.write("22", 15+instcodelen); // bid
    buf[17+instcodelen] = 30;
    buf.write("25", 18+instcodelen); // offer
    buf[20+instcodelen] = 28;
  
    conn.write(buf);*/
  } else if (msg.substr(0, 2) == "rf") {
    // real-time full record i.e. "rf:BARC.L"

    var instcode = msg.substr(3);
    var instcodelen = instcode.length;
    var buf = new Buffer(15+instcodelen);

    buf[0] = 0;
    buf[1] = 0;
    buf[2] = 0;
    buf[3] = 11+instcodelen;
    buf[4] = 28;
    buf.write("332", 5);
    buf[8] = 31;
    buf.write("mtag", 9);
    buf[13] = 29;
    buf.write(instcode, 14);
    buf[14+instcodelen] = 28;

    conn.write(buf);
  } else if (msg.substr(0, 4) == "snap") {
    // snap full record i.e. "snap:L.B***"

    var instcode = msg.substr(5);
    var instcodelen = instcode.length;
    var buf = new Buffer(15+instcodelen);

    buf[0] = 0;
    buf[1] = 0;
    buf[2] = 0;
    buf[3] = 11+instcodelen;
    buf[4] = 28;
    buf.write("333", 5);
    buf[8] = 31;
    buf.write("mtag", 9);
    buf[13] = 29;
    buf.write(instcode, 14);
    buf[14+instcodelen] = 28;

    conn.write(buf);
  } else if (msg.substr(0, 6) == "status") {
    // status request i.e. "<FS>409<US>mtag<GS>103<FS>"

    var buf = new Buffer(18);
    buf[0] = 0;
    buf[1] = 0;
    buf[2] = 0;
    buf[3] = 14;
    buf[4] = 28;
    buf.write("409", 5);
    buf[8] = 31;
    buf.write("mtag", 9);
    buf[13] = 29;
    buf.write("103", 14);
    buf[17] = 28;

    conn.write(buf);
  } else if (msg.substr(0, 4) == "halt") {
    // remove instrument from watchlist i.e. "<FS>348<US>mtag<GS>BARC.L<FS>"

    var instcode = msg.substr(5);
    var instcodelen = instcode.length;

    console.log("unsubscribe: " + instcode);

    var buf = new Buffer(15+instcodelen);

    buf[0] = 0;
    buf[1] = 0;
    buf[2] = 0;
    buf[3] = 11+instcodelen;
    buf[4] = 28;
    buf.write("348", 5);
    buf[8] = 31;
    buf.write("mtag", 9);
    buf[13] = 29;
    buf.write(instcode, 14);
    buf[14+instcodelen] = 28;

    conn.write(buf);
  }
}

function registerScripts() {
}

function getFid(field) {
  var desc = "";

  var fid = parseInt(field);

  switch (fid) {
    case -1:
      desc = "automaticturnover";
      break;
    case -2:
      desc = "autovol";
      break;
    case -6:
      desc = "industrysector";
      break;
    case -7:
      desc = "publicationlimit";
      break;
    case -8:
      desc = "issuercode";
      break;
    case -9:
      desc = "issuername";
      break;
    case -10:
      desc = "longname";
      break;
    case -11:
      desc = "shortname";
      break;
    case -12:
      desc = "quotesize";
      break;
    case -13:
      desc = "orderlotsize";
      break;
    case -14:
      desc = "minpord";
      break;
    case -15:
      desc = "minaord";
      break;
    case -16:
      desc = "normmarketsize";
      break;
    case -18:
      desc = "mnemonic";
      break;
    case -19:
      desc = "ticksize";
      break;
    case -20:
      desc = "sector";
      break;
    case -21:
      desc = "segment";
      break;
    case -22:
      desc = "stocksts";
      break;
    case -23:
      desc = "entrytype";
      break;
    case -25:
      desc = "maxpord";
      break;
    case -26:
      desc = "maxaord";
      break;
    case -27:
      desc = "minquote";
      break;
    case -28:
      desc = "maxquote";
      break;
    case -29:
      desc = "periodname";
      break;
    case -30:
      desc = "insttype";
      break;
    case -31:
      desc = "tradetype";
      break;
    case -32:
      desc = "asknetchange"
      break;
    case -33:
      desc = "maxorderdays";
      break;
    case -34:
      desc = "updatetime";
      break;
    case -49:
      desc = "suspensionprice";
      break;
    case -51:
      desc = "midhigh";
      break;
    case -52:
      desc = "midlow";
      break;
    case -53:
      desc = "midlowtime";
      break;
    case -54:
      desc = "midhightime";
      break;
    case -55:
      desc = "midtime";
      break;
    case -56:
      desc = "bidpercentchange";
      break;
    case -57:
      desc = "askpercentchange";
      break;
    case -58:
      desc = "midpercentchange";
      break;
    case -59:
      desc = "vwapalltrd";
      break;
    case -60:
      desc = "vwapautotrd";
      break;
    case -61:
      desc = "midtick";
      break;
    case -64:
      desc = "*";
      break;
    case -65:
      desc = "*";
      break;
    case -80:
      desc = "firstauctionprice";
      break;
    case -81:
      desc = "firstauctiontime";
      break;
    case -82:
      desc = "firstauctionvol";
      break;
    case -81:
      desc = "firstauctiontime";
      break;
    case -91:
      desc = "secondauctionprice";
      break;
    case -92:
      desc = "secondauctiontime";
      break;
    case -97:
      desc = "timezoneid";
      break;
    case -98:
      desc = "soldvol";
      break;
    case -99:
      desc = "boughtvol";
      break;
    case -108:
      desc = "tradeid";
      break;
    case -118:
      desc = "periodendtime";
      break;
    case -119:
      desc = "yestclosebid";
      break;
    case -120:
      desc = "yestcloseask";
      break;
    case -121:
      desc = "yestclosemid";
      break;
    case -129:
      desc = "couponrate";
      break;
    case -133:
      desc = "xdivdate";
      break;
    case -135:
      desc = "accrueddays";
      break;
    case -136:
      desc = "accruedinterest";
      break;
    case -284:
      desc = "*";
      break;
    case -308:
      desc = "*";
      break;
    case -337:
      desc = "*";
      break;
    case -338:
      desc = "icbsectornum";
      break;
    case -339:
      desc = "closedatetime";
      break;
    case -350:
      desc = "closetime";
      break;
    case -357:
      desc = "highestbid";
      break;
    case -358:
      desc = "highbidtime";
      break;
    case -359:
      desc = "highbiddate";
      break;
    case -360:
      desc = "lowestbid";
      break;
    case -361:
      desc = "lowbidtime";
      break;
    case -362:
      desc = "lowbiddate";
      break;
    case -363:
      desc = "highestoffer";
      break;
    case -364:
      desc = "highoffertime";
      break;
    case -365:
      desc = "highofferdate";
      break;
    case -366:
      desc = "lowestoffer";
      break;
    case -367:
      desc = "lowoffertime";
      break;
    case -368:
      desc = "lowofferdate";
      break;
    case -369:
      desc = "calcmidprice";
      break;
    case -373:
      desc = "yieldincadv";
      break;
    case -374:
      desc = "midnetchange";
      break;
    case -375:
      desc = "midpercentchange";
      break;
    case -376:
      desc = "midtime";
      break;
    case -377:
      desc = "middate";
      break;
    case -378:
      desc = "dailyinterest";
      break;
    case -384:
      desc = "xdivdate";
      break;
    case -385:
      desc = "adjnetchange";
      break;
    case -386:
      desc = "adjpercentchange";
      break;
    case -387:
      desc = "*";
      break;
    case -388:
      desc = "divincproposed";
      break;
    case -389:
      desc = "yieldincproposed";
      break;
    case 1:
      desc = "*";
      break;
    case 6:
      desc = "tradeprice1";
      break;
    case 12:
      desc = "tradehigh";
      break;
    case 13:
      desc = "tradelow";
      break;
    case 14:
      desc = "tradetick";
      break;
    case 15:
      desc = "currencyid";
      break;
    case 16:
      desc = "tradedate";
      break;
    case 18:
      desc = "tradetime";
      break;
    case 19:
      desc = "openingprice";
      break;
    case 22:
      desc = "bid";
      break;
    case 25:
      desc = "ask";
      break;
    case 32:
      desc = "cumulativevol";
      break;
    case 33:
      desc = "peratio";
      break;
    case 34:
      desc = "earnings";
      break;
    case 35:
      desc = "yield";
      break;
    case 36:
      desc = "midclose";
      break;
    case 54:
      desc = "*";
      break;
    case 57:
      desc = "openbid";
      break;
    case 58:
      desc = "openask";
      break;
    case 60:
      desc = "closebid";
      break;
    case 61:
      desc = "closeask";
      break;
    case 71:
      desc = "dividend";
      break;
    case 77:
      desc = "numtrades";
      break;
    case 78:
      desc = "isin";
      break;
    case 79:
      desc = "closedate";
      break;
    case 90:
      desc = "yrhigh";
      break;
    case 91:
      desc = "yrlow";
      break;
    case 100:
      desc = "dayturnover";
      break;
    case 114:
      desc = "bidnetchange";
      break;
    case 115:
      desc = "bidtick";
      break;
    case 134:
      desc = "midprice";
      break;
    case 135:
      desc = "midpricech";
      break;
    case 259:
      desc = "rtl";
      break;
    case 285:
      desc = "opentime";
      break;
    case 286:
      desc = "hightime";
      break;
    case 287:
      desc = "lowtime";
      break;
    case 350:
      desc = "yrhighdate";
      break;
    case 351:
      desc = "yrlowdate";
      break;
    case 383:
      desc = "histvol";
      break;
    case 791:
      desc = "dealtvol1";
      break;
    case 990:
      desc = "voltype1";
      break;
    case 1238:
      desc = "marketcap";
      break;
    case 1246:
      desc = "sharesinissue";
      break;
    case 1499:
      desc = "qtybuy";
      break;
    case 1500:
      desc = "qtysell";
      break;
    case 1629:
      desc = "asktick";
      break;
    case 1653:
      desc = "countryofissue";
      break;
    case -275:
    case -276:
    case -277:
    case -278:
    case -279:
    case -280:
    case -281:
    case -282:
    case -283:
    case -24:
    case -39:
    case -40:
    case -80:
    case -82:
    case -91:
    case -93:
    case -83:
    case -85:
    case -86:
    case -87:
    case -88:
    case -353:
    case -354:
    case -355:
    case -356:
    case 7:
    case 8:
    case 9:
    case 10:
    case 792:
    case 793:
    case 794:
    case 795:
    case 991:
    case 992:
    case 993:
    case 994:
    case -35:
    case -36:
    case -37:
    case -38:
    case -72:
    case -73:
    case -74:
    case -75:
    case -76:
    case -77:
    case -78:
    case -79:
    case -39:
    case -40:
    case -83:
    case -84:
    case -85:
    case -86:
    case -87:
    case -88:
    case -91:
    case 93:
    case -304:
    case -305:
    case -306:
    case -307:
    case -309:
    case -310:
    case -311:
    case -312:
    case -313:
    case -314:
    case -315:
    case -316:
    case -317:
    case -318:
    case -319:
    case -320:
    case -321:
    case -322:
    case -323:
    case -324:
    case -325:
    case -326:
    case -327:
    case -328:
    case -329:
    case -330:
    case -331:
    case -332:
    case -333:
    case -334:
    case -335:
    case -336:
    case -370:
    case -371:
    case -372:
      // ignore
      desc = "";
      break;
    default:
      desc = "";
      console.log("unknown field:" + field);
      break;
  }

  return desc;
}

function getError(errorcode) {
  var desc = "";

  var code = parseInt(errorcode);

  switch (code) {
    case 1:
      desc = "Technical Problem.";
      break;
    case 2:
      desc = "Instrument Not Found";
      break;
    case 3:
      desc = "No Permission";
      break;
    case 4:
      desc = "Record Format Failed";
      break;
    case 5:
      desc = "Update Failed";
      break;
    case 6:
      desc = "Invalid Message Received";
      break;
    case 7:
      desc = "Unknown Function Code received";
      break;
    case 8:
      desc = "Tag received when not expected, or Tag not received when expected";
      break;
    case 9:
      desc = "Missing Instrument Code";
      break;
    case 31:
      desc = "Server communication link Status change";
      break;
    case 35:
      desc = "Updates successfully halted";
      break;
    case 36:
      desc = "Updates were not active";
      break;
    case 105:
      desc = "Error occurred in the Server attempting to format output record";
      break;
    case -22:
      desc = "A wildcard request has completed.";
      break;
    case -23:
      desc = "A wildcard request has completed.";
      break;
    default:
      desc = "Unknown error";
      break;
  }

  return desc;
  instrec.midprice = "";
}

