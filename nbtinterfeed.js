/****************
* nbtinterfeed.js
* Netbuilder NBTrader Market data link
* Cantwaittotrade Limited
* Terry Johnston
* August 2014
****************/

// node libraries
var net = require('net');

// external libraries
var redis = require('redis');

// cw2t libraries
var common = require('./common.js');

// publish & subscribe channels
var clientserverchannel = 1;
var userserverchannel = 2;
var tradeserverchannel = 3;
var ifaserverchannel = 4;
var webserverchannel = 5;
var tradechannel = 6;
var priceserverchannel = 7;
var pricehistorychannel = 8;
var pricechannel = 9;

// redis
var redishost;
var redisport;
var redisauth;
var redispassword;
var redislocal = true; // local or external server

// globals
var markettype; // comes from database, 0=normal market, 1=out of hours
var host = "85.133.96.85";
var messageport = 50900;
var buf = new Buffer(1024 * 2048); // incoming data buffer
var bufbytesread = 0;
var bytestoread = 0;

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
  common.registerCommonScripts();
  registerScripts();
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
    console.log(message);
    requestData(message);
  });

  dbsub.subscribe(priceserverchannel);
}

//
// try to connect
//
console.log("Trying to connect to host: " + host + ", port:" + messageport);

var conn = net.connect(messageport, host, function() {
  console.log('Connected to: ' + host);

  subscriptions();
});

//
// incoming data
// we need to allow for receiving part-messages
// and more than one message in a single receive event
//
conn.on('data', function(data) {
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

  console.log(data);

  instrec.bid = "";
  instrec.ask = "";

  var datalen = data.length;
  console.log("---data.length="+data.length+"---");
  console.log("bytestoread="+bytestoread);

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
          console.log("incomplete message");
          return;
        }

        fileseparator = i;
        parsestate = 1;
      }
      break;
    case 1:
      if (buf[i] == 31) { // <US>
        functioncode = buf.toString('utf8', fileseparator+1, i);
        console.log("functioncode="+functioncode);
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
        console.log("instrumentcode="+instrumentcode);
        unitseparator = i;
        if (functioncode == "340") {
          parsestate = 4;
        } else {
          parsestate = 5;          
        }
      } else if (buf[i] == 30) { // <RS>
        var errorcode = buf.toString('utf8', groupseparator+1, i);
        var err = getError(errorcode);
        console.log("error: " + err);
        rtlseparator = i;
        parsestate = 7;
      }
      break;
    case 4:
      if (buf[i] == 31) { // <US>
        var recordtype = buf.toString('utf8', unitseparator+1, i);
        console.log("recordtype="+recordtype);
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
          console.log("error msg="+msg);
        } else {
          var value = buf.toString('utf8', unitseparator+1, i);

          // get the last field:value pair
          updateRec(fid, value, instrec);

          // update the database
          updateDb(functioncode, instrumentcode, instrec);
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
    console.log("read the lot");
    bufbytesread = 0;
    bytestoread = 0;
  }
});

conn.on('end', function() {
  console.log('disconnected from: ' + host);
});

conn.on('error', function(error) {
  console.log(error);
});

function subscribe(instcode) {
  console.log("subscribe");
  var instcodelen = instcode.length;
  var buf = new Buffer(25+instcodelen);

  buf[0] = 0;
  buf[1] = 0;
  buf[2] = 0;
  buf[3] = 21+instcodelen;
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
  buf[20+instcodelen] = 30;
  //buf.write("100", 21+instcodelen); // turnover
  //buf.write("114", 21+instcodelen); // bid net change
  buf.write("115", 21+instcodelen); // bid tick
  buf[24+instcodelen] = 28;
  
  conn.write(buf);
}

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
  console.log("updateDb");
  console.log(instrec);

  // create a unix timestamp
  var now = new Date();
  var timestamp = +now;

  // store a complete record for a symbol
  if (functioncode == "340") {
    db.hmset("symbol:" + instrumentcode, instrec);
  }

  // update price & history
  db.eval(common.scriptpriceupdate, 4, instrumentcode, timestamp, instrec.bid, instrec.ask, function(err, ret) {
    if (err) throw err;
    //console.log("pricehist updated: " + instrumentcode + ",ts:" + timestamp + ",bid:" + instrec.bid + ",ask:" + instrec.ask);
    console.log(ret);
  });
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
    // real-time partial record i.e. "<FS>332<US>mtag<GS>BARC.L<RS>FID<RS>FID<FS>"

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
    // real-time full record i.e. "<FS>332<US>mtag<GS>BARC.L<FS>"

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
    // snap full record i.e. "<FS>333<US>mtag<GS>BARC.L<FS>"

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

    console.log("halting:" + instcode);

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

function initDb() {
  getMarkettype();
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
      desc = "asknetchg"
      break;
    case -33:
      desc = "maxorderdays";
      break;
    case -34:
      desc = "updatetime";
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
      desc = "bidpctchg";
      break;
    case -57:
      desc = "askpctchg";
      break;
    case -58:
      desc = "midpctchg";
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
    case -80:
      desc = "firstaucprice";
      break;
    case -81:
      desc = "firstauctime";
      break;
    case -82:
      desc = "firstaucvol";
      break;
    case -97:
      desc = "timezone";
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
      desc = "calcmidnetchg";
      break;
    case -375:
      desc = "calcmidpctchg";
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
      desc = "currency";
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
      desc = "bidnetchg";
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
}

function registerScripts() {
}
