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

// globals
var markettype; // comes from database, 0=normal market, 1=out of hours
var clientserverchannel = 1;
var userserverchannel = 2;
var tradeserverchannel = 3;
var ifaserverchannel = 4;
var webserverchannel = 5;
var tradechannel = 6;
var host = "85.133.96.85";
var messageport = 50900;
var buf = new Buffer(1024 * 256); // incoming data buffer
var bufbytesread = 0;
var bytestoread = 0;

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
    sendData(message);
  });

  dbsub.subscribe("interfeed");
}

//
// try to connect
//
console.log("Trying to connect to host: " + host + ", port:" + messageport);

var conn = net.connect(messageport, host, function() {
  console.log('Connected to: ' + host);
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

  // copy this data to our global buffer, offset for any messages waiting to be read
  data.copy(buf, bytestoread);
  bytestoread += data.length;

  // go through the buffer one byte at a time, allowing for messages already read
  for (var i = bufbytesread; i < bytestoread; i++) {
    switch (parsestate) {
    case 0:
      if (buf[i] == 28) { // <FS>
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
        console.log("tag="+tag);
        groupseparator = i;
        parsestate = 3;
      }
      break;
    case 3:
      if (buf[i] == 31) { // <US>
        instrumentcode = buf.toString('utf8', groupseparator+1, i);
        console.log("instrumentcode="+instrumentcode);
        unitseparator = i;
        parsestate = 4;
      } else if (buf[i] == 30) { // <RS>
        var errorcode = buf.toString('utf8', groupseparator+1, i);
        console.log("errorcode="+errorcode);
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
        console.log("rtl="+rtl);
        rtlseparator = i;
        parsestate = 6;
      } else if (buf[i] == 28) { // <FS>
        console.log("complete msg");
        var value = buf.toString('utf8', unitseparator+1, i);
        console.log("value="+value);

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
        updateRec(parseInt(fid), value, instrec);
        rtlseparator = i;
        parsestate = 6;
      } else if (buf[i] == 28) { // <FS>
        if (functioncode == "407") {
          var msg = buf.toString('utf8', rtlseparator+1, i);
          console.log("error msg="+msg);
        } else {
          var value = buf.toString('utf8', unitseparator+1, i);

          // get the last field:value pair
          updateRec(parseInt(fid), value, instrec);

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

function updateRec(fid, value, obj) {
  var field = getFid(fid);
  if (field != "") {
    //console.log(field, value);
    obj[field] = value;
  }
}

function updateDb(functioncode, instrumentcode, instrec) {
  console.log(instrec);
  var timestamp = new Date().getTime();

  // publish to server
  //db.publish(ret[1], "quoteack:" + quoteack.quotereqid);

  // store a complete record for a symbol
  if (functioncode == "340") {
    db.hmset("symbol:" + instrumentcode, instrec);
  }

  // update price history
  db.eval(scriptpricehist, 4, instrumentcode, timestamp, instrec.bid, instrec.ask, function(err, ret) {
    if (err) throw err;
    console.log("pricehist updated");
  });
}

function loginReceived(message) {
  if (message.substr(0, 2) == "OK") {
    console.log("logged on ok");
  } else {
    console.log("login failed");
  }
}

function priceReceived(message) {
  // split string into array
  var arr = message.split("|");

  // only process a complete message
  if (arr.length < 23) {
    console.log("incomplete arr");
    return false;
  }

  var jsonmsg = "\"prices\":[";
  jsonmsg += "{\"level\":" + "1" +  ",\"" + "bid" + "\":\"" + arr[2] + "\"}";
  jsonmsg += ",{\"level\":" + "1" +  ",\"" + "offer" + "\":\"" + arr[3] + "\"}";
  jsonmsg += "]";
  jsonmsg += ",\"open\":" + arr[5];
  jsonmsg += ",\"close\":" + arr[6];
  jsonmsg += ",\"high\":" + arr[7];
  jsonmsg += ",\"low\":" + arr[8];
  jsonmsg += ",\"change\":" + arr[9];
  jsonmsg += ",\"changepercent\":" + arr[10];
  jsonmsg += ",\"volume\":" + arr[11];
  jsonmsg += ",\"52weekhigh\":" + arr[18];
  jsonmsg += ",\"52weeklow\":" + arr[19];
  jsonmsg += ",\"currency\":" + arr[21];

  // publish the price
  db.publish("ticker:" + arr[0], jsonmsg);

  // & store it
  db.hmset("ticker:" + arr[0], "bid", arr[2], "offer", arr[3], "open", arr[5], "close", arr[6], "high", arr[7], "low", arr[8], "change", arr[9], "changepercent", arr[10], "volume", arr[11], "52weekhigh", arr[18], "52weeklow", arr[19], "currency", arr[21]);

  return true;
}

function sendData(msg) {
  if (msg.substr(0, 2) == "rp") {
    // real-time partial record i.e. "<FS>332<US>mtag<GS>BARC.L<RS>FID<RS>FID<FS>"

    var instcode = msg.substr(3);
    var instcodelen = instcode.length;
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
    buf.write("22", 15+instcodelen);
    buf[17+instcodelen] = 30;
    buf.write("25", 18+instcodelen);
    buf[20+instcodelen] = 28;
  
    conn.write(buf);
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
  }
}

function initDb() {
  registerScripts();
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

function getFid(fid) {
  var field = "";

  switch (fid) {
    case -1:
      field = "automaticturnover";
      break;
    case -2:
      field = "autovol";
      break;
    case -6:
      field = "industrysector";
      break;
    case -7:
      field = "publicationlimit";
      break;
    case -8:
      field = "issuercode";
      break;
    case -9:
      field = "issuername";
      break;
    case -10:
      field = "longname";
      break;
    case -11:
      field = "shortname";
      break;
    case -12:
      field = "quotesize";
      break;
    case -13:
      field = "orderlotsize";
      break;
    case -14:
      field = "minpord";
      break;
    case -15:
      field = "minaord";
      break;
    case -16:
      field = "normmarketsize";
      break;
    case -18:
      field = "mnemonic";
      break;
    case -19:
      field = "ticksize";
      break;
    case -20:
      field = "sector";
      break;
    case -21:
      field = "segment";
      break;
    case -22:
      field = "stocksts";
      break;
    case -23:
      field = "entrytype";
      break;
    case -25:
      field = "maxpord";
      break;
    case -26:
      field = "maxaord";
      break;
    case -27:
      field = "minquote";
      break;
    case -28:
      field = "maxquote";
      break;
    case -29:
      field = "periodname";
      break;
    case -30:
      field = "insttype";
      break;
    case -31:
      field = "tradetype";
      break;
    case -32:
      field = "asknetchg"
      break;
    case -33:
      field = "maxorderdays";
      break;
    case -34:
      field = "updatetime";
      break;
    case -51:
      field = "midhigh";
      break;
    case -52:
      field = "midlow";
      break;
    case -53:
      field = "midlowtime";
      break;
    case -54:
      field = "midhightime";
      break;
    case -55:
      field = "midtime";
      break;
    case -56:
      field = "bidpctchg";
      break;
    case -57:
      field = "askpctchg";
      break;
    case -58:
      field = "midpctchg";
      break;
    case -59:
      field = "vwapalltrd";
      break;
    case -60:
      field = "vwapautotrd";
      break;
    case -61:
      field = "midtick";
      break;
    case -80:
      field = "firstaucprice";
      break;
    case -81:
      field = "firstauctime";
      break;
    case -82:
      field = "firstaucvol";
      break;
    case -97:
      field = "timezone";
      break;
    case -98:
      field = "soldvol";
      break;
    case -99:
      field = "boughtvol";
      break;
    case -108:
      field = "tradeid";
      break;
    case -118:
      field = "periodendtime";
      break;
    case -119:
      field = "yestclosebid";
      break;
    case -120:
      field = "yestcloseask";
      break;
    case -121:
      field = "yestclosemid";
      break;
    case -129:
      field = "couponrate";
      break;
    case -133:
      field = "xdivdate";
      break;
    case -135:
      field = "accrueddays";
      break;
    case -338:
      field = "icbsectornum";
      break;
    case -339:
      field = "closedatetime";
      break;
    case -350:
      field = "closetime";
      break;
    case -357:
      field = "highestbid";
      break;
    case -358:
      field = "highbidtime";
      break;
    case -359:
      field = "highbiddate";
      break;
    case -360:
      field = "lowestbid";
      break;
    case -361:
      field = "lowbidtime";
      break;
    case -362:
      field = "lowbiddate";
      break;
    case -363:
      field = "highestoffer";
      break;
    case -364:
      field = "highoffertime";
      break;
    case -365:
      field = "highofferdate";
      break;
    case -366:
      field = "lowestoffer";
      break;
    case -367:
      field = "lowoffertime";
      break;
    case -368:
      field = "lowofferdate";
      break;
    case -369:
      field = "calcmidprice";
      break;
    case -373:
      field = "yieldincadv";
      break;
    case -374:
      field = "calcmidnetchg";
      break;
    case -375:
      field = "calcmidpctchg";
      break;
    case -376:
      field = "midtime";
      break;
    case -377:
      field = "middate";
      break;
    case -378:
      field = "dailyinterest";
      break;
    case -384:
      field = "xdivdate";
      break;
    case 6:
      field = "tradeprice1";
      break;
    case 12:
      field = "tradehigh";
      break;
    case 13:
      field = "tradelow";
      break;
    case 14:
      field = "tradetick";
      break;
    case 15:
      field = "currency";
      break;
    case 16:
      field = "tradedate";
      break;
    case 18:
      field = "tradetime";
      break;
    case 19:
      field = "openingprice";
      break;
    case 22:
      field = "bid";
      break;
    case 25:
      field = "ask";
      break;
    case 32:
      field = "cumulativevol";
      break;
    case 33:
      field = "peratio";
      break;
    case 34:
      field = "earnings";
      break;
    case 35:
      field = "yield";
      break;
    case 36:
      field = "midclose";
      break;
    case 57:
      field = "openbid";
      break;
    case 58:
      field = "openask";
      break;
    case 60:
      field = "closebid";
      break;
    case 61:
      field = "closeask";
      break;
    case 71:
      field = "dividend";
      break;
    case 77:
      field = "numtrades";
      break;
    case 78:
      field = "isin";
      break;
    case 79:
      field = "closedate";
      break;
    case 90:
      field = "yrhigh";
      break;
    case 91:
      field = "yrlow";
      break;
    case 100:
      field = "dayturnover";
      break;
    case 114:
      field = "bidnetchg";
      break;
    case 115:
      field = "bidtick";
      break;
    case 134:
      field = "midprice";
      break;
    case 135:
      field = "midpricech";
      break;
    case 259:
      field = "rtl";
      break;
    case 285:
      field = "opentime";
      break;
    case 286:
      field = "hightime";
      break;
    case 287:
      field = "lowtime";
      break;
    case 350:
      field = "yrhighdate";
      break;
    case 351:
      field = "yrlowdate";
      break;
    case 383:
      field = "histvol";
      break;
    case 791:
      field = "dealtvol1";
      break;
    case 990:
      field = "voltype1";
      break;
    case 1238:
      field = "marketcap";
      break;
    case 1246:
      field = "sharesinissue";
      break;
    case 1499:
      field = "qtybuy";
      break;
    case 1500:
      field = "qtysell";
      break;
    case 1629:
      field = "asktick";
      break;
    case 1653:
      field = "countryofissue";
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
      field = "";
      // ignore
      break;
    default:
      field = "";
      console.log("unknown field:" + fid);
      break;
  }

  return field;
}

function registerScripts() {
  // add tick to price history
  // instrumentcode, timestamp, bid, offer
  scriptpricehist = '\
  --[[ get an id for this tick for this symbol ]] \
  local pricehistid = redis.call("incr", "pricehistid:" .. KEYS[1]) \
  --[[ add id to sorted set, indexed on timestamp ]] \
  redis.call("zadd", "pricehist:" .. KEYS[1], KEYS[2], pricehistid) \
  redis.call("hmset", "pricehist:" .. KEYS[1] .. ":" .. pricehistid, "timestamp", KEYS[2], "symbol", KEYS[1], "bid", KEYS[3], "offer", KEYS[4]) \
  ';
}
