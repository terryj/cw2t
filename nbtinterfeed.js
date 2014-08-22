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
var username = "cant_wait_to_trade";
var password = "tecab115";
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

  console.log(data);

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
        console.log("msglen="+msglen);
        console.log("i="+i);
        console.log("bytestoread="+bytestoread);

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
        if (functioncode == "316") {
          parsestate = 5;
        } else {
          parsestate = 4;
        }
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
        updateDb(instrumentcode, parseInt(fid), value);
        rtlseparator = i;
        parsestate = 6;
      } else if (buf[i] == 28) { // <FS>
        if (functioncode == "407") {
          var msg = buf.toString('utf8', rtlseparator+1, i);
          console.log("error msg="+msg);
        } else {
          console.log("complete msg");
          var value = buf.toString('utf8', unitseparator+1, i);
          updateDb(instrumentcode, parseInt(fid), value);
        }

        // we have finished a message
        bufbytesread = i + 1;
        parsestate = 0;
      }
      break;
    }
  }

  console.log("bufbytesread="+bufbytesread);
  console.log("bytestoread="+bytestoread);

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

function updateDb(symbol, fid, value) {
  var field = getFid(fid);
  console.log(field, value);
}

function getFid(fid) {
  var field = "";

  switch (fid) {
    case -1:
      field = "automatic_turnover";
      break;
    case -2:
      field = "auto_vol";
      break;
    case -31:
      field = "trade_type";
      break;
    case -34:
      field = "update_time";
      break;
    case -55:
      field = "mid_time";
      break;
    case -56:
      field = "bid_pct_chg";
      break;
    case -57:
      field = "ask_pct_chg";
      break;
    case -58:
      field = "mid_pct_chg";
      break;
    case -59:
      field = "vwap_alltrd";
      break;
    case -60:
      field = "vwap_autotrd";
      break;
    case -98:
      field = "sold_vol";
      break;
    case -99:
      field = "bought_vol";
      break;
    case -108:
      field = "trade_id";
      break;
    case -369:
      field = "calc_mid_price";
      break;
    case -373:
      field = "yield_inc_adv";
      break;
    case -374:
      field = "calc_mid_net_chg";
      break;
    case -375:
      field = "calc_mid_pct_chg";
      break;
    case -376:
      field = "mid_time";
      break;
    case -377:
      field = "mid_date";
      break;
    case 6:
      field = "trade_price1";
      break;
    case 16:
      field = "trade_date";
      break;
    case 18:
      field = "trade_time";
      break;
    case 32:
      field = "cumulative_vol";
      break;
    case 33:
      field = "pe_ratio";
      break;
    case 35:
      field = "yield";
      break;
    case 36:
      field = "mid_close";
      break;
    case 77:
      field = "num_trades";
      break;
    case 100:
      field = "day_turnover";
      break;
    case 114:
      field = "bid_net_chg";
      break;
    case 134:
      field = "mid_price";
      break;
    case 135:
      field = "mid_price_ch";
      break;
    case 259:
      break;
    case 791:
      field = "dealt_vol1";
      break;
    case 990:
      field = "vol_type1";
      break;
    case 1238:
      field = "market_cap";
      break;
    case 1499:
      field = "qty_buy";
      break;
    case 1500:
      field = "qty_sell";
      break;
    default:
      console.log("unknown field:" + fid);
      break;
  }

  return field;
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
  //var message = "<FS>"+333+"<US>"+"TAG"+"<GS>"+"BARC.L"+"<FS>";
  //var message = "<FS>409<US>mtag<GS>103<FS>";

  // status request i.e. "<FS>409<US>mtag<GS>103<FS>"
  /*var buf = new Buffer(18);
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
  buf[17] = 28;*/

  // stock request i.e. "<FS>"+333+"<US>"+"TAG"+"<GS>"+"BARC.L"+"<FS>"
  var buf = new Buffer(21);
  buf[0] = 0;
  buf[1] = 0;
  buf[2] = 0;
  buf[3] = 17;
  buf[4] = 28;
  buf.write("333", 5);
  buf[8] = 31;
  buf.write("mtag", 9);
  buf[13] = 29;
  buf.write(msg, 14);
  buf[14+msg.length] = 28;

  conn.write(buf);
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