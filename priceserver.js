/****************
* priceserver.js
* Digital Look server
* Cantwaittotrade Limited
* Terry Johnston
* July 2014
****************/

// node libraries
var net = require('net');

// external libraries
var redis = require('redis');

// cw2t libraries
var common = require('./commonfo.js');

// globals
var markettype; // comes from database, 0=normal market, 1=out of hours
var host = "pushfeed.digitallook.com";
var messageport = 49002;
var username = "cant_wait_to_trade";
var password = "tecab115";
var datain = "";

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

  dbsub.subscribe("digitallook");
}

console.log("Trying to connect to host:" + host + ", port:" + messageport);

var messageconn = net.connect(messageport, host, function() {
  console.log('message connection connected, trying to login...');
  messageconn.write("login{" + username + "," + password + "}\n");
});
messageconn.on('data', function(data) {
  var completemessage = true;

  // add the data to our global buffer as we may receive part messages
  datain += data.toString();
  console.log(datain);

  // split the buffer by endofline character
  var arr = datain.split("\n");
  console.log(arr);

  // process each line
  for (var i = 0; i < arr.length; i++) {
    if (arr[i].substr(0, 5) == "login") {
      loginReceived(arr[i].substr(6));
    } else if (arr[i].substr(0, 10) == "Registered") {
      console.log(arr[i].substr(11) + " registered");
    } else if (arr[i].substr(0, 13) == "Register FAIL") {
      console.log(arr[i].substr(14) + " register failed");
    } else if (arr[i].substr(0, 13) == "<<HEARTBEAT>>") {
    } else if (arr[i]) {
      // we have something but may be incomplete, the function returns true if complete
      completemessage = priceReceived(arr[i]);
    }
  }

  if (completemessage) {
    // ok, clear the buffer
    datain = "";
  } else {
    console.log("saving incomplete message");

    // set the buffer to the contents of the incomplete message
    datain = arr[arr.length - 1];
    console.log(datain);
  }
});
messageconn.on('end', function() {
  console.log('client disconnected');
});
messageconn.on('error', function(error) {
  console.log(error);
});

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

function requestData(conn) {
  //conn.write(username);
  //conn.write("setTickers{LSE:VOD}");
}

function sendData(msg) {
  messageconn.write(username + "\n");
  messageconn.write("setTickers{" + msg + "}\n");
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