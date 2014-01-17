/****************
* backoffice.js
* Back-office functions
* Cantwaittotrade Limited
* Terry Johnston
* January 2014
****************/

// node libraries

// external libraries
var redis = require("redis");
var nodemailer = require("nodemailer");

// globals
var bointerval;
var monthtext = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sept','Oct','Nov','Dec'];

// scripts
var scriptfobotrade;
var scriptfobook;
var scriptfoboerror;

// redis
var redishost = "127.0.0.1";
var redisport = 6379;

// set-up a redis client
db = redis.createClient(redisport, redishost);
db.on("connect", function(err) {
  if (err) {
    console.log(err);
    return;
  }

  console.log("Connected to Redis at " + redishost + " port " + redisport);

  registerScripts();
  startBOInterval();
});

db.on("error", function(err) {
  console.log(err);
});

var smtpTransport = nodemailer.createTransport("SMTP",{
   service: "Gmail",
   auth: {
       user: "terrymjohnston@gmail.com",
       pass: "***"
   }
});

// run every time interval to download transactions to the back-ofice
function startBOInterval() {
  bointerval = setInterval(emailTrades, 5000); // todo: make 10
  console.log("Interval timer started");
}

function emailTrades() {
  console.log("emailing trades");
  
  // get all the trades waiting to be emailed
  db.smembers("trades", function(err, tradeids) {
    if (err) {
      console.log("Error in emailTrades: " + err);
      return;
    }

    // send each one
    tradeids.forEach(function(tradeid, i) {
      db.hgetall("trade:" + tradeid, function(err, trade) {
        if (err) {
          console.log(err);
          return;
        }

        if (trade == null) {
          console.log("Trade #" + tradeid + " not found");
          return;
        }

        if (!('emailsent' in trade)) {
          // get symbol dscription
          db.hget("symbol:" + trade.symbol, "description", function(err, description) {
            if (err) {
              console.log(err);
              return;
            }

            trade.description = description;

            sendTrade(trade);
          });
        }
      });
    });
  });
}

    //smtpTransport.close(); // shut down the connection pool, no more messages

function sendTrade(trade) {
  db.hget("client:" + trade.clientid, "email", function(err, email) {
    // handle GBX from proquote
    if (trade.currency == "GBX") {
      trade.currency = "GBP";
    }

    var subject = getSubject(trade);
    var msg = htmlMsg(trade);

    // setup e-mail data
    var mailoptions = {
      from: "Terry Johnston <terrymjohnston@gmail.com>", // sender address
      to: email, // receiver address
      subject: subject, // subject line
      generateTextFromHTML: true,
      //text: "test", // plaintext body
      html: msg // html body
    }

    // send mail with defined transport object
    smtpTransport.sendMail(mailoptions, function(err, response){
      if (err) {
        console.log(err);
        return;
      }
      console.log(response);

      // mark trade as 'sent'
      db.hset("trade:" + trade.tradeid, "emailsent", 1);
    });
  });
}

function getSubject(trade) {
  var msg;

  msg = trade.symbol;

  if (trade.side == "1") {
    msg += " Buy";
  } else {
    msg += " Sell";
  }

  msg += " Trade";

  return msg;
}

function htmlMsg(trade) {
  var msg;

  msg = "<h2>Thomas Grant & Company - Contract Note</h2>";

  msg += "<p>This is to confirm that at " + formatUTCDateTime(trade.timestamp) + " you ";

  msg += "<p>";

  if (trade.side == "1") {
    msg += "Bought";
  } else {
    msg += "Sold";
  }

  msg += " " + trade.quantity + " " + trade.description + " @ " + trade.price + " for consideration of " + trade.settlcurramt + trade.settlcurrency;

  msg += "<p>For settlement T+" + trade.nosettdays + " on " + formatUTCDate(trade.futsettdate) + "</p>";

  msg += "<p>Trade id:" + trade.tradeid + "</p>";

  msg += "<p><b>Costs</b>";

  msg += "<br>Commission: " + trade.commission;
  msg += "<br>PTM Levy: " + trade.ptmlevy;
  msg += "<br>Stamp Duty: " + trade.stampduty;
  msg += "<br>Contract Charge: " + trade.contractcharge;

  if ('finance' in trade && trade.finance > 0) {
    msg += "<br>Finance Charge: " + trade.finance;
  }

  msg += "<p><p>Thomas Grant & Company Ltd."
  msg += "40A Friar Lane. Leicester. Leicestershire. LE1 5RA";
  msg += "Leicester Dealers: 0116 2255500";
  msg += "Derby Office: 0133 2370299";
  msg += "Administration: 0116 2255509";
  msg += "FAX: 0116 2258800";
  msg += "Email: info@thomasgrant.co.uk";
  msg += "FCA No: 163296";
  
  return msg;
}

function formatUTCDate(utcdate) {
  var month = parseInt(utcdate.substr(4, 2));
  return (utcdate.substr(6, 2) + " " + monthtext[month-1] + " " + utcdate.substr(0, 4));
}

function formatUTCDateTime(utcdatetime) {
  return (formatUTCDate(utcdatetime) + utcdatetime.substr(8));
}

function registerScripts() {
  scriptfobotrade = '\
  local fobomsgid = redis.call("incr", "fobomsgid") \
  redis.call("hmset", "fobo:" .. fobomsgid, "msgtype", 2, "tradeid", KEYS[1], "status", 0) \
  local isin = redis.call("hget", "symbol:" .. KEYS[2], "isin") \
  return {fobomsgid, isin} \
  ';

  // update message status & remove order/trade from their set
  scriptfobook = '\
  redis.call("hmset", "fobo:" .. KEYS[1], "status", 1) \
  local msgtype = redis.call("hget", "fobo:" .. KEYS[1], "msgtype") \
  if msgtype == "1" then \
    local orderid = redis.call("hget", "fobo:" .. KEYS[1], "orderid") \
    redis.call("srem", "orders", orderid) \
  else \
    local tradeid = redis.call("hget", "fobo:" .. KEYS[1], "tradeid") \
    redis.call("srem", "trades", tradeid) \
  end \
  return \
  ';

  // update message error, add message to errors set
  // keep trying, so don't remove
  scriptfoboerror = '\
  redis.call("hmset", "fobo:" .. KEYS[1], "error", KEYS[2]) \
  redis.call("sadd", "errors", KEYS[1]) \
  ';
}
