/****************
* symbols.js
* Add international stocks
* Cantwaittotrade Limited
* Terry Johnston
* April 2014
****************/

var redis = require("redis");

// redis server
db = redis.createClient();
db.on("connect", function (err) {
  console.log("Connected to redis");
  addSymbols();
});

db.on("error", function (err) {
  console.error(err);
});

function addSymbols() {
  db.hset("symbol:AMZN", "currency", "USD");
  db.hset("symbol:AMZN", "description", "Amazon.com Inc");
  db.hset("symbol:AMZN", "proquotesymbol", "AMZN");
  db.hset("symbol:AMZN", "isin", "US0231351067");
  db.hset("symbol:AMZN", "topic", "TIT.AMZN.O");
  db.hset("symbol:AMZN", "symbol", "AMZN");
  db.hset("symbol:AMZN", "instrumenttype", "IE");
  db.hset("symbol:AMZN", "market", "O");
  db.set("proquotesymbol:AMZN", "AMZN");
  db.sadd("instruments", "AMZN");

  console.log("instruments added ok");
}

