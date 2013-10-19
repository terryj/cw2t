/****************
* synthetics.js
* Set-up synthetic instruments in database
* Cantwaittotrade Limited
* Terry Johnston
* October 2013
****************/

var redis = require("redis");

// redis server
db = redis.createClient();
db.on("connect", function (err) {
  console.log("Connected to redis");
  cfdspb("UKX");
});

db.on("error", function (err) {
  console.error(err);
});

dbpub = redis.createClient();
dbpub.on("error", function (err) {
  console.error(err);
});

function cfdspb(index) {
  var cfd;
  var spb;

  console.log("Creating instruments for index:" + index);

  db.smembers("index:" + index, function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    replies.forEach(function(symbol, j) {
      db.hgetall("symbol:" + symbol, function(err, inst) {
        if (err) {
          console.log(err);
          return;
        }

        if (inst == null) {
          console.log("Symbol " + symbol + " not found");
          return;
        }

        cfd = symbol + ".CFD";
        spb = symbol + ".SPB";

        // create cfd
        db.hmset("symbol:" + cfd, "currency", inst.currency, "description", inst.description + " CFD", "proquotesymbol", inst.proquotesymbol, "isin", inst.isin, "exchange", inst.exchange, "topic", inst.topic, "market", inst.market, "sedol", inst.sedol, "longname", inst.longname, "instrumenttype", "CFD", "category", inst.category, "sector", inst.sector);
        db.sadd("instruments", cfd);

        // spreadbet
        db.hmset("symbol:" + spb, "currency", inst.currency, "description", inst.description + " Spreadbet", "proquotesymbol", inst.proquotesymbol, "isin", inst.isin, "exchange", inst.exchange, "topic", inst.topic, "market", inst.market, "sedol", inst.sedol, "longname", inst.longname, "instrumenttype", "SPB", "category", inst.category, "sector", inst.sector);
        db.sadd("instruments", spb);

        console.log("cfd & spreadbet added for symbol:" + symbol);
      });
    });
  });
}
