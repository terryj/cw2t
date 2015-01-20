/****************
* synthetics.js
* Set-up synthetic instruments in redis
* Cantwaittotrade Limited
* Terry Johnston
* October 2013
****************/

var redis = require("redis");

// redis server
db = redis.createClient();
db.on("connect", function (err) {
  console.log("Connected to redis");
  //cfdspb("UKX", 10);
  createCfd();
});

db.on("error", function (err) {
  console.error(err);
});

function cfdspb(index, marginpercent) {
  var cfd;
  var spb;
  var ccfd;

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
        ccfd = symbol + ".CCFD";

        // create cfd
        db.hmset("symbol:" + cfd, "currency", inst.currency, "description", inst.description + " CFD", "proquotesymbol", inst.proquotesymbol, "isin", inst.isin, "topic", inst.topic, "market", inst.market, "sedol", inst.sedol, "longname", inst.longname, "instrumenttype", "CFD", "category", inst.category, "sector", inst.sector, "marginpercent", marginpercent, "hedgesymbol", symbol, "hedge", 1, "symbol", cfd, "ptmexempt", 0);
        db.sadd("instruments", cfd);
        db.sadd("topic:" + inst.topic + ":symbols", cfd);
        db.sadd("topic:" + inst.topic + "D" + ":symbols", cfd);

        // spreadbet
        db.hmset("symbol:" + spb, "currency", inst.currency, "description", inst.description + " Spreadbet", "proquotesymbol", inst.proquotesymbol, "isin", inst.isin, "topic", inst.topic, "market", inst.market, "sedol", inst.sedol, "longname", inst.longname, "instrumenttype", "SPB", "category", inst.category, "sector", inst.sector, "marginpercent", marginpercent, "hedgesymbol", symbol, "hedge", 1, "symbol", spb, "ptmexempt", 0);
        db.sadd("instruments", spb);
        db.sadd("topic:" + inst.topic + ":symbols", spb);
        db.sadd("topic:" + inst.topic + "D" + ":symbols", spb);

        // create ccfd
        db.hmset("symbol:" + ccfd, "currency", inst.currency, "description", inst.description + " Convertible CFD", "proquotesymbol", inst.proquotesymbol, "isin", inst.isin, "topic", inst.topic, "market", inst.market, "sedol", inst.sedol, "longname", inst.longname, "instrumenttype", "CCFD", "category", inst.category, "sector", inst.sector, "marginpercent", marginpercent, "hedgesymbol", symbol, "hedge", 1, "symbol", ccfd, "ptmexempt", 0);
        db.sadd("instruments", ccfd);
        db.sadd("topic:" + inst.topic + ":symbols", ccfd);
        db.sadd("topic:" + inst.topic + "D" + ":symbols", ccfd);

        console.log("cfd, ccfd & spreadbet added for symbol:" + symbol);
      });
    });
  });
}

function createCfd() {
  var cfd;
  var spb;
  var ccfd;

  db.smembers("instruments", function(err, replies) {
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

        // only interested in underlying
        if (inst.instrumenttype == "DE" || inst.instrumenttype == "IE") {
          cfd = symbol + ".CFD";
          //spb = symbol + ".SPB";
          //ccfd = symbol + ".CCFD";

          // create cfd & add to list
          db.hmset("symbol:" + cfd, "currency", inst.currency, "shortname", inst.shortname + " CFD", "nbtsymbol", inst.nbtsymbol, "isin", inst.isin, "exchange", inst.exchange, "instrumenttype", "CFD", "marginpercent", 10, "hedgesymbol", symbol, "hedge", 1, "symbol", cfd, "ptmexempt", 1, "timezone", inst.timezone, "mnemonic", inst.mnemonic);
          db.sadd("instruments", cfd);

          // this is used by the price feed to update any symbols for a nbtsymbol
          db.sadd("nbtsymbol:" + inst.nbtsymbol + ":instruments", cfd);

          // spreadbet

          // create ccfd

          console.log("cfd added for symbol:" + symbol);
        }
      });
    });
  });
}
