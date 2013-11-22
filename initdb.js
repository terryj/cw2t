/****************
* initdb.js
* Initialise database
* Cantwaittotrade Limited
* Terry Johnston
* September 2012
****************/

var redis = require("redis");

// redis server
db = redis.createClient();
db.on("connect", function (err) {
  console.log("Connected to redis");
  initdb();
});

/*db = redis.createClient(9282, "cod.redistogo.com");
db.auth("4dfeb4b84dbb9ce73f4dd0102cc7707a", function(err) {
  console.log("authenticated");
  initdb();
});*/

db.on("error", function (err) {
  console.error(err);
});

function initdb() {
  // Comment out one or the other...
  // keep fix sequence numbers the same
  /*db.get("fixseqnumin", function(err, fixseqnumin) {
    db.set("fixseqnumin", fixseqnumin);
  });
  db.get("fixseqnumout", function(err, fixseqnumout) {
    db.set("fixseqnumout", fixseqnumout);
  });*/
  // or set them to start at 1
  db.set("fixseqnumin", 0);
  db.set("fixseqnumout", 0); // first out will be 1

  // clear all key values
  db.flushdb();

  // re-set sequential ids
  db.set("quotereqid", 0);
  db.set("quoteid", 0);
  db.set("orderid", 0);
  db.set("tradeid", 0);
  db.set("clientid", 0);
  //db.set("wffixseqnumout", 0); // used by winterflood test server
  db.set("ordercancelreqid", 0);
  db.set("fobomsgid", 0);
  db.set("positionid", 0);
  db.set("cashtransid", 0);

  // organisations
  db.hmset("organisation:1", "orgid", 1, "name", "Thomas Grant & Company");
  db.sadd("organisations", 1);

  // ifas
  db.hmset("ifa:1", "ifaid", 1, "name", "Test IFA");
  db.sadd("ifas", 1);  

  // charges
  db.set("commissionpercent", 1);
  db.set("commissionmin:GBP", 10);
  db.set("commissionmin:USD", 15);
  db.set("ptmlevylimit", 10000);
  db.set("ptmlevy", 1);
  db.set("stampdutylimit", 100);
  db.set("stampdutypercent", 0.5);
  db.set("contractcharge", 2);

  // set of clients
  db.sadd("clients", "999999");

  // clients - hash for each
  db.hmset("client:999999", "email", "tg@tg.com", "password", "tg", "orgid", 1, "clientid", 999999, "marketext", "LD", "name", "Thomas Grant & Co", "address", "", "mobile", "");

  // link between client email & id
  db.set("client:tg@tg.com", "999999");

  // set of users
  db.sadd("users", "1");
  db.sadd("users", "2");
  db.sadd("users", "3");
  db.sadd("users", "4");
  db.sadd("users", "5");
  db.sadd("users", "6");

  // user hash
  db.hmset("user:1", "email", "terry@cw2t.com", "password", "terry", "orgid", 1, "userid", 1, "name", "Terry Johnston");
  db.hmset("user:2", "email", "grant@cw2t.com", "password", "grant", "orgid", 1, "userid", 2, "name", "Grant Oliver");
  db.hmset("user:3", "email", "tina@cw2t.com", "password", "tina", "orgid", 1, "userid", 3, "name", "Tina Tyers");
  db.hmset("user:4", "email", "patrick@cw2t.com", "password", "patrick", "orgid", 1, "userid", 4, "name", "Patrick Waldron");
  db.hmset("user:5", "email", "sheila@cw2t.com", "password", "sheila", "orgid", 1, "userid", 5, "name", "Sheila");
  db.hmset("user:6", "email", "kevin@cw2t.com", "password", "kevin", "orgid", 1, "userid", 6, "name", "Kevin");

  // link between user email & id
  db.set("user:terry@cw2t.com", "1");
  db.set("user:grant@cw2t.com", "2");
  db.set("user:alex@cw2t.com", "3");
  db.set("user:dave@cw2t.com", "4");
  db.set("user:cw2t@cw2t.com", "5");
  db.set("user:tina@cw2t.com", "6");

  // stocks - add international stocks to set of stocks & hash
  db.hset("symbol:AMZN", "currency", "USD");
  db.hset("symbol:AMZN", "description", "Amazon.com, Inc");
  db.hset("symbol:AMZN", "proquotesymbol", "AMZN");
  db.hset("symbol:AMZN", "isin", "US0231351067");
  db.hset("symbol:AMZN", "exchange", "O");
  db.hset("symbol:AMZN", "topic", "TIT.AMZN");
  db.set("proquotesymbol:AMZN", "AMZN");
  db.sadd("instruments", "AMZN");
  db.hset("symbol:BARC.L", "currency", "GBP");
  db.hset("symbol:BARC.L", "description", "Barclays PLC");
  db.hset("symbol:BARC.L", "proquotesymbol", "BARC");
  db.hset("symbol:BARC.L", "isin", "GB0031348658");
  db.hset("symbol:BARC.L", "exchange", "L");
  db.hset("symbol:BARC.L", "topic", "TIT.BARC");
  db.set("proquotesymbol:BARC", "BARC.L");
  db.sadd("instruments", "BARC.L");

  // instrument types
  db.sadd("instrumenttypes", "DE");
  db.sadd("instrumenttypes", "IE");
  db.sadd("instrumenttypes", "CFD");
  db.sadd("instrumenttypes", "SPB");

  db.set("instrumenttype:DE", "UK Equities");
  db.set("instrumenttype:IE", "International Equities");
  db.set("instrumenttype:CFD", "CFD");
  db.set("instrumenttype:SPB", "Spreadbet");

  // cash transaction types
  db.sadd("cashtranstypes", "CI");
  db.sadd("cashtranstypes", "CO");
  db.sadd("cashtranstypes", "JI");
  db.sadd("cashtranstypes", "JO");

  db.set("cashtranstype:CI", "Cash In");
  db.set("cashtranstype:CO", "Cash Out");
  db.set("cashtranstype:JI", "Journal In");
  db.set("cashtranstype:JO", "Journal Out");

  // currencies
  db.sadd("currencies", "GBP");
  db.sadd("currencies", "EUR");
  db.sadd("currencies", "USD");

  // client types
  db.sadd("clienttypes", "1");
  db.sadd("clienttypes", "2");

  db.set("clienttypes:1", "Retail");
  db.set("clienttypes:2", "Hedge");

  // order types - set of order types & string for each one
  //db.sadd("ordertypes", "1");
  db.sadd("ordertypes", "2");
  //db.sadd("ordertypes", "7");
  //db.set("ordertype:1", "At Best");
  db.set("ordertype:2", "Limit");
  //db.set("ordertype:7", "At Limit");
  db.sadd("ordertypes", "D");
  db.set("ordertype:D", "Quoted");

  console.log("done");
}