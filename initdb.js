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
  db.set("clientid", 7);
  //db.set("wffixseqnumout", 0); // used by winterflood test server
  db.set("ordercancelreqid", 0);
  db.set("fobomsgid", 0);
  db.set("positionid", 0);

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
  db.sadd("clients", "1:1");
  db.sadd("clients", "1:2");
  db.sadd("clients", "1:3");
  db.sadd("clients", "1:4");
  db.sadd("clients", "1:5");
  db.sadd("clients", "1:6");
  db.sadd("clients", "1:999999");

  // clients - hash for each
  db.hmset("client:1:1", "email", "terry@cw2t.com", "password", "terry", "orgid", 1, "clientid", 1, "marketext", "LD", "name", "Terry Johnston", "address", "128 Richmond Hill", "mobile", "07429 093314");
  db.hmset("client:1:2", "email", "grant@cw2t.com", "password", "grant", "orgid", 1, "clientid", 2, "marketext", "LD", "name", "Grant Oliver", "address", "", "mobile", "");
  db.hmset("client:1:3", "email", "alex@cw2t.com", "password", "alex", "orgid", 1, "clientid", 3, "marketext", "LD", "name", "Alex Fordham", "address", "", "mobile", "");
  db.hmset("client:1:4", "email", "dave@cw2t.com", "password", "dave", "orgid", 1, "clientid", 4, "marketext", "LD", "name", "Dave Shann", "address", "", "mobile", "");
  db.hmset("client:1:5", "email", "cw2t@cw2t.com","password", "cw2t", "orgid", 1, "clientid", 5, "marketext", "LD", "name", "Cw2t test", "address", "", "mobile", "");
  db.hmset("client:1:6", "email", "tina@cw2t.com", "password", "tina", "orgid", 1, "clientid", 6, "marketext", "LD", "name", "Tina Tyers", "address", "", "mobile", "");
  db.hmset("client:1:999999", "email", "tg@tg.com", "password", "tg", "orgid", 1, "clientid", 999999, "marketext", "LD", "name", "Thomas Grant & Co", "address", "", "mobile", "");

  // link between client email & id
  db.set("client:terry@cw2t.com", "1:1");
  db.set("client:grant@cw2t.com", "1:2");
  db.set("client:alex@cw2t.com", "1:3");
  db.set("client:dave@cw2t.com", "1:4");
  db.set("client:cw2t@cw2t.com", "1:5");
  db.set("client:tina@cw2t.com", "1:6");
  db.set("client:tg@tg.com", "1:999999");

  // set of users
  db.sadd("users", "1:1");
  db.sadd("users", "1:2");
  db.sadd("users", "1:3");
  db.sadd("users", "1:4");
  db.sadd("users", "1:5");
  db.sadd("users", "1:6");

  // user hash
  db.hmset("user:1:1", "email", "terry@cw2t.com", "password", "terry", "orgid", 1, "userid", 1, "name", "Terry Johnston");
  db.hmset("user:1:2", "email", "grant@cw2t.com", "password", "grant", "orgid", 1, "userid", 2, "name", "Grant Oliver");
  db.hmset("user:1:3", "email", "tina@cw2t.com", "password", "tina", "orgid", 1, "userid", 3, "name", "Tina Tyers");
  db.hmset("user:1:4", "email", "patrick@cw2t.com", "password", "patrick", "orgid", 1, "userid", 4, "name", "Patrick Waldron");
  db.hmset("user:1:5", "email", "sheila@cw2t.com", "password", "sheila", "orgid", 1, "userid", 5, "name", "Sheila");
  db.hmset("user:1:6", "email", "kevin@cw2t.com", "password", "kevin", "orgid", 1, "userid", 6, "name", "Kevin");

  // link between user email & id
  db.set("user:terry@cw2t.com", "1:1");
  db.set("user:grant@cw2t.com", "1:2");
  db.set("user:alex@cw2t.com", "1:3");
  db.set("user:dave@cw2t.com", "1:4");
  db.set("user:cw2t@cw2t.com", "1:5");
  db.set("user:tina@cw2t.com", "1:6");

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

  // cash - set of cash for each client & string of amount
  db.sadd("1:1:cash", "USD");
  db.set("1:1:cash:USD", 100000);
  db.sadd("1:1:cash", "GBP");
  db.set("1:1:cash:GBP", 10000);
  db.sadd("1:2:cash", "USD");
  db.set("1:2:cash:USD", 100000);
  db.sadd("1:2:cash", "GBP");
  db.set("1:2:cash:GBP", 10000);
  db.sadd("1:3:cash", "USD");
  db.set("1:3:cash:USD", 10000);
  db.sadd("1:3:cash", "GBP");
  db.set("1:3:cash:GBP", 10000);
  db.sadd("1:4:cash", "USD");
  db.set("1:4:cash:USD", 10000);
  db.sadd("1:4:cash", "GBP");
  db.set("1:4:cash:GBP", 10000);
  db.sadd("1:5:cash", "USD");
  db.set("1:5:cash:USD", 10000);
  db.sadd("1:5:cash", "GBP");
  db.set("1:5:cash:GBP", 10000);
  db.sadd("1:6:cash", "USD");
  db.set("1:6:cash:USD", 10000);
  db.sadd("1:6:cash", "GBP");
  db.set("1:6:cash:GBP", 10000);

  // positions - set of positions for each client & hash for each position
  /*db.sadd("1:1:positions", "AMZN:GBP");
  db.hset("1:1:position:AMZN:GBP", "symbol", "AMZN");
  db.hset("1:1:position:AMZN:GBP", "quantity", 300);
  db.hset("1:1:position:AMZN:GBP", "cost", 2000);
  db.hset("1:1:position:AMZN:GBP", "currency", "GBP");
  db.sadd("1:1:positions", "BARC.L:GBP");
  db.hset("1:1:position:BARC.L:GBP", "symbol", "BARC.L");
  db.hset("1:1:position:BARC.L:GBP", "quantity", 200);
  db.hset("1:1:position:BARC.L:GBP", "cost", 600);
  db.hset("1:1:position:BARC.L:GBP", "currency", "GBP");*/

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