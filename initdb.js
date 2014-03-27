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
  db.get("fixseqnumin", function(err, fixseqnumin) {
    db.set("fixseqnumin", fixseqnumin);
  });
  db.get("fixseqnumout", function(err, fixseqnumout) {
    db.set("fixseqnumout", fixseqnumout);
  });
  // or set them to start at 1
  //db.set("fixseqnumin", 0);
  //db.set("fixseqnumout", 0); // first out will be 1

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
  db.set("cashtransid", 0);
  db.set("ifaid", 0);
  db.set("positionid", 0);
  db.set("chatid", 0);

  // brokers
  db.hmset("broker:1", "brokerid", 1, "name", "Thomas Grant & Company");
  db.sadd("brokers", 1);

  // default hedge client
  db.sadd("clients", "999999");

  // clients - hash for each
  db.hmset("client:999999", "email", "999999@thomasgrant.co.uk", "password", "999999", "brokerid", 1, "clientid", 999999, "marketext", "D", "name", "Thomas Grant Hedgebook", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999999, "commpercent", 0);

  // link between client email & id
  db.set("client:999999@thomasgrant.co.uk", "999999");

  // instruments types hedge client can trade
  db.sadd("999999:instrumenttypes", "CFD");
  db.sadd("999999:instrumenttypes", "SPB");
  db.sadd("999999:instrumenttypes", "DE");
  db.sadd("999999:instrumenttypes", "IE");
  db.sadd("999999:instrumenttypes", "CCFD");

  // GBP cash record for hedge book, required for summary
  db.sadd("999999:cash", "GBP");
  db.set("999999:cash:GBP", 0);

  // set of users
  db.sadd("users", "1");
  db.sadd("users", "2");
  db.sadd("users", "3");
  db.sadd("users", "4");
  db.sadd("users", "5");
  db.sadd("users", "6");
  db.sadd("users", "7");
  db.sadd("users", "8");

  // user hash
  db.hmset("user:1", "email", "terry@cw2t.com", "password", "terry", "brokerid", 1, "userid", 1, "name", "Terry Johnston", "marketext", "D");
  db.hmset("user:2", "email", "grant@thomasgrant.co.uk", "password", "grant", "brokerid", 1, "userid", 2, "name", "Grant Oliver", "marketext", "D");
  db.hmset("user:3", "email", "tina@thomasgrant.co.uk", "password", "tina", "brokerid", 1, "userid", 3, "name", "Tina Tyers", "marketext", "D");
  db.hmset("user:4", "email", "patrick@thomasgrant.co.uk", "password", "patrick", "brokerid", 1, "userid", 4, "name", "Patrick Waldron", "marketext", "D");
  db.hmset("user:5", "email", "sheila@thomasgrant.co.uk", "password", "sheila", "brokerid", 1, "userid", 5, "name", "Sheila", "marketext", "D");
  db.hmset("user:6", "email", "kevin@thomasgrant.co.uk", "password", "kevin", "brokerid", 1, "userid", 6, "name", "Kevin", "marketext", "D");
  db.hmset("user:7", "email", "louisa@thomasgrant.co.uk", "password", "louisa", "brokerid", 1, "userid", 7, "name", "Louisa", "marketext", "D");
  db.hmset("user:8", "email", "info@yearstretch.com", "password", "paul", "brokerid", 1, "userid", 8, "name", "Paul", "marketext", "D");

  // link between user email & id
  db.set("user:terry@cw2t.com", "1");
  db.set("user:grant@thomasgrant.co.uk", "2");
  db.set("user:tina@thomasgrant.co.uk", "3");
  db.set("user:patrick@thomasgrant.co.uk", "4");
  db.set("user:sheila@thomasgrant.co.uk", "5");
  db.set("user:kevin@thomasgrant.co.uk", "6");
  db.set("user:louisa@thomasgrant.co.uk", "7");
  db.set("user:info@yearstretch.com", "8");

  // stocks - add international stocks to set of stocks & hash
  /*db.hset("symbol:AMZN", "currency", "USD");
  db.hset("symbol:AMZN", "description", "Amazon.com, Inc");
  db.hset("symbol:AMZN", "proquotesymbol", "AMZN");
  db.hset("symbol:AMZN", "isin", "US0231351067");
  db.hset("symbol:AMZN", "exchange", "O");
  db.hset("symbol:AMZN", "topic", "TIT.AMZN");
  db.hset("symbol:AMZN", "symbol", "AMZN");
  db.hset("symbol:AMZN", "instrumenttype", "IE");
  db.hset("symbol:BARC.L", "market", "O");
  db.set("proquotesymbol:AMZN", "AMZN");
  db.sadd("instruments", "AMZN");
  db.hset("symbol:BARC.L", "currency", "GBP");
  db.hset("symbol:BARC.L", "description", "Barclays PLC");
  db.hset("symbol:BARC.L", "proquotesymbol", "BARC");
  db.hset("symbol:BARC.L", "isin", "GB0031348658");
  db.hset("symbol:BARC.L", "exchange", "L");
  db.hset("symbol:BARC.L", "topic", "TIT.BARC");
  db.hset("symbol:BARC.L", "symbol", "BARC.L");
  db.hset("symbol:BARC.L", "instrumenttype", "DE");
  db.hset("symbol:BARC.L", "market", "L");
  db.set("proquotesymbol:BARC", "BARC.L");
  db.sadd("instruments", "BARC.L");*/

  // instrument types
  db.sadd("instrumenttypes", "DE");
  db.sadd("instrumenttypes", "IE");
  db.sadd("instrumenttypes", "CFD");
  db.sadd("instrumenttypes", "SPB");
  db.sadd("instrumenttypes", "CCFD");

  db.set("instrumenttype:DE", "UK Equities");
  db.set("instrumenttype:IE", "International Equities");
  db.set("instrumenttype:CFD", "CFD");
  db.set("instrumenttype:SPB", "Spreadbet");
  db.set("instrumenttype:CCFD", "Convertible CFD");

  // indices
  db.sadd("index:UKX", "BARC.L");

  // cash transaction types
  db.sadd("cashtranstypes", "CA");
  db.sadd("cashtranstypes", "DI");
  db.sadd("cashtranstypes", "TC");
  db.sadd("cashtranstypes", "FI");
  db.sadd("cashtranstypes", "IN");
  db.sadd("cashtranstypes", "CO");
  db.sadd("cashtranstypes", "OT");

  db.set("cashtranstype:CA", "Cash");
  db.set("cashtranstype:DI", "Dividend");
  db.set("cashtranstype:TC", "Trade Costs");
  db.set("cashtranstype:FI", "Finance");
  db.set("cashtranstype:IN", "Interest");
  db.set("cashtranstype:CO", "Commission");
  db.set("cashtranstype:OT", "Other");

  // currencies
  db.sadd("currencies", "GBP");
  db.sadd("currencies", "EUR");
  db.sadd("currencies", "USD");

  // client types
  db.sadd("clienttypes", "1");
  db.sadd("clienttypes", "2");
  db.sadd("clienttypes", "3");

  db.set("clienttype:1", "Retail");
  db.set("clienttype:2", "Hedge");
  db.set("clienttype:3", "Certificated");

  // order types - set of order types & string for each one
  //db.sadd("ordertypes", "1");
  db.sadd("ordertypes", "2");
  //db.sadd("ordertypes", "7");
  //db.set("ordertype:1", "At Best");
  db.set("ordertype:2", "Limit");
  //db.set("ordertype:7", "At Limit");
  db.sadd("ordertypes", "D");
  db.set("ordertype:D", "Quoted");
  db.sadd("ordertypes", "X");
  db.set("ordertype:X", "Hedge");

  db.sadd("costs", "DE:GBP:1");
  db.hmset("cost:DE:GBP:1","defaultnosettdays",3,"costkey","DE:GBP:1","commissionpercent","","commissionmin","","ptmlevylimit","","ptmlevy","","stampdutylimit","","stampdutypercent","","contractcharge","","finance",""
);
  db.sadd("costs", "DE:GBP:2");
  db.hmset("cost:DE:GBP:2","defaultnosettdays",3,"costkey","DE:GBP:2","commissionpercent","","commissionmin","","ptmlevylimit","","ptmlevy","","stampdutylimit","","stampdutypercent","","contractcharge","","finance",""
);

  // logon
  db.set("trading:ipaddress", "195.26.26.67");
  db.set("trading:port", "50143");
  db.set("sendercompid", "CWTTUAT1");
  db.set("targetcompid", "PTPUAT1");

  console.log("done");
}