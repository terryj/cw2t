/****************
* initdb.js
* Initialise redis
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
  ensureredismarketmakingkeys();
  console.log("done");
});

db.on("error", function (err) {
  console.error(err);
});

function initdb() {
  // clear all key values
  db.flushdb();

  // or set them to start at 1
  db.set("fixseqnumin", 0);
  db.set("fixseqnumout", 0); // first out will be 1

  // a broker
  db.hmset("broker:1", "name", "Thomas Grant & Company", "brokerid", 1);

  // broker last ids
  db.hset("broker:1", "lasttransactionid", 0);
  db.hset("broker:1", "lastpostingid", 0);
  db.hset("broker:1", "lastquoterequestid", 0);
  db.hset("broker:1", "lastquoteid", 0);
  db.hset("broker:1", "lastorderid", 0);
  db.hset("broker:1", "lasttradeid", 0);
  db.hset("broker:1", "lastclientid", 0);
  db.hset("broker:1", "lastpositionid", 0);
  db.hset("broker:1", "lastpositionpostingid", 0);
  db.hset("broker:1", "lastaccountid", 0);

  // broker account ids
  db.hset("broker:1", "bankfundsbroker", 99);
  db.hset("broker:1", "bankfundsdesignatedclient", 98);
  db.hset("broker:1", "brokerdividend", 97);
  db.hset("broker:1", "brokertrading", 96);
  db.hset("broker:1", "nominalcommission", 95);
  db.hset("broker:1", "nominalconsideration", 94);
  db.hset("broker:1", "nominalcorporateactions", 93);
  db.hset("broker:1", "nominalptm", 92);
  db.hset("broker:1", "nominalstampduty", 91);
  db.hset("broker:1", "suppliercrest", 90);
  db.hset("broker:1", "supplierptm", 89);

  // set of users
  db.sadd("broker:1:users", "1");

  // user hash
  db.hmset("broker:1:user:1", "email", "terry@cw2t.com", "password", "terry", "brokerid", 1, "userid", 1, "name", "Terry Johnston");

  // link between user email & id
  db.set("broker:1:user:terry@cw2t.com", "1");

  // instrument types
  db.sadd("instrumenttypes", "DE");
  db.sadd("instrumenttypes", "IE");
  db.sadd("instrumenttypes", "CFD");
  db.sadd("instrumenttypes", "SPB");
  db.sadd("instrumenttypes", "CCFD");

  // instrument descriptions
  db.set("instrumenttype:DE", "UK Equities");
  db.set("instrumenttype:IE", "International Equities");
  db.set("instrumenttype:CFD", "CFD");
  db.set("instrumenttype:SPB", "Spreadbet");
  db.set("instrumenttype:CCFD", "Convertible CFD");

  db.hmset("symbol:BARC.L", "ask", "258.45", "bid", "258.4", "currencyid", "GBP", "exchangeid", "L", "hedgesymbolid", "1", "instrumenttypeid", "DE", "isin", "GB0031348658", "longname", "BARCLAYS PLC ORD 25P", "midnetchange", "5.075", "midpercentchange", "2", "mnemonic", "BARC", "nbtsymbol", "BARC.L", "shortname", "BARCLAYS", "symbolid", "BARC.L", "timezoneid", "310", "active", "1", "ptmexempt", "0", "timestamp", "Mon Nov 30 2015 13:04:50 GMT+0000 (UTC)");

  db.sadd("symbols", "BARC.L");

  // indices
  db.sadd("index:UKX", "BARC.L");

  // currencies
  db.sadd("currencies", "GBP");
  db.sadd("currencies", "EUR");
  db.sadd("currencies", "USD");

  // client types
  db.sadd("clienttypes:user", "1");
  db.sadd("clienttypes:user", "2");
  db.sadd("clienttypes:user", "3");
  db.sadd("clienttypes:ifa", "1");
  db.sadd("clienttypes:ifa", "3");

  db.set("clienttype:1", "Retail");
  db.set("clienttype:2", "Hedge");
  db.set("clienttype:3", "Certificated");

  // order types - set of order types & string for each one
  db.sadd("ordertypes", "2");
  db.set("ordertype:2", "Limit");
  db.sadd("ordertypes", "D");
  db.set("ordertype:D", "Quoted");
  db.sadd("ordertypes", "X");
  db.set("ordertype:X", "Hedge");

  db.sadd("costs", "DE:GBP:1");
  db.hmset("cost:DE:GBP:1","defaultnosettdays",3,"costkey","DE:GBP:1","commissionpercent","0","commissionmin","0","ptmlevylimit","10000","ptmlevy","1","stampdutylimit","1000","stampdutypercent","0.5","contractcharge","0","finance","0"
);
  db.sadd("costs", "DE:GBP:2");
  db.hmset("cost:DE:GBP:2","defaultnosettdays",3,"costkey","DE:GBP:2","commissionpercent","0","commissionmin","0","ptmlevylimit","10000","ptmlevy","1","stampdutylimit","0","stampdutypercent","0","contractcharge","0","finance","0"
);
  db.sadd("costs", "CFD:GBP:1");
  db.hmset("cost:DE:GBP:1","defaultnosettdays",0,"costkey","CFD:GBP:1","commissionpercent","0","commissionmin","0","ptmlevylimit","0","ptmlevy","0","stampdutylimit","0","stampdutypercent","0","contractcharge","0","finance","0"
);
  db.sadd("costs", "CFD:GBP:2");
  db.hmset("cost:DE:GBP:2","defaultnosettdays",0,"costkey","CFD:GBP:2","commissionpercent","0","commissionmin","0","ptmlevylimit","0","ptmlevy","0","stampdutylimit","0","stampdutypercent","0","contractcharge","0","finance","0"
);

  // timezone
  db.hmset("timezone:310", "openhour", 8, "openminute" , 0, "closehour", 16 ,"closeminute", 30);

  // logon
  db.set("trading:ipaddress", "82.211.104.37");
  db.set("trading:port", "60188");
  db.set("sendercompid", "CWTT_UAT");
  db.set("targetcompid", "NBT_UAT");
  db.set("onbehalfofcompid", "TGRANT");

  // testing
  db.set("testmode", "0");
}

/*
make sure redis has market making keys
*/
function ensureredismarketmakingkeys() {
db.hmset("mm:1", "mmglobalpl", "1", "mmglobalplalgor", "2",
"mmglobalpllimitloss", "3", "mmglobalpllimitprofit", "4",
"mmglobalpositioncost", "5", "mmplalgor", "6", "mmpllimitloss", "7",
"mmpllimitprofit", "8", "mmposition", "9", "mmpositionalgor", "10",
"mmpositioncost", "11", "mmpositionlimitlong", "12", "mmpositionlimitshort",
"13", "mmpricealgor", "14", "mmpriceask", "15", "mmpriceaskclose", "16",
"mmpriceaskcurrent", "17", "mmpricebid", "18", "mmpricebidclose", "19",
"mmpricebidcurrent", "20", "mmpricegapovernight", "21", "mmpricegapweekend",
"22", "mmquotesequencenumber", "23", "mmrfqsize", "24", "mmrfqsizealgor",
"25", "mmrfqsizemax", "26", "mmrfqsizemin", "27", "mmrfqsizemin", "28",
"mmspread", "29", "mmspreadalgor", "30", "mmspreadmax", "31", "mmspreadmin",
"32")
}
