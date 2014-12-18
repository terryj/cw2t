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
  db.hmset("broker:TGRANT", "clientid", 999999, "name", "Thomas Grant & Company", "brokerid", "TGRANT");
  db.sadd("brokers", "TGRANT");
  db.hmset("broker:UNKNOWN", "clientid", 999998, "name", "Unknown Counterparty", "brokerid", "UNKNOWN");
  db.sadd("brokers", "UNKNOWN");
  db.hmset("broker:WNTSGB2LBIC", "clientid", 999997, "name", "Winterfloods", "brokerid", "WNTSGB2LBIC");
  db.sadd("brokers", "WNTSGB2LBIC");
  db.hmset("broker:NETHGB21", "clientid", 999996, "name", "BMO Capital Markets", "brokerid", "NETHGB21");
  db.sadd("brokers", "NETHGB21");
  db.hmset("broker:CSTEGB21", "clientid", 999995, "name", "Canaccord", "brokerid", "CSTEGB21");
  db.sadd("brokers", "CSTEGB21");
  db.hmset("broker:CFEQGB21EPE", "clientid", 999994, "name", "Cantor Fitzgerald Europe", "brokerid", "CFEQGB21EPE");
  db.sadd("brokers", "CFEQGB21EPE");
  db.hmset("broker:CNKOGB21", "clientid", 999993, "name", "Cenkos", "brokerid", "CNKOGB21");
  db.sadd("brokers", "CNKOGB21");
  db.hmset("broker:EXEUGBMM", "clientid", 999992, "name", "Espirito Di Santo", "brokerid", "EXEUGBMM");
  db.sadd("brokers", "EXEUGBMM");
  db.hmset("broker:FCAPMM21", "clientid", 999991, "name", "Finncap", "brokerid", "FCAPMM21");
  db.sadd("brokers", "FCAPMM21");
  db.hmset("broker:FOXD1234", "clientid", 999990, "name", "Fox Davies", "brokerid", "FOXD1234");
  db.sadd("brokers", "FOXD1234");
  db.hmset("broker:DAVEIE21", "clientid", 999989, "name", "Davy", "brokerid", "DAVEIE21");
  db.sadd("brokers", "DAVEIE21");
  db.hmset("broker:GMPSGB21", "clientid", 999988, "name", "GMP Securities", "brokerid", "GMPSGB21");
  db.sadd("brokers", "GMPSGB21");
  db.hmset("broker:JEFFGB2X", "clientid", 999987, "name", "Jefferies", "brokerid", "JEFFGB2X");
  db.sadd("brokers", "JEFFGB2X");
  db.hmset("broker:JPMSGB2L", "clientid", 999986, "name", "JP Morgan Cazenove", "brokerid", "JPMSGB2L");
  db.sadd("brokers", "JPMSGB2L");
  db.hmset("broker:IHCSGB21002", "clientid", 999985, "name", "Investec", "brokerid", "IHCSGB21002");
  db.sadd("brokers", "IHCSGB21002");
  db.hmset("broker:NITEGB21888", "clientid", 999984, "name", "Knight", "brokerid", "NITEGB21888");
  db.sadd("brokers", "NITEGB21888");
  db.hmset("broker:EDMRGB21", "clientid", 999983, "name", "LCF Rothschild", "brokerid", "EDMRGB21");
  db.sadd("brokers", "EDMRGB21");
  db.hmset("broker:LOYDGB22TSY", "clientid", 999982, "name", "Lloyds TSB", "brokerid", "WNTSGB2LBIC");
  db.sadd("brokers", "LOYDGB22TSY");
  db.hmset("broker:PLHCGB2LBIC", "clientid", 999981, "name", "Peel Hunt", "brokerid", "WNTSGB2LBIC");
  db.sadd("brokers", "PLHCGB2LBIC");
  db.hmset("broker:LCAPGB21", "clientid", 999980, "name", "Liberum", "brokerid", "WNTSGB2LBIC");
  db.sadd("brokers", "LCAPGB21");
  db.hmset("broker:MAPUGB21", "clientid", 999979, "name", "MacQuarie", "brokerid", "MAPUGB21");
  db.sadd("brokers", "MAPUGB21");
  db.hmset("broker:CODCGB21", "clientid", 999978, "name", "Nomura Code", "brokerid", "CODCGB21");
  db.sadd("brokers", "CODCGB21");
  db.hmset("broker:NSMMGB21", "clientid", 999977, "name", "Novum", "brokerid", "NSMMGB21");
  db.sadd("brokers", "NSMMGB21");
  db.hmset("broker:RAZHGB2LBIC", "clientid", 999976, "name", "Numis", "brokerid", "RAZHGB2LBIC");
  db.sadd("brokers", "RAZHGB2LBIC");
  db.hmset("broker:ORSNGB21", "clientid", 999975, "name", "Oriel Securities", "brokerid", "ORSNGB21");
  db.sadd("brokers", "ORSNGB21");
  db.hmset("broker:PMURGB2L", "clientid", 999974, "name", "Panmure", "brokerid", "PMURGB2L");
  db.sadd("brokers", "PMURGB2L");
  db.hmset("broker:ROYCCG22", "clientid", 999973, "name", "RBC", "brokerid", "ROYCCG22");
  db.sadd("brokers", "ROYCCG22");
  db.hmset("broker:SHOCGB2L", "clientid", 999972, "name", "Shore Capital", "brokerid", "SHOCGB2L");
  db.sadd("brokers", "SHOCGB2L");
  db.hmset("broker:SNGRGB21", "clientid", 999971, "name", "Singer Capital Markets", "brokerid", "SNGRGB21");
  db.sadd("brokers", "SNGRGB21");
  db.hmset("broker:SISSIE21", "clientid", 999970, "name", "Susquehanna International", "brokerid", "WNTSGB2LBIC");
  db.sadd("brokers", "SISSIE21");
  db.hmset("broker:WSMM1234", "clientid", 999969, "name", "Westhouse Securities", "brokerid", "WSMM1234");
  db.sadd("brokers", "WSMM1234");
  db.hmset("broker:WHISGBMM", "clientid", 999968, "name", "WH Ireland", "brokerid", "WHISGBMM");
  db.sadd("brokers", "WHISGBMM");
  db.hmset("broker:CAPXGB21", "clientid", 999967, "name", "XCAP", "brokerid", "CAPXGB21");
  db.sadd("brokers", "CAPXGB21");

  // clients
  db.sadd("clients", 999999);
  db.hmset("client:999999", "email", "tgrant@thomasgrant.co.uk", "password", "tgrant", "brokerid", "TGRANT", "clientid", 999999, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999999, "commpercent", 0);
  db.set("email:tgrant@thomasgrant.co.uk", 999999);
  db.sadd("clients", 999998);
  db.hmset("client:999998", "email", "unknown@thomasgrant.co.uk", "password", "unknown", "brokerid", "UNKNOWN", "clientid", 999998, "marketext", "D", "name", "Unknown Counterparty", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999998, "commpercent", 0);
  db.set("email:unknown@thomasgrant.co.uk", 999998);
  db.sadd("clients", 999997);
  db.hmset("client:999997", "email", "winn@thomasgrant.co.uk", "password", "winn", "brokerid", "WNTSGB2LBIC", "clientid", 999997, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999997, "commpercent", 0);
  db.set("email:winn@thomasgrant.co.uk", 999997);
  db.sadd("clients", 999996);
  db.hmset("client:999996", "email", "bmo@thomasgrant.co.uk", "password", "bmo", "brokerid", "NETHGB21", "clientid", 999996, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999996, "commpercent", 0);
  db.set("email:bmo@thomasgrant.co.uk", 999996);
  db.sadd("clients", 999995);
  db.hmset("client:999995", "email", "canaccord@thomasgrant.co.uk", "password", "canaccord", "brokerid", "CSTEGB21", "clientid", 999995, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999995, "commpercent", 0);
  db.set("email:canaccord@thomasgrant.co.uk", 999995);
  db.sadd("clients", 999994);
  db.hmset("client:999994", "email", "cantor@thomasgrant.co.uk", "password", "cantor", "brokerid", "CFEQGB21EPE", "clientid", 999994, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999994, "commpercent", 0);
  db.set("email:cantor@thomasgrant.co.uk", 999994);
  db.sadd("clients", 999993);
  db.hmset("client:999993", "email", "cenkos@thomasgrant.co.uk", "password", "cenkos", "brokerid", "CNKOGB21", "clientid", 999993, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999993, "commpercent", 0);
  db.set("email:cenkos@thomasgrant.co.uk", 999993);
  db.sadd("clients", 999992);
  db.hmset("client:999992", "email", "espirito@thomasgrant.co.uk", "password", "espirito", "brokerid", "EXEUGBMM", "clientid", 999992, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999992, "commpercent", 0);
  db.set("email:espirito@thomasgrant.co.uk", 999992);
  db.sadd("clients", 999991);
  db.hmset("client:999991", "email", "finncap@thomasgrant.co.uk", "password", "finncap", "brokerid", "FCAPMM21", "clientid", 999991, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999991, "commpercent", 0);
  db.set("email:finncap@thomasgrant.co.uk", 999991);
  db.sadd("clients", 999990);
  db.hmset("client:999990", "email", "fox@thomasgrant.co.uk", "password", "fox", "brokerid", "FOXD1234", "clientid", 999990, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999990, "commpercent", 0);
  db.set("email:fox@thomasgrant.co.uk", 999990);
  db.sadd("clients", 999989);
  db.hmset("client:999989", "email", "davy@thomasgrant.co.uk", "password", "davy", "brokerid", "DAVEIE21", "clientid", 999989, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999989, "commpercent", 0);
  db.set("email:davy@thomasgrant.co.uk", 999989);
  db.sadd("clients", 999988);
  db.hmset("client:999988", "email", "gmp@thomasgrant.co.uk", "password", "gmp", "brokerid", "GMPSGB21", "clientid", 999988, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999988, "commpercent", 0);
  db.set("email:gmp@thomasgrant.co.uk", 999988);
  db.sadd("clients", 999987);
  db.hmset("client:999987", "email", "jefferies@thomasgrant.co.uk", "password", "jefferies", "JEFFGB2X", "TGRANT", "clientid", 999987, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999987, "commpercent", 0);
  db.set("email:jefferies@thomasgrant.co.uk", 999987);
  db.sadd("clients", 999986);
  db.hmset("client:999986", "email", "jpmorgan@thomasgrant.co.uk", "password", "jpmorgan", "JPMSGB2L", "TGRANT", "clientid", 999986, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999986, "commpercent", 0);
  db.set("email:jpmorgan@thomasgrant.co.uk", 999986);
  db.sadd("clients", 999985);
  db.hmset("client:999985", "email", "investec@thomasgrant.co.uk", "password", "investec", "IHCSGB21002", "TGRANT", "clientid", 999985, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999985, "commpercent", 0);
  db.set("email:investec@thomasgrant.co.uk", 999985);
  db.sadd("clients", 999984);
  db.hmset("client:999984", "email", "knight@thomasgrant.co.uk", "password", "knight", "brokerid", "NITEGB21888", "clientid", 999984, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999984, "commpercent", 0);
  db.set("email:knight@thomasgrant.co.uk", 999984);
  db.sadd("clients", 999983);
  db.hmset("client:999983", "email", "rothschild@thomasgrant.co.uk", "password", "rothschild", "brokerid", "EDMRGB21", "clientid", 999983, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999983, "commpercent", 0);
  db.set("email:rothschild@thomasgrant.co.uk", 999983);
  db.sadd("clients", 999982);
  db.hmset("client:999982", "email", "lloyds@thomasgrant.co.uk", "password", "lloyds", "brokerid", "LOYDGB22TSY", "clientid", 999982, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999982, "commpercent", 0);
  db.set("email:lloyds@thomasgrant.co.uk", 999982);
  db.sadd("clients", 999981);
  db.hmset("client:999981", "email", "peelhunt@thomasgrant.co.uk", "password", "peelhunt", "brokerid", "PLHCGB2LBIC", "clientid", 999981, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999981, "commpercent", 0);
  db.set("email:peelhunt@thomasgrant.co.uk", 999981);
  db.sadd("clients", 999980);
  db.hmset("client:999980", "email", "liberum@thomasgrant.co.uk", "password", "liberum", "brokerid", "LCAPGB21", "clientid", 999980, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999980, "commpercent", 0);
  db.set("email:liberum@thomasgrant.co.uk", 999980);
  db.sadd("clients", 999979);
  db.hmset("client:999979", "email", "macquarie@thomasgrant.co.uk", "password", "macquarie", "brokerid", "MAPUGB21", "clientid", 999979, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999979, "commpercent", 0);
  db.set("email:macquarie@thomasgrant.co.uk", 999979);
  db.sadd("clients", 999978);
  db.hmset("client:999978", "email", "nomura@thomasgrant.co.uk", "password", "nomura", "brokerid", "CODCGB21", "clientid", 999978, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999978, "commpercent", 0);
  db.set("email:nomura@thomasgrant.co.uk", 999978);
  db.sadd("clients", 999977);
  db.hmset("client:999977", "email", "novum@thomasgrant.co.uk", "password", "novum", "brokerid", "NSMMGB21", "clientid", 999977, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999977, "commpercent", 0);
  db.set("email:novum@thomasgrant.co.uk", 999977);
  db.sadd("clients", 999976);
  db.hmset("client:999976", "email", "numis@thomasgrant.co.uk", "password", "numis", "brokerid", "RAZHGB2LBIC", "clientid", 999976, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999976, "commpercent", 0);
  db.set("email:numis@thomasgrant.co.uk", 999976);
  db.sadd("clients", 999975);
  db.hmset("client:999975", "email", "oriel@thomasgrant.co.uk", "password", "oriel", "brokerid", "ORSNGB21", "clientid", 999975, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999975, "commpercent", 0);
  db.set("email:oriel@thomasgrant.co.uk", 999975);
  db.sadd("clients", 999974);
  db.hmset("client:999974", "email", "panmure@thomasgrant.co.uk", "password", "panmure", "brokerid", "PMURGB2L", "clientid", 999974, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999974, "commpercent", 0);
  db.set("email:panmure@thomasgrant.co.uk", 999974);
  db.sadd("clients", 999973);
  db.hmset("client:999973", "email", "rbc@thomasgrant.co.uk", "password", "rbc", "brokerid", "ROYCCG22", "clientid", 999973, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999973, "commpercent", 0);
  db.set("email:rbc@thomasgrant.co.uk", 999973);
  db.sadd("clients", 999972);
  db.hmset("client:999972", "email", "shore@thomasgrant.co.uk", "password", "shore", "brokerid", "SHOCGB2L", "clientid", 999972, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999972, "commpercent", 0);
  db.set("email:shore@thomasgrant.co.uk", 999972);
  db.sadd("clients", 999971);
  db.hmset("client:999971", "email", "singer@thomasgrant.co.uk", "password", "singer", "brokerid", "SNGRGB21", "clientid", 999971, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999971, "commpercent", 0);
  db.set("email:singer@thomasgrant.co.uk", 999971);
  db.sadd("clients", 999970);
  db.hmset("client:999970", "email", "susquehanna@thomasgrant.co.uk", "password", "susquehanna", "brokerid", "SISSIE21", "clientid", 999970, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999970, "commpercent", 0);
  db.set("email:susquehanna@thomasgrant.co.uk", 999970);
  db.sadd("clients", 999969);
  db.hmset("client:999969", "email", "westhouse@thomasgrant.co.uk", "password", "westhouse", "brokerid", "WSMM1234", "clientid", 999969, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999969, "commpercent", 0);
  db.set("email:westhouse@thomasgrant.co.uk", 999969);
  db.sadd("clients", 999968);
  db.hmset("client:999968", "email", "whireland@thomasgrant.co.uk", "password", "whireland", "brokerid", "WHISGBMM", "clientid", 999968, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999968, "commpercent", 0);
  db.set("email:whireland@thomasgrant.co.uk", 999968);
  db.sadd("clients", 999967);
  db.hmset("client:999967", "email", "xcap@thomasgrant.co.uk", "password", "xcap", "brokerid", "CAPXGB21", "clientid", 999967, "marketext", "D", "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999967, "commpercent", 0);
  db.set("email:xcap@thomasgrant.co.uk", 999967);

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
  db.sadd("clienttypes:user", "1");
  db.sadd("clienttypes:user", "2");
  db.sadd("clienttypes:user", "3");
  db.sadd("clienttypes:ifa", "1");
  db.sadd("clienttypes:ifa", "3");

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
  db.set("trading:ipaddress", "82.211.104.37");
  db.set("trading:port", "60144");
  db.set("sendercompid", "CWTT_UAT");
  db.set("targetcompid", "NBT_UAT");
  db.set("onbehalfofcompid", "TGRANT");

  db.set("markettype", 0);

  console.log("done");
}