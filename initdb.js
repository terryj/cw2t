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

  // broker account ids
  db.hset("broker:1", "clientcontrolaccountid", 1);
  db.hset("broker:1", "bankcontrolaccountid", 2);
  db.hset("broker:1", "nominaltradeaccountid", 3);
  db.hset("broker:1", "nominalcommissionaccountid", 4);
  db.hset("broker:1", "nominalptmaccountid", 5);
  db.hset("broker:1", "nominalstampdutyaccountid", 6);

  // re-set sequential ids
  /*db.set("quotereqid", 0);
  db.set("quoteid", 0);
  db.set("orderid", 0);
  db.set("tradeid", 0);
  db.set("clientid", 0);*/
  //db.set("wffixseqnumout", 0); // used by winterflood test server
  /*db.set("ordercancelreqid", 0);
  db.set("fobomsgid", 0);
  db.set("cashtransid", 0);
  db.set("ifaid", 0);
  db.set("positionid", 0);
  db.set("chatid", 0);*/

  // brokers
  /*db.hmset("broker:TGRANT", "clientid", 999999, "name", "Thomas Grant & Company", "brokerid", "TGRANT");
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
  db.sadd("brokers", "CAPXGB21");*/

  // clients
  /*db.sadd("clients", 999999);
  db.hmset("client:999999", "email", "999999@thomasgrant.co.uk", "password", "999999", "brokerid", "TGRANT", "clientid", 999999, "name", "Thomas Grant & Company", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999999, "commpercent", 0);
  db.set("client:999999@thomasgrant.co.uk", 999999);
  db.sadd("clients", 999998);
  db.hmset("client:999998", "email", "999998@thomasgrant.co.uk", "password", "999998", "brokerid", "UNKNOWN", "clientid", 999998, "name", "Unknown Counterparty", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999998, "commpercent", 0);
  db.set("client:999998@thomasgrant.co.uk", 999998);
  db.sadd("clients", 999997);
  db.hmset("client:999997", "email", "999997@thomasgrant.co.uk", "password", "999997", "brokerid", "WNTSGB2LBIC", "clientid", 999997, "name", "Winterfloods", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999997, "commpercent", 0);
  db.set("client:999997@thomasgrant.co.uk", 999997);
  db.sadd("clients", 999996);
  db.hmset("client:999996", "email", "999996@thomasgrant.co.uk", "password", "999996", "brokerid", "NETHGB21", "clientid", 999996, "name", "BMO Capital Markets", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999996, "commpercent", 0);
  db.set("client:999996@thomasgrant.co.uk", 999996);
  db.sadd("clients", 999995);
  db.hmset("client:999995", "email", "999995@thomasgrant.co.uk", "password", "999995", "brokerid", "CSTEGB21", "clientid", 999995, "name", "Canaccord", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999995, "commpercent", 0);
  db.set("client:999995@thomasgrant.co.uk", 999995);
  db.sadd("clients", 999994);
  db.hmset("client:999994", "email", "999994@thomasgrant.co.uk", "password", "999994", "brokerid", "CFEQGB21EPE", "clientid", 999994, "name", "Cantor Fitzgerald Europe", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999994, "commpercent", 0);
  db.set("client:999994@thomasgrant.co.uk", 999994);
  db.sadd("clients", 999993);
  db.hmset("client:999993", "email", "999993@thomasgrant.co.uk", "password", "999993", "brokerid", "CNKOGB21", "clientid", 999993, "name", "Cenkos", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999993, "commpercent", 0);
  db.set("client:999993@thomasgrant.co.uk", 999993);
  db.sadd("clients", 999992);
  db.hmset("client:999992", "email", "999992@thomasgrant.co.uk", "password", "999992", "brokerid", "EXEUGBMM", "clientid", 999992, "name", "Espirito Di Santo", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999992, "commpercent", 0);
  db.set("client:999992@thomasgrant.co.uk", 999992);
  db.sadd("clients", 999991);
  db.hmset("client:999991", "email", "999991@thomasgrant.co.uk", "password", "999991", "brokerid", "FCAPMM21", "clientid", 999991, "name", "Finncap", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999991, "commpercent", 0);
  db.set("client:999991@thomasgrant.co.uk", 999991);
  db.sadd("clients", 999990);
  db.hmset("client:999990", "email", "999990@thomasgrant.co.uk", "password", "999990", "brokerid", "FOXD1234", "clientid", 999990, "name", "Fox Davies", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999990, "commpercent", 0);
  db.set("client:999990@thomasgrant.co.uk", 999990);
  db.sadd("clients", 999989);
  db.hmset("client:999989", "email", "999989@thomasgrant.co.uk", "password", "999989", "brokerid", "DAVEIE21", "clientid", 999989, "name", "Davy", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999989, "commpercent", 0);
  db.set("client:999989@thomasgrant.co.uk", 999989);
  db.sadd("clients", 999988);
  db.hmset("client:999988", "email", "999988@thomasgrant.co.uk", "password", "999988", "brokerid", "GMPSGB21", "clientid", 999988, "name", "GMP Securities", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999988, "commpercent", 0);
  db.set("client:999988@thomasgrant.co.uk", 999988);
  db.sadd("clients", 999987);
  db.hmset("client:999987", "email", "999987@thomasgrant.co.uk", "password", "999987", "JEFFGB2X", "TGRANT", "clientid", 999987, "name", "Jefferies", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999987, "commpercent", 0);
  db.set("client:999987@thomasgrant.co.uk", 999987);
  db.sadd("clients", 999986);
  db.hmset("client:999986", "email", "999986@thomasgrant.co.uk", "password", "999986", "JPMSGB2L", "TGRANT", "clientid", 999986, "name", "JP Morgan Cazenove", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999986, "commpercent", 0);
  db.set("client:999986@thomasgrant.co.uk", 999986);
  db.sadd("clients", 999985);
  db.hmset("client:999985", "email", "999985@thomasgrant.co.uk", "password", "999985", "IHCSGB21002", "TGRANT", "clientid", 999985, "name", "Investec", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999985, "commpercent", 0);
  db.set("client:999985@thomasgrant.co.uk", 999985);
  db.sadd("clients", 999984);
  db.hmset("client:999984", "email", "999984@thomasgrant.co.uk", "password", "999984", "brokerid", "NITEGB21888", "clientid", 999984, "name", "Knight", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999984, "commpercent", 0);
  db.set("client:999984@thomasgrant.co.uk", 999984);
  db.sadd("clients", 999983);
  db.hmset("client:999983", "email", "999983@thomasgrant.co.uk", "password", "999983", "brokerid", "EDMRGB21", "clientid", 999983, "name", "LCF Rothschild", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999983, "commpercent", 0);
  db.set("client:999983@thomasgrant.co.uk", 999983);
  db.sadd("clients", 999982);
  db.hmset("client:999982", "email", "999982@thomasgrant.co.uk", "password", "999982", "brokerid", "LOYDGB22TSY", "clientid", 999982, "name", "Lloyds TSB", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999982, "commpercent", 0);
  db.set("client:999982@thomasgrant.co.uk", 999982);
  db.sadd("clients", 999981);
  db.hmset("client:999981", "email", "999981@thomasgrant.co.uk", "password", "999981", "brokerid", "PLHCGB2LBIC", "clientid", 999981, "name", "Peel Hunt", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999981, "commpercent", 0);
  db.set("client:999981@thomasgrant.co.uk", 999981);
  db.sadd("clients", 999980);
  db.hmset("client:999980", "email", "999980@thomasgrant.co.uk", "password", "999980", "brokerid", "LCAPGB21", "clientid", 999980, "name", "Liberum", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999980, "commpercent", 0);
  db.set("client:999980@thomasgrant.co.uk", 999980);
  db.sadd("clients", 999979);
  db.hmset("client:999979", "email", "999979@thomasgrant.co.uk", "password", "999979", "brokerid", "MAPUGB21", "clientid", 999979, "name", "MacQuarie", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999979, "commpercent", 0);
  db.set("client:999979@thomasgrant.co.uk", 999979);
  db.sadd("clients", 999978);
  db.hmset("client:999978", "email", "999978@thomasgrant.co.uk", "password", "999978", "brokerid", "CODCGB21", "clientid", 999978, "name", "Nomura Code", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999978, "commpercent", 0);
  db.set("client:999978@thomasgrant.co.uk", 999978);
  db.sadd("clients", 999977);
  db.hmset("client:999977", "email", "999977@thomasgrant.co.uk", "password", "999977", "brokerid", "NSMMGB21", "clientid", 999977, "name", "Novum", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999977, "commpercent", 0);
  db.set("client:999977@thomasgrant.co.uk", 999977);
  db.sadd("clients", 999976);
  db.hmset("client:999976", "email", "999976@thomasgrant.co.uk", "password", "999976", "brokerid", "RAZHGB2LBIC", "clientid", 999976, "name", "Numis", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999976, "commpercent", 0);
  db.set("client:999976@thomasgrant.co.uk", 999976);
  db.sadd("clients", 999975);
  db.hmset("client:999975", "email", "999975@thomasgrant.co.uk", "password", "999975", "brokerid", "ORSNGB21", "clientid", 999975, "name", "Oriel Securities", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999975, "commpercent", 0);
  db.set("client:999975@thomasgrant.co.uk", 999975);
  db.sadd("clients", 999974);
  db.hmset("client:999974", "email", "999974@thomasgrant.co.uk", "password", "999974", "brokerid", "PMURGB2L", "clientid", 999974, "name", "Panmure", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999974, "commpercent", 0);
  db.set("client:999974@thomasgrant.co.uk", 999974);
  db.sadd("clients", 999973);
  db.hmset("client:999973", "email", "999973@thomasgrant.co.uk", "password", "999973", "brokerid", "ROYCCG22", "clientid", 999973, "name", "RBC", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999973, "commpercent", 0);
  db.set("client:999973@thomasgrant.co.uk", 999973);
  db.sadd("clients", 999972);
  db.hmset("client:999972", "email", "999972@thomasgrant.co.uk", "password", "999972", "brokerid", "SHOCGB2L", "clientid", 999972, "name", "Shore Capital", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999972, "commpercent", 0);
  db.set("client:999972@thomasgrant.co.uk", 999972);
  db.sadd("clients", 999971);
  db.hmset("client:999971", "email", "999971@thomasgrant.co.uk", "password", "999971", "brokerid", "SNGRGB21", "clientid", 999971, "name", "Singer Capital Markets", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999971, "commpercent", 0);
  db.set("client:999971@thomasgrant.co.uk", 999971);
  db.sadd("clients", 999970);
  db.hmset("client:999970", "email", "999970@thomasgrant.co.uk", "password", "999970", "brokerid", "SISSIE21", "clientid", 999970, "name", "Susquehanna International", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999970, "commpercent", 0);
  db.set("client:999970@thomasgrant.co.uk", 999970);
  db.sadd("clients", 999969);
  db.hmset("client:999969", "email", "999969@thomasgrant.co.uk", "password", "999969", "brokerid", "WSMM1234", "clientid", 999969, "name", "Westhouse Securities", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999969, "commpercent", 0);
  db.set("client:999969@thomasgrant.co.uk", 999969);
  db.sadd("clients", 999968);
  db.hmset("client:999968", "email", "999968@thomasgrant.co.uk", "password", "999968", "brokerid", "WHISGBMM", "clientid", 999968, "name", "WH Ireland", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999968, "commpercent", 0);
  db.set("client:999968@thomasgrant.co.uk", 999968);
  db.sadd("clients", 999967);
  db.hmset("client:999967", "email", "999967@thomasgrant.co.uk", "password", "999967", "brokerid", "CAPXGB21", "clientid", 999967, "name", "XCAP", "address", "", "mobile", "", "hedge", 0, "type", 2, "brokerclientcode", 999967, "commpercent", 0);
  db.set("client:999967@thomasgrant.co.uk", 999967);*/

  // instruments types hedge client can trade
  /*db.sadd("999999:instrumenttypes", "CFD");
  db.sadd("999999:instrumenttypes", "SPB");
  db.sadd("999999:instrumenttypes", "DE");
  db.sadd("999999:instrumenttypes", "IE");*/

  // GBP cash record for hedge book, required for summary
  /*db.sadd("999999:cash", "GBP");
  db.set("999999:cash:GBP", 0);*/

  // set of users
  db.sadd("broker:1:users", "1");
  /*db.sadd("users", "2");
  db.sadd("users", "3");
  db.sadd("users", "4");
  db.sadd("users", "5");
  db.sadd("users", "6");
  db.sadd("users", "7");
  db.sadd("users", "8");*/

  // user hash
  db.hmset("broker:1:user:1", "email", "terry@cw2t.com", "password", "terry", "brokerid", 1, "userid", 1, "name", "Terry Johnston");
  /*db.hmset("user:2", "email", "grant@thomasgrant.co.uk", "password", "grant", "brokerid", 1, "userid", 2, "name", "Grant Oliver", "marketext", "D");
  db.hmset("user:3", "email", "tina@thomasgrant.co.uk", "password", "tina", "brokerid", 1, "userid", 3, "name", "Tina Tyers", "marketext", "D");
  db.hmset("user:4", "email", "patrick@thomasgrant.co.uk", "password", "patrick", "brokerid", 1, "userid", 4, "name", "Patrick Waldron", "marketext", "D");
  db.hmset("user:5", "email", "sheila@thomasgrant.co.uk", "password", "sheila", "brokerid", 1, "userid", 5, "name", "Sheila", "marketext", "D");
  db.hmset("user:6", "email", "kevin@thomasgrant.co.uk", "password", "kevin", "brokerid", 1, "userid", 6, "name", "Kevin", "marketext", "D");
  db.hmset("user:7", "email", "louisa@thomasgrant.co.uk", "password", "louisa", "brokerid", 1, "userid", 7, "name", "Louisa", "marketext", "D");
  db.hmset("user:8", "email", "info@yearstretch.com", "password", "paul", "brokerid", 1, "userid", 8, "name", "Paul", "marketext", "D");*/

  // link between user email & id
  db.set("broker:1:user:terry@cw2t.com", "1");
  /*db.set("user:grant@thomasgrant.co.uk", "2");
  db.set("user:tina@thomasgrant.co.uk", "3");
  db.set("user:patrick@thomasgrant.co.uk", "4");
  db.set("user:sheila@thomasgrant.co.uk", "5");
  db.set("user:kevin@thomasgrant.co.uk", "6");
  db.set("user:louisa@thomasgrant.co.uk", "7");
  db.set("user:info@yearstretch.com", "8");*/

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
  /*db.sadd("cashtranstypes", "CD");
  db.sadd("cashtranstypes", "CW");
  db.sadd("cashtranstypes", "BT");
  db.sadd("cashtranstypes", "ST");
  db.sadd("cashtranstypes", "CO");
  db.sadd("cashtranstypes", "DI");
  db.sadd("cashtranstypes", "FI");
  db.sadd("cashtranstypes", "IN");
  db.sadd("cashtranstypes", "PL");
  db.sadd("cashtranstypes", "CC");
  db.sadd("cashtranstypes", "SD");
  db.sadd("cashtranstypes", "OT");*/

  /*db.set("cashtranstype:CD", "Cash Deposit");
  db.set("cashtranstype:CW", "Cash Withdrawal");
  db.set("cashtranstype:BT", "Buy Trade");
  db.set("cashtranstype:ST", "Sell Trade");
  db.set("cashtranstype:CO", "Commission");
  db.set("cashtranstype:DI", "Dividend");
  db.set("cashtranstype:FI", "Finance");
  db.set("cashtranstype:IN", "Interest");
  db.set("cashtranstype:PL", "PTM Levy");
  db.set("cashtranstype:CC", "Contract Charge");
  db.set("cashtranstype:SD", "Stamp Duty");
  db.set("cashtranstype:OT", "Other");*/

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
  db.set("trading:port", "60144");
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
