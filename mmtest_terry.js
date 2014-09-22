/****************

* mmtest.js

* market maker testing only

* Cantwaittotrade Limited

* Paul J. Weighell

* September 2014

****************/

/* cw2t database */

var

redis = require("redis");
/* cw2t library */

var

common = require('./common.js');
console.log(

"MMTest starting.");
console.log(

"Creating REDIS client...");
db = redis.createClient(6379,

"127.0.0.1");
console.log(

"Creating REDIS client...Done.");
db.on(

"connect", function (err) {
console.log(

"MMTest Connected to REDIS.");
start();

});

/*we start here, if & when connected to redis*/

function

start() {
initialise();

awaywego();

}

/* any initialisation stuff */

function

initialise() {
/* register scripts */
console.log(

"registerCommonScripts...");
common.registerCommonScripts();

console.log(

"registerCommonScripts...Done.");
checkrediskeys();

pubsub();

}

/* make sure redis has keys we need*/

function

checkrediskeys() {
console.log(

"Creating REDIS keys...");
db.set(

"mmPosition", "50");
db.set(

"mmPositionCost", "90");
db.set(

"mmLimit", "100");
db.set(

"mmPriceAsk", "99");
db.set(

"mmPriceBid", "101");
db.set(

"mmProduct", "TEST.L");
db.set(

"mmRate", "0.05");
db.set(

"mmSpread", "2");
console.log(

"Creating REDIS keys...Done.");
}

/* set-up publish & subscribe connection */

function

pubsub() {
console.log(

"pubsub...");
dbsub = redis.createClient(6379,

"127.0.0.1");
dbsub.on(

"subscribe", function (channel, count) {
console.log(

"subscribed to:" + channel + ", num. channels:" + count);
});

dbsub.on(

"unsubscribe", function (channel, count) {
console.log(

"unsubscribed from:" + channel + ", num. channels:" + count);
});

/* receive published messages here */
dbsub.on(

"message", function (channel, message) {
console.log(message);

});

/* subscribe to any channels here */
/* dbsub.subscribe(pricechannel);*/
console.log(

"pubsub...Done.");
}

function

awaywego() {
console.log(

"testing price update...");
newPrice("AFS.L","101","99");
console.log(

"testing price update...Done.");
console.log(

"Up and running...");
}

function

newPrice(symbol, bid, ask) {
/* create a unix timestamp */
var timestamp = new Date().getTime();
/* update price & history */
console.log(

"scriptupdate symbol: " + symbol + " timestamp: " + timestamp + " bid: " + bid + " ask: " + ask);
db.eval(common.scriptpriceupdate, 4, symbol, timestamp, bid, ask,

function (err, ret) {
if (err) throw err;
console.log(

"pricehist updated: " + symbol);
});

}

function test() {

}

  scripttest = '\
  return "script test ok" \
  ';


 