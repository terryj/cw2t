/****************
* startofday.js
* Reset fix sequence numbers
* Cantwaittotrade Limited
* Terry Johnston
* April 2014
****************/

var redis = require("redis");

// redis server
db = redis.createClient();
db.on("connect", function (err) {
  console.log("Connected to redis");
  resetNos();
});

db.on("error", function (err) {
  console.error(err);
});

function resetNos() {
  // we use incr to increment & use, so first will be 1
  db.set("fixseqnumin", 0);
  db.set("fixseqnumout", 0);

  db.get("fixseqnumin", function(err, fixseqnumin) {
    if (err) {
      console.log(err);
      return;
    }

    if (fixseqnumin == "0") {
      console.log("fix sequence numbers reset ok");
    } else {
      console.log("fix sequence numbers not reset");      
    }
    
    process.exit(0);
  });
}