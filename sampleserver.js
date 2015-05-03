/****************
* sampleserver.js
* Example server
* Cantwaittotrade Limited
* Terry Johnston
* April 2015
****************/

// node libraries

// external libraries
var redis = require('redis');

// cw2t libraries
var common = require('./common.js');

// redis
var redishost = "127.0.0.1";
var redisport = 6379;

// globals

// set-up a redis client
db = redis.createClient(redisport, redishost);
db.on("connect", function(err) {
  if (err) {
    console.log(err);
    return;
  }

  console.log("Connected to Redis at " + redishost + " port " + redisport);

  initialise();
  start();
});
db.on("error", function(err) {
  console.log(err);
});

function initialise() {
  console.log("initialising...");
  common.registerScripts();
  registerScripts();
}

//
// we start here
//
function start() {
  var userid = 1;

  // from the front-end
  var client = {};
  client.name = "john smith";
  client.email = "johnsmith@test.com"
  client.mobile = "07777 123456";
  client.address = "test road, london, abc123";
  client.ifaid = "";
  client.clienttype = 1;
  client.insttypes = ["CFD","DE"]
  client.hedge = 0;
  client.brokerclientcode = "abc123";
  client.commissionpercent = "";
  client.active = 1;

  newClient(client, userid)
}

//
// example of calling redis directly & calling a Lua script
//
function newClient(client, userid) {
  // get the broker for this client
  db.hget("user:" + userid, "brokerid", function(err, brokerid) {
    if (err) {
      console.log(err);
      return;
    }

    // create new client
    db.eval(common.scriptnewclient, 0, brokerid, client.name, client.email, client.mobile, client.address, client.ifaid, client.clienttype, client.insttypes, client.hedge, client.brokerclientcode, client.commissionpercent, client.active, function(err, ret) {
      if (err) throw err;

      if (ret[0] != 0) {
        console.log("Error in scriptnewclient: " + common.getErrorcode(ret[0]));

        // send error message to front-end
        // sendErrorMsg(ret[0], conn);
        return;
      }

      console.log("client #" + ret[1] + " set-up ok");

      // send new client to front-end
      client.clientid = ret[1];
      //...
    });
  });
}

// local scripts
function registerScripts() {
}