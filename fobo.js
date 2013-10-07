/****************
* fobo.js
* Front-office to Back-office link
* Cantwaittotrade Limited
* Terry Johnston
* October 2013
****************/

// node libraries
var http = require('http');

// external libraries
var redis = require('redis');

// globals
var options = {
  host: 'ec2-54-235-66-162.compute-1.amazonaws.com',
  //port: 80,
  //path: '',///focalls',
  //method: 'GET'
};
var bointerval;

// redis
var redishost = "127.0.0.1";
var redisport = 6379;

// set-up a redis client
db = redis.createClient(redisport, redishost);
db.on("connect", function(err) {
  if (err) {
    console.log(err);
    return;
  }

  console.log("Connected to Redis at " + redishost + " port " + redisport);

  startBOInterval();
});

db.on("error", function(err) {
  console.log(err);
});

// run every time interval to download transactions to the back-ofice
function startBOInterval() {
  bointerval = setInterval(bodump, 10000);
  console.log("Interval timer started");
}

callback = function(res) {
  var str = '';

  res.on('data', function(chunk) {
    str += chunk;
  });

  res.on('end', function() {
    console.log(str);
  });
}

function bodump() {
  console.log("dumping to bo");
  
  // get all the orders waiting to be sent to the back-office
  db.smembers("orders", function(err, orderids) {
    if (err) {
      console.log("Error in unsubscribeTopics:" + err);
      return;
    }

    // send each one
    orderids.forEach(function(orderid, i) {
      db.hgetall("order:" + orderid, function(err, order) {
        if (err) {
          console.log(err);
          return;
        }

        if (order == null) {
          console.log("Order #" + orderid + " not found");
          return;
        }

        sendOrder(order);
      });
    });
  });
}

/* http://ec2-54-235-66-162.compute-1.amazonaws.com/focalls/orderadd.aspx?
msgid=123&
ackid=LM125635115526208600752&
clientid=1765&currency=GBP&
currencyindtoorg=*&
currencyratetoorg=1.2345&
expiredate=20130808&
expiretime=08:51:26&
externalorderid=8aDnmVca9YI7lLkQhTpHfVKxdoQx2z&
futsettdate=20130802&
isin=GB00B1XZS820&
margin=12.34&
markettype=1&
orderid=62&
ordertype=D&
orgid=1&
partfill=1&
price=3.388&
quantity=295&
quoteid=38&
reason=1001&
reasontext=hgfdsa&
remquantity=0&
side=1&
status=B&
timeinforce=0&
timestamp=20130808-08:51:26&
userid=33
*/

function sendOrder(order) {
  var str = "/focalls/orderadd.aspx?"
          + "msgid=125" + "&"
          + "orgid=" + order.orgid + "&"
          + "clientid=" + order.clientid + "&"
          + "symbol=" + order.symbol + "&"
          + "side=" + order.side + "&"
          + "quantity=" + order.quantity + "&"
          + "price=" + order.price + "&"
          + "ordertype=" + order.ordertype + "&"
          + "remquantity=" + order.remquantity + "&"
          + "status=" + order.status + "&"
          + "reason=" + order.reason + "&"
          + "markettype=" + order.markettype + "&"
          + "futsettdate=" + order.futsettdate + "&"
          + "partfill=" + order.partfill + "&"
          + "quoteid=" + order.quoteid + "&"
          + "currency=" + order.currency + "&"
          + "timestamp=" + order.timestamp + "&"
          + "margin=" + order.margin + "&"
          + "timeinforce=" + order.timeinforce + "&"
          + "expiredate=" + order.expiredate + "&"
          + "expiretime=" + order.expiretime + "&"
          + "settlcurrency=" + order.settlcurrency + "&"
          + "text=" + order.text + "&"
          + "orderid=" + order.orderid + "&";
          + "ackid=" + order.ackid + "&"
          + "currencyindtoorg=" + order.currencyindtoorg + "&"
          + "currencyratetoorg=" + order.currencyratetoorg;

  console.log(str);

  // make the request
  options.path = str;

  http.request(options, function(res) {
    callback(res);
  }).on('error', function(e) {
    console.log(e);
  }).end();
}
