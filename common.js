/****************
* common.js
* Front-office common server functions
* Cantwaittotrade Limited
* Terry Johnston
* December 2013
****************/

function broadcastLevelOne(symbol, connections) {
  var bestbid = 0.00;
  var bestoffer = 0.00;
  var count;

  db.zrange(symbol, 0, -1, "WITHSCORES", function(err, orders) {
    if (err) {
      console.log(err);
      return;
    }

    count = orders.length;
    if (count == 0) {
      sendLevelOne(symbol, bestbid, bestoffer, connections);
      return;
    }

    orders.forEach(function (reply, i) {
      if (i % 2 != 0) {
        if (i == 1) { // first score
          if (reply < 0) {
            bestbid = -parseFloat(reply);
          } else if (reply > 0) {
            bestoffer = parseFloat(reply);
          }
        } else if (bestoffer == 0.00) {
          if (reply > 0) {
            bestoffer = parseFloat(reply);
          }
        }
      }

      count--;
      if (count <= 0) {
        sendLevelOne(symbol, bestbid, bestoffer, connections);
      }
    });
  });
}

exports.broadcastLevelOne = broadcastLevelOne;

/*function broadcastLevelTwo(symbol, connections) {
  var orderbook = {prices : []};
  var lastprice = 0;
  var lastside = 0;
  var firstbid = true;
  var firstoffer = true;
  var bidlevel = 0;
  var offerlevel = 0;
  var count;

  console.log("broadcastLevelTwo:"+symbol);

  orderbook.symbol = symbol;

  db.zrange(symbol, 0, -1, "WITHSCORES", function(err, orders) {
    if (err) {
      console.log("zrange error:" + err + ", symbol:" + symbol);
      return;
    }

    count = orders.length;
    if (count == 0) {
      // build & send a message showing no orders in the order book
      var levelbid = {};
      levelbid.bid = 0;
      levelbid.bidsize = 0;
      orderbook.prices[0] = levelbid;
      var leveloffer = {};
      leveloffer.offer = 0;
      leveloffer.offersize = 0;
      orderbook.prices[1] = leveloffer;

      if (conn != null) {
        conn.write("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
      } else {
        publishMessage("{\"orderbook\":" + JSON.stringify(orderbook) + "}", connections);
      }
      return;
    }

    orders.forEach(function (reply, i) {
      if (err) {
        console.log(err);
        return;
      }

      console.log(reply);

      if (i % 2 != 0) {
        // get order hash
        db.hgetall("order:" + orderid, function(err, order) {
          if (err) {
            console.log(err);
            return;
          }

          var level = {};

          if (order.price != lastprice || order.side != lastside) {
            if (parseInt(order.side) == 1) {
              level.bid = order.price;
              level.bidsize = parseInt(order.remquantity);
              orderbook.prices[bidlevel] = level;
              bidlevel++;
          } else {
            if (!firstoffer) {
              offerlevel++;
            } else {
              firstoffer = false;
            }

            if (offerlevel <= bidlevel && !firstbid) {
              orderbook.prices[offerlevel].offer = order.price;
              orderbook.prices[offerlevel].offersize = parseInt(order.remquantity);
            } else {
              level.offer = order.price;
              level.offersize = parseInt(order.remquantity);
              orderbook.prices[offerlevel] = level;
            }
          }

          lastprice = order.price;
          lastside = order.side;
        } else {
          if (parseInt(order.side) == 1) {
            orderbook.prices[bidlevel].bidsize += parseInt(order.remquantity);
          } else {
            orderbook.prices[offerlevel].offersize += parseInt(order.remquantity);
          }
        }

        count--;
        if (count <= 0) {
          if (conn != null) {
            conn.write("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
          } else {
            // broadcast to all interested parties
            publishMessage("{\"orderbook\":" + JSON.stringify(orderbook) + "}", connections);
          }
        }
      });
    });
  });
}

exports.broadcastLevelTwo = broadcastLevelTwo;*/

function sendCurrentOrderBook(symbol, topic, conn) {
  var orderbook = {prices : []};
  var level1 = {};
  var level2 = {};
  var level3 = {};
  var level4 = {};
  var level5 = {};
  var level6 = {};

  db.hgetall("topic:" + topic, function(err, topicrec) {
    if (err) {
      console.log(err);
      return;
    }

    if (!topicrec) {
      console.log("topic:" + topic + " not found");

      // send zeros
      topicrec = {};
      topicrec.bid1 = 0;
      topicrec.offer1 = 0;
      topicrec.bid2 = 0;
      topicrec.offer2 = 0;
      topicrec.bid3 = 0;
      topicrec.offer3 = 0;
    }

    // 3 levels
    level1.bid = topicrec.bid1;
    level1.level = 1;
    orderbook.prices.push(level1);
    level2.offer = topicrec.offer1;
    level2.level = 1;
    orderbook.prices.push(level2);
    level3.bid = topicrec.bid2;
    level3.level = 2;
    orderbook.prices.push(level3);
    level4.offer = topicrec.offer2;
    level4.level = 2;
    orderbook.prices.push(level4);
    level5.bid = topicrec.bid3;
    level5.level = 3;
    orderbook.prices.push(level5);
    level6.offer = topicrec.offer3;
    level6.level = 3;
    orderbook.prices.push(level6);

    orderbook.symbol = symbol;

    if (conn != null) {
      conn.write("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
    }
  });
}

exports.sendCurrentOrderBook = sendCurrentOrderBook;

function newPrice(topic, servertype, msg, connections) {
  // which symbols are subscribed to for this topic (may be more than 1 as covers derivatives)
  db.smembers("topic:" + topic + ":" + servertype + ":symbols", function(err, symbols) {
    if (err) throw err;

    symbols.forEach(function(symbol, i) {
      // build the message according to the symbol
      var jsonmsg = "{\"orderbook\":{\"symbol\":\"" + symbol + "\"," + msg + "}}";

      // get the users watching this symbol
      db.smembers("topic:" + topic + ":symbol:" + symbol + ":" + servertype, function(err, users) {
        if (err) throw err;

        // send the message to each user
        users.forEach(function(user, j) {
          if (user in connections) {
            connections[user].write(jsonmsg);
          }
        });
      });
    });
  });
}

exports.newPrice = newPrice;

//
// send level one to everyone
//
function sendLevelOne(symbol, bestbid, bestoffer, connections) {
  var msg = "{\"orderbook\":{\"symbol\":\"" + symbol + "\",\"prices\":[";
  msg += "{\"level\":1,\"bid\":" + bestbid + "}";
  msg += ",{\"level\":1,\"offer\":" + bestoffer + "}";
  msg += "]}}";
  publishMessage(msg, connections);
}

function publishMessage(message, connections) {
  // todo: alter to just cater for interested parties
  for (var c in connections) {
    if (connections.hasOwnProperty(c)) {
      connections[c].write(message);
    }
  }
}

exports.publishMessage = publishMessage;

//
// returns valid trading day as date object, taking into account weekends and holidays
// from passed date, number of settlement days (i.e. T+n) and list of holidays
//
function getSettDate(dt, nosettdays, holidays) {
  var days = 0;

  if (nosettdays > 0) {
    while (true) {
      // add a day
      dt.setDate(dt.getDate() + 1);

      // ignore weekends & holidays
      if (dt.getDay() == 6 || dt.getDay() == 0) {
      } else if (isHoliday(dt, holidays)) {
      } else {
        // add to days & check to see if we are there
        days++;

        if (days >= nosettdays) {
          break;
        }
      }
    }
  }

  return dt;
}

exports.getSettDate = getSettDate;

function isHoliday(datetocheck, holidays) {
  var found = false;

  var datetocheckstr = getUTCDateString(datetocheck);

  if (datetocheckstr in holidays) {
    found = true;
  }

  return found;
}

//
// returns a UTC datetime string from a passed date object
//
function getUTCTimeStamp(timestamp) {
    var year = timestamp.getUTCFullYear();
    var month = timestamp.getUTCMonth() + 1; // flip 0-11 -> 1-12
    var day = timestamp.getUTCDate();
    var hours = timestamp.getUTCHours();
    var minutes = timestamp.getUTCMinutes();
    var seconds = timestamp.getUTCSeconds();
    //var millis = timestamp.getUTCMilliseconds();

    if (month < 10) {month = '0' + month;}

    if (day < 10) {day = '0' + day;}

    if (hours < 10) {hours = '0' + hours;}

    if (minutes < 10) {minutes = '0' + minutes;}

    if (seconds < 10) {seconds = '0' + seconds;}

    /*if (millis < 10) {
        millis = '00' + millis;
    } else if (millis < 100) {
        millis = '0' + millis;
    }*/

    //var ts = [year, month, day, '-', hours, ':', minutes, ':', seconds, '.', millis].join('');
    var ts = [year, month, day, '-', hours, ':', minutes, ':', seconds].join('');

    return ts;
}

exports.getUTCTimeStamp = getUTCTimeStamp;

//
// get a UTC date string from a passed date object
//
function getUTCDateString(date) {
    var year = date.getUTCFullYear();
    var month = date.getUTCMonth() + 1; // flip 0-11 -> 1-12
    var day = date.getUTCDate();

    if (month < 10) {month = '0' + month;}

    if (day < 10) {day = '0' + day;}

    var utcdate = "" + year + month + day;

    return utcdate;
}

exports.getUTCDateString = getUTCDateString;

function getReasonDesc(reason) {
  var desc;

  switch (parseInt(reason)) {
  case 1001:
    desc = "No currency held for this instrument";
    break;
  case 1002:
    desc = "Insufficient cash in settlement currency";
    break;
  case 1003:
    desc = "No position held in this instrument";
    break;
  case 1004:
    desc = "Insufficient position size in this instrument";
    break;
  case 1005:
    desc = "System error";
    break;
  case 1006:
    desc = "Invalid order";
    break;
  case 1007:
    desc = "Invalid instrument";
    break;
  case 1008:
    desc = "Order already cancelled";
    break;
  case 1009:
    desc = "Order not found";
    break;
  case 1010:
    desc = "Order already filled";
    break;
  case 1011:
    desc = "Order currency does not match symbol currency";
    break;
  case 1012:
    desc = "Order already rejected";
    break;
  case 1013:
    desc = "Ordercancelrequest not found";
    break;
  case 1014:
    desc = "Quoterequest not found";
    break;
  case 1015:
    desc = "Symbol not found";
    break;
  case 1016:
    desc = "Proquote symbol not found";
    break;
  case 1017:
    desc = "Client not found";
    break;
  case 1018:
    desc = "Client not authorised to trade this type of product";
    break;
  case 1019:
    desc = "Quantity greater than position quantity";
    break;
  case 1020:
    desc = "Insufficient free margin";
    break;
  case 1021:
    desc = "Order held externally";
    break;
  case 1022:
    desc = "Order already expired";
    break;
  default:
    desc = "Unknown reason";
  }

  return desc;
}

exports.getReasonDesc = getReasonDesc;

//
// Get the nuber of seconds between two UTC datetimes
//
function getSeconds(startutctime, finishutctime) {
  var startdt = new Date(getDateString(startutctime));
  var finishdt = new Date(getDateString(finishutctime));
  return ((finishdt - startdt) / 1000);
}

exports.getSeconds = getSeconds;

//
// Convert a UTC datetime to a valid string for creating a date object
//
function getDateString(utcdatetime) {
    return (utcdatetime.substr(0,4) + "/" + utcdatetime.substr(4,2) + "/" + utcdatetime.substr(6,2) + " " + utcdatetime.substr(9,8));
}

exports.getDateString = getDateString;

//
// Convert a "yyyymmdd" date string to a date object
//
function dateFromUTCString(utcdatestring) {
  var dt = new Date(utcdatestring.substr(0,4), utcdatestring.substr(4,2) - 1, utcdatestring.substr(6,2));
  return dt;
}

exports.dateFromUTCString = dateFromUTCString;

function getPTPQuoteRejectReason(reason) {
  var desc;

  switch (parseInt(reason)) {
  case 1:
    desc = "Unknown symbol";
    break;
  case 2:
    desc = "Exchange closed";
    break;
  case 3:
    desc = "Quote Request exceeds limit";
    break;
  case 4:
    desc = "Too late to enter";
    break;
  case 5:
    desc = "Unknown Quote";
    break;
  case 6:
    desc = "Duplicate Quote";
    break;
  case 7:
    desc = "Invalid bid/ask spread";
    break;
  case 8:
    desc = "Invalid price";
    break;
  case 9:
    desc = "Not authorized to quote security";
    break;
  default:
    desc = "Unknown reason";
  }

  return desc;
}

exports.getPTPQuoteRejectReason = getPTPQuoteRejectReason;

function getPTPOrderCancelRejectReason(reason) {
  var desc;

  switch (parseInt(reason)) {
  case 0:
    desc = "Too late to cancel";
    break;
  case 1:
    desc = "Unknown order";
    break;
  case 2:
    desc = "Broker Option";
    break;
  case 3:
    desc = "Order already in Pending Cancel or Pending Replace status";
    break;
  }

  return desc;
}

exports.getPTPOrderCancelRejectReason = getPTPOrderCancelRejectReason;


exports.registerCommonScripts = function () {
	var subscribeinstrument;
	var unsubscribeinstrument;
  var updatecash;
  var gettotalpositions;
  var getcash;
  var getfreemargin;
  var getunrealisedpandl;
  var calcfinance;
  var round;
  var gettrades;
  var getquoterequests;

  round = '\
  local round = function(num, dp) \
    local mult = 10 ^ (dp or 0) \
    return math.floor(num * mult + 0.5) / mult \
  end \
  ';

  exports.round = round;

  updatecash = '\
  local updatecash = function(clientid, currency, transtype, amount, drcr, desc, reference, timestamp, settldate, operatortype, operatorid) \
    amount = tonumber(amount) \
    if amount == 0 then \
      return {0, 0} \
    end \
    local cashtransid = redis.call("incr", "cashtransid") \
    if not cashtransid then return {1005} end \
    redis.call("hmset", "cashtrans:" .. cashtransid, "clientid", clientid, "currency", currency, "transtype", transtype, "amount", amount, "drcr", drcr, "description", desc, "reference", reference, "timestamp", timestamp, "settldate", settldate, "operatortype", operatortype, "operatorid", operatorid, "cashtransid", cashtransid) \
    redis.call("sadd", clientid .. ":cashtrans", cashtransid) \
    local cashkey = clientid .. ":cash:" .. currency \
    local cashskey = clientid .. ":cash" \
    local cash = redis.call("get", cashkey) \
    --[[ adjust for credit]] \
    if tonumber(drcr) == 2 then \
      amount = -amount \
    end \
    if not cash then \
      redis.call("set", cashkey, amount) \
      redis.call("sadd", cashskey, currency) \
    else \
      local adjamount = tonumber(cash) + amount \
      if adjamount == 0 then \
        redis.call("del", cashkey) \
        redis.call("srem", cashskey, currency) \
      else \
        redis.call("set", cashkey, adjamount) \
      end \
    end \
    return {0, cashtransid} \
  end \
  ';

  exports.updatecash = updatecash;

  getcash = '\
  local getcash = function(clientid, currency) \
    local cash = redis.call("get", clientid .. ":cash:" .. currency) \
    if not cash then \
      cash = 0 \
    end \
    return cash \
  end \
  ';

  exports.getcash = getcash;

  calcfinance = round + '\
  local calcfinance = function(instrumenttype, consid, currency, side, nosettdays) \
    local finance = 0 \
    local costkey = "cost:" .. instrumenttype .. ":" .. currency .. ":" .. side \
    local financerate = redis.call("hget", costkey, "finance") \
    if financerate and tonumber(financerate) ~= nil then \
      local daystofinance = 0 \
      --[[ nosettdays = 0 represents rolling settlement, so set it to 1 day for interest calculation ]] \
      if tonumber(nosettdays) == 0 then \
        daystofinance = 1 \
      else \
        local defaultnosettdays = redis.call("hget", costkey, "defaultnosettdays") \
        if defaultnosettdays and tonumber(defaultnosettdays) ~= nil then \
          defaultnosettdays = tonumber(defaultnosettdays) \
        else \
          defaultnosettdays = 0 \
        end \
        if tonumber(nosettdays) > defaultnosettdays then \
          daystofinance = tonumber(nosettdays) - defaultnosettdays \
        end \
      end \
      finance = round(consid * daystofinance / 365 * tonumber(financerate) / 100, 2) \
   end \
   return finance \
  end \
  ';

  exports.calcfinance = calcfinance;

  getunrealisedpandl = '\
  local getunrealisedpandl = function(symbol, quantity, side, avgcost) \
    local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    if not topic then return {0, 0} end \
    --[[ get delayed topic - todo: review ]] \
    topic = topic .. "D" \
    local bidprice = redis.call("hget", "topic:" .. topic, "bid1") \
    local offerprice = redis.call("hget", "topic:" .. topic, "offer1") \
    local unrealisedpandl = 0 \
    local price = 0 \
    local qty = tonumber(quantity) \
    if tonumber(side) == 1 then \
      if bidprice and tonumber(bidprice) ~= 0 then \
        price = tonumber(bidprice) / 100 \
      end \
    else \
      if offerprice and tonumber(offerprice) ~= 0 then \
        price = tonumber(offerprice) / 100 \
      end \
      --[[ take account of short position ]] \
      qty = -qty \
    end \
    if price ~= 0 then \
      unrealisedpandl = qty * (price - tonumber(avgcost)) \
    end \
    return {unrealisedpandl, price} \
  end \
  ';

  exports.getunrealisedpandl = getunrealisedpandl;

  // only dealing with trade currency for the time being - todo: review
  gettotalpositions = getunrealisedpandl + '\
  local gettotalpositions = function(clientid, currency) \
    local positions = redis.call("smembers", clientid .. ":positions") \
    local fields = {"symbol", "currency", "quantity", "margin", "averagecostpershare", "realisedpandl", "side"} \
    local vals \
    local totalmargin = 0 \
    local totalrealisedpandl = 0 \
    local totalunrealisedpandl = 0 \
    for index = 1, #positions do \
      vals = redis.call("hmget", clientid .. ":position:" .. positions[index], unpack(fields)) \
      if vals[2] == currency then \
        totalmargin = totalmargin + tonumber(vals[4]) \
        totalrealisedpandl = totalrealisedpandl + tonumber(vals[6]) \
        local unrealisedpandl = getunrealisedpandl(vals[1], vals[3], vals[7], vals[5]) \
        totalunrealisedpandl = totalunrealisedpandl + unrealisedpandl[1] \
      end \
    end \
    return {totalmargin, totalrealisedpandl, totalunrealisedpandl} \
  end \
  ';

  exports.gettotalpositions = gettotalpositions;

  getfreemargin = getcash + gettotalpositions + '\
  local getfreemargin = function(clientid, currency) \
    local cash = getcash(clientid, currency) \
    local totalpositions = gettotalpositions(clientid, currency) \
    --[[ totalpositions[1] = margin, [2] = realised p&l, [3] = unrealised p&l ]] \
    local balance = tonumber(cash) + totalpositions[2] \
    local equity = balance + totalpositions[3] \
    local freemargin = equity - totalpositions[1] \
    return freemargin \
  end \
  ';

  exports.getfreemargin = getfreemargin;

  //
  // get a range of trades from passed ids
  //
  gettrades = '\
  local gettrades = function(trades) \
    local tblresults = {} \
    local fields = {"clientid","orderid","symbol","side","quantity","price","currency","currencyratetoorg","currencyindtoorg","commission","ptmlevy","stampduty","contractcharge","counterpartyid","markettype","externaltradeid","futsettdate","timestamp","lastmkt","externalorderid","tradeid","settlcurrency","settlcurramt","settlcurrfxrate","settlcurrfxratecalc","nosettdays","margin","finance"} \
    local vals \
    for index = 1, #trades do \
      vals = redis.call("hmget", "trade:" .. trades[index], unpack(fields)) \
      table.insert(tblresults, {clientid=vals[1],orderid=vals[2],symbol=vals[3],side=vals[4],quantity=vals[5],price=vals[6],currency=vals[7],currencyratetoorg=vals[8],currencyindtoorg=vals[9],commission=vals[10],ptmlevy=vals[11],stampduty=vals[12],contractcharge=vals[13],counterpartyid=vals[14],markettype=vals[15],externaltradeid=vals[16],futsettdate=vals[17],timestamp=vals[18],lastmkt=vals[19],externalorderid=vals[20],tradeid=vals[21],settlcurrency=vals[22],settlcurramt=vals[23],settlcurrfxrate=vals[24],settlcurrfxratecalc=vals[25],nosettdays=vals[26],margin=vals[27],finance=vals[28]}) \
    end \
    return tblresults \
  end \
  ';

  exports.gettrades = gettrades;

  getquoterequests = '\
  local getquoterequests = function(quoterequests) \
    local tblresults = {} \
    local fields = {"clientid","symbol","quantity","cashorderqty","currency","settlcurrency","nosettdays","futsettdate","quotestatus","timestamp","quoteid","quoterejectreason","quotereqid","operatortype","operatorid"} \
    local vals \
    for index = 1, #quoterequests do \
      vals = redis.call("hmget", "quoterequest:" .. quoterequests[index], unpack(fields)) \
      table.insert(tblresults, {clientid=vals[1],symbol=vals[2],quantity=vals[3],cashorderqty=vals[4],currency=vals[5],settlcurrency=vals[6],nosettdays=vals[7],futsettdate=vals[8],quotestatus=vals[9],timestamp=vals[10],quoteid=vals[11],quoterejectreason=vals[12],quotereqid=vals[13],operatortype=vals[14],operatorid=vals[15]}) \
    end \
    return tblresults \
  end \
  ';

  exports.getquoterequests = getquoterequests;

	subscribeinstrument = '\
  local subscribeinstrument = function(symbol, id, servertype) \
    local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    if not topic then \
      return {0, ""} \
    end \
    local marketext = redis.call("hget", servertype .. ":" .. id, "marketext") \
    if marketext ~= nil then \
      	topic = topic .. marketext \
    end \
    redis.call("sadd", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols", symbol) \
    redis.call("sadd", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype, id) \
    redis.call("sadd", "topic:" .. topic .. ":" .. servertype .. ":symbols", symbol) \
    local needtosubscribe = 0 \
    if redis.call("scard", "topic:" .. topic .. ":" .. servertype) == 0 then \
      redis.call("sadd", "topic:" .. topic .. ":" .. servertype, id) \
      needtosubscribe = 1 \
      if redis.call("scard", "topic:" .. topic .. ":servers") == 0 then \
        redis.call("publish", "proquote", "subscribe:" .. topic) \
      end \
      redis.call("sadd", "topic:" .. topic .. ":servers", servertype) \
    end \
    redis.call("sadd", "topics", topic) \
    redis.call("sadd", "server:" .. servertype .. ":topics", topic) \
    redis.call("sadd", servertype .. ":" .. id .. ":topics", topic) \
    return {needtosubscribe, topic} \
  end \
  ';

  exports.subscribeinstrument = subscribeinstrument;

  unsubscribeinstrument = '\
  local unsubscribeinstrument = function(symbol, id, servertype) \
    local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    if not topic then \
      return {0, ""} \
    end \
    local marketext = redis.call("hget", servertype .. ":" .. id, "marketext") \
    if marketext ~= nil then \
      topic = topic .. marketext \
    end \
    local needtounsubscribe = 0 \
    redis.call("srem", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype, id) \
    redis.call("srem", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols", symbol) \
    if redis.call("scard", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype) == 0 then \
    	redis.call("srem", "topic:" .. topic .. ":" .. servertype .. ":symbols", symbol) \
    end \
   	if redis.call("scard", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols") == 0 then \
      redis.call("srem", servertype .. ":" .. id .. ":topics", topic) \
      redis.call("srem", "topic:" .. topic .. ":" .. servertype, id) \
      if redis.call("scard", "topic:" .. topic .. ":" .. servertype) == 0 then \
        needtounsubscribe = 1 \
        redis.call("srem", "topic:" .. topic .. ":servers", servertype) \
      	redis.call("srem", "server:" .. servertype .. ":topics", topic) \
      	if redis.call("scard", "topic:" .. topic .. ":servers") == 0 then \
          redis.call("publish", "proquote", "unsubscribe:" .. topic) \
      		redis.call("srem", "topics", topic) \
        end \
      end \
    end \
    return {needtounsubscribe, topic} \
  end \
  ';

  //
  // a cash transaction
  //
  exports.scriptcashtrans = updatecash + '\
  local ret = updatecash(KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8], KEYS[9], KEYS[10], KEYS[11]) \
  return ret \
  ';

  //
  // pass client id
  //
  exports.scriptgetquoterequests = getquoterequests + '\
  local quoterequests = redis.call("smembers", KEYS[1] .. ":quoterequests") \
  local tblresults = getquoterequests(quoterequests) \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id
  //
  exports.scriptgetquotes = '\
  local tblresults = {} \
  local quotes = redis.call("smembers", KEYS[1] .. ":quotes") \
  local fields = {"quotereqid","clientid","quoteid","bidquoteid","offerquoteid","symbol","bestbid","bestoffer","bidpx","offerpx","bidquantity","offerquantity","bidsize","offersize","validuntiltime","transacttime","currency","settlcurrency","bidqbroker","offerqbroker","nosettdays","futsettdate","bidfinance","offerfinance","orderid"} \
  local vals \
  for index = 1, #quotes do \
    vals = redis.call("hmget", "quote:" .. quotes[index], unpack(fields)) \
    table.insert(tblresults, {quotereqid=vals[1],clientid=vals[2],quoteid=vals[3],bidquoteid=vals[4],offerquoteid=vals[5],symbol=vals[6],bestbid=vals[7],bestoffer=vals[8],bidpx=vals[9],offerpx=vals[10],bidquantity=vals[11],offerquantity=vals[12],bidsize=vals[13],offersize=vals[14],validuntiltime=vals[15],transacttime=vals[16],currency=vals[17],settlcurrency=vals[18],bidqbroker=vals[19],offerqbroker=vals[20],nosettdays=vals[21],futsettdate=vals[22],bidfinance=vals[23],offerfinance=vals[24],orderid=vals[25]}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id
  //
  exports.scriptgetorders = '\
  local tblresults = {} \
  local orders = redis.call("smembers", KEYS[1] .. ":orders") \
  local fields = {"clientid","symbol","side","quantity","price","ordertype","remquantity","status","markettype","futsettdate","partfill","quoteid","currency","currencyratetoorg","currencyindtoorg","timestamp","margin","timeinforce","expiredate","expiretime","settlcurrency","settlcurrfxrate","settlcurrfxratecalc","orderid","externalorderid","execid","nosettdays","operatortype","operatorid","hedgeorderid","reason","text"} \
  local vals \
  for index = 1, #orders do \
    vals = redis.call("hmget", "order:" .. orders[index], unpack(fields)) \
    table.insert(tblresults, {clientid=vals[1],symbol=vals[2],side=vals[3],quantity=vals[4],price=vals[5],ordertype=vals[6],remquantity=vals[7],status=vals[8],markettype=vals[9],futsettdate=vals[10],partfill=vals[11],quoteid=vals[12],currency=vals[13],currencyratetoorg=vals[14],currencyindtoorg=vals[15],timestamp=vals[16],margin=vals[17],timeinforce=vals[18],expiredate=vals[19],expiretime=vals[20],settlcurrency=vals[21],settlcurrfxrate=vals[22],settlcurrfxratecalc=vals[23],orderid=vals[24],externalorderid=vals[25],execid=vals[26],nosettdays=vals[27],operatortype=vals[28],operatorid=vals[29],hedgeorderid=vals[30],reason=vals[31],text=vals[32]}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id
  //
  exports.scriptgettrades = gettrades + '\
  local trades = redis.call("smembers", KEYS[1] .. ":trades") \
  local tblresults = gettrades(trades) \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id, positionkey
  //
  exports.scriptgetpostrades = gettrades + '\
  local postrades = redis.call("smembers", KEYS[1] .. ":trades:" .. KEYS[2]) \
  local tblresults = gettrades(postrades) \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id
  // todo: need to guarantee order
  exports.scriptgetcashhistory = '\
  local tblresults = {} \
  local cashhistory = redis.call("smembers", KEYS[1] .. ":cashtrans") \
  local fields = {"clientid","currency","amount","transtype","drcr","description","reference","timestamp","settldate","cashtransid"} \
  local vals \
  local balance = 0 \
  for index = 1, #cashhistory do \
    vals = redis.call("hmget", "cashtrans:" .. cashhistory[index], unpack(fields)) \
    --[[ match the currency ]] \
    if vals[2] == KEYS[2] then \
      --[[ adjust balance according to debit/credit ]] \
      if tonumber(vals[5]) == 1 then \
        balance = balance + tonumber(vals[3]) \
      else \
        balance = balance - tonumber(vals[3]) \
      end \
      table.insert(tblresults, {datetime=vals[1],currency=vals[2],amount=vals[3],transtype=vals[4],drcr=vals[5],description=vals[6],reference=vals[7],timestamp=vals[8],settldate=vals[9],cashtransid=vals[10],balance=balance}) \
    end \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id
  //
  exports.scriptgetpositions = getunrealisedpandl + '\
  local tblresults = {} \
  local positions = redis.call("smembers", KEYS[1] .. ":positions") \
  local fields = {"clientid","symbol","side","quantity","cost","currency","margin","positionid","averagecostpershare","realisedpandl"} \
  local vals \
  for index = 1, #positions do \
    vals = redis.call("hmget", KEYS[1] .. ":position:" .. positions[index], unpack(fields)) \
    --[[ value the position ]] \
    local unrealisedpandl = getunrealisedpandl(vals[2], vals[4], vals[3], vals[9]) \
    table.insert(tblresults, {clientid=vals[1],symbol=vals[2],side=vals[3],quantity=vals[4],cost=vals[5],currency=vals[6],margin=vals[7],positionid=vals[8],averagecostpershare=vals[9],realisedpandl=vals[10],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id
  //
  exports.scriptgetcash = '\
  local tblresults = {} \
  local cash = redis.call("smembers", KEYS[1] .. ":cash") \
  for index = 1, #cash do \
    local amount = redis.call("get", KEYS[1] .. ":cash:" .. cash[index]) \
    table.insert(tblresults, {currency=cash[index],amount=amount}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // assumes there is cash for any currency with positions
  //
  exports.scriptgetaccount = gettotalpositions + '\
  local tblresults = {} \
  local cash = redis.call("smembers", KEYS[1] .. ":cash") \
  for index = 1, #cash do \
    local amount = redis.call("get", KEYS[1] .. ":cash:" .. cash[index]) \
    local totalpositions = gettotalpositions(KEYS[1], cash[index]) \
    local balance = tonumber(amount) + totalpositions[2] \
    local equity = balance + totalpositions[3] \
    local freemargin = equity - totalpositions[1] \
    table.insert(tblresults, {currency=cash[index],cash=amount,realisedpandl=totalpositions[2],balance=balance,unrealisedpandl=totalpositions[3],equity=equity,margin=totalpositions[1],freemargin=freemargin}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // subscribe to a new instrument
  // params: symbol, client/user id, servertype
  //
  exports.scriptsubscribeinstrument = subscribeinstrument + '\
  redis.call("sadd", "orderbook:" .. KEYS[1] .. ":" .. KEYS[3], KEYS[2]) \
  redis.call("sadd", KEYS[3] .. ":" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  local ret = subscribeinstrument(KEYS[1], KEYS[2], KEYS[3]) \
  return ret \
  ';

  //
  // unsubscribe from an instrument
  // params: symbol, client/user id, servertype
  //
  exports.scriptunsubscribeinstrument = unsubscribeinstrument + '\
  redis.call("srem", "orderbook:" .. KEYS[1] .. ":" .. KEYS[3], KEYS[2]) \
  redis.call("srem", KEYS[3] .. ":" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  local ret = unsubscribeinstrument(KEYS[1], KEYS[2], KEYS[3]) \
  return ret \
  ';

  //
  // unsubscribe a user/client/other connection
  // params: servertype, client/user id
  //
  exports.scriptunsubscribeid = unsubscribeinstrument + '\
  local orderbooks = redis.call("smembers", KEYS[2] .. ":" .. KEYS[1] .. ":orderbooks") \
  local unsubscribetopics = {} \
  for i = 1, #orderbooks do \
    local ret = unsubscribeinstrument(orderbooks[i], KEYS[1], KEYS[2]) \
    if ret[1] == 1 then \
      table.insert(unsubscribetopics, ret[2]) \
    end \
  end \
  return unsubscribetopics \
  ';

  	//
  	// unsubscribe everything for a server type
  	// params: servertype
  	//
  	/*exports.scriptunsubscribeserver = '\
  	local topics = redis.call("smembers", "server:" .. KEYS[1] .. ":topics") \
  	for i = 1, #topics do \
  		local symbols = redis.call("smembers", "topic:" .. topics[i] .. ":" .. KEYS[1] .. ":symbols") \
  		for j = 1, #symbols do \
    		local users = redis.call("smembers", "topic:" .. topics[i] .. ":symbol:" .. symbols[j] .. ":" .. KEYS[1]) \
    		for k = 1, #users do \
     			redis.call("srem", "topic:" .. topics[i] .. ":symbol:" .. symbols[j] .. ":" .. KEYS[1], users[k]) \
    			redis.call("srem", "topic:" .. topics[i] .. ":" .. KEYS[1] .. ":" .. users[k] .. ":symbols", symbols[j]) \
    		end \
    		redis.call("srem", "topic:" .. topics[i] .. ":" .. KEYS[1] .. ":symbols", symbols[j]) \
  		end \
  		local connections = redis.call("smembers", "topic:" .. topics[i] .. ":" .. KEYS[1]) \
  		for j = 1, #connections do \
  			redis.call("srem", "topic:" .. topics[i] .. ":" .. KEYS[1], connections[j]) \
      		redis.call("srem", KEYS[1] .. ":" .. connections[j] .. ":topics", topics[i]) \
  		end \
  		redis.call("srem", "topic:" .. topics[i] .. ":servers", KEYS[1]) \
  		redis.call("srem", "server:" .. KEYS[1] .. ":topics", topics[i]) \
  	end \
  	';

  	exports.scriptunsubscribeserver = '\
  	local connections = redis.call("smembers", "connections:" + KEYS[1]) \
  	for i = 1, #connections do \
  	end \
  	';*/

  //
  // get holidays for a market, i.e. "L" = London...assume "L" for the time being?
  //
  exports.scriptgetholidays = '\
  local tblresults = {} \
  local holidays = redis.call("smembers", "holidays:" .. KEYS[1]) \
  for index = 1, #holidays do \
    table.insert(tblresults, {holidays[index]}) \
  end \
  return cjson.encode(tblresults) \
  ';
};
