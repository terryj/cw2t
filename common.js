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

function sendCurrentOrderbook(symbol, topic, conn, feedtype) {
  if (feedtype == "proquote") {
    sendCurrentOrderbookPQ(symbol, topic, conn);
  } else if (feedtype == "digitallook") {
    sendCurrentOrderbookDL(symbol, topic, conn);
  } else if (feedtype == "nbtrader") {
    sendCurrentOrderbookNBT(symbol, topic, conn);
  }
}

exports.sendCurrentOrderbook = sendCurrentOrderbook;

function sendCurrentOrderbookPQ(symbol, topic, conn) {
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

function sendCurrentOrderbookDL(symbol, topic, conn) {
  var orderbook = {prices : []};
  var level1 = {};
  var level2 = {};
  var level3 = {};
  var level4 = {};
  var level5 = {};
  var level6 = {};

  db.hgetall("ticker:" + topic, function(err, topicrec) {
    if (err) {
      console.log(err);
      return;
    }

    if (!topicrec) {
      console.log("ticker:" + topic + " not found");

      // send zeros
      topicrec = {};
      topicrec.bid = 0;
      topicrec.offer = 0;
    }

    // 3 levels
    level1.bid = topicrec.bid;
    level1.level = 1;
    orderbook.prices.push(level1);
    level2.offer = topicrec.offer;
    level2.level = 1;
    orderbook.prices.push(level2);
    level3.bid = 0;
    level3.level = 2;
    orderbook.prices.push(level3);
    level4.offer = 0;
    level4.level = 2;
    orderbook.prices.push(level4);
    level5.bid = 0;
    level5.level = 3;
    orderbook.prices.push(level5);
    level6.offer = 0;
    level6.level = 3;
    orderbook.prices.push(level6);

    orderbook.symbol = symbol;

    if (conn != null) {
      conn.write("{\"orderbook\":" + JSON.stringify(orderbook) + "}");
    }
  });
}

function sendCurrentOrderbookNBT(symbol, topic, conn) {
  // prices are stored against 'topic', as in the symbol used by the feed, so this is used to look up a price
  db.hgetall("price:" + topic, function(err, price) {
    if (err) {
      console.log(err);
      return;
    }

    var msg;

    if (!price) {
      var today = new Date();
      var timestamp = getUTCTimeStamp(today);

      msg = "{\"prices\":[{\"symbol\":\"" + symbol + "\",\"bid\":" + "0.00" + "\",\"ask\":" + "0.00" + ",\"level\":" + "1" + ",\"timestamp\":" + timestamp + "}]}";
    } else {
      msg = "{\"prices\":[{\"symbol\":\"" + symbol + "\",\"bid\":" + price.bid + "\",\"ask\":" + price.ask + ",\"level\":" + "1" + ",\"timestamp\":" + price.timestamp + "}]}";
    }

    if (conn != null) {
      conn.write(msg);
    }
  });
}

function sendErrorMsg(error, conn) {
  conn.write("{\"errormsg\":" + JSON.stringify(getReasonDesc(error)) + "}");
}

exports.sendErrorMsg = sendErrorMsg;

function newPrice(topic, serverid, msg, connections, feedtype) {
  if (feedtype == "proquote") {
    // which symbols are subscribed to for this topic (may be more than 1 as covers derivatives)
    db.smembers("topic:" + topic + ":" + serverid + ":symbols", function(err, symbols) {
      if (err) throw err;

      symbols.forEach(function(symbol, i) {
        // build the message according to the symbol
        var jsonmsg = "{\"orderbook\":{\"symbol\":\"" + symbol + "\"," + msg + "}}";

        // get the users watching this symbol
        db.smembers("topic:" + topic + ":symbol:" + symbol + ":" + serverid, function(err, ids) {
          if (err) throw err;

          // send the message to each user
          ids.forEach(function(id, j) {
            if (id in connections) {
              connections[id].write(jsonmsg);
            }
          });
        });
      });
    });
  } else if (feedtype == "digitallook") {
    // which symbols are subscribed to for this topic (may be more than 1 as covers derivatives)
    db.smembers(topic + ":" + serverid + ":symbols", function(err, symbols) {
      if (err) throw err;

      symbols.forEach(function(symbol, i) {
        // build the message according to the symbol
        var jsonmsg = "{\"orderbook\":{\"symbol\":\"" + symbol + "\"," + msg + "}}";

        // get the users watching this symbol
        db.smembers(topic + ":symbol:" + symbol + ":" + serverid, function(err, ids) {
          if (err) throw err;

          // send the message to each user
          ids.forEach(function(id, j) {
            if (id in connections) {
              connections[id].write(jsonmsg);
            }
          });
        });
      });
    });
  } else if (feedtype == "nbtrader") {
    // get the users watching this symbol on this server
    db.smembers("symbol:" + topic + ":serverid:" + serverid + ":ids", function(err, ids) {
      if (err) throw err;

      // send the message to each user
      ids.forEach(function(id, j) {
        if (id in connections) {
          connections[id].write(msg);
        }
      });
    });
  }
}

exports.newPrice = newPrice;

function sendIndex(index, conn) {
  var i = {symbols: []};
  var count;

  // todo: remove this stuff?
  i.name = index;

  db.smembers("index:" + index, function(err, replies) {
    if (err) {
      console.log(err);
      return;
    }

    count = replies.length;
    if (count == 0) {
      console.log("Index:" + index + " not found");
      return;
    }

    replies.forEach(function(symbol, j) {
      db.hgetall("symbol:" + symbol, function(err, inst) {
        var instrument = {};
        if (err) {
          console.log(err);
          return;
        }

        if (inst == null) {
          console.log("Symbol:" + symbol + " not found");
          count--;
          return;
        }

        instrument.symbol = symbol;
        instrument.shortname = inst.shortname;

        // add the order to the array
        i.symbols.push(instrument);

        // send array if we have added the last item
        count--;
        if (count <= 0) {
          conn.write("{\"index\":" + JSON.stringify(i) + "}");
        }
      });
    });
  });
}

exports.sendIndex = sendIndex;

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
  case 1023:
    desc = "Email already exists";
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
  var updatecash;
  var gettotalpositions;
  var getcash;
  var getfreemargin;
  var getunrealisedpandl;
  var calcfinance;
  var round;
  var gettrades;
  var getquoterequests;
  var stringsplit;

  // publish & subscribe channels
  exports.clientserverchannel = 1;
  exports.userserverchannel = 2;
  exports.tradeserverchannel = 3;
  exports.ifaserverchannel = 4;
  exports.webserverchannel = 5;
  exports.tradechannel = 6;
  exports.priceserverchannel = 7;
  exports.pricehistorychannel = 8;
  exports.pricechannel = 9;
  exports.positionchannel = 10;
  exports.quoterequestchannel = 11;

  round = '\
  local round = function(num, dp) \
    local mult = 10 ^ (dp or 0) \
    return math.floor(num * mult + 0.5) / mult \
  end \
  ';

  exports.round = round;

  //
  // function to split a string into an array of substrings, based on a character
  // parameters are the string & character
  // i.e. stringsplit("abc,def,hgi", ",") = ["abc", "def", "hgi"]
  //
  stringsplit = '\
  local stringsplit = function(str, inSplitPattern) \
    local outResults = {} \
    local theStart = 1 \
    local theSplitStart, theSplitEnd = string.find(str, inSplitPattern, theStart) \
    while theSplitStart do \
      table.insert(outResults, string.sub(str, theStart, theSplitStart-1)) \
      theStart = theSplitEnd + 1 \
      theSplitStart, theSplitEnd = string.find(str, inSplitPattern, theStart) \
    end \
    table.insert(outResults, string.sub(str, theStart)) \
    return outResults \
  end \
  ';

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

  //
  // proquote version
  //
  getunrealisedpandlpq = '\
  local getunrealisedpandlpq = function(symbol, quantity, side, avgcost) \
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

  getunrealisedpandl = round + '\
  local getunrealisedpandl = function(symbol, quantity, cost) \
    local bidprice = redis.call("hget", "price:" .. symbol, "bid") \
    local askprice = redis.call("hget", "price:" .. symbol, "ask") \
    local unrealisedpandl = 0 \
    local price = 0 \
    local qty = tonumber(quantity) \
    if qty > 0 then \
      if bidprice and tonumber(bidprice) ~= 0 then \
        price = tonumber(bidprice) / 100 \
      end \
    else \
      if askprice and tonumber(askprice) ~= 0 then \
        price = tonumber(askprice) / 100 \
      end \
    end \
    if price ~= 0 then \
      unrealisedpandl = round(qty * price - cost, 2) \
    end \
    return {unrealisedpandl, price} \
  end \
  ';

  exports.getunrealisedpandl = getunrealisedpandl;

  // only dealing with trade currency for the time being - todo: review
  gettotalpositions = getunrealisedpandl + '\
  local gettotalpositions = function(clientid, currency) \
    local positions = redis.call("smembers", clientid .. ":positions") \
    local fields = {"symbol", "currency", "quantity", "margin", "cost"} \
    local vals \
    local totalmargin = 0 \
    local totalunrealisedpandl = 0 \
    for index = 1, #positions do \
      vals = redis.call("hmget", clientid .. ":position:" .. positions[index], unpack(fields)) \
      if vals[2] == currency then \
        totalmargin = totalmargin + tonumber(vals[4]) \
        local unrealisedpandl = getunrealisedpandl(vals[1], vals[3], vals[5]) \
        totalunrealisedpandl = totalunrealisedpandl + unrealisedpandl[1] \
      end \
    end \
    return {totalmargin, totalunrealisedpandl} \
  end \
  ';

  exports.gettotalpositions = gettotalpositions;

  getfreemargin = getcash + gettotalpositions + '\
  local getfreemargin = function(clientid, currency) \
    local cash = getcash(clientid, currency) \
    local totalpositions = gettotalpositions(clientid, currency) \
    --[[ totalpositions[1] = margin, [2] = unrealised p&l ]] \
    local balance = tonumber(cash) \
    local equity = balance + totalpositions[2] \
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
      local brokerclientcode = redis.call("hget", "client:" .. vals[1], "brokerclientcode") \
      table.insert(tblresults, {clientid=vals[1],brokerclientcode=brokerclientcode,orderid=vals[2],symbol=vals[3],side=vals[4],quantity=vals[5],price=vals[6],currency=vals[7],currencyratetoorg=vals[8],currencyindtoorg=vals[9],commission=vals[10],ptmlevy=vals[11],stampduty=vals[12],contractcharge=vals[13],counterpartyid=vals[14],markettype=vals[15],externaltradeid=vals[16],futsettdate=vals[17],timestamp=vals[18],lastmkt=vals[19],externalorderid=vals[20],tradeid=vals[21],settlcurrency=vals[22],settlcurramt=vals[23],settlcurrfxrate=vals[24],settlcurrfxratecalc=vals[25],nosettdays=vals[26],margin=vals[27],finance=vals[28]}) \
    end \
    return tblresults \
  end \
  ';

  exports.gettrades = gettrades;

  //
  // params: array of quotereqid's, symbol ("" = any symbol)
  // returns array of quote requests
  //
  getquoterequests = '\
  local getquoterequests = function(quoterequests, symbol) \
    local tblresults = {} \
    local fields = {"clientid","symbol","quantity","cashorderqty","currency","settlcurrency","nosettdays","futsettdate","quotestatus","timestamp","quoteid","quoterejectreason","quotereqid","operatortype","operatorid"} \
    local vals \
    for index = 1, #quoterequests do \
      vals = redis.call("hmget", "quoterequest:" .. quoterequests[index], unpack(fields)) \
      if symbol == "" or symbol == vals[2] then \
        table.insert(tblresults, {clientid=vals[1],symbol=vals[2],quantity=vals[3],cashorderqty=vals[4],currency=vals[5],settlcurrency=vals[6],nosettdays=vals[7],futsettdate=vals[8],quotestatus=vals[9],timestamp=vals[10],quoteid=vals[11],quoterejectreason=vals[12],quotereqid=vals[13],operatortype=vals[14],operatorid=vals[15]}) \
      end \
    end \
    return tblresults \
  end \
  ';

  exports.getquoterequests = getquoterequests;

  // compare hour & minute with timezone open/close times to determine in/out of hours - 0=in hours, 1=ooh
  // todo: review days
  getmarkettype = '\
  local getmarkettype = function(symbol, hour, minute, day) \
    local markettype = 0 \
    local timezone = redis.call("hget", "symbol:" .. symbol, "timezone") \
    if not timezone then \
      return markettype \
    end \
    local fields = {"openhour","openminute","closehour","closeminute"} \
    local vals = redis.call("hmget", "timezone:" .. timezone, unpack(fields)) \
    if tonumber(day) == 0 or tonumber(day) == 6 then \
      markettype = 1 \
    elseif tonumber(hour) < tonumber(vals[1]) or tonumber(hour) > tonumber(vals[3]) then \
      markettype = 1 \
    elseif tonumber(hour) == tonumber(vals[1]) and tonumber(minute) < tonumber(vals[2]) then \
      markettype = 1 \
    elseif tonumber(hour) == tonumber(vals[3]) and tonumber(minute) > tonumber(vals[4]) then \
      markettype = 1 \
    end \
    return markettype \
  end \
  ';

  exports.getmarkettype = getmarkettype;

	subscribeinstrumentpq = '\
  local subscribeinstrumentpq = function(symbol, id, servertype) \
    local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    if not topic then \
      return {0, ""} \
    end \
    local marketext = redis.call("hget", servertype .. ":" .. id, "marketext") \
    if not marketext then \
      topic = topic .. "D" \
    elseif marketext ~= nil then \
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

  exports.subscribeinstrumentpq = subscribeinstrumentpq;

  //
  // subscribe to an instrument with nbtrader
  // any number of symbols can subscribe to a nbtsymbol to support derivatives
  //
  subscribeinstrumentnbt = getmarkettype + '\
  local subscribeinstrumentnbt = function(symbol, id, serverid, hour, minute, day) \
    local nbtsymbol = redis.call("hget", "symbol:" .. symbol, "nbtsymbol") \
    if not nbtsymbol then \
      return {0, ""} \
    end \
    local subscribe = 0 \
    redis.call("sadd", "symbol:" .. symbol .. ":serverid:" .. serverid .. ":ids", id) \
    redis.call("sadd", "symbol:" .. symbol .. ":serverids", serverid) \
    redis.call("sadd", "nbtsymbol:" .. nbtsymbol .. ":symbols", symbol) \
    if redis.call("sismember", "nbtsymbols", nbtsymbol) == 0 then \
      redis.call("sadd", "nbtsymbols", nbtsymbol) \
      --[[ tell the price server to subscribe ]] \
       redis.call("publish", 7, "rp:" .. nbtsymbol) \
      --[[ subscribing from db will be done on separate connection ]] \
      subscribe = 1 \
    end \
    --[[ get latest price ]] \
    local fields = {"bid", "ask", "timestamp"} \
    local vals = redis.call("hmget", "price:" .. symbol, unpack(fields)) \
    --[[ get in/out of hours ]] \
    local markettype = getmarkettype(symbol, hour, minute, day) \
    redis.call("sadd", "serverid:" .. serverid .. ":id:" .. id .. ":symbols", symbol) \
    redis.call("sadd", "serverid:" .. serverid .. ":ids", id) \
    return {subscribe, vals[1], vals[2], vals[3], markettype} \
  end \
  ';

  exports.subscribeinstrumentnbt = subscribeinstrumentnbt;

  //
  // unsubscribe an instrument from the nbtrader feed
  //
  unsubscribeinstrumentnbt = '\
  local unsubscribeinstrumentnbt = function(symbol, id, serverid) \
    local nbtsymbol = redis.call("hget", "symbol:" .. symbol, "nbtsymbol") \
    if not nbtsymbol then \
      return {0, ""} \
    end \
    --[[ deal with unsubscribing nbtsymbol ]] \
    local unsubscribe = 0 \
    redis.call("srem", "symbol:" .. symbol .. ":serverid:" .. serverid .. ":ids", id) \
    if redis.call("scard", "symbol:" .. symbol .. ":serverid:" .. serverid .. ":ids") == 0 then \
      redis.call("srem", "symbol:" .. symbol .. ":serverids", serverid) \
      if redis.call("scard", "symbol:" .. symbol .. ":serverids") == 0 then \
        redis.call("srem", "nbtsymbol:" .. nbtsymbol .. ":symbols", symbol) \
        if redis.call("scard", "nbtsymbol:" .. nbtsymbol .. ":symbols") == 0 then \
          redis.call("srem", "nbtsymbols", nbtsymbol) \
          --[[ tell the price server to unsubscribe ]] \
          redis.call("publish", 7, "halt:" .. nbtsymbol) \
          --[[ unsubscribing from db will be done on separate connection ]] \
          unsubscribe = 1 \
        end \
      end \
    end \
    --[[ deal with symbols/ids required by servers ]] \
    redis.call("srem", "serverid:" .. serverid .. ":id:" .. id .. ":symbols", symbol) \
    if redis.call("scard", "serverid:" .. serverid .. ":id:" .. id .. ":symbols") == 0 then \
      redis.call("srem", "serverid:" .. serverid .. ":ids", id) \
    end \
    return {unsubscribe, nbtsymbol} \
  end \
  ';

  //
  // subscribe to an instrument with digitallook
  //
  subscribeinstrumentdl = '\
  local subscribeinstrumentdl = function(symbol, id, servertype) \
    local ticker = redis.call("hget", "symbol:" .. symbol, "ticker") \
    if not ticker then \
      return {0, "", 0, ""} \
    end \
    redis.call("sadd", "ticker:" .. ticker .. ":" .. servertype .. ":" .. id .. ":symbols", symbol) \
    redis.call("sadd", "ticker:" .. ticker .. ":symbol:" .. symbol .. ":" .. servertype, id) \
    redis.call("sadd", "ticker:" .. ticker .. ":" .. servertype .. ":symbols", symbol) \
    local needtosubscribe = 0 \
    local needtopublish = 0 \
    local tickers = {} \
    if redis.call("scard", "ticker:" .. ticker .. ":" .. servertype) == 0 then \
      redis.call("sadd", "ticker:" .. ticker .. ":" .. servertype, id) \
      needtosubscribe = 1 \
      if redis.call("scard", "ticker:" .. ticker .. ":servers") == 0 then \
        redis.call("sadd", "tickers", ticker) \
        tickers = redis.call("smembers", "tickers") \
        needtopublish = 1 \
      end \
      redis.call("sadd", "ticker:" .. ticker .. ":servers", servertype) \
    end \
    redis.call("sadd", "server:" .. servertype .. ":tickers", ticker) \
    redis.call("sadd", servertype .. ":" .. id .. ":tickers", ticker) \
    return {needtosubscribe, ticker, needtopublish, tickers} \
  end \
  ';

  exports.subscribeinstrumentdl = subscribeinstrumentdl;

  unsubscribeinstrumentpq = '\
  local unsubscribeinstrumentpq = function(symbol, id, servertype) \
    local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    if not topic then \
      return {0, ""} \
    end \
    local marketext = redis.call("hget", servertype .. ":" .. id, "marketext") \
    if not marketext then \
      topic = topic .. "D" \
    elseif marketext ~= nil then \
      topic = topic .. marketext \
    end \
    redis.call("srem", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype, id) \
    redis.call("srem", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols", symbol) \
    if redis.call("scard", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype) == 0 then \
    	redis.call("srem", "topic:" .. topic .. ":" .. servertype .. ":symbols", symbol) \
    end \
    local needtounsubscribe = 0 \
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
  // unsubscribe an instrument from the digitallook feed
  //
  unsubscribeinstrumentdl = '\
  local unsubscribeinstrumentdl = function(symbol, id, servertype) \
    local ticker = redis.call("hget", "symbol:" .. symbol, "ticker") \
    if not ticker then \
      return {0, "", 0, ""} \
    end \
    redis.call("srem", "ticker:" .. ticker .. ":symbol:" .. symbol .. ":" .. servertype, id) \
    redis.call("srem", "ticker:" .. ticker .. ":" .. servertype .. ":" .. id .. ":symbols", symbol) \
    if redis.call("scard", "ticker:" .. ticker .. ":symbol:" .. symbol .. ":" .. servertype) == 0 then \
      redis.call("srem", "ticker:" .. ticker .. ":" .. servertype .. ":symbols", symbol) \
    end \
    local needtounsubscribe = 0 \
    local needtopublish = 0 \
    local tickers = {} \
    if redis.call("scard", "ticker:" .. ticker .. ":" .. servertype .. ":" .. id .. ":symbols") == 0 then \
      redis.call("srem", servertype .. ":" .. id .. ":tickers", ticker) \
      redis.call("srem", "ticker:" .. ticker .. ":" .. servertype, id) \
      if redis.call("scard", "ticker:" .. ticker .. ":" .. servertype) == 0 then \
        needtounsubscribe = 1 \
        redis.call("srem", "ticker:" .. ticker .. ":servers", servertype) \
        redis.call("srem", "server:" .. servertype .. ":tickers", ticker) \
        if redis.call("scard", "ticker:" .. ticker .. ":servers") == 0 then \
          redis.call("srem", "tickers", ticker) \
          tickers = redis.call("smembers", "tickers") \
          needtopublish = 1 \
        end \
      end \
    end \
    return {needtounsubscribe, ticker, needtopublish, tickers} \
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
  // get open quote requests for a symbol
  // params: symbol
  //
  exports.scriptgetopenquoterequests = getquoterequests + '\
  local quoterequests = redis.call("smembers", "openquoterequests") \
  local tblresults = getquoterequests(quoterequests, KEYS[1]) \
  return cjson.encode(tblresults) \
  ';

  //
  // get quotes made by a client for a quote request
  //
  exports.scriptgetmyquotes = '\
  local fields = {"quotereqid","clientid","quoteid","bidquoteid","offerquoteid","symbol","bestbid","bestoffer","bidpx","offerpx","bidquantity","offerquantity","bidsize","offersize","validuntiltime","transacttime","currency","settlcurrency","bidqbroker","offerqbroker","nosettdays","futsettdate","bidfinance","offerfinance","orderid","qclientid"} \
  local vals \
  local tblresults = {} \
  local quotes = redis.call("smembers", "quoterequest:" .. KEYS[1] .. ":quotes") \
  for index = 1, #quotes do \
    vals = redis.call("hmget", "quote:" .. quotes[index], unpack(fields)) \
    --[[ only include if quote was by this client ]] \
    if vals[26] == KEYS[2] then \
      table.insert(tblresults, {quotereqid=vals[1],clientid=vals[2],quoteid=vals[3],bidquoteid=vals[4],offerquoteid=vals[5],symbol=vals[6],bestbid=vals[7],bestoffer=vals[8],bidpx=vals[9],offerpx=vals[10],bidquantity=vals[11],offerquantity=vals[12],bidsize=vals[13],offersize=vals[14],validuntiltime=vals[15],transacttime=vals[16],currency=vals[17],settlcurrency=vals[18],bidqbroker=vals[19],offerqbroker=vals[20],nosettdays=vals[21],futsettdate=vals[22],bidfinance=vals[23],offerfinance=vals[24],orderid=vals[25]}) \
    end \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // pass client id - todo: add symbol as an option
  //
  exports.scriptgetquoterequests = getquoterequests + '\
  local quoterequests = redis.call("smembers", KEYS[1] .. ":quoterequests") \
  local tblresults = getquoterequests(quoterequests, "") \
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
  // params: client id
  //
  exports.scriptgetpositions = getunrealisedpandl + '\
  local tblresults = {} \
  local positions = redis.call("smembers", KEYS[1] .. ":positions") \
  local fields = {"clientid","symbol","quantity","cost","currency","margin","positionid","costpershare"} \
  local vals \
  for index = 1, #positions do \
    vals = redis.call("hmget", KEYS[1] .. ":position:" .. positions[index], unpack(fields)) \
    --[[ value the position ]] \
    local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
    table.insert(tblresults, {clientid=vals[1],symbol=vals[2],quantity=vals[3],cost=vals[4],currency=vals[5],margin=vals[6],positionid=vals[7],costpershare=vals[8],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // params: client id, symbol
  //
  exports.scriptgetposition = getunrealisedpandl + '\
  local fields = {"clientid","symbol","quantity","cost","currency","margin","positionid","costpershare"} \
  local vals = redis.call("hmget", KEYS[1] .. ":position:" .. KEYS[2], unpack(fields)) \
  local pos = {} \
  if vals[0] ~= nil then \
    --[[ value the position ]] \
    local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
    pos = {clientid=vals[1],symbol=vals[2],quantity=vals[3],cost=vals[4],currency=vals[5],margin=vals[6],positionid=vals[7],costpershare=vals[8],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]} \
  end \
  return cjson.encode(pos) \
  ';

  //
  // params: client id
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
  // calculates account p&l, margin & equity for a client
  // assumes there is cash for any currency with positions
  // params: client id
  //
  exports.scriptgetaccount = gettotalpositions + '\
  local tblresults = {} \
  local cash = redis.call("smembers", KEYS[1] .. ":cash") \
  for index = 1, #cash do \
    local amount = redis.call("get", KEYS[1] .. ":cash:" .. cash[index]) \
    local totalpositions = gettotalpositions(KEYS[1], cash[index]) \
    local balance = tonumber(amount) \
    local equity = balance + totalpositions[2] \
    local freemargin = equity - totalpositions[1] \
    table.insert(tblresults, {currency=cash[index],cash=amount,balance=balance,unrealisedpandl=totalpositions[2],equity=equity,margin=totalpositions[1],freemargin=freemargin}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // subscribe to a new instrument
  // params: symbol, client/user/ifa id, serverid, feedtype
  // i.e. "BARC.L", 1, 1, "digitallook"
  //
  exports.scriptsubscribeinstrument = subscribeinstrumentpq + subscribeinstrumentdl + subscribeinstrumentnbt + '\
  local ret = {0, ""} \
  if KEYS[4] == "proquote" then \
    ret = subscribeinstrumentpq(KEYS[1], KEYS[2], KEYS[3]) \
  elseif KEYS[4] == "digitallook" then \
    ret = subscribeinstrumentdl(KEYS[1], KEYS[2], KEYS[3]) \
  elseif KEYS[4] == "nbtrader" then \
    ret = subscribeinstrumentnbt(KEYS[1], KEYS[2], KEYS[3], KEYS[5], KEYS[6], KEYS[7]) \
  end \
  return ret \
  ';

  //
  // unsubscribe from an instrument
  // params: symbol, client/user id, serverid, feedtype
  // i.e. "BARC.L", 1, 1, "digitallook"
  //
  exports.scriptunsubscribeinstrument = unsubscribeinstrumentpq + unsubscribeinstrumentdl + unsubscribeinstrumentnbt + '\
  local symbol = KEYS[1] \
  local id = KEYS[2] \
  local serverid = KEYS[3] \
  local ret = {0, ""} \
  if KEYS[4] == "proquote" then \
    ret = unsubscribeinstrumentpq(symbol, id, serverid) \
  elseif KEYS[4] == "digitallook" then \
    ret = unsubscribeinstrumentdl(symbol, id, serverid) \
  elseif KEYS[4] == "nbtrader" then \
    ret = unsubscribeinstrumentnbt(symbol, id, serverid) \
  end \
  return ret \
  ';

  //
  // unsubscribe a user/client/other connection
  // params: client/user id, serverid, feedtype
  // i.e. 1, 1, "digitallook"
  //
  exports.scriptunsubscribeid = unsubscribeinstrumentpq + unsubscribeinstrumentdl + unsubscribeinstrumentnbt + '\
  local id = KEYS[1] \
  local serverid = KEYS[2] \
  local symbols = redis.call("smembers", "serverid:" .. serverid .. ":id:" .. id .. ":symbols") \
  local unsubscribetopics = {} \
  for i = 1, #symbols do \
    local ret = {0, ""} \
    if KEYS[3] == "proquote" then \
      ret = unsubscribeinstrumentpq(symbols[i], id, serverid) \
    elseif KEYS[3] == "digitallook" then \
      ret = unsubscribeinstrumentdl(symbols[i], id, serverid) \
    elseif KEYS[3] == "nbtrader" then \
      ret = unsubscribeinstrumentnbt(symbols[i], id, serverid) \
    end \
    if ret[1] == 1 then \
      table.insert(unsubscribetopics, ret[2]) \
    end \
  end \
  return unsubscribetopics \
  ';

  //
  // unsubscribe a server from nbtrader
  // params: serverid, i.e. 1
  //
  exports.scriptunsubscribeserver = unsubscribeinstrumentnbt + '\
  local serverid = KEYS[1] \
  local unsubscribetopics = {} \
  local ids = redis.call("smembers", "serverid:" .. serverid .. ":ids") \
  for i = 1, #ids do \
    local symbols = redis.call("smembers", "serverid:" .. serverid .. ":id:" .. ids[i] .. ":symbols") \
    for j = 1, #symbols do \
      local ret = unsubscribeinstrumentnbt(symbols[j], ids[i], serverid) \
      if ret[1] == 1 then \
        table.insert(unsubscribetopics, ret[2]) \
      end \
    end \
  end \
  return unsubscribetopics \
  ';

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

  exports.scriptnewclient = stringsplit + '\
  --[[ check email is unique ]] \
  local emailexists = redis.call("get", "client:" .. KEYS[3]) \
  if emailexists then return {1023} end \
  local clientid = redis.call("incr", "clientid") \
  if not clientid then return {1005} end \
  --[[ store the client ]] \
  redis.call("hmset", "client:" .. clientid, "clientid", clientid, "brokerid", KEYS[1], "name", KEYS[2], "email", KEYS[3], "password", KEYS[3], "mobile", KEYS[4], "address", KEYS[5], "ifaid", KEYS[6], "type", KEYS[7], "hedge", KEYS[9], "brokerclientcode", KEYS[10], "marketext", "D", "commissionpercent", KEYS[11]) \
  --[[ add to set of clients ]] \
  redis.call("sadd", "clients", clientid) \
  --[[ add route to find client from email ]] \
  redis.call("set", "client:" .. KEYS[3], clientid) \
  --[[ add tradeable instrument types ]] \
  if KEYS[8] ~= "" then \
    local insttypes = stringsplit(KEYS[8], ",") \
    for i = 1, #insttypes do \
      redis.call("sadd", clientid .. ":instrumenttypes", insttypes[i]) \
    end \
  end \
  return {0, clientid} \
  ';

  exports.scriptupdateclient = stringsplit + '\
  local clientkey = "client:" .. KEYS[1] \
  --[[ get existing email, in case we need to change email->client link ]] \
  local email = redis.call("hget", clientkey, "email") \
  if not email then return 1017 end \
  --[[ update client ]] \
  redis.call("hmset", "client:" .. KEYS[1], "clientid", KEYS[1], "brokerid", KEYS[2], "name", KEYS[3], "email", KEYS[4], "mobile", KEYS[5], "address", KEYS[6], "ifaid", KEYS[7], "type", KEYS[8], "hedge", KEYS[10], "brokerclientcode", KEYS[11], "commissionpercent", KEYS[12]) \
  --[[ remove old email link and add new one ]] \
  if KEYS[4] ~= email then \
    redis.call("del", "client:" .. email) \
    redis.call("set", "client:" .. KEYS[4], KEYS[1]) \
  end \
  --[[ add/remove tradeable instrument types ]] \
  local insttypes = redis.call("smembers", "instrumenttypes") \
  local clientinsttypes = stringsplit(KEYS[9], ",") \
  for i = 1, #insttypes do \
    local found = false \
    for j = 1, #clientinsttypes do \
      if clientinsttypes[j] == insttypes[i] then \
        redis.call("sadd", KEYS[1] .. ":instrumenttypes", insttypes[i]) \
        found = true \
        break \
      end \
    end \
    if not found then \
      redis.call("srem", KEYS[1] .. ":instrumenttypes", insttypes[i]) \
    end \
  end \
  return 0 \
  ';

  exports.scriptgetclienttypes = '\
  local clienttypes = redis.call("sort", "clienttypes:" .. KEYS[1], "ALPHA") \
  local clienttype = {} \
  local val \
  for index = 1, #clienttypes do \
    val = redis.call("get", "clienttype:" .. clienttypes[index]) \
    table.insert(clienttype, {clienttypeid = clienttypes[index], description = val}) \
  end \
  return cjson.encode(clienttype) \
  ';

  // update the latest price & add a tick to price history
  // params: symbol, timestamp, bid, offer
  scriptpriceupdate = '\
  --[[ get an id for this tick ]] \
  local pricehistoryid = redis.call("incr", "pricehistoryid") \
  --[[ may only get bid or ask, so make sure we have the latest of both ]] \
  local nbtsymbol = KEYS[1] \
  local bid = KEYS[3] \
  local ask = KEYS[4] \
  local pricemsg = "" \
  if bid == "" then \
    bid = redis.call("hget", "price:" .. nbtsymbol, "bid") \
    if ask == "" then \
      return \
    else \
      pricemsg = cjson.encode("ask") .. ":" .. ask \
    end \
  else \
    if ask == "" then \
      ask = redis.call("hget", "price:" .. nbtsymbol, "ask") \
      pricemsg = cjson.encode("bid") .. ":" .. bid \
    else \
      pricemsg = cjson.encode("bid") .. ":" .. bid .. "," .. cjson.encode("ask") .. ":" .. ask \
    end \
  end \
  --[[ publish a price message, store latest price & history for any symbols subscribed to that use this nbtsymbol ]] \
  local symbols = redis.call("smembers", "nbtsymbol:" .. nbtsymbol .. ":symbols") \
  for index = 1, #symbols do \
    pricemsg = "{" .. cjson.encode("price") .. ":{" .. cjson.encode("symbol") .. ":" .. cjson.encode(symbols[index]) .. "," .. pricemsg .. "}}" \
    --[[ publish price ]] \
    redis.call("publish", "price:" .. symbols[index], pricemsg) \
    --[[ store latest price ]] \
    redis.call("hmset", "price:" .. symbols[index], "bid", bid, "ask", ask, "timestamp", KEYS[2]) \
    --[[ add id to sorted set, indexed on timestamp ]] \
    redis.call("zadd", "pricehistory:" .. symbols[index], KEYS[2], pricehistoryid) \
    redis.call("hmset", "pricehistory:" .. pricehistoryid, "timestamp", KEYS[2], "symbol", symbols[index], "bid", bid, "ask", ask, "id", pricehistoryid) \
  end \
  return {pricehistoryid, nbtsymbol, bid, ask} \
  ';

  exports.scriptpriceupdate = scriptpriceupdate;
};
