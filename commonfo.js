/****************
* commonfo.js
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
    local cashsetkey = clientid .. ":cash" \
    local cash = redis.call("get", cashkey) \
    --[[ adjust for credit]] \
    if tonumber(drcr) == 2 then \
      amount = -amount \
    end \
    if not cash then \
      redis.call("set", cashkey, amount) \
      redis.call("sadd", cashsetkey, currency) \
    else \
      local adjamount = tonumber(cash) + amount \
      if adjamount == 0 then \
        redis.call("del", cashkey) \
        redis.call("srem", cashsetkey, currency) \
      else \
        redis.call("set", cashkey, adjamount) \
      end \
    end \
    local key \
    if transtype == "ST" then \
      key = clientid .. ":selltrades:" .. currency \
    elseif transtype == "BT" then \
      key = clientid .. ":buytrades:" .. currency \
    elseif transtype == "CO" then \
      key = clientid .. ":commission:" .. currency \
    elseif transtype == "PL" then \
      key = clientid .. ":ptmlevy:" .. currency \
    elseif transtype == "CC" then \
      key = clientid .. ":contractcharge:" .. currency \
    elseif transtype == "SD" then \
      key = clientid .. ":stampduty:" .. currency \
    elseif transtype == "DI" then \
      key = clientid .. ":dividend:" .. currency \
    elseif transtype == "FI" then \
      key = clientid .. ":finance:" .. currency \
    elseif transtype == "IN" then \
      key = clientid .. ":interest:" .. currency \
    elseif transtype == "CD" then \
      key = clientid .. ":deposits:" .. currency \
    elseif transtype == "CW" then \
      key = clientid .. ":withdrawals:" .. currency \
    elseif transtype == "OT" then \
      key = clientid .. ":other:" .. currency \
    else \
      key = clientid .. ":unknown:" .. currency \
    end \
    local val = redis.call("get", key) \
    if not val then val = 0 end \
    redis.call("set", key, val - amount) \
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
    local unrealisedpandl = 0 \
    local price = 0 \
    local qty = tonumber(quantity) \
    if qty > 0 then \
      local bidprice = redis.call("hget", "price:" .. symbol, "bid") \
      if bidprice and tonumber(bidprice) ~= 0 then \
        price = tonumber(bidprice) \
        if price ~= 0 then \
          unrealisedpandl = round(qty * price / 100 - cost, 2) \
        end \
      end \
    else \
      local askprice = redis.call("hget", "price:" .. symbol, "ask") \
      if askprice and tonumber(askprice) ~= 0 then \
        price = tonumber(askprice) \
        if price ~= 0 then \
          unrealisedpandl = round(qty * price / 100 + cost, 2) \
        end \
      end \
    end \
    return {unrealisedpandl, price} \
  end \
  ';

  exports.getunrealisedpandl = getunrealisedpandl;

  getmargin = round + '\
  local getmargin = function(symbol, quantity) \
    local margin = 0 \
    local price = 0 \
    local qty = tonumber(quantity) \
    if qty > 0 then \
      local bidprice = redis.call("hget", "price:" .. symbol, "bid") \
      if bidprice and tonumber(bidprice) ~= 0 then \
        price = tonumber(bidprice) \
      end \
    else \
      local askprice = redis.call("hget", "price:" .. symbol, "ask") \
      if askprice and tonumber(askprice) ~= 0 then \
        price = tonumber(askprice) \
      end \
    end \
    if price ~= 0 then \
      local instrumenttype = redis.call("hget", "symbol:" .. symbol, "instrumenttype") \
      if instrumenttype ~= "DE" and instrumenttype ~= "IE" then \
        local marginpercent = redis.call("hget", "symbol:" .. symbol, "marginpercent") \
        if marginpercent then \
          margin = round(math.abs(qty) * price / 100 * tonumber(marginpercent) / 100, 2) \
        end \
      end \
    end \
    return margin \
  end \
  ';

  exports.getmargin = getmargin;

  // only dealing with trade currency for the time being - todo: review
  gettotalpositions = getunrealisedpandl + getmargin + '\
  local gettotalpositions = function(clientid, currency) \
    local positions = redis.call("smembers", clientid .. ":positions") \
    local fields = {"symbol", "currency", "quantity", "cost"} \
    local vals \
    local totalmargin = 0 \
    local totalunrealisedpandl = 0 \
    for index = 1, #positions do \
      vals = redis.call("hmget", clientid .. ":position:" .. positions[index], unpack(fields)) \
      if vals[2] == currency then \
        local margin = getmargin(vals[1], vals[3]) \
        totalmargin = totalmargin + margin \
        local unrealisedpandl = getunrealisedpandl(vals[1], vals[3], vals[4]) \
        totalunrealisedpandl = totalunrealisedpandl + unrealisedpandl[1] \
      end \
    end \
    return {totalmargin, totalunrealisedpandl} \
  end \
  ';

  exports.gettotalpositions = gettotalpositions;

  //
  // calculates free margin for a client and currency
  // balance = cash
  // equity = balance + unrealised p&l
  // free margin = equity - margin used to hold positions
  //
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

  //
  // compare hour & minute with timezone open/close times to determine in/out of hours
  // returns: 0=in hours, 1=ooh
  // todo: review days
  //
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

	subscribesymbolpq = '\
  local subscribesymbolpq = function(symbol, id, servertype) \
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

  exports.subscribesymbolpq = subscribesymbolpq;

  //
  // subscribe to an instrument with nbtrader
  // any number of symbols can subscribe to a nbtsymbol to support derivatives
  //
  subscribesymbolnbt = '\
  local subscribesymbolnbt = function(symbol, id, serverid) \
    local nbtsymbol = redis.call("hget", "symbol:" .. symbol, "nbtsymbol") \
    if not nbtsymbol then \
      return {0, ""} \
    end \
    local subscribe = 0 \
    redis.call("sadd", "symbol:" .. symbol .. ":serverid:" .. serverid .. ":ids", id) \
    if redis.call("sismember", "symbol:" .. symbol .. ":serverids", serverid) == 0 then \
      redis.call("sadd", "symbol:" .. symbol .. ":serverids", serverid) \
      --[[ this server needs to subscribe to this symbol - subscribing will be done on a separate connection ]] \
      subscribe = 1 \
    end \
    redis.call("sadd", "nbtsymbol:" .. nbtsymbol .. ":symbols", symbol) \
    if redis.call("sismember", "nbtsymbols", nbtsymbol) == 0 then \
      redis.call("sadd", "nbtsymbols", nbtsymbol) \
      --[[ tell the price server to subscribe ]] \
       redis.call("publish", 7, "rp:" .. nbtsymbol) \
    end \
    if redis.call("sismember", "serverid:" .. serverid .. ":id:" .. id .. ":symbols", symbol) == 0 then \
      redis.call("sadd", "serverid:" .. serverid .. ":id:" .. id .. ":symbols", symbol) \
      redis.call("sadd", "serverid:" .. serverid .. ":ids", id) \
    end \
    --[[ get latest price ]] \
    local fields = {"bid", "ask", "timestamp", "midnetchg", "midpctchg"} \
    local vals = redis.call("hmget", "price:" .. symbol, unpack(fields)) \
    return {subscribe, vals[1], vals[2], vals[3], vals[4], vals[5]} \
  end \
  ';

  exports.subscribesymbolnbt = subscribesymbolnbt;

  //
  // unsubscribe an instrument from the nbtrader feed
  //
  unsubscribesymbolnbt = '\
  local unsubscribesymbolnbt = function(symbol, id, serverid) \
    local nbtsymbol = redis.call("hget", "symbol:" .. symbol, "nbtsymbol") \
    if not nbtsymbol then \
      return {0, ""} \
    end \
    --[[ deal with unsubscribing symbol ]] \
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
        end \
      end \
      --[[ unsubscribing from db will be done on separate connection ]] \
      unsubscribe = 1 \
    end \
    --[[ deal with symbols/ids required by servers ]] \
    redis.call("srem", "serverid:" .. serverid .. ":id:" .. id .. ":symbols", symbol) \
    if redis.call("scard", "serverid:" .. serverid .. ":id:" .. id .. ":symbols") == 0 then \
      redis.call("srem", "serverid:" .. serverid .. ":ids", id) \
    end \
    return {unsubscribe, nbtsymbol} \
  end \
  ';

  exports.unsubscribesymbolnbt = unsubscribesymbolnbt;

  //
  // subscribe to an instrument with digitallook
  //
  subscribesymboldl = '\
  local subscribesymboldl = function(symbol, id, servertype) \
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

  exports.subscribesymboldl = subscribesymboldl;

  unsubscribesymbolpq = '\
  local unsubscribesymbolpq = function(symbol, id, servertype) \
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
  unsubscribesymboldl = '\
  local unsubscribesymboldl = function(symbol, id, servertype) \
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
  // params: client id - todo: add symbol as an option
  //
  exports.scriptgetquoterequests = getquoterequests + '\
  local quoterequests = redis.call("smembers", KEYS[1] .. ":quoterequests") \
  local tblresults = getquoterequests(quoterequests, "") \
  return cjson.encode(tblresults) \
  ';

  //
  // params: client id
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
  // params: client id
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
  // get trades, most recent first
  // params: client id
  //
  exports.scriptgettrades = gettrades + '\
  local trades = redis.call("sort", KEYS[1] .. ":trades", "DESC") \
  local tblresults = gettrades(trades) \
  return cjson.encode(tblresults) \
  ';

  //
  // params: client id, positionkey
  //
  exports.scriptgetpostrades = gettrades + '\
  local postrades = redis.call("smembers", KEYS[1] .. ":trades:" .. KEYS[2]) \
  local tblresults = gettrades(postrades) \
  return cjson.encode(tblresults) \
  ';

  //
  // params: client id
  // todo: need to guarantee order
  exports.scriptgetcashhistory = '\
  local tblresults = {} \
  local cashhistory = redis.call("sort", KEYS[1] .. ":cashtrans") \
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
  // get positions for a client
  // params: client id
  // returns an array of positions
  //
  exports.scriptgetpositions = getunrealisedpandl + getmargin + '\
  local tblresults = {} \
  local positions = redis.call("smembers", KEYS[1] .. ":positions") \
  local fields = {"clientid","symbol","quantity","cost","currency","positionid","futsettdate"} \
  local vals \
  for index = 1, #positions do \
    vals = redis.call("hmget", KEYS[1] .. ":position:" .. positions[index], unpack(fields)) \
    local margin = getmargin(vals[2], vals[3]) \
    --[[ value the position ]] \
    local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
    table.insert(tblresults, {clientid=vals[1],symbol=vals[2],quantity=vals[3],cost=vals[4],currency=vals[5],margin=margin,positionid=vals[6],futsettdate=vals[7],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
  end \
  return tblresults \
  ';

  //
  // get position(s) for a client for a single symbol
  // params: client id, symbol
  // may return none/one or a number of positions as may be more than one settlement date
  //
  exports.scriptgetposition = getunrealisedpandl + getmargin + '\
  local fields = {"clientid","symbol","quantity","cost","currency","positionid","futsettdate"} \
  local tblresults = {} \
  local instrumenttype = redis.call("hget", "symbol:" .. KEYS[2], "instrumenttype") \
  if instrumenttype == "CFD" or instrumenttype == "SPB" then \
    local positions = redis.call("smembers", KEYS[1] .. ":positions:" .. KEYS[2]) \
    for index = 1, #positions do \
      local vals = redis.call("hmget", KEYS[1] .. ":position:" .. KEYS[2] .. ":" .. positions[index], unpack(fields)) \
      local margin = getmargin(vals[2], vals[3]) \
      --[[ value the position ]] \
      local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
      table.insert(tblresults, {clientid=vals[1],symbol=vals[2],quantity=vals[3],cost=vals[4],currency=vals[5],margin=margin,positionid=vals[6],futsettdate=vals[7],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
    end \
  else \
    local vals = redis.call("hmget", KEYS[1] .. ":position:" .. KEYS[2], unpack(fields)) \
    if vals[1] then \
      local margin = getmargin(vals[2], vals[3]) \
      --[[ value the position ]] \
      local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
      table.insert(tblresults, {clientid=vals[1],symbol=vals[2],quantity=vals[3],cost=vals[4],currency=vals[5],margin=margin,positionid=vals[6],futsettdate=vals[7],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
    end \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // get positions for a client & subscribe client & server to the position symbols
  // params: client id, server id
  //
  exports.scriptsubscribepositions = getunrealisedpandl + subscribesymbolnbt + getmargin + '\
  local tblresults = {} \
  local tblsubscribe = {} \
  local positions = redis.call("smembers", KEYS[1] .. ":positions") \
  local fields = {"clientid","symbol","quantity","cost","currency","positionid","futsettdate"} \
  local vals \
  for index = 1, #positions do \
    vals = redis.call("hmget", KEYS[1] .. ":position:" .. positions[index], unpack(fields)) \
    local margin = getmargin(vals[2], vals[3]) \
    --[[ value the position ]] \
    local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
    table.insert(tblresults, {clientid=vals[1],symbol=vals[2],quantity=vals[3],cost=vals[4],currency=vals[5],margin=margin,positionid=vals[6],futsettdate=vals[7],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
    --[[ subscribe to this symbol, so as to get prices to the f/e for p&l calc ]] \
    local subscribe = subscribesymbolnbt(vals[2], KEYS[1], KEYS[2]) \
    if subscribe[1] == 1 then \
      table.insert(tblsubscribe, vals[2]) \
    end \
  end \
  return {cjson.encode(tblresults), tblsubscribe} \
  ';

  //
  // get positions for a client and unsubscribe client & server to the position symbols
  // params: client id, server id
  // 
  exports.scriptunsubscribepositions = unsubscribesymbolnbt + '\
  local tblunsubscribe = {} \
  local positions = redis.call("smembers", KEYS[1] .. ":positions") \
  for index = 1, #positions do \
    local unsubscribe = unsubscribesymbolnbt(positions[index], KEYS[1], KEYS[2]) \
    if unsubscribe[1] == 1 then \
      table.insert(tblunsubscribe, positions[index]) \
    end \
  end \
  return tblunsubscribe \
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
  exports.scriptsubscribesymbol = subscribesymbolpq + subscribesymboldl + subscribesymbolnbt + '\
  local ret = {0, ""} \
  if KEYS[4] == "proquote" then \
    ret = subscribesymbolpq(KEYS[1], KEYS[2], KEYS[3]) \
  elseif KEYS[4] == "digitallook" then \
    ret = subscribesymboldl(KEYS[1], KEYS[2], KEYS[3]) \
  elseif KEYS[4] == "nbtrader" then \
    ret = subscribesymbolnbt(KEYS[1], KEYS[2], KEYS[3]) \
  end \
  return ret \
  ';

  //
  // unsubscribe from an instrument
  // params: symbol, client/user id, serverid, feedtype
  // i.e. "BARC.L", 1, 1, "digitallook"
  //
  exports.scriptunsubscribesymbol = unsubscribesymbolpq + unsubscribesymboldl + unsubscribesymbolnbt + '\
  local symbol = KEYS[1] \
  local id = KEYS[2] \
  local serverid = KEYS[3] \
  local ret = {0, ""} \
  if KEYS[4] == "proquote" then \
    ret = unsubscribesymbolpq(symbol, id, serverid) \
  elseif KEYS[4] == "digitallook" then \
    ret = unsubscribesymboldl(symbol, id, serverid) \
  elseif KEYS[4] == "nbtrader" then \
    ret = unsubscribesymbolnbt(symbol, id, serverid) \
  end \
  return ret \
  ';

  //
  // unsubscribe a user/client/other connection
  // params: client/user id, serverid, feedtype
  // i.e. 1, 1, "digitallook"
  //
  exports.scriptunsubscribeid = unsubscribesymbolpq + unsubscribesymboldl + unsubscribesymbolnbt + '\
  local id = KEYS[1] \
  local serverid = KEYS[2] \
  local symbols = redis.call("smembers", "serverid:" .. serverid .. ":id:" .. id .. ":symbols") \
  local unsubscribetopics = {} \
  for i = 1, #symbols do \
    local ret = {0, ""} \
    if KEYS[3] == "proquote" then \
      ret = unsubscribesymbolpq(symbols[i], id, serverid) \
    elseif KEYS[3] == "digitallook" then \
      ret = unsubscribesymboldl(symbols[i], id, serverid) \
    elseif KEYS[3] == "nbtrader" then \
      ret = unsubscribesymbolnbt(symbols[i], id, serverid) \
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
  exports.scriptunsubscribeserver = unsubscribesymbolnbt + '\
  local serverid = KEYS[1] \
  local unsubscribetopics = {} \
  local ids = redis.call("smembers", "serverid:" .. serverid .. ":ids") \
  for i = 1, #ids do \
    local symbols = redis.call("smembers", "serverid:" .. serverid .. ":id:" .. ids[i] .. ":symbols") \
    for j = 1, #symbols do \
      local ret = unsubscribesymbolnbt(symbols[j], ids[i], serverid) \
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

  //
  // update & publish the latest price, together with the change, used for variation margin calculation
  // params: nbtsymbol, timestamp, bid, offer, calcmidnetchg, calcmidpctchg
  // any symbols related to this nbtsymbol will be updated
  //
  scriptpriceupdate = round + '\
  local nbtsymbol = KEYS[1] \
  local bid = KEYS[3] \
  local ask = KEYS[4] \
  local publish = false \
  local symbols = redis.call("smembers", "nbtsymbol:" .. nbtsymbol .. ":instruments") \
  for index = 1, #symbols do \
    local pricemsg = "{" .. cjson.encode("price") .. ":{" .. cjson.encode("symbol") .. ":" .. cjson.encode(symbols[index]) \
    --[[ may get all or none of params ]] \
    if KEYS[3] ~= "" then \
      local oldbid = redis.call("hget", "price:" .. symbols[index], "bid") \
      if not oldbid then oldbid = 0 end \
      local bidchange = round(tonumber(KEYS[3]) - tonumber(oldbid), 4) \
      pricemsg = pricemsg .. "," .. cjson.encode("bid") .. ":" .. KEYS[3] .. "," .. cjson.encode("bidchange") .. ":" .. bidchange \
      redis.call("hmset", "price:" .. symbols[index], "bid", KEYS[3], "timestamp", KEYS[2]) \
      publish = true \
    end \
    if KEYS[4] ~= "" then \
      local oldask = redis.call("hget", "price:" .. symbols[index], "ask") \
      if not oldask then oldask = 0 end \
      local askchange = round(tonumber(KEYS[4]) - tonumber(oldask), 4) \
      pricemsg = pricemsg .. "," .. cjson.encode("ask") .. ":" .. KEYS[4] .. "," .. cjson.encode("askchange") .. ":" .. askchange \
      redis.call("hmset", "price:" .. symbols[index], "ask", KEYS[4], "timestamp", KEYS[2]) \
      publish = true \
    end \
    if KEYS[5] ~= "" then \
      pricemsg = pricemsg .. "," .. cjson.encode("midnetchg") .. ":" .. KEYS[5] \
      redis.call("hmset", "price:" .. symbols[index], "midnetchg", KEYS[5], "timestamp", KEYS[2]) \
      publish = true \
    end \
    if KEYS[6] ~= "" then \
      pricemsg = pricemsg .. "," .. cjson.encode("midpctchg") .. ":" .. cjson.encode(KEYS[6]) \
      redis.call("hmset", "price:" .. symbols[index], "midpctchg", KEYS[6], "timestamp", KEYS[2]) \
      publish = true \
    end \
    if publish then \
      --[[ publish msg ]] \
      pricemsg = pricemsg .. "}}" \
      redis.call("publish", "price:" .. symbols[index], pricemsg) \
    end \
  end \
  return \
  ';

  exports.scriptpriceupdate = scriptpriceupdate;

  // update the latest price & add a tick to price history
  // params: symbol, timestamp, bid, offer
  // todo: needs updating for devivs
  scriptpricehistoryupdate = '\
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
  return {nbtsymbol, bid, ask} \
  ';

  exports.scriptpricehistoryupdate = scriptpricehistoryupdate;

  //
  // get watchlist for a client
  // params: client id, server id, server type
  //
  scriptgetwatchlist = subscribesymbolnbt + '\
    local tblresults = {} \
    local tblsubscribe = {} \
    local fields = {"bid", "ask", "midnetchg", "midpctchg"} \
    local watchlist = redis.call("smembers", KEYS[3] .. ":" .. KEYS[1] .. ":watchlist") \
    for index = 1, #watchlist do \
      --[[ subscribe to this symbol ]] \
      local subscribe = subscribesymbolnbt(watchlist[index], KEYS[1], KEYS[2]) \
      if subscribe[1] == 1 then \
        table.insert(tblsubscribe, watchlist[index]) \
      end \
      --[[ get current prices ]] \
      local vals = redis.call("hmget", "price:" .. watchlist[index], unpack(fields)) \
      table.insert(tblresults, {symbol=watchlist[index], bid=vals[1], ask=vals[2], midnetchg=vals[3], midpctchg=vals[4]}) \
    end \
    return {cjson.encode(tblresults), tblsubscribe} \
  ';

  exports.scriptgetwatchlist = scriptgetwatchlist;

  //
  // unsubscribe from watchlist for this client
  // params: client id, server id, server type
  //
  scriptunwatchlist = unsubscribesymbolnbt + '\
    local tblunsubscribe = {} \
    local watchlist = redis.call("smembers", KEYS[3] .. ":" .. KEYS[1] .. ":watchlist") \
    for index = 1, #watchlist do \
      --[[ unsubscribe from this symbol ]] \
      local unsubscribe = unsubscribesymbolnbt(watchlist[index], KEYS[1], KEYS[2]) \
      if unsubscribe[1] == 1 then \
        table.insert(tblunsubscribe, watchlist[index]) \
      end \
    end \
    return tblunsubscribe \
  ';

  exports.scriptunwatchlist = scriptunwatchlist;

  //
  // add a symbol to a watchlist
  // params: symbol, client id, server id, server type
  //
  scriptaddtowatchlist = subscribesymbolnbt + '\
    redis.call("sadd", KEYS[4] .. ":" .. KEYS[2] .. ":watchlist", KEYS[1]) \
    local subscribe = subscribesymbolnbt(KEYS[1], KEYS[2], KEYS[3]) \
    --[[ get current prices ]] \
    local fields = {"bid", "ask", "midnetchg", "midpctchg"} \
    local vals = redis.call("hmget", "price:" .. KEYS[1], unpack(fields)) \
    local tblresults = {} \
    table.insert(tblresults, {symbol=KEYS[1], bid=vals[1], ask=vals[2], midnetchg=vals[3], midpctchg=vals[4]}) \
    return {cjson.encode(tblresults), subscribe[1]} \
  ';

  exports.scriptaddtowatchlist = scriptaddtowatchlist;

  //
  // remove a symbol from a watchlist
  // params: symbol, client id, server id, server type
  //
  scriptremovewatchlist = unsubscribesymbolnbt + '\
    redis.call("srem", KEYS[4] .. ":" .. KEYS[2] .. ":watchlist", KEYS[1]) \
    local unsubscribe = unsubscribesymbolnbt(KEYS[1], KEYS[2], KEYS[3]) \
    return unsubscribe[1] \
  ';

  exports.scriptremovewatchlist = scriptremovewatchlist;
};