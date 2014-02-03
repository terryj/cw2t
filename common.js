/****************
* common.js
* Front-office common server functions
* Cantwaittotrade Limited
* Terry Johnston
* December 2013
****************/

//
// returns trading day, number of days after passed date
//
function getSettDate(dt, nosettdays) {
  var days = 0;

  if (nosettdays > 0) {
    while (true) {
      // ignore weekends & holidays
      if (dt.getDay() == 6) {
        dt.setDate(dt.getDate() + 2);
      } else if (dt.getDay() == 0) {
        dt.setDate(dt.getDate() + 1);
      } else if (isHoliday(dt)) {
        dt.setDate(dt.getDate() + 1);
      } else {
        dt.setDate(dt.getDate() + 1);
        days++;
      }

      if (days >= nosettdays) {
        break;
      }
    }
  }

  return dt;
}

exports.getSettDate = getSettDate;

function isHoliday(datetocheck) {
  var ret;
  var datetocheckstr = "";

  datetocheckstr += datetocheck.getFullYear();
  datetocheckstr += datetocheck.getMonth() + 1;
  datetocheckstr += datetocheck.getDate();

  db.sismember("holidays", datetocheckstr, function(err, found) {
    if (err) throw err;
    ret = found;
  });

  return ret;
}

exports.isHoliday = isHoliday;

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
  default:
    desc = "Unknown reason";
  }

  return desc;
}

exports.getReasonDesc = getReasonDesc;

/*
* Get the nuber of seconds between two UTC datetimes
*/
function getSeconds(startutctime, finishutctime) {
  var startdt = new Date(getDateString(startutctime));
  var finishdt = new Date(getDateString(finishutctime));
  return ((finishdt - startdt) / 1000);
}

exports.getSeconds = getSeconds;

/*
* Convert a UTC datetime to a valid string for creating a date object
*/
function getDateString(utcdatetime) {
    return (utcdatetime.substr(0,4) + "/" + utcdatetime.substr(4,2) + "/" + utcdatetime.substr(6,2) + " " + utcdatetime.substr(9,8));
}

exports.getDateString = getDateString;

function dateFromUTCString(utcdatestring) {
  var dt = new Date(utcdatestring.substr(0,4), utcdatestring.substr(4,2), utcdatestring.substr(6,2));
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
  }

  return desc;
}

exports.getPTPQuoteRejectReason = getPTPQuoteRejectReason;

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
    --[[ adjust for debit / credit]] \
    if drcr == 1 then \
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
      return 0 \
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
    local price = redis.call("get", "price:" .. symbol) \
    local unrealisedpandl = 0 \
    --[[ only calculate a p&l if we have a price ]] \
    if price then \
      --[[ take account of short positions ]] \
      local qty = tonumber(quantity) \
      if tonumber(side) == 2 then \
        qty = -qty \
      end \
      unrealisedpandl = qty * (tonumber(price) - tonumber(avgcost)) \
    else \
      price = 0 \
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

	subscribeinstrument = '\
  local subscribeinstrument = function(symbol, id, servertype) \
    local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    local marketext = redis.call("hget", servertype .. ":" .. id, "marketext") \
    if marketext then \
      	topic = topic .. marketext \
    end \
    redis.call("sadd", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols", symbol) \
    redis.call("sadd", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype, id) \
    redis.call("sadd", "topic:" .. topic .. ":" .. servertype .. ":symbols", symbol) \
    local needtosubscribe = 0 \
    if redis.call("scard", "topic:" .. topic .. ":" .. servertype) == 0 then \
      redis.call("publish", "proquote", "subscribe:" .. topic) \
      if redis.call("scard", "topic:" .. topic .. ":servers") == 0 then \
    		redis.call("sadd", "topics", topic) \
      	needtosubscribe = 1 \
      end \
    end \
    redis.call("sadd", "topic:" .. topic .. ":servers", servertype) \
    redis.call("sadd", "server:" .. servertype .. ":topics", topic) \
    redis.call("sadd", "topic:" .. topic .. ":" .. servertype, id) \
    redis.call("sadd", servertype .. ":" .. id .. ":topics", topic) \
    return {needtosubscribe, topic} \
  end \
  ';

  unsubscribeinstrument = '\
  local unsubscribeinstrument = function(symbol, id, servertype) \
    local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    local marketext = redis.call("hget", servertype .. ":" .. id, "marketext") \
    if marketext then \
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
        redis.call("publish", "proquote", "unsubscribe:" .. topic) \
        redis.call("srem", "topic:" .. topic .. ":servers", servertype) \
      	redis.call("srem", "server:" .. servertype .. ":topics", topic) \
      	if redis.call("scard", "topic:" .. topic .. ":servers") == 0 then \
      		redis.call("srem", "topics", topic) \
        	needtounsubscribe = 1 \
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
  // subscribe to a new instrument
  // params: symbol, id, servertype
  //
  exports.scriptsubscribeinstrument = subscribeinstrument + '\
  redis.call("sadd", "orderbook:" .. KEYS[1] .. ":" .. KEYS[3], KEYS[2]) \
  redis.call("sadd", KEYS[3] .. ":" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  local ret = subscribeinstrument(KEYS[1], KEYS[2], KEYS[3]) \
  return ret \
  ';

  //
  // unsubscribe from an instrument
  // params: symbol, id, servertype
  //
  exports.scriptunsubscribeinstrument = unsubscribeinstrument + '\
  redis.call("srem", "orderbook:" .. KEYS[1] .. ":" .. KEYS[3], KEYS[2]) \
  redis.call("srem", KEYS[3] .. ":" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  local ret = unsubscribeinstrument(KEYS[1], KEYS[2], KEYS[3]) \
  return ret \
  ';

  //
  // unsubscribe a user/client/other connection
  // params: servertype, id
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
};
