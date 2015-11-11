/****************
* commonbo.js
* Common back-office functions
* Cantwaittotrade Limited
* Terry Johnston
* June 2015
****************/

exports.registerScripts = function () {
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

  /*** Functions ***/

  round = '\
  local round = function(num, dp) \
    local mult = 10 ^ (dp or 0) \
    return math.floor(num * mult + 0.5) / mult \
  end \
  ';

  exports.round = round;

  //
  // returns a UTC datetime string in "YYYYMMDD-HH:MM:SS" format from a passed date object
  //
  function getUTCTimeStamp(timestamp) {
    var year = timestamp.getFullYear();
    var month = timestamp.getMonth() + 1; // flip 0-11 -> 1-12
    var day = timestamp.getDate();
    var hours = timestamp.getHours();
    var minutes = timestamp.getMinutes();
    var seconds = timestamp.getSeconds();
    //var millis = timestamp.getMilliseconds();

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
  // get a UTC date string in "YYYYMMDD" format from a passed date object
  //
  function getUTCDateString(date) {
    var year = date.getFullYear();
    var month = date.getMonth() + 1; // flip 0-11 -> 1-12
    var day = date.getDate();

    if (month < 10) {month = '0' + month;}

    if (day < 10) {day = '0' + day;}

    var utcdate = "" + year + month + day;

    return utcdate;
  }

  exports.getUTCDateString = getUTCDateString;

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

  //
  // Get the nuber of seconds between two UTC datetimes
  //
  function getSeconds(startutctime, finishutctime) {
    var startdt = new Date(getDateString(startutctime));
    var finishdt = new Date(getDateString(finishutctime));
    return ((finishdt - startdt) / 1000);
  }

  exports.getSeconds = getSeconds;

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
      desc = "Symbol data not found";
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
    case 1024:
      desc = "Quote not found";
      break;
    case 1025:
      desc = "Account not found";
      break;
    case 1026:
      desc = "Credit check failed";
      break;
    case 1027:
      desc = "Corporate action not found";
      break;
    case 1028:
      desc = "Broker account for corporate action not found";
      break;
    case 1029:
      desc = "No closing price for symbol found";
      break;
    case 1030:
      desc = "New symbol required for rights";
      break;
    case 1031:
      desc = "Currency not found";
      break;
    default:
      desc = "Unknown reason";
    }

    return desc;
  }

  exports.getReasonDesc = getReasonDesc;


  function isHoliday(datetocheck, holidays) {
    var found = false;

    var datetocheckstr = getUTCDateString(datetocheck);

    if (datetocheckstr in holidays) {
      found = true;
    }

    return found;
  }

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

  //
  // compare hour & minute with timezone open/close times to determine in/out of hours
  // returns: 0=in hours, 1=ooh
  // todo: review days
  //
  getmarkettype = '\
  local getmarkettype = function(symbolid, hour, minute, day) \
    local markettype = 0 \
    local timezoneid = redis.call("hget", "symbol:" .. symbolid, "timezoneid") \
    if not timezoneid then \
      return markettype \
    end \
    local fields = {"openhour","openminute","closehour","closeminute"} \
    local vals = redis.call("hmget", "timezone:" .. timezoneid, unpack(fields)) \
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

  /*
  * setsymbolkey()
  * creates a symbol key for positions, adding a settlement date for derivatives
  * params: accountid, brokerid, futsettdate, positionid, symbolid
  */
  setsymbolkey = '\
  local setsymbolkey = function(accountid, brokerid, futsettdate, positionid, symbolid) \
    local symbolkey = symbolid \
    --[[ add settlement date to symbol key for devivs ]] \
    if futsettdate ~= "" then \
      local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
      if instrumenttypeid == "CFD" or instrumenttypeid == "SPD" or instrumenttypeid == "CCFD" then \
        symbolkey = symbolkey .. ":" .. futsettdate \
      end \
    end \
    redis.call("set", "broker:" .. brokerid .. ":account:" .. accountid .. ":symbol:" .. symbolkey, positionid) \
    redis.call("sadd", "broker:" .. brokerid .. ":symbol:" .. symbolkey .. ":positions", positionid) \
  end \
  ';

  /*
  * getsymbolkey()
  * gets a symbol key for positions based on symbolid & futsettdate
  * params: futsettdate, symbolid
  * returns: symbol key
  */
  getsymbolkey = '\
  local getsymbolkey = function(futsettdate, symbolid) \
    local symbolkey = symbolid \
    --[[ add settlement date to symbol key for devivs ]] \
    if futsettdate ~= "" then \
      local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
      if instrumenttypeid == "CFD" or instrumenttypeid == "SPD" or instrumenttypeid == "CCFD" then \
        symbolkey = symbolkey .. ":" .. futsettdate \
      end \
    end \
    return symbolkey \
  end \
  ';

  exports.getsymbolkey = getsymbolkey;

  getmargin = round + '\
  local getmargin = function(symbolid, quantity) \
    local margin = 0 \
    local price = 0 \
    local qty = tonumber(quantity) \
    if qty > 0 then \
      local bidprice = redis.call("hget", "symbol:" .. symbolid, "bid") \
      if bidprice and tonumber(bidprice) ~= 0 then \
        price = tonumber(bidprice) \
      end \
    else \
      local askprice = redis.call("hget", "symbol:" .. symbolid, "ask") \
      if askprice and tonumber(askprice) ~= 0 then \
        price = tonumber(askprice) \
      end \
    end \
    if price ~= 0 then \
      local instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
      if instrumenttypeid == "CFD" or instrumenttypeid == "SPB" or instrumenttypeid == "CCFD" then \
        local marginpercent = redis.call("hget", "symbol:" .. symbolid, "marginpercent") \
        if marginpercent then \
          margin = round(math.abs(qty) * price / 100 * tonumber(marginpercent) / 100, 2) \
        end \
      end \
    end \
    return margin \
  end \
  ';

  exports.getmargin = getmargin;

  /*
  * getunrealisedpandl()
  * calculate the unrealised profit/loss for a position
  */
  getunrealisedpandl = round + '\
  local getunrealisedpandl = function(symbolid, quantity, cost) \
    local unrealisedpandl = 0 \
    local price = 0 \
    local qty = tonumber(quantity) \
    if qty > 0 then \
      local bidprice = redis.call("hget", "symbol:" .. symbolid, "bid") \
      if bidprice and tonumber(bidprice) ~= 0 then \
        --[[ show price as pounds rather than pence ]] \
        price = tonumber(bidprice) / 100 \
        if price ~= 0 then \
          unrealisedpandl = round(qty * price - cost, 2) \
        end \
      end \
    else \
      local askprice = redis.call("hget", "symbol:" .. symbolid, "ask") \
      if askprice and tonumber(askprice) ~= 0 then \
        --[[ show price as pounds rather than pence ]] \
        price = tonumber(askprice) / 100 \
        if price ~= 0 then \
          unrealisedpandl = round(qty * price + cost, 2) \
        end \
      end \
    end \
    return {unrealisedpandl, price} \
  end \
  ';

  exports.getunrealisedpandl = getunrealisedpandl;

  calcfinance = round + '\
  local calcfinance = function(instrumenttypeid, consid, currencyid, side, nosettdays) \
    local finance = 0 \
    local costkey = "cost:" .. instrumenttypeid .. ":" .. currencyid .. ":" .. side \
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

  gethashvalues = '\
  local gethashvalues = function(key) \
    local vals = {} \
    local rawvals = redis.call("hgetall", key) \
    for index = 1, #rawvals, 2 do \
      vals[rawvals[index]] = rawvals[index + 1] \
    end \
    return vals \
  end \
  ';

  exports.gethashvalues = gethashvalues;

  /*
  * getaccountbalance()
  * params: accountid, brokerid
  * returns: account balance
  */
  getaccountbalance = '\
  local getaccountbalance = function(accountid, brokerid) \
    local accountbalance = redis.call("hget", "broker:" .. brokerid .. ":account:" .. accountid, "balance") \
    return accountbalance \
  end \
  ';

  /*
  * updateaccountbalance()
  * updates account balance & local currency balance
  * amount & localamount can be -ve
  */
  updateaccountbalance = '\
  local updateaccountbalance = function(accountid, amount, brokerid, localamount) \
    redis.log(redis.LOG_DEBUG, "updateaccountbalance") \
    local accountkey = "broker:" .. brokerid .. ":account:" .. accountid \
    redis.call("hincrbyfloat", accountkey, "balance", amount) \
    redis.call("hincrbyfloat", accountkey, "localbalance", localamount) \
  end \
  ';

  /*
  * getclientfromaccount()
  * get a client from an account
  * params: accountid, brokerid
  * returns: clientid
  */
  getclientfromaccount = '\
  local getclientfromaccount = function(accountid, brokerid) \
    local clientid = redis.call("get", "broker:" .. brokerid .. ":account:" .. accountid .. ":client") \
    return clientid \
  end \
  ';

  /*
  * newposting()
  * creates a posting record & updates balances for an account
  * params: accountid, amount, brokerid, localamount, transactionid, timestamp in milliseconds
  */
  newposting = updateaccountbalance + '\
  local newposting = function(accountid, amount, brokerid, localamount, transactionid, milliseconds) \
    local brokerkey = "broker:" .. brokerid \
    local postingid = redis.call("hincrby", brokerkey, "lastpostingid", 1) \
    redis.call("hmset", brokerkey .. ":posting:" .. postingid, "accountid", accountid, "brokerid", brokerid, "amount", amount, "localamount", localamount, "postingid", postingid, "transactionid", transactionid) \
    redis.call("sadd", brokerkey .. ":transaction:" .. transactionid .. ":postings", postingid) \
    redis.call("sadd", brokerkey .. ":account:" .. accountid .. ":postings", postingid) \
    --[[ add a sorted set for time based queries ]] \
    redis.call("zadd", brokerkey .. ":account:" .. accountid .. ":postingsbydate", milliseconds, postingid) \
    redis.call("sadd", brokerkey .. ":postings", postingid) \
    redis.call("sadd", brokerkey .. ":postingid", "posting:" .. postingid) \
    updateaccountbalance(accountid, amount, brokerid, localamount) \
  return postingid \
  end \
  ';

  /*
  * newtransaction()
  * creates a transaction record
  * params: amount, brokerid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid
  */
  newtransaction = '\
  local newtransaction = function(amount, brokerid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid) \
    local transactionid = redis.call("hincrby", "broker:" .. brokerid, "lasttransactionid", 1) \
    redis.call("hmset", "broker:" .. brokerid .. ":transaction:" .. transactionid, "amount", amount, "brokerid", brokerid, "currencyid", currencyid, "localamount", localamount, "note", note, "rate", rate, "reference", reference, "timestamp", timestamp, "transactiontypeid", transactiontypeid, "transactionid", transactionid) \
    redis.call("sadd", "broker:" .. brokerid .. ":transactions", transactionid) \
    redis.call("sadd", "broker:" .. brokerid .. ":transactionid", "transaction:" .. transactionid) \
    return transactionid \
  end \
  ';

  /*
  * gettransaction()
  * get a transaction
  * params: brokerid, transactionid
  */
  gettransaction = gethashvalues + '\
  local gettransaction = function(brokerid, transactionid) \
    local transaction = gethashvalues("broker:" .. brokerid .. ":transaction:" .. transactionid) \
    return transaction \
  end \
  ';

  /*
  * getpostingsbydate()
  * gets postings for an account between two dates, sorted by datetime
  * params: accountid, brokerid, positionid, start of period, end of period - datetimes expressed in milliseconds
  * returns: array of postings
  */
  getpostingsbydate = gettransaction + '\
  local getpostingsbydate = function(accountid, brokerid, startmilliseconds, endmilliseconds) \
    redis.log(redis.LOG_WARNING, "getpostingsbydate") \
    local tblpostings = {} \
    local brokerkey = "broker:" .. brokerid \
    local postings = redis.call("zrangebyscore", brokerkey .. ":account:" .. accountid .. ":postingsbydate", startmilliseconds, endmilliseconds) \
    for i = 1, #postings do \
      local posting = gethashvalues(brokerkey .. ":posting:" .. postings[i]) \
      local transaction = gettransaction(brokerid, posting["transactionid"]) \
      posting["note"] = transaction["note"] \
      posting["reference"] = transaction["reference"] \
      posting["timestamp"] = transaction["timestamp"] \
      posting["type"] = transaction["transactiontypeid"] \
      table.insert(tblpostings, posting) \
    end \
    return tblpostings \
  end \
  ';

  /*
  * newposition()
  * create a new position
  * params: accountid, brokerid, cost, futsettdate, quantity, symbolid
  * returns: positionid
  */
  newposition = setsymbolkey + '\
  local newposition = function(accountid, brokerid, cost, futsettdate, quantity, symbolid) \
    redis.log(redis.LOG_WARNING, "newposition") \
    local brokerkey = "broker:" .. brokerid \
    local positionid = redis.call("hincrby", brokerkey, "lastpositionid", 1) \
    redis.call("hmset", brokerkey .. ":position:" .. positionid, "brokerid", brokerid, "accountid", accountid, "symbolid", symbolid, "quantity", quantity, "cost", cost, "positionid", positionid, "futsettdate", futsettdate) \
    setsymbolkey(accountid, brokerid, futsettdate, positionid, symbolid) \
    redis.call("sadd", brokerkey .. ":positions", positionid) \
    redis.call("sadd", brokerkey .. ":account:" .. accountid .. ":positions", positionid) \
    redis.call("sadd", brokerkey .. ":positionid", "position:" .. positionid) \
    return positionid \
  end \
  ';

  /*
  * getposition()
  * gets a position
  * params: brokerid, positionid
  * returns: all fields in a position as a table
  */
  getposition = gethashvalues + '\
  local getposition = function(brokerid, positionid) \
    local position = gethashvalues("broker:" .. brokerid .. ":position:" .. positionid) \
    --[[ add isin as external systems seem to rely on this ]] \
    if position["symbolid"] then \
      local isin = redis.call("hget", "symbol:" .. position["symbolid"], "isin") \
      position["isin"] = isin \
    end \
    return position \
  end \
  ';

  exports.getposition = getposition;

  /*
  * publishposition()
  * publish a position
  * params: brokerid, positionid, channel
  */
  publishposition = getposition + '\
  local publishposition = function(brokerid, positionid, channel) \
    redis.log(redis.LOG_WARNING, "publishposition") \
    local position = getposition(brokerid, positionid) \
    redis.call("publish", channel, "{" .. cjson.encode("position") .. ":" .. cjson.encode(position) .. "}") \
  end \
  ';

  /*
  * updateposition()
  * update an existing position
  * quantity & cost can be +ve/-ve
  */
  updateposition = setsymbolkey + publishposition + '\
  local updateposition = function(accountid, brokerid, cost, futsettdate, positionid, quantity, symbolid) \
    redis.log(redis.LOG_WARNING, "updateposition") \
    local positionkey = "broker:" .. brokerid .. ":position:" .. positionid \
    local position = gethashvalues(positionkey) \
    local updatedquantity = tonumber(position["quantity"]) + tonumber(quantity) \
    local updatedcost = tonumber(position["cost"]) + tonumber(cost) \
    redis.call("hmset", positionkey, "accountid", accountid, "symbolid", symbolid, "quantity", updatedquantity, "cost", updatedcost, "futsettdate", futsettdate) \
    if symbolid ~= position["symbolid"] or (futsettdate ~= "" and futsettdate ~= position["futsettdate"]) then \
      setsymbolkey(accountid, brokerid, futsettdate, positionid, symbolid) \
    end \
    publishposition(brokerid, positionid, 10) \
  end \
  ';

  /*
  * newpositionposting()
  * creates a positionposting for a position
  */
  newpositionposting = '\
  local newpositionposting = function(brokerid, cost, linkid, positionid, positionpostingtypeid, quantity, timestamp, milliseconds) \
    redis.log(redis.LOG_WARNING, "newpositionposting") \
    local brokerkey = "broker:" .. brokerid \
    local positionpostingid = redis.call("hincrby", brokerkey, "lastpositionpostingid", 1) \
    redis.call("hmset", brokerkey .. ":positionposting:" .. positionpostingid, "brokerid", brokerid, "cost", cost, "linkid", linkid, "positionid", positionid, "positionpostingid", positionpostingid, "positionpostingtypeid", positionpostingtypeid, "quantity", quantity, "timestamp", timestamp) \
    redis.call("sadd", brokerkey .. ":position:" .. positionid .. ":positionpostings", positionpostingid) \
    redis.call("zadd", brokerkey .. ":position:" .. positionid .. ":positionpostingsbydate", milliseconds, positionpostingid) \
    redis.call("sadd", brokerkey .. ":positionpostings", positionpostingid) \
    redis.call("sadd", brokerkey .. ":positionpostingid", "positionposting:" .. positionpostingid) \
    return positionpostingid \
  end \
  ';

  /*
  * getpositionpostingsbydate()
  * gets position postings for a position between two dates, sorted by datetime
  * params: brokerid, positionid, start of period, end of period - datetimes expressed in milliseconds
  * returns: array of postings
  */
  getpositionpostingsbydate = gethashvalues + '\
  local getpositionpostingsbydate = function(brokerid, positionid, startmilliseconds, endmilliseconds) \
    redis.log(redis.LOG_WARNING, "getpositionpostingsbydate") \
    local tblpositionpostings = {} \
    local brokerkey = "broker:" .. brokerid \
    local positionpostings = redis.call("zrangebyscore", brokerkey .. ":position:" .. positionid .. ":positionpostingsbydate", startmilliseconds, endmilliseconds) \
    for i = 1, #positionpostings do \
      local positionposting = gethashvalues(brokerkey .. ":positionposting:" .. positionpostings[i]) \
      table.insert(tblpositionpostings, positionposting) \
    end \
    return tblpositionpostings \
  end \
  ';

  /*
  * getpositionid()
  * gets a position id
  * params: accountid, brokerid, symbolid, futsettdate
  * returns: positionid
  */
  getpositionid = getsymbolkey + '\
  local getpositionid = function(accountid, brokerid, symbolid, futsettdate) \
    local symbolkey = getsymbolkey(futsettdate, symbolid) \
    local positionid = redis.call("get", "broker:" .. brokerid .. ":account:" .. accountid .. ":symbol:" .. symbolkey) \
    return positionid \
  end \
  ';

  exports.getpositionid = getpositionid;

 /*
  * getpositionbysymbol()
  * gets a position by symbolid
  * params: accountid, brokerid, symbolid, futsettdate
  * returns: all fields in a position as an array
  */
  getpositionbysymbol = getpositionid + getposition + '\
  local getpositionbysymbol = function(accountid, brokerid, symbolid, futsettdate) \
    local position \
    local positionid = getpositionid(accountid, brokerid, symbolid, futsettdate) \
    if positionid then \
      position = getposition(brokerid, positionid) \
    end \
    return position \
  end \
  ';

  exports.getpositionbysymbol = getpositionbysymbol;

  /*
  * getpositionvalue()
  * values a position
  * params: brokerid, positionid
  * returns: position & p&l
  */
  getpositionvalue = getposition + getmargin + getunrealisedpandl + '\
  local getpositionvalue = function(brokerid, positionid) \
    local position = getposition(brokerid, positionid) \
    if position["positionid"] then \
      local margin = getmargin(position["symbolid"], position["quantity"]) \
      local unrealisedpandl = getunrealisedpandl(position["symbolid"], position["quantity"], position["cost"]) \
      position["margin"] = margin \
      position["price"] = unrealisedpandl[2] \
      position["unrealisedpandl"] = unrealisedpandl[1] \
    end \
    return position \
  end \
  ';

  exports.getpositionvalue = getpositionvalue;

  /*
  * getpositions()
  * gets all positions for an account
  * params: accountid, brokerid
  * returns: all position records as a table
  */
  getpositions = getposition + '\
  local getpositions = function(accountid, brokerid) \
    local tblresults = {} \
    local positions = redis.call("smembers", "broker:" .. brokerid .. ":account:" .. accountid .. ":positions") \
    for index = 1, #positions do \
      local vals = getposition(brokerid, positions[index]) \
      table.insert(tblresults, vals) \
    end \
    return tblresults \
  end \
  ';

  /*
  * getpositionvalues()
  * values all positions for an account
  * params: accountid, brokerid
  * returns: all positions with margin & p&l
  */
  getpositionvalues = getpositionvalue + '\
  local getpositionvalues = function(accountid, brokerid) \
    redis.log(redis.LOG_WARNING, "getpositionvalues") \
    local tblresults = {} \
    local positions = redis.call("smembers", "broker:" .. brokerid .. ":account:" .. accountid .. ":positions") \
    for index = 1, #positions do \
      local vals = getpositionvalue(brokerid, positions[index]) \
      table.insert(tblresults, vals) \
    end \
    return tblresults \
  end \
  ';

  /*
  * getpositionsbysymbol()
  * gets all positions for a symbol
  * params: brokerid, symbolid
  * returns: a table of positions
  */
  getpositionsbysymbol = getposition + '\
  local getpositionsbysymbol = function(brokerid, symbolid) \
    redis.log(redis.LOG_WARNING, "getpositionsbysymbol") \
    local tblresults = {} \
    local positions = redis.call("smembers", "broker:" .. brokerid .. ":symbol:" .. symbolid .. ":positions") \
    for index = 1, #positions do \
      local vals = getposition(brokerid, positions[index]) \
      table.insert(tblresults, vals) \
    end \
    return tblresults \
  end \
  ';

  /*
  * getpositionsbysymbolbydate()
  * gets all positions for a symbol as at a date
  * params: brokerid, symbolid, date in milliseconds
  * returns: a table of positions
  */
  getpositionsbysymbolbydate = getpositionsbysymbol + getpositionpostingsbydate + '\
  local getpositionsbysymbolbydate = function(brokerid, symbolid, milliseconds) \
    redis.log(redis.LOG_WARNING, "getpositionsbysymbolbydate") \
    local tblresults = {} \
    local positions = getpositionsbysymbol(brokerid, symbolid) \
    for i = 1, #positions do \
      --[[ get the position postings for this position since the date ]] \
      local positionpostings = getpositionpostingsbydate(brokerid, positions[i]["positionid"], milliseconds, "inf") \
      --[[ adjust the position to reflect these postings ]] \
      for j = #positionpostings, 1, -1 do \
        positions[i]["quantity"] = tonumber(positions[i]["quantity"]) - tonumber(positionpostings[j]["quantity"]) \
      end \
      table.insert(tblresults, positions[i]) \
    end \
    return tblresults \
  end \
  ';

  /*
  * get all the positions for a nominee account
  *
  */
  getpositionsnominee = getpositions + '\
  local getpositionsnominee = function(brokerid, nomineeaccountid) \
    local tblresults = {} \
    local accounts = redis.call("smembers", "broker:" .. brokerid .. ":nomineeaccount:" .. nomineeaccountid .. ":accounts") \
    for index = 1, #accounts do \
      local positions = getpositions(accounts[index], brokerid) \
      table.insert(tblresults, positions) \
    end \
    return tblresults \
  end \
  ';

  /*
  * gettotalpositionvalue()
  * gets sum of margin & p&l for all positions for an account
  * params: accountid, brokerid
  * returns: totalmargin, totalunrealisedpandl
  */
  gettotalpositionvalue = getpositionvalues + '\
  local gettotalpositionvalue = function(accountid, brokerid) \
    redis.log(redis.LOG_WARNING, "gettotalpositionvalue") \
    local positionvalues = getpositionvalues(accountid, brokerid) \
    local totalpositionvalue = {} \
    totalpositionvalue["margin"] = 0 \
    totalpositionvalue["unrealisedpandl"] = 0 \
    for index = 1, #positionvalues do \
      redis.log(redis.LOG_WARNING, positionvalues[index]["symbolid"]) \
      redis.log(redis.LOG_WARNING, "price") \
      redis.log(redis.LOG_WARNING, positionvalues[index]["price"]) \
      redis.log(redis.LOG_WARNING, "unrealisedpandl") \
      redis.log(redis.LOG_WARNING, positionvalues[index]["unrealisedpandl"]) \
      totalpositionvalue["margin"] = totalpositionvalue["margin"] + tonumber(positionvalues[index]["margin"]) \
      totalpositionvalue["unrealisedpandl"] = totalpositionvalue["unrealisedpandl"] + tonumber(positionvalues[index]["unrealisedpandl"]) \
    end \
    return totalpositionvalue \
  end \
  ';

  exports.gettotalpositionvalue = gettotalpositionvalue;

  /*
  * getpositionsatadate()
  * gets all positions for a specified stock and date
  * params: brokerid, date, symbolid
  * returns: all position records as a table
  */
  getpositionsatadate = getpositions + '\
  local getpositions = function(brokerid, exdate, symbolid) \
    local tblresults = {} \
    return tblresults \
  end \
  ';

  /*
  * getbrokeraccountid()
  * gets a broker accountid for a default broker account
  * params: brokerid, currencyid, account name
  * returns: accountid if found, else 0
  */
  getbrokeraccountid = '\
  local getbrokeraccountid = function(brokerid, currencyid, name) \
    local brokerkey = "broker:" .. brokerid \
    local brokeraccountsmapid = redis.call("get", brokerkey .. ":" .. name .. ":" .. currencyid) \
    local accountid = 0 \
    if brokeraccountsmapid then \
      accountid = redis.call("hget", brokerkey .. ":brokeraccountsmap:" .. brokeraccountsmapid, "accountid") \
    end \
    return accountid \
  end \
  ';

  exports.getbrokeraccountid = getbrokeraccountid;

  /*
  * getclientaccountid()
  * gets the account of a designated type for a client
  * params: brokerid, clientid, accounttypeid
  * returns the first account id found for the account type
  */
  getclientaccountid = '\
  local getclientaccountid = function(brokerid, clientid, accounttypeid) \
    local acctid \
    local clientaccounts = redis.call("smembers", "broker:" .. brokerid .. ":client:" .. clientid .. ":clientaccounts") \
    for index = 1, #clientaccounts do \
      local accttypeid = redis.call("hget", "broker:" .. brokerid .. ":account:" .. clientaccounts[index], "accounttypeid") \
      if tonumber(accttypeid) == tonumber(accounttypeid) then \
        acctid = clientaccounts[index] \
        break \
      end \
    end \
    return acctid \
  end \
  ';

  exports.getclientaccountid = getclientaccountid; 

  /*
  * calculates free margin for an account
  * balance = cash
  * equity = balance + unrealised p&l
  * free margin = equity - margin used to hold positions
  */
  getfreemargin = getaccountbalance + gettotalpositionvalue + '\
  local getfreemargin = function(accountid, brokerid) \
    redis.log(redis.LOG_WARNING, "getfreemargin") \
    local freemargin = 0 \
    local accountbalance = getaccountbalance(accountid, brokerid) \
    if accountbalance then \
      redis.log(redis.LOG_WARNING, "balance") \
      redis.log(redis.LOG_WARNING, accountbalance) \
      local totalpositionvalue = gettotalpositionvalue(accountid, brokerid) \
      local equity = tonumber(accountbalance) + totalpositionvalue["unrealisedpandl"] \
      redis.log(redis.LOG_WARNING, "totunrealisedpandl") \
      redis.log(redis.LOG_WARNING, totalpositionvalue["unrealisedpandl"]) \
      redis.log(redis.LOG_WARNING, "margin") \
      redis.log(redis.LOG_WARNING, totalpositionvalue["margin"]) \
      freemargin = equity - totalpositionvalue["margin"] \
    end \
    return freemargin \
  end \
  ';

  exports.getfreemargin = getfreemargin;

  /*
  * newtradeaccounttransaction()
  * create transaction & postings for the cash side of a trade
  * params: amount, brokerid, clientaccountid, currencyid, localamount, nominalaccountid, note, rate, timestamp, tradeid, transactiontype, timestamp in milliseconds
  */
  newtradeaccounttransaction = newtransaction + newposting + getbrokeraccountid + '\
  local newtradeaccounttransaction = function(amount, brokerid, clientaccountid, currencyid, localamount, nominalaccountid, note, rate, timestamp, tradeid, transactiontype, milliseconds) \
    local clientcontrolaccountid = getbrokeraccountid(brokerid, currencyid, "clientcontrolaccount") \
    local transactionid = newtransaction(amount, brokerid, currencyid, localamount, note, rate, "trade:" .. tradeid, timestamp, transactiontype) \
    if transactiontype == "TR" then \
      --[[ receipt from broker point of view ]] \
      newposting(clientaccountid, -amount, brokerid, -localamount, transactionid, milliseconds) \
      newposting(clientcontrolaccountid, -amount, brokerid, -localamount, transactionid, milliseconds) \
      newposting(nominalaccountid, amount, brokerid, localamount, transactionid, milliseconds) \
    else \
      --[[ pay from broker point of view ]] \
      newposting(clientaccountid, amount, brokerid, localamount, transactionid, milliseconds) \
      newposting(clientcontrolaccountid, amount, brokerid, localamount, transactionid, milliseconds) \
      newposting(nominalaccountid, -amount, brokerid, -localamount, transactionid, milliseconds) \
    end \
  end \
  ';

  /*
  * newtradeaccounttransactions()
  * cash side of a client trade
  * creates a separate transaction for the consideration & each of the cost items
  */
  newtradeaccounttransactions = newtradeaccounttransaction + '\
  local newtradeaccounttransactions = function(consideration, commission, ptmlevy, stampduty, brokerid, clientaccountid, currencyid, localamount, note, rate, timestamp, tradeid, side, milliseconds) \
    redis.log(redis.LOG_WARNING, "newtradeaccounttransactions") \
    local nominaltradeaccountid = getbrokeraccountid(brokerid, currencyid, "nominaltradeaccount") \
    local nominalcommissionaccountid = getbrokeraccountid(brokerid, currencyid, "nominalcommissionaccount") \
    local nominalptmaccountid = getbrokeraccountid(brokerid, currencyid, "nominalptmaccount") \
    local nominalstampdutyaccountid = getbrokeraccountid(brokerid, currencyid, "nominalstampdutyaccount") \
    --[[ side determines pay / receive ]] \
    if tonumber(side) == 1 then \
      --[[ client buy, so cash received from broker point of view ]] \
      newtradeaccounttransaction(consideration, brokerid, clientaccountid, currencyid, localamount, nominaltradeaccountid, note, rate, timestamp, tradeid, "TR", milliseconds) \
    else \
      --[[ cash paid from broker point of view ]] \
      newtradeaccounttransaction(consideration, brokerid, clientaccountid, currencyid, localamount, nominaltradeaccountid, note, rate, timestamp, tradeid, "TP", milliseconds) \
    end \
    --[[ broker always receives costs ]] \
    newtradeaccounttransaction(commission, brokerid, clientaccountid, currencyid, commission, nominalcommissionaccountid, note .. " Commission", rate, timestamp, tradeid, "TR", milliseconds) \
    newtradeaccounttransaction(ptmlevy, brokerid, clientaccountid, currencyid, ptmlevy, nominalptmaccountid, note .. " PTM Levy", rate, timestamp, tradeid, "TR", milliseconds) \
    newtradeaccounttransaction(stampduty, brokerid, clientaccountid, currencyid, stampduty, nominalstampdutyaccountid, note .. " Stamp Duty", rate, timestamp, tradeid, "TR", milliseconds) \
    return 0 \
  end \
  ';

  exports.newtradeaccounttransactions = newtradeaccounttransactions;

  /*
  * newpositiontransaction()
  * a transaction to either create or update a position and create a position posting
  */
  newpositiontransaction = getpositionid + newposition + updateposition + newpositionposting + '\
  local newpositiontransaction = function(accountid, brokerid, cost, futsettdate, linkid, positionpostingtypeid, quantity, symbolid, timestamp, milliseconds) \
    local positionid = getpositionid(accountid, brokerid, symbolid, futsettdate) \
    if not positionid then \
      --[[ no positiion, so create a new one ]] \
      positionid = newposition(accountid, brokerid, cost, futsettdate, quantity, symbolid) \
    else \
      --[[ just update it ]] \
      updateposition(accountid, brokerid, cost, futsettdate, positionid, quantity, symbolid) \
    end \
    newpositionposting(brokerid, cost, linkid, positionid, positionpostingtypeid, quantity, timestamp, milliseconds) \
  end \
  ';

  exports.newpositiontransaction = newpositiontransaction;

  /*
  * geteodprice()
  * get end of day prices for a symbol as a date
  * params: eoddate, symbolid
  * returns: all fields in eodprices hash
  */
  geteodprice = gethashvalues + '\
  local geteodprice = function(eoddate, symbolid) \
    local eodprice = gethashvalues("symbol:" .. symbolid .. ":eoddate:" .. eoddate) \
    return eodprice \
  end \
  ';

  publishtrade = gethashvalues + '\
  local publishtrade = function(brokerid, tradeid, channel) \
    redis.log(redis.LOG_WARNING, "publishtrade") \
    local trade = gethashvalues("broker:" .. brokerid .. ":trade:" .. tradeid) \
    redis.call("publish", channel, "{" .. cjson.encode("trade") .. ":" .. cjson.encode(trade) .. "}") \
  end \
  ';

  /*
  * newtrade()
  * stores a trade & updates cash & position
  */
  newtrade = newpositiontransaction + newtradeaccounttransactions + publishtrade + '\
  local newtrade = function(accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, costs, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, operatortype, operatorid, finance, milliseconds) \
    redis.log(redis.LOG_WARNING, "newtrade") \
    local brokerkey = "broker:" .. brokerid \
    local tradeid = redis.call("hincrby", brokerkey, "lasttradeid", 1) \
    if not tradeid then return 0 end \
    redis.call("hmset", brokerkey .. ":trade:" .. tradeid, "accountid", accountid, "brokerid", brokerid, "clientid", clientid, "orderid", orderid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "commission", costs[1], "ptmlevy", costs[2], "stampduty", costs[3], "contractcharge", costs[4], "counterpartyid", counterpartyid, "counterpartytype", counterpartytype, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", tradeid, "settlcurrencyid", settlcurrencyid, "settlcurramt", settlcurramt, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "margin", margin, "finance", finance, "tradesettlestatusid", 0) \
    redis.call("sadd", brokerkey .. ":trades", tradeid) \
    redis.call("sadd", brokerkey .. ":account:" .. accountid .. ":trades", tradeid) \
    redis.call("sadd", brokerkey .. ":order:" .. orderid .. ":trades", tradeid) \
    local cost \
    local note \
    if tonumber(side) == 1 then \
      cost = settlcurramt \
      note = "Bought " .. quantity .. " " .. symbolid .. " @ " .. price \
    else \
      quantity = -tonumber(quantity) \
      cost = -tonumber(settlcurramt) \
      note = "Sold " .. quantity .. " " .. symbolid .. " @ " .. price \
    end \
    local retval = newtradeaccounttransactions(settlcurramt, costs[1], costs[2], costs[3], brokerid, accountid, settlcurrencyid, settlcurramt, note, 1, timestamp, tradeid, side, milliseconds) \
    newpositiontransaction(accountid, brokerid, cost, futsettdate, tradeid, 1, quantity, symbolid, timestamp, milliseconds) \
    publishtrade(brokerid, tradeid, 6) \
    return tradeid \
  end \
  ';

  exports.newtrade = newtrade;

  /*
  * getcorporateactionsclientdecisionbyclient()
  * get a corporate action client decision
  * params: brokerid, clientid, corporateactionid
  * returns: corporateactiondecision as a string or nil if not found
  */
  getcorporateactionsclientdecisionbyclient = '\
  local getcorporateactionsclientdecisionbyclient = function(brokerid, clientid, corporateactionid) \
    redis.log(redis.LOG_WARNING, "getcorporateactionsclientdecisionbyclient") \
    local brokerkey = "broker:" .. brokerid \
    local corporateactiondecisionid = redis.call("get", brokerkey .. ":client:" .. clientid .. ":corporateaction:" .. corporateactionid .. ":corporateactiondecision") \
    local corporateactiondecision \
    if corporateactiondecisionid then \
      corporateactiondecision = redis.call("hget", brokerkey .. ":corporateactiondecision:" .. corporateactiondecisionid, "decision") \
    end \
    return corporateactiondecision \
  end \
  ';

  /*
  * getsharesdue()
  * get the number of shares due & any remainder for a scrip issue
  * params: position quantity, sharespershare ratio
  * returns: whole number of shares due, remainder
  */
  getsharesdue = round + '\
  local getsharesdue = function(posqty, sharespershare) \
    redis.log(redis.LOG_WARNING, "getsharesdue") \
    local sharesdue = round(tonumber(posqty) * tonumber(sharespershare), 2) \
    local sharesdueint = math.floor(sharesdue) \
    local sharesduerem = sharesdue - sharesdueint \
    redis.log(redis.LOG_WARNING, "sharesdue") \
    redis.log(redis.LOG_WARNING, sharesdue) \
    redis.log(redis.LOG_WARNING, "sharesdueint") \
    redis.log(redis.LOG_WARNING, sharesdueint) \
    redis.log(redis.LOG_WARNING, "remainder") \
    redis.log(redis.LOG_WARNING, sharesduerem) \
    return {sharesdueint, sharesduerem} \
  end \
  ';

  /*** Scripts ***/

  /*
  *
  * get holidays for a market, i.e. "L" = London...assume "L" for the time being?
  */
  exports.scriptgetholidays = '\
  local tblresults = {} \
  local holidays = redis.call("smembers", "holidays:" .. ARGV[1]) \
  for index = 1, #holidays do \
    table.insert(tblresults, {holidays[index]}) \
  end \
  return cjson.encode(tblresults) \
  ';

  /*
  * scriptgetquoterequests
  * get quote requests for an account, most recent first
  * params: accountid, brokerid
  */
  exports.scriptgetquoterequests = gethashvalues + '\
  local tblresults = {} \
  local quoterequests = redis.call("sort", "broker:" .. ARGV[1] .. ":account:" .. ARGV[2] .. ":quoterequests", "DESC") \
  for index = 1, #quoterequests do \
    local quoterequest = gethashvalues("broker:" .. ARGV[1] .. ":quoterequest:" .. quoterequests[index]) \
    table.insert(tblresults, quoterequest) \
  end \
  return cjson.encode(tblresults) \
  ';

  /*
  * scriptgetquotes
  * get quotes for an account, most recent first
  * params: accountid, brokerid
  */
  exports.scriptgetquotes = gethashvalues + '\
  local tblresults = {} \
  local quotes = redis.call("sort", "broker:" .. ARGV[1] .. ":account:" .. ARGV[2] .. ":quotes", "DESC") \
  for index = 1, #quotes do \
    local quote = gethashvalues("broker:" .. ARGV[1] .. ":quote:" .. quotes[index]) \
    table.insert(tblresults, quote) \
  end \
  return cjson.encode(tblresults) \
  ';

  /*
  * scriptgetorders
  * get orders for an account, most recent first
  * params: accountid, brokerid
  */
  exports.scriptgetorders = gethashvalues + '\
  local tblresults = {} \
  local orders = redis.call("sort", "broker:" .. ARGV[1] .. ":account:" .. ARGV[2] .. ":orders", "DESC") \
  for index = 1, #orders do \
    local order = gethashvalues("broker:" .. ARGV[1] .. ":order:" .. orders[index]) \
    table.insert(tblresults, order) \
  end \
  return cjson.encode(tblresults) \
  ';

  /*
  * scriptgettrades
  * get trades for an account, most recent first
  * params: accountid, brokerid
  */
  exports.scriptgettrades = gethashvalues + '\
  local tblresults = {} \
  local trades = redis.call("sort", "broker:" .. ARGV[1] .. ":account:" .. ARGV[2] .. ":trades", "DESC") \
  for index = 1, #trades do \
    local trade = gethashvalues("broker:" .. ARGV[1] .. ":trade:" .. trades[index]) \
    table.insert(tblresults, trade) \
  end \
  return cjson.encode(tblresults) \
  ';

  /*
  * get positions for an account
  * params: accountid, brokerid
  * returns an array of positions as JSON
  */
  exports.scriptgetpositions = getpositions + '\
  local positions = getpositions(ARGV[1], ARGV[2]) \
  return cjson.encode(positions) \
  ';

  /*
  * scriptgetpositionvalues
  * get positions for an account
  * params: accountid, brokerid
  * returns: positions with their values
  */
  exports.scriptgetpositionvalues = getpositionvalues + '\
  local positionvalues = getpositionvalues(ARGV[1], ARGV[2]) \
  return cjson.encode(positionvalues) \
  ';

  /*
  * scriptgetpositionsbysymbol
  * get positions for a symbol
  * params: brokerid, symbolid
  * returns: a table of positions
  */
  exports.scriptgetpositionsbysymbol = getpositionsbysymbol + '\
    redis.log(redis.LOG_WARNING, "scriptgetpositionsbysymbol") \
    local positions = getpositionsbysymbol(ARGV[1], ARGV[2]) \
    return positions \
  ';

  /*
  * scriptgetpositionsbysymbolbydate
  * get positions as at a date
  * params: brokerid, symbolid, millisecond representation of the date
  * returns: a table of positions
  */
  exports.scriptgetpositionsbysymbolbydate = getpositionsbysymbolbydate + '\
    redis.log(redis.LOG_WARNING, "scriptgetpositionsbysymbolbydate") \
    local positions = getpositionsbysymbolbydate(ARGV[1], ARGV[2], ARGV[3]) \
    return positions \
  ';

  /*
  * scriptgetpositionpostings
  * params: brokerid, positionid, startmilliseconds, endmilliseconds
  * returns: array of postings
  */
  exports.scriptgetpositionpostings = getpositionpostingsbydate + '\
  local positionpostings = getpositionpostingsbydate(ARGV[1], ARGV[2], ARGV[3], ARGV[4]) \
  return cjson.encode(positionpostings) \
  ';

  /*
  * scriptgetaccountsummary
  * calculates account p&l, margin & equity for a client
  * params: accountid, brokerid
  */
  exports.scriptgetaccountsummary = getaccountbalance + gettotalpositionvalue + '\
  redis.log(redis.LOG_WARNING, "scriptgetaccountsummary") \
  local tblresults = {} \
  local accountbalance = getaccountbalance(ARGV[1], ARGV[2]) \
  local totalpositionvalue = gettotalpositionvalue(ARGV[1], ARGV[2]) \
  local equity = tonumber(accountbalance) + totalpositionvalue["unrealisedpandl"] \
  local freemargin = equity - totalpositionvalue["margin"] \
  table.insert(tblresults, {accountid=ARGV[1],balance=accountbalance,unrealisedpandl=totalpositionvalue["unrealisedpandl"],equity=equity,margin=totalpositionvalue["margin"],freemargin=freemargin}) \
  return cjson.encode(tblresults) \
  ';

  /*
  * newClientFundsTransfer
  * script to handle client deposits & withdrawals
  * keys: broker:<brokerid>
  * args: amount, bankaccountid, brokerid, clientaccountid, localamount, note, rate, reference, timestamp, transactiontypeid, timestampms
  * returns: 0 if successful, else 1 & an error message code if unsuccessful
  */
  exports.newClientFundsTransfer = newtransaction + newposting + getbrokeraccountid + '\
    redis.log(redis.LOG_WARNING, "newClientFundsTransfer") \
    local currencyid = redis.call("hget", "broker:" .. ARGV[3] .. ":account:" ..ARGV[4], "currencyid") \
    if not currencyid then \
      return {1, 1031} \
    end \
    local controlclientaccountid = getbrokeraccountid(ARGV[3], currencyid, "controlclient") \
    local amount \
    local localamount \
    if ARGV[10] == "CD" then \
      amount = tonumber(ARGV[1]) \
      localamount = tonumber(ARGV[5]) \
    else \
      amount = -tonumber(ARGV[1]) \
      localamount = -tonumber(ARGV[5]) \
    end \
    local transactionid = newtransaction(ARGV[1], ARGV[3], currencyid, ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10]) \
    --[[ update client account ]] \
    newposting(ARGV[4], amount, ARGV[3], localamount, transactionid, ARGV[11]) \
    --[[ client control account ]] \
    newposting(controlclientaccountid, amount, ARGV[3], localamount, transactionid, ARGV[11]) \
    --[[ update bank account ]] \
    newposting(ARGV[2], amount, ARGV[3], localamount, transactionid, ARGV[11]) \
    return {0} \
  ';

  /*
  * newTradeSettlementTransaction
  * script to handle settlement of trades
  * params: amount, brokerid, fromaccountid, localamount, note, rate, timestamp, toaccountid, tradeid, transactiontypeid, timestampms
  * returns: 0 if ok, else error message
  */
  exports.newTradeSettlementTransaction = newtransaction + newposting + '\
    redis.log(redis.LOG_WARNING, "newTradeSettlementTransaction") \
    local currencyid = redis.call("hget", "broker:" .. ARGV[2] .. ":account:" ..ARGV[4], "currencyid") \
    --[[ transactiontypeid may be passed, else derive it ]] \
    local transactiontypeid = ARGV[10] \
    if transactiontypeid == "" then \
      local brokeraccountkey = "broker:" .. ARGV[2] .. ":account:" \
      local fromaccountgroupid = redis.call("hget", brokeraccountkey .. ARGV[4], "accountgroupid") \
      local toaccountgroupid = redis.call("hget", brokeraccountkey .. ARGV[9], "accountgroupid") \
      if not fromaccountgroupid or not toaccountgroupid then return "Account not found" end \
      if tonumber(fromaccountgroupid) == 1 and tonumber(toaccountgroupid) == 5 then \
        transactiontypeid = "BP" \
      elseif tonumber(fromaccountgroupid) == 5 and tonumber(toaccountgroupid) == 1 then \
        transactiontypeid = "BR" \
      else \
        return "Invalid account group" \
      end \
    end \
    local transactionid = newtransaction(ARGV[1], ARGV[2], currencyid, ARGV[3], ARGV[5], ARGV[6], ARGV[7], ARGV[8], transactiontypeid) \
    newposting(ARGV[4], -tonumber(ARGV[1]), ARGV[2], -tonumber(ARGV[3]), transactionid, ARGV[11]) \
    newposting(ARGV[9], ARGV[1], ARGV[2], ARGV[3], transactionid, ARGV[11]) \
    return 0 \
  ';

  /*
  * newSupplierFundsTransfer
  * script to handle receipts from and payments to a supplier
  * params: amount, bankaccountid, brokerid, localamount, note, rate, reference, supplieraccountid, timestamp, transactiontypeid, timestampms
  * returns: 0
  */
  exports.newSupplierFundsTransfer = newtransaction + newposting + '\
    redis.log(redis.LOG_WARNING, "newSupplierFundsTransfer") \
    local amount \
    local localamount \
    local currencyid = redis.call("hget", "broker:" ..ARGV[3] .. ":account:" ..ARGV[8], "currencyid") \
    if ARGV[10] == "SP" then \
      amount = -tonumber(ARGV[1]) \
      localamount = -tonumber(ARGV[4]) \
    else \
      amount = ARGV[1] \
      localamount = ARGV[4] \
    end \
    local transactionid = newtransaction(ARGV[1], ARGV[3], currencyid, ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[9], ARGV[10]) \
    newposting(ARGV[8], amount, ARGV[3], localamount, transactionid, ARGV[11]) \
    newposting(ARGV[2], amount, ARGV[3], localamount, transactionid, ARGV[11]) \
    return 0 \
  ';

  /*
  * scriptgetstatement
  * prepares a statement for an account between two dates
  * params: accountid, brokerid, start date, end date
  */
  exports.scriptgetstatement = getaccountbalance + getpostingsbydate + '\
    redis.log(redis.LOG_WARNING, "scriptgetstatement") \
    local tblresults = {} \
    --[[ get the currenct account balance ]] \
    local accountbalance = tonumber(getaccountbalance(ARGV[1], ARGV[2])) \
    --[[ get all the postings from the start date to now ]] \
    local postings = getpostingsbydate(ARGV[1], ARGV[2], ARGV[3], ARGV[4]) \
    --[[ go backwards through the postings to calculate a start balance ]] \
    for index = #postings, 1, -1 do \
      accountbalance = accountbalance - tonumber(postings[index]["amount"]) \
    end \
    --[[ go forwards through the postings, calculating the account balance after each one ]]\
    for index = 1, #postings do \
      accountbalance = accountbalance + tonumber(postings[index]["amount"]) \
      postings[index]["balance"] = accountbalance \
      table.insert(tblresults, postings[index]) \
    end \
    return cjson.encode(tblresults) \
  ';

  /*
  * applycacashdividend()
  * script to apply a cash dividend
  * params: brokerid, corporateactionid, exdatems, timestamp, timestampms
  * returns: 0 if ok, else 1 + an error message if unsuccessful
  */
  exports.applycacashdividend = getpositionsbysymbolbydate + round + newtransaction + getbrokeraccountid + newposting + '\
    redis.log(redis.LOG_WARNING, "applycacashdividend") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local exdatems = ARGV[3] \
    local timestamp = ARGV[4] \
    local timestampms = ARGV[5] \
    local numaccountsupdated = 0 \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
    --[[ get the symbol ]] \
    local symbol = gethashvalues("symbol:" .. corporateaction["symbolid"]) \
    if not symbol["symbolid"] then \
      return {1, 1015} \
    end \
    --[[ get the relevant broker accounts ]] \
    local nominalcorporateactions = getbrokeraccountid(brokerid, symbol["currencyid"], "nominalcorporateactions") \
    local bankfundsbroker = getbrokeraccountid(brokerid, symbol["currencyid"], "bankfundsbroker") \
    if not nominalcorporateactions or not bankfundsbroker then \
      return {1, 1027} \
    end \
    --[[ get all positions in the stock of the corporate action as at the ex-date ]] \
    local positions = getpositionsbysymbolbydate(brokerid, corporateaction["symbolid"], exdatems) \
    for i = 1, #positions do \
      redis.log(redis.LOG_WARNING, "accountid") \
      redis.log(redis.LOG_WARNING, positions[i]["accountid"]) \
      redis.log(redis.LOG_WARNING, "quantity") \
      redis.log(redis.LOG_WARNING, positions[i]["quantity"]) \
      local posqty = tonumber(positions[i]["quantity"]) \
      --[[ may have a position with no quantity ]] \
      if posqty ~= 0 then \
        local dividend = round(posqty * tonumber(corporateaction["cashpershare"]), 2) \
        redis.log(redis.LOG_WARNING, "dividend") \
        redis.log(redis.LOG_WARNING, dividend) \
        if dividend ~= 0 then \
          local nominalcorporateactionstransactiontype \
          local bankfundsbrokertransactiontype \
          if dividend > 0 then \
            nominalcorporateactionstransactiontype = "AR" \
            bankfundsbrokertransactiontype = "AP" \
          else \
            nominalcorporateactionstransactiontype = "AP" \
            bankfundsbrokertransactiontype = "AR" \
          end \
          --[[ create transactions & postings ]] \
          local transactionid = newtransaction(dividend, brokerid, symbol["currencyid"], dividend, corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, bankfundsbrokertransactiontype) \
          newposting(bankfundsbroker, dividend, brokerid, dividend, transactionid, timestampms) \
          transactionid = newtransaction(dividend, brokerid, symbol["currencyid"], dividend, corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, nominalcorporateactionstransactiontype) \
          newposting(nominalcorporateactions, dividend, brokerid, dividend, transactionid, timestampms) \
          newposting(positions[i]["accountid"], dividend, brokerid, dividend, transactionid, timestampms) \
          numaccountsupdated = numaccountsupdated + 1 \
        end \
      end \
    end \
    return {0, numaccountsupdated} \
  ';

  /*
  * applycadividendscrip()
  * script to apply a scrip dividend
  * params: brokerid, corporateactionid, exdatems, timestamp, timestampms
  * returns: 0 if ok, else 1 + an error message if unsuccessful
  */
  exports.applycadividendscrip = getbrokeraccountid + getpositionsbysymbolbydate + getclientfromaccount + getcorporateactionsclientdecisionbyclient + getsharesdue + newpositiontransaction + newtransaction + newposting + round + '\
    redis.log(redis.LOG_WARNING, "applycadividendscrip") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local exdatems = ARGV[3] \
    local timestamp = ARGV[4] \
    local timestampms = ARGV[5] \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction then \
      return {1, 1028} \
    end \
    --[[ get the symbol ]] \
    local symbol = gethashvalues("symbol:" .. corporateaction["symbolid"]) \
    if not symbol["symbolid"] then \
      return {1, 1015} \
    end \
    --[[ get the broker account ids ]] \
    local bankfundsbroker = getbrokeraccountid(brokerid, symbol["currencyid"], "bankfundsbroker") \
    local nominalcorporateactions = getbrokeraccountid(brokerid, symbol["currencyid"], "nominalcorporateactions") \
    --[[ get all positions in the stock of the corporate action as at the ex-date ]] \
    local positions = getpositionsbysymbolbydate(brokerid, corporateaction["symbolid"], exdatems) \
    for i = 1, #positions do \
      redis.log(redis.LOG_WARNING, "accountid") \
      redis.log(redis.LOG_WARNING, positions[i]["accountid"]) \
      redis.log(redis.LOG_WARNING, "quantity") \
      redis.log(redis.LOG_WARNING, positions[i]["quantity"]) \
      if tonumber(positions[i]["quantity"]) > 0 then \
        --[[ get the client ]] \
        local clientid = getclientfromaccount(positions[i]["accountid"], brokerid) \
        if not clientid then \
          return {1, 1017} \
        end \
        --[[ get the client decision to take cash or scrip, default is cash ]] \
        local corporateactiondecision = getcorporateactionsclientdecisionbyclient(brokerid, clientid, corporateactionid) \
        if corporateactiondecision == "SCRIP" then \
          local sharesdue = getsharesdue(positions[i]["quantity"], corporateaction["sharespershare"]) \
          if sharesdue[1] > 0 then \
            --[[ add to position ]] \
            newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, sharesdue[1], corporateaction["symbolid"], timestamp, timestampms) \
          end \
          if sharesdue[2] > 0 then \
            --[[ create transactions & postings ]] \
            local transactionid = newtransaction(sharesdue[2], brokerid, symbol["currencyid"], sharesdue[2], corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, "AP") \
            newposting(bankfundsbroker, -sharesdue[2], brokerid, -sharesdue[2], transactionid, timestampms) \
            transactionid = newtransaction(sharesdue[2], brokerid, symbol["currencyid"], sharesdue[2], corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, "AR") \
            newposting(nominalcorporateactions, sharesdue[2], brokerid, sharesdue[2], transactionid, timestampms) \
            newposting(positions[i]["accountid"], sharesdue[2], brokerid, sharesdue[2], transactionid, timestampms) \
          end \
        else \
          local dividend = round(corporateaction["cashpershare"] * tonumber(positions[i]["quantity"]), 2) \
          if dividend > 0 then \
            --[[ create transactions & postings ]] \
            local transactionid = newtransaction(dividend, brokerid, symbol["currencyid"], dividend, corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, "AP") \
            newposting(bankfundsbroker, -dividend, brokerid, -dividend, transactionid, timestampms) \
            newposting(nominalcorporateactions, dividend, brokerid, dividend, transactionid, timestampms) \
            newposting(positions[i]["accountid"], dividend, brokerid, dividend, transactionid, timestampms) \
          end \
        end \
      end \
    end \
    return {0} \
  ';

  /*
  * applycarightsexdate()
  * script to apply the first part of a rights issue, as at ex-date
  * params: brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms
  * note: exdate is actual exdate - 1 & exdatems is millisecond representation of time at the end of exdate - 1
  * returns: 0 if ok, else 1 + an error message if unsuccessful
  */
  exports.applycarightsexdate = getbrokeraccountid + getpositionsbysymbolbydate + getsharesdue + geteodprice + newpositiontransaction + newtransaction + newposting + '\
    redis.log(redis.LOG_WARNING, "applycarightsexdate") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local exdate = ARGV[3] \
    local exdatems = ARGV[4] \
    local timestamp = ARGV[5] \
    local timestampms = ARGV[6] \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
    --[[ get the symbol ]] \
    local symbol = gethashvalues("symbol:" .. corporateaction["symbolid"]) \
    if not symbol["symbolid"] then \
      return {1, 1015} \
    end \
    --[[ make sure a symbol for the rights exists ]] \
    if not corporateaction["symbolidnew"] then \
      return {1, 1030} \
    end \
    --[[ get the symbol for the rights ]] \
    local symbolnew = gethashvalues("symbol:" .. corporateaction["symbolidnew"]) \
    if not symbolnew["symbolid"] then \
      return {1, 1030} \
    end \
    --[[ get the closing price for the stock as at the exdate ]] \
    local eodprice = geteodprice(exdate, corporateaction["symbolid"]) \
    if not eodprice["bid"] then \
      return {1, 1029} \
    end \
    --[[ get the relevant broker accounts ]] \
    local nominalcorporateactions = getbrokeraccountid(brokerid, symbol["currencyid"], "nominalcorporateactions") \
    local bankfundsbroker = getbrokeraccountid(brokerid, symbol["currencyid"], "bankfundsbroker") \
    if not nominalcorporateactions or not bankfundsbroker then \
      return {1, 1027} \
    end \
    --[[ get all positions in the stock of the corporate action as at the ex date ]] \
    local positions = getpositionsbysymbolbydate(brokerid, corporateaction["symbolid"], exdatems) \
    for i = 1, #positions do \
      --[[ only interested in long positions ]] \
      if tonumber(positions[i]["quantity"]) > 0 then \
        redis.log(redis.LOG_WARNING, "accountid") \
        redis.log(redis.LOG_WARNING, positions[i]["accountid"]) \
        redis.log(redis.LOG_WARNING, "quantity") \
        redis.log(redis.LOG_WARNING, positions[i]["quantity"]) \
        --[[ get shares due & any remainder ]] \
        local sharesdue = getsharesdue(positions[i]["quantity"], corporateaction["sharespershare"]) \
        if sharesdue[1] > 0 then \
          --[[ create a position in the rights ]] \
          newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, sharesdue[1], corporateaction["symbolidnew"], timestamp, timestampms) \
        end \
        if sharesdue[2] > 0 then \
          --[[ calculate how much cash is due ]] \
          local stubcash = round(sharesdue[2] * eodprice["bid"], 2) / 100 \
          --[[ create transactions & postings ]] \
          local transactionid = newtransaction(stubcash, brokerid, symbol["currencyid"], stubcash, corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, "AP") \
          newposting(bankfundsbroker, stubcash, brokerid, stubcash, transactionid, timestampms) \
          transactionid = newtransaction(stubcash, brokerid, symbol["currencyid"], stubcash, corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, "AR") \
          newposting(nominalcorporateactions, stubcash, brokerid, stubcash, transactionid, timestampms) \
          newposting(positions[i]["accountid"], stubcash, brokerid, stubcash, transactionid, timestampms) \
        end \
      end \
    end \
    return {0} \
  ';

  /*
  * applycarightspaydate()
  * script to apply the second part of a rights issue, as at pay-date
  * params: brokerid, corporateactionid, paydatems, timestamp, timestampms, operatortype, operatorid
  * returns: 0 if ok, else 1 + an error message if unsuccessful
  */
  exports.applycarightspaydate = getpositionsbysymbolbydate + getclientfromaccount + getcorporateactionsclientdecisionbyclient + newtrade + newpositiontransaction + '\
    redis.log(redis.LOG_WARNING, "applycarightspaydate") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local paydatems = ARGV[3] \
    local timestamp = ARGV[4] \
    local timestampms = ARGV[5] \
    local operatortype = ARGV[6] \
    local operatorid = ARGV[7] \
    local currencyratetoorg = 1 \
    local currencyindtoorg = 1 \
    local costs = {0,0,0,0} \
    local settlcurrfxrate = 1 \
    local settlcurrfxratecalc = 1 \
    local side = 1 \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
    --[[ get the symbol ]] \
    local symbol = gethashvalues("symbol:" .. corporateaction["symbolid"]) \
    if not symbol["symbolid"] then \
      return {1, 1015} \
    end \
    --[[ get all positions in the rights symbol as at the pay date ]] \
    local positions = getpositionsbysymbolbydate(brokerid, corporateaction["symbolidnew"], paydatems) \
    for i = 1, #positions do \
      --[[ only interested in long positions ]] \
      if tonumber(positions[i]["quantity"]) > 0 then \
        --[[ get the client decision ]] \
        local clientid = getclientfromaccount(positions[i]["accountid"], brokerid) \
        if not clientid then \
          return {1, 1017} \
        end \
        local corporateactiondecision = getcorporateactionsclientdecisionbyclient(brokerid, clientid, corporateactionid) \
        if corporateactiondecision == "EXERCISE" then \
          --[[ create a trade in the original symbol at the rights price ]] \
          newtrade(positions[i]["accountid"], brokerid, clientid, "", corporateaction["symbolid"], side, positions[i]["quantity"], corporateaction["price"], symbol["currencyid"], currencyratetoorg, currencyindtoorg, costs, "", "", 0, "", "", timestamp, "", "", symbol["currencyid"], 0, settlcurrfxrate, settlcurrfxratecalc, 0, operatortype, operatorid, 0, timestampms) \
        end \
        --[[ zero the position in the rights ]] \
        newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, -tonumber(positions[i]["quantity"]), corporateaction["symbolidnew"], timestamp, timestampms) \
      end \
    end \
    return {0} \
  ';

  /*
  * applycastocksplit
  * script to apply a stock split
  * params: corporateactionid, exdate, exdatems, timestamp, timestampms
  * returns: 0 if ok, else 1 + an error message if unsuccessful
  * note: this corporate action is applied across all brokers
  */
  exports.applycastocksplit = geteodprice + getpositionsbysymbolbydate + getsharesdue + newpositiontransaction + getbrokeraccountid + round + newtransaction + newposting + '\
    redis.log(redis.LOG_WARNING, "applycastocksplit") \
    local corporateactionid = ARGV[1] \
    local exdate = ARGV[2] \
    local exdatems = ARGV[3] \
    local timestamp = ARGV[4] \
    local timestampms = ARGV[5] \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
    --[[ get the symbol ]] \
    local symbol = gethashvalues("symbol:" .. corporateaction["symbolid"]) \
    if not symbol["symbolid"] then \
      return {1, 1015} \
    end \
    --[[ get the closing price for the stock as at the exdate ]] \
    local eodprice = geteodprice(exdate, corporateaction["symbolid"]) \
    if not eodprice["bid"] then \
      return {1, 1029} \
    end \
    --[[ we are applying the split across all positions, so need to interate through all brokers ]] \
    local brokers = redis.call("smembers", "brokers") \
    for j = 1, #brokers do \
      --[[ get all positions in the stock of the corporate action as at the ex date for this broker ]] \
      local positions = getpositionsbysymbolbydate(brokers[j], corporateaction["symbolid"], exdatems) \
      for i = 1, #positions do \
        --[[ only interested in long positions ]] \
        if tonumber(positions[i]["quantity"]) > 0 then \
          redis.log(redis.LOG_WARNING, "accountid") \
          redis.log(redis.LOG_WARNING, positions[i]["accountid"]) \
          redis.log(redis.LOG_WARNING, "quantity") \
          redis.log(redis.LOG_WARNING, positions[i]["quantity"]) \
          --[[ get shares due & any remainder ]] \
          local sharesdue = getsharesdue(positions[i]["quantity"], corporateaction["sharespershare"]) \
          if sharesdue[1] > 0 then \
            --[[ update the position ]] \
            newpositiontransaction(positions[i]["accountid"], brokers[j], 0, "", corporateactionid, 2, sharesdue[1], corporateaction["symbolid"], timestamp, timestampms) \
          end \
          if sharesdue[2] > 0 then \
            --[[ get the relevant accounts for this broker ]] \
            local nominalcorporateactions = getbrokeraccountid(brokers[j], symbol["currencyid"], "nominalcorporateactions") \
            local bankfundsclient = getbrokeraccountid(brokers[j], symbol["currencyid"], "bankfundsclient") \
            if not nominalcorporateactions or not bankfundsclient then \
              return {1, 1027} \
            end \
            --[[ calculate how much cash is due ]] \
            local stubcash = round(sharesdue[2] * eodprice["bid"], 2) / 100 \
            --[[ create transaction & postings ]] \
            local transactionid = newtransaction(stubcash, brokers[j], symbol["currencyid"], stubcash, corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, "AR") \
            newposting(nominalcorporateactions, stubcash, brokers[j], stubcash, transactionid, timestampms) \
            newposting(bankfundsclient, stubcash, brokers[j], stubcash, transactionid, timestampms) \
            newposting(positions[i]["accountid"], stubcash, brokers[j], stubcash, transactionid, timestampms) \
          end \
        end \
      end \
    end \
    return {0} \
  ';

  /*
  * applycascripissue()
  * script to apply a scrip issue
  * params: brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms
  * note: exdate is actually exdate - 1 in "YYYYMMDD" format, exdatems is millisecond representation of time at the end of exdate - 1
  * returns: 0 if ok, else 1 + an error message if unsuccessful
  */
  exports.applycascripissue = geteodprice + getbrokeraccountid + getpositionsbysymbolbydate + getsharesdue + newpositiontransaction + newtransaction + newposting + '\
    redis.log(redis.LOG_WARNING, "applycascripissue") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local exdate = ARGV[3] \
    local exdatems = ARGV[4] \
    local timestamp = ARGV[5] \
    local timestampms = ARGV[6] \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
    --[[ get the symbol ]] \
    local symbol = gethashvalues("symbol:" .. corporateaction["symbolid"]) \
    if not symbol["symbolid"] then \
      return {1, 1015} \
    end \
    --[[ get the closing price for the stock as at the exdate ]] \
    local eodprice = geteodprice(exdate, corporateaction["symbolid"]) \
    if not eodprice["bid"] then \
      return {1, 1029} \
    end \
    --[[ get the relevant broker accounts ]] \
    local nominalcorporateactions = getbrokeraccountid(brokerid, symbol["currencyid"], "nominalcorporateactions") \
    local bankfundsbroker = getbrokeraccountid(brokerid, symbol["currencyid"], "bankfundsbroker") \
    if not nominalcorporateactions or not bankfundsbroker then \
      return {1, 1027} \
    end \
    --[[ get all positions in the stock of the corporate action as at the ex date ]] \
    local positions = getpositionsbysymbolbydate(brokerid, corporateaction["symbolid"], exdatems) \
    for i = 1, #positions do \
      --[[ only interested in long positions ]] \
      if tonumber(positions[i]["quantity"]) > 0 then \
        redis.log(redis.LOG_WARNING, "accountid") \
        redis.log(redis.LOG_WARNING, positions[i]["accountid"]) \
        redis.log(redis.LOG_WARNING, "quantity") \
        redis.log(redis.LOG_WARNING, positions[i]["quantity"]) \
        --[[ get shares due & any remainder ]] \
        local sharesdue = getsharesdue(positions[i]["quantity"], corporateaction["sharespershare"]) \
        if sharesdue[1] > 0 then \
          --[[ update the position ]] \
          newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, sharesdue[1], corporateaction["symbolid"], timestamp, timestampms) \
        end \
        if sharesdue[2] > 0 then \
          --[[ calculate how much cash is due ]] \
          local stubcash = round(sharesdue[2] * eodprice["bid"], 2) / 100 \
          --[[ create transactions & postings ]] \
          local transactionid = newtransaction(stubcash, brokerid, symbol["currencyid"], stubcash, corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, "AP") \
          newposting(bankfundsbroker, stubcash, brokerid, stubcash, transactionid, timestampms) \
          transactionid = newtransaction(stubcash, brokerid, symbol["currencyid"], stubcash, corporateaction["description"], 1, "corporateaction:" .. corporateactionid, timestamp, "AR") \
          newposting(nominalcorporateactions, stubcash, brokerid, stubcash, transactionid, timestampms) \
          newposting(positions[i]["accountid"], stubcash, brokerid, stubcash, transactionid, timestampms) \
        end \
      end \
    end \
    return {0} \
  ';
}
