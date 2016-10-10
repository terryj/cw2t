/****************
* commonbo.js
* Common back-office functions
* Cantwaittotrade Limited
* Terry Johnston
* June 2015
* Changes:
* 1 September 2016 - modified corporate action functions to take account of default client decisions - added dividendasshares(), dividendascash()
* 1 September 2016 - modified corporate action functions to use account rather than symbol for currency of transactions
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

  /*
  * Convert dd-mmm-yyyy to FIX date format 'yyyymmdd'
  */
  function getFixDate(date) {
    var month;

    var day = date.substr(0,2);
    var monthstr = date.substr(3,3);
    var year = date.substr(7,4);

    if (monthstr == "Jan") {
      month = "1";
    } else if (monthstr == "Feb") {
      month = "2";
    } else if (monthstr == "Mar") {
      month = "3";
    } else if (monthstr == "Apr") {
      month = "4";
    } else if (monthstr == "May") {
      month = "5";
    } else if (monthstr == "Jun") {
      month = "6";
    } else if (monthstr == "Jul") {
      month = "7";
    } else if (monthstr == "Aug") {
      month = "8";
    } else if (monthstr == "Sep") {
      month = "9";
    } else if (monthstr == "Oct") {
      month = "10";
    } else if (monthstr == "Nov") {
      month = "11";
    } else if (monthstr == "Dec") {
      month = "12";
    }

    if (month.length == 1) {
      month = "0" + month;
    }

    return year + month + day;
  }

  exports.getFixDate = getFixDate;

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
    case 1032:
      desc = "Principle client account required for derivative products";
      break;
    case 1033:
      desc = "Cannot calculate a consideration";
      break;
    case 1034:
      desc = "Either quantity or cashorderqty must be present";
      break;
    case 1035:
      desc = "Please enter a shares per share figure for this corporate action";
      break;
    case 1036:
      desc = "Please enter a cash per share figure for this corporate action";
      break;
    case 1037:
      desc = "Scheme does not exist";
      break;
    case 1038:
      desc = "No scheme cash";
      break;
    case 1039:
      desc = "No clients found in scheme";
      break;
    case 1040:
      desc = "Symbol does not have a price";
      break;
    case 1041:
      desc = "Insufficient funds";
      break;
    default:
      desc = "Unknown reason";
    }

    return desc;
  }

  exports.getReasonDesc = getReasonDesc;

  /*
  * gethashvalues()
  * function to read all field values from a table for a given key
  */
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
  * isHoliday()
  * checks whether a date is a holiday
  * params: date object, set of holidays
  * returns: true if the date is a holiday else false
  */
  function isHoliday(datetocheck, holidays) {
    var datetocheckstr = getUTCDateString(datetocheck);
    if (datetocheckstr in holidays) {
      return true;
    }

    return false;
  }

  /*
  * getSettDate()
  * returns valid trading day as date object, taking into account weekends and holidays
  * params: date object, number of settlement days
  * returns: settlement date object
  * note: this is in javascript as the lua date library is not available from redis
  */
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

  /*
  * function to split a string based on a pattern
  * params: string, pattern
  * returns: table of strings
  */
  split = '\
  local split = function(str, sep) \
    local t = {} \
    for k in string.gmatch(str, "[^" .. sep .. "]+") do \
      table.insert(t, k) \
    end \
    return t \
  end \
  ';

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

  /*
  * rejectorder()
  * rejects an order with a reason & any additional text
  */
  rejectorder = '\
  local rejectorder = function(brokerid, orderid, orderrejectreasonid, text) \
    redis.log(redis.LOG_NOTICE, "order rejected: " .. text) \
    if orderid ~= "" then \
      redis.call("hmset", "broker:" .. brokerid .. ":order:" .. orderid, "orderstatusid", "8", "orderrejectreasonid", orderrejectreasonid, "text", text) \
    end \
  end \
  ';

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

  /*
  * getmargin()
  * calculates margin for a position
  * params: symbol, quantity
  * returns: margin
  */
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
  * getcurrencyrate()
  * gets current mid price between two currencies
  * params: currency id 1, currency id 2
  * returns: midprice if symbol is found, else 0
  */
  getcurrencyrate = '\
  local getcurrencyrate = function(currencyid1, currencyid2) \
    local midprice = 1 \
    if currencyid1 ~= currencyid2 then \
      midprice = redis.call("hget", "symbol:" .. currencyid1 .. "/" .. currencyid2, "midprice") \
      if not midprice then \
        midprice = 0 \
      end \
    end \
    return midprice \
  end \
  ';

  /*
  * getunrealisedpandl()
  * calculate the unrealised profit/loss for a position
  * params: position
  * returns: table as follows:
  * price = price of stock in account currency
  * value = value of positon in account currency
  * unrealisedpandl = unrealised p&l in account currency
  * symbolcurrencyid = currency of symbol  
  * symbolcurrencyprice = price in currency of symbol
  * currencyrate - currency rate used to convert price 
  */
  getunrealisedpandl = getcurrencyrate + round + '\
  local getunrealisedpandl = function(position) \
    local ret = {} \
    ret["price"] = 0 \
    ret["value"] = 0 \
    ret["unrealisedpandl"] = 0 \
    --[[ get the account currency & symbol currency as the symbol may be priced in a different currency ]] \
    local accountcurrencyid = redis.call("hget", "broker:" .. position.brokerid .. ":account:" .. position["accountid"], "currencyid") \
    ret["symbolcurrencyid"] = redis.call("hget", "symbol:" .. position.symbolid, "currencyid") \
    ret["symbolcurrencyprice"] = 0 \
    ret["currencyrate"] = 1 \
    local qty = tonumber(position.quantity) \
    if qty > 0 then \
      --[[ position is long, so we would sell, so use the bid price ]] \
      local bidprice = redis.call("hget", "symbol:" .. position.symbolid, "bid") \
      if bidprice and tonumber(bidprice) ~= 0 then \
        ret["symbolcurrencyprice"] = tonumber(bidprice) \
        if ret["symbolcurrencyid"] ~= accountcurrencyid then \
          --[[ get currency rate & adjust price ]] \
          ret["currencyrate"] = getcurrencyrate(ret["symbolcurrencyid"], accountcurrencyid) \
          ret["price"] = ret["symbolcurrencyprice"] * ret["currencyrate"] \
        else \
          ret["price"] = ret["symbolcurrencyprice"] \
        end \
        ret["value"] = qty * ret["price"] \
        ret["unrealisedpandl"] = round(ret["value"] - position.cost, 2) \
      end \
    elseif qty < 0 then \
      --[[ position is short, so we would buy, so use the ask price ]] \
      local askprice = redis.call("hget", "symbol:" .. position.symbolid, "ask") \
      if askprice and tonumber(askprice) ~= 0 then \
        ret["symbolcurrencyprice"] = tonumber(askprice) \
        if ret["symbolcurrencyid"] ~= accountcurrencyid then \
          --[[ get currency rate & adjust price ]] \
          ret["currencyrate"] = getcurrencyrate(ret["symbolcurrencyid"], accountcurrencyid) \
          ret["price"] = ret["symbolcurrencyprice"] * ret["currencyrate"] \
        else \
          ret["price"] = ret["symbolcurrencyprice"] \
        end \
        ret["value"] = qty * ret["price"] \
        ret["unrealisedpandl"] = round(ret["value"] + position.cost, 2) \
      end \
    end \
    return ret \
  end \
  ';

  exports.getunrealisedpandl = getunrealisedpandl;

  /*
  * getunrealisedpandlvaluedate()
  * calculate the unrealised profit/loss for a position as at a date
  * params: position, value date
  * returns: table as follows:
  * price = end of day price of stock in account currency as at value date
  * value = value of positon in account currency as at value date
  * unrealisedpandl = unrealised p&l in account currency as at value date
  * symbolcurrencyid = currency of symbol  
  * symbolcurrencyprice = price in currency of symbol at end of value date
  * currencyrate - currency rate at end of value date used to convert price 
  */
  getunrealisedpandlvaluedate = getcurrencyrate + geteodprice + round + '\
  local getunrealisedpandlvaluedate = function(position, valuedate) \
    redis.log(redis.LOG_NOTICE, "getunrealisedpandlvaluedate") \
    local ret = {} \
    ret.price = 0 \
    ret.value = 0 \
    ret.unrealisedpandl = 0 \
    --[[ get the account currency & symbol currency as may be different ]] \
    local accountcurrencyid = redis.call("hget", "broker:" .. position.brokerid .. ":account:" .. position.accountid, "currencyid") \
    ret.symbolcurrencyid = redis.call("hget", "symbol:" .. position.symbolid, "currencyid") \
    ret.symbolcurrencyprice = 0 \
    ret.currencyrate = 1 \
    --[[ get the end of day price for the valuedate ]] \
    local eodprice = geteodprice(valuedate, position.symbolid) \
    local qty = tonumber(position.quantity) \
    if qty > 0 then \
      --[[ position is long, so we would sell, so use the bid price ]] \
      if eodprice.bid and tonumber(eodprice.bid) ~= 0 then \
        ret.symbolcurrencyprice = tonumber(eodprice.bid) \
        if ret.symbolcurrencyid ~= accountcurrencyid then \
          --[[ get currency rate & adjust price ]] \
          ret.currencyrate = getcurrencyrate(ret.symbolcurrencyid, accountcurrencyid) \
          ret.price = ret.symbolcurrencyprice * ret.currencyrate \
        else \
          ret.price = ret.symbolcurrencyprice \
        end \
        ret.value = qty * ret.price \
        ret.unrealisedpandl = round(ret.value - position.cost, 2) \
      end \
    elseif qty < 0 then \
      --[[ position is short, so we would buy, so use the ask price ]] \
      if eodprice.ask and tonumber(eodprice.ask) ~= 0 then \
        ret.symbolcurrencyprice = tonumber(eodprice.ask) \
        if ret.symbolcurrencyid ~= accountcurrencyid then \
          --[[ get currency rate & adjust price ]] \
          ret.currencyrate = getcurrencyrate(ret.symbolcurrencyid, accountcurrencyid) \
          ret.price = ret.symbolcurrencyprice * ret.currencyrate \
        else \
          ret.price = ret.symbolcurrencyprice \
        end \
        ret.value = qty * ret.price \
        ret.unrealisedpandl = round(ret.value + position.cost, 2) \
      end \
    end \
    return ret \
  end \
  ';

  /*
  * calcfinance()
  * calculate finance for a trade
  */
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

  /*
  * getinitialmargin()
  * calcualtes initial margin required for an order, including costs
  * params: brokerid, symbolid, consid, totalcosts
  * returns: initialmargin
  */
  getinitialmargin = '\
  local getinitialmargin = function(brokerid, symbolid, consid, totalcost) \
    local marginpercent = redis.call("hget", "broker:" .. brokerid .. "brokersymbol:" .. symbolid, "marginpercent") \
    if not marginpercent then marginpercent = 100 end \
    local initialmargin = tonumber(consid) * tonumber(marginpercent) / 100 \
    return initialmargin \
  end \
  ';

  /*
  * getaccountbalance()
  * params: accountid, brokerid
  * returns: cleared & local currency cleared balances
  */
  getaccountbalance = '\
  local getaccountbalance = function(accountid, brokerid) \
    local fields = {"balance","localbalance"} \
    local vals = redis.call("hmget", "broker:" .. brokerid .. ":account:" .. accountid, unpack(fields)) \
    return vals \
  end \
  ';

  /*
  * getaccountbalanceuncleared()
  * params: accountid, brokerid
  * returns: uncleared & local currency uncleared balances
  */
  getaccountbalanceuncleared = '\
  local getaccountbalanceuncleared = function(accountid, brokerid) \
    local fields = {"balanceuncleared","localbalanceuncleared"} \
    local vals = redis.call("hmget", "broker:" .. brokerid .. ":account:" .. accountid, unpack(fields)) \
    return vals \
  end \
  ';

  /*
  * getaccount()
  * params: accountid, brokerid
  * returns: all fields in an account
  */
  getaccount = gethashvalues + '\
  local getaccount = function(accountid, brokerid) \
    local account = gethashvalues("broker:" .. brokerid .. ":account:" .. accountid) \
    return account \
  end \
  ';

  exports.getaccount = getaccount;

  /*
  * updateaccountbalance()
  * updates cleared account balance & local currency balance
  * note: amount & localamount can be -ve
  */
  updateaccountbalance = getaccountbalance + '\
  local updateaccountbalance = function(accountid, amount, brokerid, localamount) \
    local vals = getaccountbalance(accountid, brokerid) \
    if not vals[1] then return end \
    local balance = tonumber(vals[1]) + tonumber(amount) \
    local localbalance = tonumber(vals[2]) + tonumber(localamount) \
    redis.call("hmset", "broker:" .. brokerid .. ":account:" .. accountid, "balance", tostring(balance), "localbalance", tostring(localbalance)) \
  end \
  ';

  /*
  * updateaccountbalanceuncleared()
  * updates uncleared account balance & local currency balance
  * note: amount & localamount can be -ve
  */
  updateaccountbalanceuncleared = getaccountbalanceuncleared + '\
  local updateaccountbalanceuncleared = function(accountid, amount, brokerid, localamount) \
    local vals = getaccountbalanceuncleared(accountid, brokerid) \
    if not vals[1] then return end \
    local balanceuncleared = tonumber(vals[1]) + tonumber(amount) \
    local localbalanceuncleared = tonumber(vals[2]) + tonumber(localamount) \
    redis.call("hmset", "broker:" .. brokerid .. ":account:" .. accountid, "balanceuncleared", tostring(balanceuncleared), "localbalanceuncleared", tostring(localbalanceuncleared)) \
  end \
  ';

  /*
  * updatefieldindexes()
  * creates/updates sorted sets to enable searching/sorting on fields in hash tables
  * params: brokerid, table name, table of field/score/key
  */
  updatefieldindexes = '\
  local updatefieldindexes = function(brokerid, tblname, fieldscorekeys) \
    for i = 1, #fieldscorekeys, 3 do \
      redis.call("zadd", "broker:" .. brokerid .. ":" .. tblname .. ":" .. fieldscorekeys[i], fieldscorekeys[i+1], fieldscorekeys[i+2]) \
    end \
  end \
  ';

  /*
  * getrecordsbyfieldindex()
  * gets a range of records sorted by a numeric field
  * params: brokerid, table name, field name, minimum score, maximum score
  * returns: table of records for specified hash table
  */
  getrecordsbyfieldindex = gethashvalues + '\
  local getrecordsbyfieldindex = function(brokerid, tblname, fldname, min, max) \
    redis.log(redis.LOG_NOTICE, "getrecordsbyfieldindex") \
    local tblrecords = {} \
    local ids = redis.call("zrangebyscore", "broker:" .. brokerid .. ":" .. tblname .. ":" .. fldname, min, max) \
    for i = 1, #ids do \
      local record = gethashvalues("broker:" .. brokerid .. ":" .. tblname .. ":" .. ids[i]) \
      table.insert(tblrecords, record) \
    end \
    return tblrecords \
  end \
  ';

  /*
  * getrecordsbystringfieldindex()
  * gets a range of records sorted by a string field
  * params: brokerid, table name, field name, start of range, end of range
  * returns: table of records for specified hash table
  */
  getrecordsbystringfieldindex = split + gethashvalues + '\
  local getrecordsbystringfieldindex = function(brokerid, tblname, fldname, min, max) \
    redis.log(redis.LOG_NOTICE, "getrecordsbystringfieldindex") \
    local tblrecords = {} \
    --[[ replace last character of string with next ascii char for end of search ]] \
    local len = string.len(max) \
    local lastchar = string.sub(max, len) \
    local nextchar = string.char(string.byte(lastchar) + 1) \
    local endofsearch = string.sub(max, 0, len-1) .. nextchar \
    local valids = redis.call("zrangebylex", "broker:" .. brokerid .. ":" .. tblname .. ":" .. fldname, "[" .. min, "(" .. endofsearch) \
    for i = 1, #valids do \
      local id = split(valids[i], ":") \
      local record = gethashvalues("broker:" .. brokerid .. ":" .. tblname .. ":" .. id[2]) \
      table.insert(tblrecords, record) \
    end \
    return tblrecords \
  end \
  ';

 /*
  * updatetradesettlestatusindex()
  * creates/updates system wide sorted set for trade settlement status
  * note: uses fixed score so as to use lexigraphical indexing as settlement status is character based
  * params: brokerid, tradeid, tradesettlestatusid
  */
  updatetradesettlestatusindex = '\
  local updatetradesettlestatusindex = function(brokerid, tradeid, tradesettlestatusid) \
    redis.call("zadd", "trade:tradesettlestatus", 0, tradesettlestatusid .. ":" .. brokerid .. ":" .. tradeid) \
  end \
  ';

 /*
  * removetradesettlestatusindex()
  * removes member from system wide sorted set for trade settlement status
  * params: brokerid, tradeid, tradesettlestatusid
  */
  removetradesettlestatusindex = '\
  local removetradesettlestatusindex = function(brokerid, tradeid, tradesettlestatusid) \
    redis.call("zrem", "trade:tradesettlestatus", tradesettlestatusid .. ":" .. brokerid .. ":" .. tradeid) \
  end \
  ';

  /*
  * gettradesbysettlementstatus()
  * gets trades sorted by settlement status
  * params: minimum tradesettlementstatusid, maximum tradesettlementstatusid
  * returns: table of trades
  */
  gettradesbysettlementstatus = split + gethashvalues + '\
  local gettradesbysettlementstatus = function(mintradesettlementstatusid, maxtradesettlementstatusid) \
    local tbltrades = {} \
    redis.log(redis.LOG_NOTICE, "gettradesbysettlementstatus") \
    --[[ get the character beyond the maximum requested to mark the end of the search ]] \
    local nextchar = string.char(string.byte(maxtradesettlementstatusid) + 1) \
    local trades = redis.call("zrangebylex", "trade:tradesettlestatus", "[" .. mintradesettlementstatusid, "(" .. nextchar) \
    for i = 1, #trades do \
      local brokertradeids = split(trades[i], ":") \
      local trade = gethashvalues("broker:" .. brokertradeids[2] .. ":trade:" .. brokertradeids[3]) \
      table.insert(tbltrades, trade) \
    end \
    return tbltrades \
  end \
  ';

 /*
  * newposting()
  * creates a posting record
  * params: accountid, amount, brokerid, localamount, transactionid, timestamp in milliseconds
  */
  newposting = updatefieldindexes + '\
  local newposting = function(accountid, amount, brokerid, localamount, transactionid, timestampms) \
    local brokerkey = "broker:" .. brokerid \
    local postingid = redis.call("hincrby", brokerkey, "lastpostingid", 1) \
    local stramount = tostring(amount) \
    redis.call("hmset", brokerkey .. ":posting:" .. postingid, "accountid", accountid, "brokerid", brokerid, "amount", stramount, "localamount", tostring(localamount), "postingid", postingid, "transactionid", transactionid) \
    redis.call("sadd", brokerkey .. ":transaction:" .. transactionid .. ":postings", postingid) \
    redis.call("sadd", brokerkey .. ":account:" .. accountid .. ":postings", postingid) \
    --[[ add a sorted set for time based queries by account ]] \
    redis.call("zadd", brokerkey .. ":account:" .. accountid .. ":postingsbydate", timestampms, postingid) \
    redis.call("sadd", brokerkey .. ":postings", postingid) \
    redis.call("sadd", brokerkey .. ":postingid", "posting:" .. postingid) \
    --[[ add sorted sets for columns that require sorting capability ]] \
    local indexamount = tonumber(stramount) * 100 \
    local fieldscorekeys = {"amount", indexamount, postingid, "accountid", accountid, postingid} \
    updatefieldindexes(brokerid, "posting", fieldscorekeys) \
    return postingid \
  end \
  ';

  /*
  * newtransaction()
  * creates a transaction record
  * params: amount, brokerid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid, timestamp in milliseconds
  */
  newtransaction = updatefieldindexes + '\
  local newtransaction = function(amount, brokerid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid, timestampms) \
    local transactionid = redis.call("hincrby", "broker:" .. brokerid, "lasttransactionid", 1) \
    local brokerkey = "broker:" .. brokerid \
    local stramount = tostring(amount) \
    redis.call("hmset", brokerkey .. ":transaction:" .. transactionid, "amount", stramount, "brokerid", brokerid, "currencyid", currencyid, "localamount", tostring(localamount), "note", note, "rate", rate, "reference", reference, "timestamp", timestamp, "transactiontypeid", transactiontypeid, "transactionid", transactionid) \
    redis.call("sadd", brokerkey .. ":transactions", transactionid) \
    redis.call("sadd", brokerkey .. ":transactionid", "transaction:" .. transactionid) \
    --[[ add sorted sets for columns that require sorting capability ]] \
    local indexamount = tonumber(stramount) * 100 \
    local fieldscorekeys = {"amount", indexamount, transactionid, "timestamp", timestampms, transactionid, "transactiontypeid", 0, transactiontypeid .. ":" .. transactionid} \
    updatefieldindexes(brokerid, "transaction", fieldscorekeys) \
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
  * gettradedescription()
  * get a description of a trade for a client statement
  * params: trade reference i.e. broker:1:trade:1
  * returns: a description of the trade or empty string if not found
  */
  gettradedescription = gethashvalues + '\
  local gettradedescription = function(traderef) \
    local desc = "" \
    local trade = gethashvalues(traderef) \
    if trade["tradeid"] then \
      local symbol = gethashvalues("symbol:" .. trade["symbolid"]) \
      if tonumber(trade["side"]) == 1 then \
        desc = "Bought " \
      else \
        desc = "Sold " \
      end \
      desc = desc .. trade["quantity"] .. " " .. symbol["shortname"] .. " @ " .. trade["price"] \
    end \
    return desc \
  end \
  ';

  /*
  * getpostingsbydate()
  * gets postings for an account between two dates, sorted by datetime
  * params: accountid, brokerid, start of period, end of period - datetimes expressed in milliseconds
  * returns: array of postings
  */
  getpostingsbydate = gettransaction + gettradedescription + '\
  local getpostingsbydate = function(accountid, brokerid, startmilliseconds, endmilliseconds) \
    redis.log(redis.LOG_NOTICE, "getpostingsbydate") \
    local tblpostings = {} \
    local brokerkey = "broker:" .. brokerid \
    local postings = redis.call("zrangebyscore", brokerkey .. ":account:" .. accountid .. ":postingsbydate", startmilliseconds, endmilliseconds) \
    for i = 1, #postings do \
      --[[ get the posting ]] \
      local posting = gethashvalues(brokerkey .. ":posting:" .. postings[i]) \
      --[[ get additional details from the transaction ]] \
      local transaction = gettransaction(brokerid, posting["transactionid"]) \
      local note \
      if transaction["transactiontypeid"] == "TPC" or transaction["transactiontypeid"] == "TRC" then \
        note = gettradedescription(brokerkey .. ":" .. transaction["reference"]) \
      else \
        note = transaction["note"] \
      end \
      posting["note"] = note \
      posting["reference"] = transaction["reference"] \
      posting["timestamp"] = transaction["timestamp"] \
      posting["transactiontypeid"] = transaction["transactiontypeid"] \
      table.insert(tblpostings, posting) \
    end \
    return tblpostings \
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
  * addunclearedcashlistitem()
  * add an item to the uncleared cash list
  */
  addunclearedcashlistitem = '\
  local addunclearedcashlistitem = function(brokerid, clearancedate, clientaccountid, transactionid) \
    redis.log(redis.LOG_NOTICE, "addunclearedcashlistitem") \
    local brokerkey = "broker:" .. brokerid \
    local unclearedcashlistid = redis.call("hincrby", brokerkey, "lastunclearedcashlistid", 1) \
    redis.call("hmset", brokerkey .. ":unclearedcashlist:" .. unclearedcashlistid, "clientaccountid", clientaccountid, "brokerid", brokerid, "clearancedate", clearancedate, "transactionid", transactionid, "unclearedcashlistid", unclearedcashlistid) \
    redis.call("sadd", brokerkey .. ":unclearedcashlist", unclearedcashlistid) \
    redis.call("sadd", brokerkey .. ":unclearedcashlistid", "unclearedcashlist:" .. unclearedcashlistid) \
    redis.call("zadd", brokerkey .. ":unclearedcashlist:unclearedcashlistbydate", clearancedate, unclearedcashlistid) \
  end \
  ';

  /*
  * creditcheckwithdrawal()
  * checks a requested withdrawal amount against cleared cash balance
  * returns: 0 if ok, 1 if fails
  */
  creditcheckwithdrawal = getaccountbalance + '\
  local creditcheckwithdrawal = function(brokerid, clientaccountid, withdrawalamount) \
    redis.log(redis.LOG_NOTICE, "creditcheckwithdrawal") \
    local balance = getaccountbalance(clientaccountid, brokerid) \
    if not balance[1] then \
      return 1 \
    elseif tonumber(balance[1]) >= tonumber(withdrawalamount) then \
      return 0 \
    else \
      return 1 \
    end \
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

  /*
  * publishposition()
  * publish a position
  * params: brokerid, positionid, channel
  */
  publishposition = getposition + '\
  local publishposition = function(brokerid, positionid, channel) \
    local position = getposition(brokerid, positionid) \
    redis.call("publish", channel, "{" .. cjson.encode("position") .. ":" .. cjson.encode(position) .. "}") \
  end \
  ';

  /*
  * newposition()
  * create a new position
  * params: accountid, brokerid, cost, futsettdate, quantity, symbolid
  * returns: positionid
  */
  newposition = setsymbolkey + publishposition + '\
  local newposition = function(accountid, brokerid, cost, futsettdate, quantity, symbolid) \
    redis.log(redis.LOG_NOTICE, "newposition") \
    local brokerkey = "broker:" .. brokerid \
    local positionid = redis.call("hincrby", brokerkey, "lastpositionid", 1) \
    redis.call("hmset", brokerkey .. ":position:" .. positionid, "brokerid", brokerid, "accountid", accountid, "symbolid", symbolid, "quantity", quantity, "cost", tostring(cost), "positionid", positionid, "futsettdate", futsettdate) \
    setsymbolkey(accountid, brokerid, futsettdate, positionid, symbolid) \
    redis.call("sadd", brokerkey .. ":positions", positionid) \
    redis.call("sadd", brokerkey .. ":account:" .. accountid .. ":positions", positionid) \
    redis.call("sadd", brokerkey .. ":positionid", "position:" .. positionid) \
    publishposition(brokerid, positionid, 10) \
    return positionid \
  end \
  ';

  exports.getposition = getposition;

  /*
  * updateposition()
  * update an existing position
  * quantity & cost can be +ve/-ve
  */
  updateposition = round + setsymbolkey + publishposition + '\
  local updateposition = function(accountid, brokerid, cost, futsettdate, positionid, quantity, symbolid) \
    redis.log(redis.LOG_NOTICE, "updateposition") \
    local positionkey = "broker:" .. brokerid .. ":position:" .. positionid \
    local position = gethashvalues(positionkey) \
    local newquantity = tonumber(position["quantity"]) + tonumber(quantity) \
    local newcost \
    if newquantity == 0 then \
      newcost = 0 \
    elseif tonumber(quantity) > 0 then \
      newcost = tonumber(position["cost"]) + tonumber(cost) \
    else \
      newcost = round(newquantity / tonumber(position["quantity"]) * tonumber(position["cost"]), 2) \
    end \
    redis.call("hmset", positionkey, "accountid", accountid, "symbolid", symbolid, "quantity", newquantity, "cost", tostring(newcost), "futsettdate", futsettdate) \
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
    redis.log(redis.LOG_NOTICE, "newpositionposting") \
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
    redis.log(redis.LOG_NOTICE, "getpositionpostingsbydate") \
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
      local upandl = getunrealisedpandl(position) \
      position["margin"] = margin \
      position["price"] = upandl["price"] \
      position["value"] = upandl["value"] \
      position["unrealisedpandl"] = upandl["unrealisedpandl"] \
      position["symbolcurrencyid"] = upandl["symbolcurrencyid"] \
      position["symbolcurrencyprice"] = upandl["symbolcurrencyprice"] \
      position["currencyrate"] = upandl["currencyrate"] \
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
    redis.log(redis.LOG_NOTICE, "getpositionvalues") \
    local tblresults = {} \
    local positionids = redis.call("smembers", "broker:" .. brokerid .. ":account:" .. accountid .. ":positions") \
    for index = 1, #positionids do \
      local vals = getpositionvalue(brokerid, positionids[index]) \
      table.insert(tblresults, vals) \
    end \
    return tblresults \
  end \
  ';

  /*
  * getpositionbydatereverseorder()
  * calculates a position as at a point in time
  * params: brokerid, positionid, datetime in milliseconds
  * returns: position quantity & cost
  * note: goes through postings in reverse order - should give the same result as getpositionbydate()
  */
  getpositionbydatereverseorder = getposition + getpositionpostingsbydate + round + '\
  local getpositionbydatereverseorder = function(brokerid, positionid, dtmilli) \
    --[[ get the position ]] \
    local position = getposition(brokerid, positionid) \
    --[[ get the position postings for this position since the date ]] \
    local positionpostings = getpositionpostingsbydate(brokerid, positionid, dtmilli, "inf") \
    --[[ adjust the position to reflect these postings ]] \
    for i = #positionpostings, 1, -1 do \
      local positionquantity = tonumber(position["quantity"]) \
      position["quantity"] = positionquantity - tonumber(positionpostings[i]["quantity"]) \
      if tonumber(positionpostings[i]["quantity"]) > 0 then \
        position["cost"] = tonumber(position["cost"]) - tonumber(positionpostings[i]["cost"]) \
      elseif positionquantity == 0 then  \
        position["cost"] = tonumber(positionpostings[i]["cost"]) \
      else \
        position["cost"] = round(position["quantity"] / positionquantity * tonumber(position["cost"], 2) \
      end \
    end \
    return position \
  end \
 ';

  /*
  * getpositionbydate()
  * calculates a position as at a point in time
  * params: brokerid, positionid, datetime in milliseconds
  * returns: position quantity & cost
  * note: goes through postings in normal order, because this direction is required to see what trades made up the position at the time
  */
  getpositionbydate = getposition + getpositionpostingsbydate + round + '\
  local getpositionbydate = function(brokerid, positionid, dtmilli) \
    --[[ get the position ]] \
    local position = getposition(brokerid, positionid) \
    position["quantity"] = 0 \
    position["cost"] = 0 \
    --[[ get the position postings for this position since the beginning of time ]] \
    local positionpostings = getpositionpostingsbydate(brokerid, positionid, "-inf", dtmilli) \
    --[[ update quantity and cost to reflect these postings ]] \
    for i = 1, #positionpostings do \
      local postingquantity = tonumber(positionpostings[i]["quantity"]) \
      local newquantity = position["quantity"] + postingquantity \
      if newquantity == 0 then \
        position["cost"] = 0 \
      elseif postingquantity > 0 then \
        position["cost"] = position["cost"] + tonumber(positionpostings[i]["cost"]) \
      else \
        position["cost"] = round(newquantity / position["quantity"] * position["cost"], 2) \
      end \
      position["quantity"] = newquantity \
    end \
    return position \
  end \
 ';

  /*
  * getpositionvaluebydate()
  * calculates & values a position at a value date
  * params: brokerid, positionid, valuedate, valuedate in milliseconds
  * returns: position quantity, cost & value
  */
  getpositionvaluebydate = getpositionbydate + getunrealisedpandlvaluedate + '\
  local getpositionvaluebydate = function(brokerid, positionid, valuedate, valuedatems) \
    redis.log(redis.LOG_NOTICE, "getpositionvaluebydate") \
    --[[ get the position at the value date]] \
    local position = getpositionbydate(brokerid, positionid, valuedatems) \
    if tonumber(position["quantity"]) > 0 then \
      --[[ value the position ]] \
      local upandl = getunrealisedpandlvaluedate(position, valuedate) \
      position["price"] = upandl["price"] \
      position["value"] = upandl["value"] \
      position["unrealisedpandl"] = upandl["unrealisedpandl"] \
      position["symbolcurrencyid"] = upandl["symbolcurrencyid"] \
      position["symbolcurrencyprice"] = upandl["symbolcurrencyprice"] \
      position["currencyrate"] = upandl["currencyrate"] \
    end \
    return position \
  end \
 ';

  /*
  * getpositionquantitiesbydate()
  * calculates the quantity with settled and unsettled portion of a position as at a date
  * params: brokerid, symbolid, date in milliseconds
  * returns: quantity, unsettledquantity, settledquantity
  * note: calculates quantities only
  */
  getpositionquantitiesbydate = getposition + getpositionpostingsbydate + gethashvalues + '\
  local getpositionquantitiesbydate = function(brokerid, positionid, dtmilli) \
    redis.log(redis.LOG_NOTICE, "getpositionquantitiesbydate") \
    --[[ get the position ]] \
    local position = getposition(brokerid, positionid) \
    position["quantity"] = 0 \
    position["unsettledquantity"] = 0 \
    position["settledquantity"] = 0 \
    --[[ get the position postings for this position up to the date ]] \
    local positionpostings = getpositionpostingsbydate(brokerid, positionid, "-inf", dtmilli) \
    for i = 1, #positionpostings do \
      --[[ adjust the quantity ]] \
      position["quantity"] = position["quantity"] + tonumber(positionpostings[i]["quantity"]) \
      if position["quantity"] == 0 then \
        --[[ no position, so reset settled & unsettled ]] \
        position["unsettledquantity"] = 0 \
        position["settledquantity"] = 0 \
      else \
        --[[ if posting is a trade, see if it is unsettled, otherwise treat is as settled ]] \
        if tonumber(positionpostings[i]["positionpostingtypeid"]) == 1 then \
          local trade = gethashvalues("broker:" .. brokerid .. ":trade:" .. positionpostings[i]["linkid"]) \
          if trade["tradeid"] and tonumber(trade["tradesettlestatusid"]) < 4 then \
            position["unsettledquantity"] = position["unsettledquantity"] + tonumber(positionpostings[i]["quantity"]) \
          else \
            position["settledquantity"] = position["settledquantity"] + tonumber(positionpostings[i]["quantity"]) \
          end \
        else \
          position["settledquantity"] = position["settledquantity"] + tonumber(positionpostings[i]["quantity"]) \
        end \
      end \
    end \
    return position \
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
    redis.log(redis.LOG_NOTICE, "getpositionsbysymbol") \
    local tblresults = {} \
    local positions = redis.call("smembers", "broker:" .. brokerid .. ":symbol:" .. symbolid .. ":positions") \
    for index = 1, #positions do \
      local vals = getposition(brokerid, positions[index]) \
      if tonumber(vals["quantity"]) > 0 then \
        table.insert(tblresults, vals) \
      end \
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
  getpositionsbysymbolbydate = getpositionbydate + '\
  local getpositionsbysymbolbydate = function(brokerid, symbolid, milliseconds) \
    redis.log(redis.LOG_NOTICE, "getpositionsbysymbolbydate") \
    local tblresults = {} \
    --[[ get position ids ]] \
    local positionids = redis.call("smembers", "broker:" .. brokerid .. ":symbol:" .. symbolid .. ":positions") \
    for i = 1, #positionids do \
      local position = getpositionbydate(brokerid, positionids[i], milliseconds) \
      --[[ only interested in non-zero positions ]] \
      if position["quantity"] ~= 0 then \
        table.insert(tblresults, position) \
      end \
    end \
    return tblresults \
  end \
  ';

  /*
  * getpositionquantitiesbysymbolbydate()
  * gets all position quantities for a symbol as at a date
  * params: brokerid, symbolid, date in milliseconds
  * returns: a table of position quantities
  */
  getpositionquantitiesbysymbolbydate = getpositionquantitiesbydate + '\
  local getpositionquantitiesbysymbolbydate = function(brokerid, symbolid, milliseconds) \
    redis.log(redis.LOG_NOTICE, "getpositionquantitiesbysymbolbydate") \
    local tblresults = {} \
    --[[ get position ids ]] \
    local positionids = redis.call("smembers", "broker:" .. brokerid .. ":symbol:" .. symbolid .. ":positions") \
    for i = 1, #positionids do \
      local position = getpositionquantitiesbydate(brokerid, positionids[i], milliseconds) \
      --[[ only interested in non-zero positions ]] \
      if position["quantity"] ~= 0 then \
        table.insert(tblresults, position) \
      end \
    end \
    return tblresults \
  end \
  ';

  /*
  * get all the positions for a nominee account
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
  * getpositionsbybroker()
  * gets all positions for a broker
  * params: brokerid
  * returns: a table of positions
  */
  getpositionsbybroker = getposition + '\
  local getpositionsbybroker = function(brokerid) \
    redis.log(redis.LOG_NOTICE, "getpositionsbybroker") \
    local tblresults = {} \
    local positions = redis.call("smembers", "broker:" .. brokerid .. ":positions") \
    for index = 1, #positions do \
      local vals = getposition(brokerid, positions[index]) \
      table.insert(tblresults, vals) \
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
    redis.log(redis.LOG_NOTICE, "gettotalpositionvalue") \
    local positionvalues = getpositionvalues(accountid, brokerid) \
    local totalpositionvalue = {} \
    totalpositionvalue["margin"] = 0 \
    totalpositionvalue["cost"] = 0 \
    totalpositionvalue["value"] = 0 \
    totalpositionvalue["unrealisedpandl"] = 0 \
    for index = 1, #positionvalues do \
      totalpositionvalue["margin"] = totalpositionvalue["margin"] + tonumber(positionvalues[index]["margin"]) \
      totalpositionvalue["cost"] = totalpositionvalue["cost"] + tonumber(positionvalues[index]["cost"]) \
      totalpositionvalue["value"] = totalpositionvalue["value"] + tonumber(positionvalues[index]["value"]) \
      totalpositionvalue["unrealisedpandl"] = totalpositionvalue["unrealisedpandl"] + tonumber(positionvalues[index]["unrealisedpandl"]) \
    end \
    return totalpositionvalue \
  end \
  ';

  exports.gettotalpositionvalue = gettotalpositionvalue;

  /*
  * Account Summary Notes:
  * balance = cleared + uncleared cash
  * equity = balance + unrealised p&l across all positions
  * free margin = equity - margin used to hold positions
  */

  /*
  * getaccountsummary()
  * calculates account p&l, margin & equity for a client account
  * params: accountid, brokerid
  * returns: account cash balances, unrealised p&l, equity, free margin
  */
  getaccountsummary = getaccount + gettotalpositionvalue + '\
  local getaccountsummary = function(accountid, brokerid) \
    redis.log(redis.LOG_NOTICE, "getaccountsummary") \
    local accountsummary = {} \
    local account = getaccount(accountid, brokerid) \
    if account["balance"] then \
      local totalpositionvalue = gettotalpositionvalue(accountid, brokerid) \
      local equity = tonumber(account["balance"]) + tonumber(account["balanceuncleared"]) + totalpositionvalue["unrealisedpandl"] \
      local freemargin = equity - totalpositionvalue["margin"] \
      accountsummary["balance"] = account["balance"] \
      accountsummary["balanceuncleared"] = account["balanceuncleared"] \
      accountsummary["positioncost"] = totalpositionvalue["cost"] \
      accountsummary["positionvalue"] = totalpositionvalue["value"] \
      accountsummary["unrealisedpandl"] = totalpositionvalue["unrealisedpandl"] \
      accountsummary["equity"] = equity \
      accountsummary["margin"] = totalpositionvalue["margin"] \
      accountsummary["freemargin"] = freemargin \
    end \
    return accountsummary \
  end \
  ';

 /*
  * getfreemargin()
  * calculates free margin for an account
  * params: accountid, brokerid
  * returns: accounnt free margin if ok, else 0
 */
  getfreemargin = getaccountsummary + '\
  local getfreemargin = function(accountid, brokerid) \
    redis.log(redis.LOG_NOTICE, "getfreemargin") \
    local accountsummary = getaccountsummary(accountid, brokerid) \
    if accountsummary["freemargin"] then \
      return accountsummary["freemargin"] \
    else \
      return 0 \
    end \
  end \
  ';

  exports.getfreemargin = getfreemargin;

  /*
  * getbrokeraccountsmapid()
  * gets a broker accountid for a default broker account
  * params: brokerid, currencyid, account name
  * returns: accountid if found, else 0
  */
  getbrokeraccountsmapid = '\
  local getbrokeraccountsmapid = function(brokerid, currencyid, name) \
    local brokerkey = "broker:" .. brokerid \
    local brokeraccountsmapid = redis.call("get", brokerkey .. ":" .. name .. ":" .. currencyid) \
    local accountid = 0 \
    if brokeraccountsmapid then \
      accountid = redis.call("hget", brokerkey .. ":brokeraccountsmap:" .. brokeraccountsmapid, "accountid") \
    end \
    return accountid \
  end \
  ';

  exports.getbrokeraccountsmapid = getbrokeraccountsmapid;

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
  * creditcheck()
  * credit checks an order or trade
  * params: accountid, brokerid, orderid, clientid, symbolid, side, quantity, price, settlcurramt, currencyid, futsettdate, totalcost, instrumenttypeid 
  * returns: 0=fail, error code/1=succeed, inialmargin
  */
  creditcheck = rejectorder + getinitialmargin + getpositionbysymbol + getfreemargin + getaccount + '\
  local creditcheck = function(accountid, brokerid, orderid, clientid, symbolid, side, quantity, price, settlcurramt, currencyid, futsettdate, totalcost, instrumenttypeid) \
    redis.log(redis.LOG_NOTICE, "creditcheck") \
    side = tonumber(side) \
    quantity = tonumber(quantity) \
    --[[ calculate margin required for order ]] \
    local initialmargin = getinitialmargin(brokerid, symbolid, settlcurramt, totalcost) \
    --[[ get position, if there is one, as may be a closing buy or sell ]] \
    local position = getpositionbysymbol(accountid, brokerid, symbolid, futsettdate) \
    if position then \
      --[[ we have a position - always allow closing trades ]] \
      local posqty = tonumber(position["quantity"]) \
      if (side == 1 and posqty < 0) or (side == 2 and posqty > 0) then \
        if quantity <= math.abs(posqty) then \
          --[[ closing trade, so ok ]] \
          return {1, initialmargin} \
        end \
        if side == 2 and (instrumenttypeid == "DE" or instrumenttypeid == "IE") then \
            --[[ equity, so cannot sell more than we have ]] \
            rejectorder(brokerid, orderid, 0, "Quantity greater than position quantity") \
            return {0, 1019} \
        end \
        --[[ we are trying to close a quantity greater than current position, so need to check we can open a new position ]] \
        local freemargin = getfreemargin(accountid, brokerid) \
        --[[ add the margin returned by closing the position ]] \
        local margin = getmargin(symbolid, math.abs(posqty)) \
        --[[ get initial margin for remaining quantity after closing position ]] \
        initialmargin = getinitialmargin(brokerid, symbolid, (quantity - math.abs(posqty)) * price, totalcost) \
        if initialmargin + totalcost > freemargin + margin then \
          rejectorder(brokerid, orderid, 0, "Insufficient free margin") \
          return {0, 1020} \
        end \
        --[[ closing trade or enough margin to close & open a new position, so ok ]] \
        return {1, initialmargin} \
      end \
    end \
    if instrumenttypeid == "DE" or instrumenttypeid == "IE" then \
      if side == 1 then \
        local account = getaccount(accountid, brokerid) \
        if tonumber(account["balance"]) + tonumber(account["balanceuncleared"]) + tonumber(account["creditlimit"]) < settlcurramt + totalcost then \
          rejectorder(brokerid, orderid, 0, "Insufficient funds") \
          return {0, 1041} \
        end \
      else \
        --[[ allow ifa certificated equity sells ]] \
        if instrumenttypeid == "DE" then \
          if redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "clienttypeid") == "3" then \
            return {1, initialmargin} \
          end \
        end \
        --[[ check there is a position ]] \
        if not position then \
          rejectorder(brokerid, orderid, 0, "No position held in this instrument") \
          return {0, 1003} \
        end \
        --[[ check the position is of sufficient size ]] \
        local posqty = tonumber(position["quantity"]) \
        if posqty < 0 or quantity > posqty then \
          rejectorder(brokerid, orderid, 0, "Insufficient position size in this instrument") \
          return {0, 1004} \
        end \
      end \
    elseif instrumenttypeid == "CFD" or instrumenttypeid == "SPB" or instrumenttypeid == "CCFD" then \
      --[[ check free margin for all derivative trades ]] \
      local freemargin = getfreemargin(accountid, brokerid) \
      if initialmargin + totalcost > freemargin then \
        rejectorder(brokerid, orderid, 0, "Insufficient free margin") \
        return {0, 1020} \
      end \
    end \
    return {1, initialmargin} \
  end \
  ';

  /*
  * settradestatus()
  */
  settradestatus = '\
  local settradestatus = function(brokerid, tradeid, status) \
    redis.call("hset", "broker:" .. brokerid .. ":trade:" .. tradeid, "status", status) \
  end \
  ';

  /*
  * newtradetransaction()
  * cash side of a client trade
  */
  newtradetransaction = getbrokeraccountsmapid + newtransaction + newposting + updateaccountbalanceuncleared + updateaccountbalance + '\
  local newtradetransaction = function(consideration, commission, ptmlevy, stampduty, contractcharge, brokerid, clientaccountid, currencyid, rate, timestamp, tradeid, side, timestampms) \
    redis.log(redis.LOG_NOTICE, "newtradetransaction") \
    --[[ get broker accounts ]] \
    local considerationaccountid = getbrokeraccountsmapid(brokerid, currencyid, "Stock B/S") \
    local commissionaccountid = getbrokeraccountsmapid(brokerid, currencyid, "Commission") \
    local ptmaccountid = getbrokeraccountsmapid(brokerid, currencyid, "PTM levy") \
    local sdrtaccountid = getbrokeraccountsmapid(brokerid, currencyid, "SDRT") \
    --[[ calculate amounts in broker currency ]] \
    consideration = tonumber(consideration) \
    local considerationlocalamount = consideration * rate \
    local commissionlocalamount = commission * rate \
    local transactionid \
    if tonumber(side) == 1 then \
      --[[ buy includes all costs ]] \
      local totalamount = consideration + commission + stampduty + ptmlevy \
      --[[ calculate amounts in local currency ]] \
      local localamount = totalamount * rate \
      local ptmlevylocalamount = ptmlevy * rate \
      local stampdutylocalamount = stampduty * rate \
      --[[ the transaction ]] \
      transactionid = newtransaction(totalamount, brokerid, currencyid, localamount, "Trade receipt", rate, "trade:" .. tradeid, timestamp, "TRC", timestampms) \
      --[[ client account posting - note: update cleared balance ]] \
      newposting(clientaccountid, -totalamount, brokerid, -localamount, transactionid, timestampms) \
      updateaccountbalance(clientaccountid, -totalamount, brokerid, -localamount) \
      --[[ consideration posting ]] \
      newposting(considerationaccountid, consideration, brokerid, considerationlocalamount, transactionid, timestampms) \
      updateaccountbalance(considerationaccountid, consideration, brokerid, considerationlocalamount) \
      --[[ commission posting ]] \
      if commission > 0 then \
        newposting(commissionaccountid, commission, brokerid, commissionlocalamount, transactionid, timestampms) \
        updateaccountbalance(commissionaccountid, commission, brokerid, commissionlocalamount) \
      end \
      --[[ ptm levy posting ]] \
      if ptmlevy > 0 then \
        newposting(ptmaccountid, ptmlevy, brokerid, ptmlevylocalamount, transactionid, timestampms) \
        updateaccountbalance(ptmaccountid, ptmlevy, brokerid, ptmlevylocalamount) \
      end \
      --[[ sdrt posting ]] \
      if stampduty > 0 then \
        newposting(sdrtaccountid, stampduty, brokerid, stampdutylocalamount, transactionid, timestampms) \
        updateaccountbalance(sdrtaccountid, stampduty, brokerid, stampdutylocalamount) \
      end \
   else \
      --[[ we are selling so only commission & PTM apply ]] \
      local totalamount = consideration - commission - ptmlevy \
      local localamount = totalamount * rate \
      --[[ the transaction ]] \
      transactionid = newtransaction(totalamount, brokerid, currencyid, localamount, "Trade payment", rate, "trade:" .. tradeid, timestamp, "TPC", timestampms) \
      --[[ client account posting - note: update uncleared balance ]] \
      newposting(clientaccountid, totalamount, brokerid, localamount, transactionid, timestampms) \
      updateaccountbalanceuncleared(clientaccountid, totalamount, brokerid, localamount) \
      --[[ consideration posting ]] \
      newposting(considerationaccountid, -consideration, brokerid, -considerationlocalamount, transactionid, timestampms) \
      updateaccountbalance(considerationaccountid, -consideration, brokerid, -considerationlocalamount) \
      --[[ commission posting ]] \
      if commission > 0 then \
        newposting(commissionaccountid, commission, brokerid, commissionlocalamount, transactionid, timestampms) \
        updateaccountbalance(commissionaccountid, commission, brokerid, commissionlocalamount) \
      end \
       --[[ ptm levy posting ]] \
      if ptmlevy > 0 then \
        newposting(ptmaccountid, ptmlevy, brokerid, ptmlevylocalamount, transactionid, timestampms) \
        updateaccountbalance(ptmaccountid, ptmlevy, brokerid, ptmlevylocalamount) \
      end \
   end \
  end \
  ';

  /*
  * deletetradetransaction()
  * cash side of deleting a client trade
  */
  deletetradetransaction = getbrokeraccountsmapid + newtransaction + newposting + updateaccountbalanceuncleared + updateaccountbalance + '\
  local deletetradetransaction = function(consideration, commission, ptmlevy, stampduty, brokerid, clientaccountid, currencyid, rate, timestamp, tradeid, side, timestampms) \
    redis.log(redis.LOG_NOTICE, "deletetradetransaction") \
    --[[ get broker accounts ]] \
    local considerationaccountid = getbrokeraccountsmapid(brokerid, currencyid, "Stock B/S") \
    local commissionaccountid = getbrokeraccountsmapid(brokerid, currencyid, "Commission") \
    local ptmaccountid = getbrokeraccountsmapid(brokerid, currencyid, "PTM levy") \
    local sdrtaccountid = getbrokeraccountsmapid(brokerid, currencyid, "SDRT") \
    --[[ get reverse amounts because we are deleting ]] \
    consideration = -tonumber(consideration) \
    commission = -tonumber(commission) \
    stampduty = -tonumber(stampduty) \
    ptmlevy = -tonumber(ptmlevy) \
    --[[ calculate amounts in broker currency ]] \
    rate = tonumber(rate) \
    local considerationlocalamount = consideration * rate \
    local commissionlocalamount = commission * rate \
    local transactionid \
    if tonumber(side) == 1 then \
      --[[ buy includes all costs ]] \
      local totalamount = consideration + commission + stampduty + ptmlevy \
      --[[ calculate amounts in local currency ]] \
      local localamount = totalamount * rate \
      local ptmlevylocalamount = ptmlevy * rate \
      local stampdutylocalamount = stampduty * rate \
      --[[ the transaction ]] \
      transactionid = newtransaction(totalamount, brokerid, currencyid, localamount, "Trade receipt reversal", rate, "trade:" .. tradeid, timestamp, "TPC", timestampms) \
      --[[ client account posting - note: update cleared balance ]] \
      newposting(clientaccountid, -totalamount, brokerid, -localamount, transactionid, timestampms) \
      updateaccountbalance(clientaccountid, -totalamount, brokerid, -localamount) \
      --[[ consideration posting ]] \
      newposting(considerationaccountid, consideration, brokerid, considerationlocalamount, transactionid, timestampms) \
      updateaccountbalance(considerationaccountid, consideration, brokerid, considerationlocalamount) \
      --[[ commission posting ]] \
      if commission ~= 0 then \
        newposting(commissionaccountid, commission, brokerid, commissionlocalamount, transactionid, timestampms) \
        updateaccountbalance(commissionaccountid, commission, brokerid, commissionlocalamount) \
      end \
      --[[ ptm levy posting ]] \
      if ptmlevy ~= 0 then \
        newposting(ptmaccountid, ptmlevy, brokerid, ptmlevylocalamount, transactionid, timestampms) \
        updateaccountbalance(ptmaccountid, ptmlevy, brokerid, ptmlevylocalamount) \
      end \
      --[[ sdrt posting ]] \
      if stampduty ~= 0 then \
        newposting(sdrtaccountid, stampduty, brokerid, stampdutylocalamount, transactionid, timestampms) \
        updateaccountbalance(sdrtaccountid, stampduty, brokerid, stampdutylocalamount) \
      end \
   else \
      --[[ we are selling so only commission & PTM apply ]] \
      local totalamount = consideration - commission - ptmlevy \
      local localamount = totalamount * rate \
      --[[ the transaction ]] \
      transactionid = newtransaction(totalamount, brokerid, currencyid, localamount, "Trade payment reversal", rate, "trade:" .. tradeid, timestamp, "TRC", timestampms) \
      --[[ client account posting - note: update uncleared balance ]] \
      newposting(clientaccountid, totalamount, brokerid, localamount, transactionid, timestampms) \
      updateaccountbalanceuncleared(clientaccountid, totalamount, brokerid, localamount) \
      --[[ consideration posting ]] \
      newposting(considerationaccountid, -consideration, brokerid, -considerationlocalamount, transactionid, timestampms) \
      updateaccountbalance(considerationaccountid, -consideration, brokerid, -considerationlocalamount) \
      --[[ commission posting ]] \
      if commission ~= 0 then \
        newposting(commissionaccountid, commission, brokerid, commissionlocalamount, transactionid, timestampms) \
        updateaccountbalance(commissionaccountid, commission, brokerid, commissionlocalamount) \
      end \
       --[[ ptm levy posting ]] \
      if ptmlevy ~= 0 then \
        newposting(ptmaccountid, ptmlevy, brokerid, ptmlevylocalamount, transactionid, timestampms) \
        updateaccountbalance(ptmaccountid, ptmlevy, brokerid, ptmlevylocalamount) \
      end \
   end \
  end \
  ';

  /*
  * newpositiontransaction()
  * a transaction to either create or update a position and create a position posting
  */
  newpositiontransaction = getpositionid + newposition + updateposition + newpositionposting + '\
  local newpositiontransaction = function(accountid, brokerid, cost, futsettdate, linkid, positionpostingtypeid, quantity, symbolid, timestamp, milliseconds) \
    local positionid = getpositionid(accountid, brokerid, symbolid, futsettdate) \
    if not positionid then \
      --[[ no position, so create a new one ]] \
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
  * deletetrade()
  * function to handle processing for deleting a trade
  * params: trade, timestamp, timestampms
  */
  deletetrade = gethashvalues + deletetradetransaction + newpositiontransaction + settradestatus + '\
  local deletetrade = function(trade, timestamp, timestampms) \
  local rate = 1 \
  local quantity \
  local cost \
  if tonumber(trade["side"]) == 1 then \
    quantity = trade["quantity"]) \
    cost = trade["settlcurramt"] \
  else \
    quantity = -tonumber(trade["quantity"]) \
    cost = -tonumber(trade["settlcurramt"]) \
  end \
  deletetradetransaction(trade["settlcurramt"], trade["commission"], trade["ptmlevy"], trade["stampduty"], trade["brokerid"], trade["accountid"], trade["currencyid"], rate, timestamp, trade["tradeid"], trade["side"], timestampms) \
  newpositiontransaction(trade["accountid"], trade["brokerid"], -cost, trade["futsettdate"], trade["tradeid"], 5, -quantity, trade["symbolid"], timestamp, timestampms) \
  settradestatus(trade["brokerid"], trade["tradeid"], 3) \
  --[[ ensure trade is removed from CREST list ]] \
  removetradesettlestatusindex(trade["brokerid"], trade["tradeid"], trade["tradesettlestatusid"]) \
  --[[ remove from list for sending contract notes ]] \
  redis.call("lrem", "contractnotes", 1, trade["brokerid"] .. ":" .. trade["tradeid"]) \
 end \
  ';

  /*
  * publishtrade()
  * publish a trade
  */
  publishtrade = gethashvalues + '\
  local publishtrade = function(brokerid, tradeid, channel) \
    redis.log(redis.LOG_NOTICE, "publishtrade") \
    local trade = gethashvalues("broker:" .. brokerid .. ":trade:" .. tradeid) \
    redis.call("publish", channel, "{" .. cjson.encode("trade") .. ":" .. cjson.encode(trade) .. "}") \
  end \
  ';

  /*
  * newtrade()
  * stores a trade & updates cash & position
  */
  newtrade = newtradetransaction + newpositiontransaction + updatetradesettlestatusindex + updatefieldindexes + publishtrade + '\
  local newtrade = function(accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, contractcharge, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, operatortype, operatorid, finance, timestampms) \
    redis.log(redis.LOG_NOTICE, "newtrade") \
    local brokerkey = "broker:" .. brokerid \
    local tradeid = redis.call("hincrby", brokerkey, "lasttradeid", 1) \
    if not tradeid then return 0 end \
    local strsettlcurramt = tostring(settlcurramt) \
    redis.call("hmset", brokerkey .. ":trade:" .. tradeid, "accountid", accountid, "brokerid", brokerid, "clientid", clientid, "orderid", orderid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "commission", tostring(commission), "ptmlevy", tostring(ptmlevy), "stampduty", tostring(stampduty), "contractcharge", tostring(contractcharge), "counterpartyid", counterpartyid, "counterpartytype", counterpartytype, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", tradeid, "settlcurrencyid", settlcurrencyid, "settlcurramt", strsettlcurramt, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "margin", margin, "finance", finance, "tradesettlestatusid", 0) \
    redis.call("sadd", brokerkey .. ":tradeid", "trade:" .. tradeid) \
    redis.call("sadd", brokerkey .. ":trades", tradeid) \
    redis.call("sadd", brokerkey .. ":account:" .. accountid .. ":trades", tradeid) \
    redis.call("sadd", brokerkey .. ":order:" .. orderid .. ":trades", tradeid) \
    --[[ add to a system wide index of trades by settlementstatus for CREST ]] \
    updatetradesettlestatusindex(brokerid, tradeid, 0) \
    --[[ add to a system wide list of items for sending contract notes ]] \
    redis.call("rpush", "contractnotes", brokerid .. ":" .. tradeid) \
    --[[ add sorted sets for columns that require sorting capability ]] \
    local indexsettlcurramt = tonumber(strsettlcurramt) * 100 \
    local fieldscorekeys = {"symbolid", 0, symbolid .. ":" .. tradeid, "timestamp", timestampms, tradeid, "settlcurramt", indexsettlcurramt, tradeid} \
    updatefieldindexes(brokerid, "trade", fieldscorekeys) \
    local cost \
    if tonumber(side) == 1 then \
      cost = settlcurramt \
    else \
      quantity = -tonumber(quantity) \
      cost = -tonumber(settlcurramt) \
    end \
    local retval = newtradetransaction(settlcurramt, commission, ptmlevy, stampduty, contractcharge, brokerid, accountid, settlcurrencyid, 1, timestamp, tradeid, side, timestampms) \
    newpositiontransaction(accountid, brokerid, cost, futsettdate, tradeid, 1, quantity, symbolid, timestamp, timestampms) \
    publishtrade(brokerid, tradeid, 6) \
    return tradeid \
  end \
  ';

  exports.newtrade = newtrade;

  /*
  * updatetrade()
  * update trade values
  */
  updatetrade = '\
  local updatetrade = function(accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, contractcharge, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, operatortype, operatorid, finance, timestampms, tradeid, tradesettlestatusid) \
    redis.log(redis.LOG_NOTICE, "updatetrade") \
    redis.call("hmset", brokerkey .. ":trade:" .. tradeid, "accountid", accountid, "brokerid", brokerid, "clientid", clientid, "orderid", orderid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "commission", tostring(commission), "ptmlevy", tostring(ptmlevy), "stampduty", tostring(stampduty), "contractcharge", tostring(contractcharge), "counterpartyid", counterpartyid, "counterpartytype", counterpartytype, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", tradeid, "settlcurrencyid", settlcurrencyid, "settlcurramt", strsettlcurramt, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "margin", margin, "finance", finance, "tradesettlestatusid", tradesettlestatusid) \
  end \
  ';

  /*
  * getcorporateactionclientdecisionbyclient()
  * get a corporate action client decision
  * params: brokerid, clientid, corporateactionid
  * returns: corporate action client decision as a string or nil if not found
  */
  getcorporateactionclientdecisionbyclient = '\
  local getcorporateactionclientdecisionbyclient = function(brokerid, clientid, corporateactionid) \
    redis.log(redis.LOG_NOTICE, "getcorporateactionclientdecisionbyclient") \
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
    redis.log(redis.LOG_NOTICE, "getsharesdue") \
    local sharesdue = round(tonumber(posqty) * tonumber(sharespershare), 2) \
    local sharesdueint = math.floor(sharesdue) \
    local sharesduerem = sharesdue - sharesdueint \
    return {sharesdueint, sharesduerem} \
  end \
  ';

  /*
  * transactiondividend()
  * cash dividend transaction
  * params: clientaccountid, dividend, unsettled dividend, brokerid, currencyid, dividend in local currency, unsettled dividend in local currency, description, rate, reference, timestamp, transactiontypeid, timestampms
  * returns: 0 if ok, else 1 followed by error message
  */
  transactiondividend = getbrokeraccountsmapid + newtransaction + newposting + updateaccountbalance + '\
  local transactiondividend = function(clientaccountid, dividend, unsettleddividend, brokerid, currencyid, dividendlocal, unsettleddividendlocal, description, rate, reference, timestamp, transactiontypeid, timestampms) \
    --[[ get the relevant broker accounts ]] \
    local clientfundsaccount = getbrokeraccountsmapid(brokerid, currencyid, "Client funds") \
    if not clientfundsaccount then \
      return {1, 1027} \
    end \
    local clientsettlementaccount = getbrokeraccountsmapid(brokerid, currencyid, "Client settlement") \
    if not clientsettlementaccount then \
      return {1, 1027} \
    end \
    local cmaaccount = getbrokeraccountsmapid(brokerid, currencyid, "CMA") \
    if not cmaaccount then \
      return {1, 1027} \
    end \
    --[[ create the transaction ]] \
    local transactionid = newtransaction(dividend, brokerid, currencyid, dividendlocal, description, rate, reference, timestamp, transactiontypeid, timestampms) \
    --[[ client posting ]] \
    newposting(clientaccountid, dividend, brokerid, dividendlocal, transactionid, timestampms) \
    updateaccountbalance(clientaccountid, dividend, brokerid, dividendlocal) \
    --[[ broker side of client posting ]] \
    newposting(clientfundsaccount, -dividend, brokerid, -dividendlocal, transactionid, timestampms) \
    updateaccountbalance(clientfundsaccount, -dividend, brokerid, -dividendlocal) \
    if unsettleddividend ~= 0 then \
      --[[ unsettled transaction ]] \
      local unsettledtransactionid = newtransaction(unsettleddividend, brokerid, currencyid, unsettleddividendlocal, description, rate, reference, timestamp, transactiontypeid, timestampms) \
      newposting(cmaaccount, unsettleddividend, brokerid, unsettleddividendlocal, unsettledtransactionid, timestampms) \
      updateaccountbalance(cmaaccount, unsettleddividend, brokerid, unsettleddividendlocal) \
      newposting(clientsettlementaccount, -unsettleddividend, brokerid, -unsettleddividendlocal, unsettledtransactionid, timestampms) \
      updateaccountbalance(clientsettlementaccount, -unsettleddividend, brokerid, -unsettleddividendlocal) \
    end \
    return {0} \
  end \
  ';

  /*
  * dividendasshares()
  * processing for dividend taken as shares
  * params: brokerid, position, corporateaction, timestamp, timestampms, mode (1=test,2=apply,3=reverse)
  * returns: whole number of shares due, remainder of shares expressed as cash based on end of exdate price
  */
  dividendasshares = getsharesdue + newpositiontransaction + getaccount + geteodprice + transactiondividend + '\
  local dividendasshares = function(brokerid, position, corporateaction, timestamp, timestampms, mode) \
    redis.log(redis.LOG_NOTICE, "dividendasshares") \
    local sharesdue = getsharesdue(position["quantity"], corporateaction["sharespershare"]) \
    local residuecash = 0 \
    if sharesdue[1] > 0 then \
      --[[ add any shares to position ]] \
      if mode == 2 then \
        newpositiontransaction(position["accountid"], brokerid, 0, "", corporateaction["corporateactionid"], 2, sharesdue[1], corporateaction["symbolid"], timestamp, timestampms) \
      elseif mode == 3 then \
        newpositiontransaction(position["accountid"], brokerid, 0, "", corporateaction["corporateactionid"], 2, -sharesdue[1], corporateaction["symbolid"], timestamp, timestampms) \
      end \
    end \
    if sharesdue[2] > 0 then \
      --[[ get the account for the currency ]] \
      local account = getaccount(position["accountid"], brokerid) \
      --[[ we are assuming GBP dividend - todo: get fx rate if necessary ]] \
      local rate = 1 \
      --[[ any residue shares amount is taken as cash - we need the closing price as at the exdate to value this ]] \
      local eodprice = geteodprice(corporateaction["exdate"], corporateaction["symbolid"]) \
      if not eodprice["bid"] then \
        return {1, 1029} \
      end \
      --[[ calculate the amount cash as the stub * closing price ]] \
      residuecash = round(sharesdue[2] * eodprice["bid"], 2) \
      if residuecash > 0 then \
        local residuecashlocal = residuecash * rate \
        if mode == 2 then \
          local retval = transactiondividend(position["accountid"], residuecash, 0, brokerid, account["currencyid"], residuecashlocal, 0, corporateaction["description"], rate, "corporateaction:" .. corporateaction["corporateactionid"], timestamp, "DVP", timestampms) \
          if retval[1] == 1 then \
            return retval \
          end \
        elseif mode == 3 then \
          local retval = transactiondividend(position["accountid"], -residuecash, 0, brokerid, account["currencyid"], -residuecashlocal, 0, corporateaction["description"] .. " - REVERSAL", rate, "corporateaction:" .. corporateaction["corporateactionid"], timestamp, "DVR", timestampms) \
          if retval[1] == 1 then \
            return retval \
          end \
        end \
      end \
    end \
    redis.log(redis.LOG_NOTICE, "sharesdue: " .. sharesdue[1] .. ", residuecash: " .. residuecash) \
    return {sharesdue[1], residuecash} \
  end \
  ';

  /*
  * dividendascash()
  * processing for dividend taken as cash
  * params: brokerid, position, corporateaction, timestamp, timestampms, mode (1=test,2=apply,3=reverse)
  * returns: dividend amount
  */
  dividendascash = getaccount + transactiondividend + '\
  local dividendascash = function(brokerid, position, corporateaction, timestamp, timestampms, mode) \
    redis.log(redis.LOG_NOTICE, "dividendascash") \
    --[[ we are assuming GBP dividend - todo: get fx rate if necessary ]] \
    local rate = 1 \
    local dividend = round(corporateaction["cashpershare"] * position["quantity"], 2) \
    local dividendlocal = dividend * rate \
    if dividend > 0 then \
      --[[ get the account for the currency ]] \
      local account = getaccount(position["accountid"], brokerid) \
      if mode == 2 then \
        --[[ apply transactions & postings ]] \
        local retval = transactiondividend(position["accountid"], dividend, 0, brokerid, account["currencyid"], dividendlocal, 0, corporateaction["description"], rate, "corporateaction:" .. corporateaction["corporateactionid"], timestamp, "DVP", timestampms) \
        if retval[1] == 1 then \
          return retval \
        end \
      elseif mode == 3 then \
        --[[ reverse transactions & postings ]] \
        local retval = transactiondividend(position["accountid"], -dividend, 0, brokerid, account["currencyid"], -dividendlocal, 0, corporateaction["description"] .. " - REVERSAL", rate, "corporateaction:" .. corporateaction["corporateactionid"], timestamp, "DVR", timestampms) \
        if retval[1] == 1 then \
          return retval \
        end \
      end \
    end \
    redis.log(redis.LOG_NOTICE, "dividend: " .. dividend) \
    return dividend \
  end \
  ';

  /*** Scripts ***/

  /*
  * scriptgetholidays
  * get holidays
  * note: assumes UK holidays
  */
  exports.scriptgetholidays = '\
  local tblresults = {} \
  local holidays = redis.call("smembers", "holidays" .. ARGV[1]) \
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
    local positions = getpositionsbysymbol(ARGV[1], ARGV[2]) \
    return cjson.encode(positions) \
  ';

  /*
  * scriptgetpositionsbysymbolbydate
  * get positions as at a date
  * params: brokerid, symbolid, millisecond representation of the date
  * returns: a table of positions
  */
  exports.scriptgetpositionsbysymbolbydate = getpositionsbysymbolbydate + '\
    local positions = getpositionsbysymbolbydate(ARGV[1], ARGV[2], ARGV[3]) \
    return cjson.encode(positions) \
  ';

  /*
  * scriptgetpositionsbybroker
  * get positions for a broker
  * params: brokerid
  * returns: a table of positions
  */
  exports.scriptgetpositionsbybroker = getpositionsbybroker + '\
    local positions = getpositionsbybroker(ARGV[1]) \
    return cjson.encode(positions) \
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
  * returns: array of values as JSON string
  */
  exports.scriptgetaccountsummary = getaccountsummary + '\
  local accountsummary = getaccountsummary(ARGV[1], ARGV[2]) \
  return cjson.encode(accountsummary) \
  ';

 /*
  * scriptvaluation
  * value a portfolio as at a date
  * params: accountid, brokerid, value date, end of value date in milliseconds, 1=current or 2=historic
  * returns: array of positions with their associated values in JSON
  */
  exports.scriptvaluation = getpositionvalue + getpositionvaluebydate + '\
    redis.log(redis.LOG_NOTICE, "scriptvaluation") \
    local accountid = ARGV[1] \
    local brokerid = ARGV[2] \
    local valuedate = ARGV[3] \
    local valuedatems = ARGV[4] \
    local valuetype = ARGV[5] \
    local tblvaluation = {} \
    --[[ get the ids for the positions held by this account ]] \
    local positionids = redis.call("smembers", "broker:" .. brokerid .. ":account:" .. accountid .. ":positions") \
    --[[ calculate each position together with value as at the value date ]] \
    for index = 1, #positionids do \
      local position \
      if tonumber(valuetype) == 1 then \
        position = getpositionvalue(brokerid, positionids[index]) \
      else \
        position = getpositionvaluebydate(brokerid, positionids[index], valuedate, valuedatems) \
      end \
      if position["quantity"] ~= 0 then \
        table.insert(tblvaluation, position) \
      end \
    end \
    return cjson.encode(tblvaluation) \
  ';

  /*
  * newclientfundstransfer
  * script to handle client deposits & withdrawals
  * keys: broker:<brokerid>
  * args: 1=action, 2=amount, 3=brokerid, 4=clientaccountid, 5=currencyid, 6=localamount, 7=note, 8=paymenttypeid, 9=rate, 10=reference, 11=timestamp, 12=timestampms, 13=clearancedate
  * returns: 0 if successful, else 1 & an error message code if unsuccessful
  */
  exports.newclientfundstransfer = newtransaction + newposting + updateaccountbalanceuncleared + updateaccountbalance + getbrokeraccountsmapid + addunclearedcashlistitem + creditcheckwithdrawal + '\
    redis.log(redis.LOG_NOTICE, "newclientfundstransfer") \
    local action = tonumber(ARGV[1]) \
    local paymenttypeid = ARGV[8] \
    if paymenttypeid == "DCR" then \
      if action == 1 then \
        local transactionid = newtransaction(ARGV[2], ARGV[3], ARGV[5], ARGV[6], ARGV[7], ARGV[9], ARGV[10], ARGV[11], "CRR", ARGV[12]) \
        newposting(ARGV[4], ARGV[2], ARGV[3], ARGV[6], transactionid, ARGV[12]) \
        updateaccountbalanceuncleared(ARGV[4], ARGV[2], ARGV[3], ARGV[6]) \
        local clientsettlementaccountid = getbrokeraccountsmapid(ARGV[3], ARGV[5], "Client settlement") \
        newposting(clientsettlementaccountid, -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6]), transactionid, ARGV[12]) \
        updateaccountbalance(clientsettlementaccountid, -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6])) \
        addunclearedcashlistitem(ARGV[3], ARGV[13], ARGV[4],  transactionid) \
      else \
      end \
    elseif paymenttypeid == "BAC" then \
      if action == 1 then \
        local transactionid = newtransaction(ARGV[2], ARGV[3], ARGV[5], ARGV[6], ARGV[7], ARGV[9], ARGV[10], ARGV[11], "CAR", ARGV[12]) \
        newposting(ARGV[4], ARGV[2], ARGV[3], ARGV[6], transactionid, ARGV[12]) \
        updateaccountbalance(ARGV[4], ARGV[2], ARGV[3], ARGV[6]) \
        local clientfundsaccount = getbrokeraccountsmapid(ARGV[3], ARGV[5], "Client funds") \
        newposting(clientfundsaccount, -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6]), transactionid, ARGV[12]) \
        updateaccountbalance(clientfundsaccount, -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6])) \
      else \
        if creditcheckwithdrawal(ARGV[3], ARGV[4], ARGV[2]) == 1 then \
          return {1, "Insufficient cleared funds"} \
        end \
        local transactionid = newtransaction(ARGV[2], ARGV[3], ARGV[5], ARGV[6], ARGV[7], ARGV[9], ARGV[10], ARGV[11], "CAP", ARGV[12]) \
        newposting(ARGV[4], -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6]), transactionid, ARGV[12]) \
        updateaccountbalance(ARGV[4], -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6])) \
        local clientfundsaccount = getbrokeraccountsmapid(ARGV[3], ARGV[5], "Client funds") \
        newposting(clientfundsaccount, ARGV[2], ARGV[3], ARGV[6], transactionid, ARGV[12]) \
        updateaccountbalance(clientfundsaccount, ARGV[2], ARGV[3], ARGV[6]) \
      end \
    elseif paymenttypeid == "CHQ" then \
      if action == 1 then \
      else \
      end \
    end \
   return {0} \
  ';

  /*
  * scriptgetstatement
  * prepares a statement for an account between two dates
  * params: accountid, brokerid, start date, end date
  */
  exports.scriptgetstatement = getpostingsbydate + '\
    redis.log(redis.LOG_NOTICE, "scriptgetstatement") \
    local tblresults = {} \
    local balance = 0 \
    --[[ get all the postings up to the end date ]] \
    local postings = getpostingsbydate(ARGV[1], ARGV[2], "-inf", ARGV[4]) \
    --[[ go through all the postings, adjusting the cleared/uncleared balances as we go ]] \
    for index = 1, #postings do \
      balance = balance + tonumber(postings[index]["amount"]) \
      postings[index]["balance"] = balance \
      table.insert(tblresults, postings[index]) \
    end \
    return cjson.encode(tblresults) \
  ';

  /*
  * scriptgethistory
  * prepares a history for an account from beginning to now
  * params: accountid, brokerid
  */
  exports.scriptsgethistory = getpostingsbydate + '\
    redis.log(redis.LOG_NOTICE, "scriptsgethistory") \
    local tblresults = {} \
    local balance = 0 \
    --[[ get all the postings up to the end date ]] \
    local postings = getpostingsbydate(ARGV[1], ARGV[2], "-inf", "+inf") \
    --[[ go through all the postings, adjusting the cleared/uncleared balances as we go ]] \
    for index = 1, #postings do \
      balance = balance + tonumber(postings[index]["amount"]) \
      postings[index]["balance"] = balance \
      table.insert(tblresults, postings[index]) \
    end \
    return cjson.encode(tblresults) \
  ';

  /*
  * cacashdividend()
  * script to test/apply/reverse a cash dividend
  * params: brokerid, corporateactionid, exdatems, timestamp, timestampms, mode (1=test,2=apply,3=reverse)
  * returns: 0, total position quantity, total unsettled quantity, total dividend, total unsettled dividend, number of accounts updated if successful, else 1 + an error message if unsuccessful
  */
  exports.cacashdividend = getpositionquantitiesbysymbolbydate + getaccount + round + transactiondividend + '\
    redis.log(redis.LOG_NOTICE, "cacashdividend") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local exdatems = ARGV[3] \
    local timestamp = ARGV[4] \
    local timestampms = ARGV[5] \
    local mode = tonumber(ARGV[6]) \
    local totquantity = 0 \
    local totunsettledquantity = 0 \
    local totdividend = 0 \
    local totunsettleddividend = 0 \
    local numaccounts = 0 \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
   --[[ we are assuming GBP dividend - todo: get fx rate if necessary ]] \
    local rate = 1 \
    --[[ get all positions in the symbol of the corporate action as at the ex-date ]] \
    local positions = getpositionquantitiesbysymbolbydate(brokerid, corporateaction["symbolid"], exdatems) \
    for i = 1, #positions do \
      redis.log(redis.LOG_NOTICE, "accountid: " .. positions[i]["accountid"] .. ", quantity: " .. positions[i]["quantity"] .. ", unsettledquantity: " .. positions[i]["unsettledquantity"] .. ", settledquantity: " .. positions[i]["settledquantity"]) \
      --[[ may have a position with no quantity ]] \
      if positions[i]["quantity"] ~= 0 then \
        --[[ get the account for the currency - may change - todo: consider ]] \
        local account = getaccount(positions[i]["accountid"], brokerid) \
        local dividend = round(positions[i]["quantity"] * tonumber(corporateaction["cashpershare"]), 2) \
        local unsettleddividend = round(positions[i]["unsettledquantity"] * tonumber(corporateaction["cashpershare"]), 2) \
        redis.log(redis.LOG_NOTICE, "dividend: " .. dividend) \
        local dividendlocal = dividend * rate \
        local unsettleddividendlocal = unsettleddividend * rate \
        if dividend ~= 0 then \
          if mode == 2 then \
            --[[ apply the dividend ]] \
            local retval = transactiondividend(positions[i]["accountid"], dividend, unsettleddividend, brokerid, account["currencyid"], dividendlocal, unsettleddividendlocal, corporateaction["description"], rate, "corporateaction:" .. corporateactionid, timestamp, "DVP", timestampms) \
            if retval[1] == 1 then \
              return retval \
            end \
          elseif mode == 3 then \
            --[[ reverse the dividend ]] \
            local retval = transactiondividend(positions[i]["accountid"], -dividend, -unsettleddividend, brokerid, account["currencyid"], -dividendlocal, -unsettleddividendlocal, corporateaction["description"] .. " - REVERSAL", rate, "corporateaction:" .. corporateactionid, timestamp, "DVR", timestampms) \
            if retval[1] == 1 then \
              return retval \
            end \
          end \
          totquantity = totquantity + positions[i]["quantity"] \
          totunsettledquantity = totunsettledquantity + positions[i]["unsettledquantity"] \
          totdividend = totdividend + dividend \
          totunsettleddividend = totunsettleddividend + unsettleddividend \
          numaccounts = numaccounts + 1 \
        end \
      end \
    end \
    redis.log(redis.LOG_NOTICE, "totquantity: " .. totquantity .. ", totdividend: " .. totdividend) \
    return {0, tostring(totquantity), tostring(totunsettledquantity), tostring(totdividend), tostring(totunsettleddividend), numaccounts} \
  ';

  /*
  * cadividendscrip()
  * script to test/apply/reverse a scrip dividend
  * params: brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms, mode (1=test,2=apply,3=reverse)
  * returns: 0, total position quantity, total shares due, total residue cash, total dividend as cash if ok, else 1 + an error message if unsuccessful
  */
  exports.cadividendscrip = getpositionsbysymbolbydate + getclientfromaccount + getcorporateactionclientdecisionbyclient + dividendasshares + dividendascash + '\
    redis.log(redis.LOG_NOTICE, "cadividendscrip") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local exdate = ARGV[3] \
    local exdatems = ARGV[4] \
    local timestamp = ARGV[5] \
    local timestampms = ARGV[6] \
    local mode = tonumber(ARGV[7]) \
    local totquantity = 0 \
    local totsharesdue = 0 \
    local totresiduecash = 0 \
    local totdividendascash = 0 \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction then \
      return {1, 1028} \
    end \
    --[[ there needs to be a shares per share figure for this ca ]] \
    if not corporateaction["sharespershare"] then \
      return {1, 1035} \
    end \
    --[[ there needs to be a cash per share figure for this ca ]] \
    if not corporateaction["cashpershare"] then \
      return {1, 1036} \
    end \
   --[[ get all positions in the symbol of the corporate action as at the ex-date ]] \
    local positions = getpositionsbysymbolbydate(brokerid, corporateaction["symbolid"], exdatems) \
    for i = 1, #positions do \
      redis.log(redis.LOG_NOTICE, "accountid: " .. positions[i]["accountid"] .. ", quantity: " .. positions[i]["quantity"]) \
      if positions[i]["quantity"] > 0 then \
        --[[ get the client ]] \
        local clientid = getclientfromaccount(positions[i]["accountid"], brokerid) \
        if not clientid then \
          return {1, 1017} \
        end \
        --[[ get the client decision to take cash or scrip & act on it, else use default ]] \
        local corporateactiondecision = getcorporateactionclientdecisionbyclient(brokerid, clientid, corporateactionid) \
        if not corporateactiondecision then \
          if corporateaction["decisiondefault"] == "SCRIP" then \
            local retdivscrip = dividendasshares(brokerid, positions[i], corporateaction, timestamp, timestampms, mode) \
            totsharesdue = totsharesdue + retdivscrip[1] \
            totresiduecash = totresiduecash + retdivscrip[2] \
          else \
            local dividend = dividendascash(brokerid, positions[i], corporateaction, timestamp, timestampms, mode) \
            totdividendascash = totdividendascash + dividend \
          end \
        elseif corporateactiondecision == "SCRIP" then \
          local retdivscrip = dividendasshares(brokerid, positions[i], corporateaction, timestamp, timestampms, mode) \
          totsharesdue = totsharesdue + retdivscrip[1] \
          totresiduecash = totresiduecash + retdivscrip[2] \
        else \
          local dividend = dividendascash(brokerid, positions[i], corporateaction, timestamp, timestampms, mode) \
          totdividendascash = totdividendascash + dividend \
        end \
        totquantity = totquantity + positions[i]["quantity"] \
      end \
    end \
    return {0, tostring(totquantity), tostring(totsharesdue), tostring(totresiduecash), tostring(totdividendascash)} \
  ';

  /*
  * carightsexdate()
  * script to apply the first part of a rights issue, as at the ex-date
  * params: brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms, mode (1=test,2=apply,3=reverse)
  * note: exdate is actual exdate - 1 & exdatems is millisecond representation of time at the end of exdate - 1
  * returns: 0, total position quantity, total rights quantity, total residue if ok, else 1 + an error message if unsuccessful
  */
  exports.carightsexdate = getpositionsbysymbolbydate + getsharesdue + getaccount + geteodprice + newpositiontransaction + transactiondividend + '\
    redis.log(redis.LOG_NOTICE, "carightsexdate") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local exdate = ARGV[3] \
    local exdatems = ARGV[4] \
    local timestamp = ARGV[5] \
    local timestampms = ARGV[6] \
    local mode = ARGV[7] \
    local totquantity = 0 \
    local totrightsquantity = 0 \
    local totresidue = 0 \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
    --[[ there needs to be a shares per share figure for this ca ]] \
    if not corporateaction["sharespershare"] then \
      return {1, 1035} \
    end \
    --[[ make sure a symbol for the rights exists ]] \
    if not corporateaction["symbolidnew"] then \
      return {1, 1030} \
    end \
    --[[ get the symbol ]] \
    local symbol = gethashvalues("symbol:" .. corporateaction["symbolid"]) \
    if not symbol["symbolid"] then \
      return {1, 1015} \
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
    --[[ we are assuming GBP dividend - todo: get fx rate if necessary ]] \
    local rate = 1 \
    --[[ get all positions in the stock of the corporate action as at the ex date ]] \
    local positions = getpositionsbysymbolbydate(brokerid, corporateaction["symbolid"], exdatems) \
    for i = 1, #positions do \
      --[[ only interested in long positions ]] \
      if positions[i]["quantity"] > 0 then \
        redis.log(redis.LOG_NOTICE, "accountid: " .. positions[i]["accountid"] .. ", quantity: " .. positions[i]["quantity"]) \
        --[[ get shares due & any remainder ]] \
        local sharesdue = getsharesdue(positions[i]["quantity"], corporateaction["sharespershare"]) \
        if sharesdue[1] > 0 then \
          --[[ create a position in the rights ]] \
          newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, sharesdue[1], corporateaction["symbolidnew"], timestamp, timestampms) \
          totrightsquantity = totrightsquantity + sharesdue[1] \
        end \
        if sharesdue[2] > 0 then \
          --[[ get the account for the currency ]] \
          local account = getaccount(positions[i]["accountid"], brokerid) \
          --[[ calculate how much cash is due ]] \
          local stubcash = round(sharesdue[2] * eodprice["bid"], 2) \
          if stubcash > 0 then \
            local stubcashlocal = stubcash * rate \
            if mode == 2 then \
              --[[ apply the transaction ]] \
              local retval = transactiondividend(positions[i]["accountid"], stubcash, 0, brokerid, account["currencyid"], stubcashlocal, 0, corporateaction["description"], rate, "corporateaction:" .. corporateactionid, timestamp, timestampms) \
              if retval[1] == 1 then \
                return retval \
              end \
            elseif mode == 3 then \
              --[[ reverse the transaction ]] \
              local retval = transactiondividend(positions[i]["accountid"], -stubcash, 0, brokerid, account["currencyid"], -stubcashlocal, 0, corporateaction["description"] .. " - REVERSAL", rate, "corporateaction:" .. corporateactionid, timestamp, timestampms) \
              if retval[1] == 1 then \
                return retval \
              end \
            end \
            totresidue = totresidue + stubcash \
          end \
        end \
        totquantity = totquantity + positions[i]["quantity"] \
      end \
    end \
    return {0, tostring(totquantity), tostring(totrightsquantity), tostring(totresidue)} \
  ';

  /*
  * carightspaydate()
  * script to apply the second part of a rights issue, as at pay-date
  * params: brokerid, corporateactionid, paydatems, timestamp, timestampms, operatortype, operatorid, mode (1=test,2=apply,3=reverse) 
  * returns: 0, total position quantity, total exercised if ok, else 1 + an error message if unsuccessful
  */
  exports.carightspaydate = getpositionsbysymbolbydate + getaccount + getclientfromaccount + getcorporateactionclientdecisionbyclient + newtrade + newpositiontransaction + '\
    redis.log(redis.LOG_NOTICE, "carightspaydate") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local paydatems = ARGV[3] \
    local timestamp = ARGV[4] \
    local timestampms = ARGV[5] \
    local operatortype = ARGV[6] \
    local operatorid = ARGV[7] \
    local mode = ARGV[8] \
    local currencyratetoorg = 1 \
    local currencyindtoorg = 1 \
    local settlcurrfxrate = 1 \
    local settlcurrfxratecalc = 1 \
    local side = 1 \
    local totquantity = 0 \
    local totexercised = 0 \
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
    --[[ get all positions in the rights symbol as at the pay date ]] \
    local positions = getpositionsbysymbolbydate(brokerid, corporateaction["symbolidnew"], paydatems) \
    for i = 1, #positions do \
      --[[ only interested in long positions ]] \
      if positions[i]["quantity"] > 0 then \
        --[[ get the account for the currency ]] \
        local account = getaccount(positions[i]["accountid"], brokerid) \
        --[[ get the client decision ]] \
        local clientid = getclientfromaccount(positions[i]["accountid"], brokerid) \
        if not clientid then \
          return {1, 1017} \
        end \
        local corporateactiondecision = getcorporateactionclientdecisionbyclient(brokerid, clientid, corporateactionid) \
        if not corporateactiondecision then \
          if corporateaction["decisiondefault"] == "EXERCISE" then \
            if mode == 2 then \
              --[[ create a trade in the original symbol at the rights price ]] \
              newtrade(positions[i]["accountid"], brokerid, clientid, "", corporateaction["symbolid"], side, positions[i]["quantity"], corporateaction["price"], account["currencyid"], currencyratetoorg, currencyindtoorg, 0, 0, 0, 0, "", "", 0, "", "", timestamp, "", "", account["currencyid"], 0, settlcurrfxrate, settlcurrfxratecalc, 0, operatortype, operatorid, 0, timestampms) \
            elseif mode == 3 then \
              --[[ create the reverse trade in the original symbol at the rights price ]] \
              newtrade(positions[i]["accountid"], brokerid, clientid, "", corporateaction["symbolid"], side, -positions[i]["quantity"], corporateaction["price"], account["currencyid"], currencyratetoorg, currencyindtoorg, 0, 0, 0, 0, "", "", 0, "", "", timestamp, "", "", account["currencyid"], 0, settlcurrfxrate, settlcurrfxratecalc, 0, operatortype, operatorid, 0, timestampms) \
            end \
            totexercised = totexercised + positions[i]["quantity"] \
          end \
        elseif corporateactiondecision == "EXERCISE" then \
          if mode == 2 then \
            --[[ create a trade in the original symbol at the rights price ]] \
            newtrade(positions[i]["accountid"], brokerid, clientid, "", corporateaction["symbolid"], side, positions[i]["quantity"], corporateaction["price"], account["currencyid"], currencyratetoorg, currencyindtoorg, 0, 0, 0, 0, "", "", 0, "", "", timestamp, "", "", account["currencyid"], 0, settlcurrfxrate, settlcurrfxratecalc, 0, operatortype, operatorid, 0, timestampms) \
          elseif mode == 3 then \
            --[[ create the reverse trade in the original symbol at the rights price ]] \
            newtrade(positions[i]["accountid"], brokerid, clientid, "", corporateaction["symbolid"], side, -positions[i]["quantity"], corporateaction["price"], account["currencyid"], currencyratetoorg, currencyindtoorg, 0, 0, 0, 0, "", "", 0, "", "", timestamp, "", "", account["currencyid"], 0, settlcurrfxrate, settlcurrfxratecalc, 0, operatortype, operatorid, 0, timestampms) \
          end \
          totexercised = totexercised + positions[i]["quantity"] \
        end \
        if mode == 2 then \
          --[[ zero the position in the rights ]] \
          newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, -positions[i]["quantity"], corporateaction["symbolidnew"], timestamp, timestampms) \
        elseif mode == 3 then \
          --[[ recreate a position in the rights ]] \
          newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, positions[i]["quantity"], corporateaction["symbolidnew"], timestamp, timestampms) \
        end \
        totquantity = totquantity + positions[i]["quantity"] \
      end \
    end \
    return {0, tostring(totquantity), tostring(totexercised)} \
  ';

  /*
  * castocksplit
  * script to apply a stock split
  * params: corporateactionid, exdate, exdatems, timestamp, timestampms, mode (1=test,2=apply,3=reverse)
  * returns: 0, total shares due, total residue if ok, else 1 + an error message if unsuccessful
  * note: this corporate action is applied across all brokers
  */
  exports.castocksplit = geteodprice + getpositionsbysymbolbydate + getsharesdue + getaccount + newpositiontransaction + transactiondividend + round + '\
    redis.log(redis.LOG_NOTICE, "castocksplit") \
    local corporateactionid = ARGV[1] \
    local exdate = ARGV[2] \
    local exdatems = ARGV[3] \
    local timestamp = ARGV[4] \
    local timestampms = ARGV[5] \
    local mode = ARGV[6] \
    local totsharesdue = 0 \
    local totresidue = 0 \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
    --[[ there needs to be a shares per share figure for this ca ]] \
    if not corporateaction["sharespershare"] then \
      return {1, 1035} \
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
    --[[ we are assuming GBP dividend - todo: get fx rate if necessary ]] \
    local rate = 1 \
    --[[ we are applying the split across all positions, so need to interate through all brokers ]] \
    local brokers = redis.call("smembers", "brokers") \
    for j = 1, #brokers do \
      --[[ get all positions in the stock of the corporate action as at the ex date for this broker ]] \
      local positions = getpositionsbysymbolbydate(brokers[j], corporateaction["symbolid"], exdatems) \
      for i = 1, #positions do \
        --[[ only interested in long positions ]] \
        if positions[i]["quantity"] > 0 then \
          redis.log(redis.LOG_NOTICE, "accountid: " .. positions[i]["accountid"] .. ", quantity: " .. positions[i]["quantity"]) \
          --[[ get shares due & any remainder ]] \
          local sharesdue = getsharesdue(positions[i]["quantity"], corporateaction["sharespershare"]) \
          if sharesdue[1] > 0 then \
            if mode == 2 then \
              --[[ update the position ]] \
              newpositiontransaction(positions[i]["accountid"], brokers[j], 0, "", corporateactionid, 2, sharesdue[1], corporateaction["symbolid"], timestamp, timestampms) \
            elseif mode == 3 then \
              --[[ reverse the effect on the position ]] \
              newpositiontransaction(positions[i]["accountid"], brokers[j], 0, "", corporateactionid, 2, -sharesdue[1], corporateaction["symbolid"], timestamp, timestampms) \
            end \
            totsharesdue = totsharesdue + sharesdue[1] \
          end \
          if sharesdue[2] > 0 then \
            --[[ get the account for the currency ]] \
            local account = getaccount(positions[i]["accountid"], brokerid) \
            --[[ calculate how much cash is due ]] \
            local stubcash = round(sharesdue[2] * eodprice["bid"], 2) \
            if stubcash > 0 then \
              local stubcashlocal = stubcash * rate \
              if mode == 2 then \
                local retval = transactiondividend(positions[i]["accountid"], stubcash, brokers[j], account["currencyid"], stubcashlocal, corporateaction["description"], rate, "corporateaction:" .. corporateactionid, timestamp, timestampms) \
                if retval[1] == 1 then \
                  return retval \
                end \
              elseif mode == 3 then \
                local retval = transactiondividend(positions[i]["accountid"], -stubcash, brokers[j], account["currencyid"], -stubcashlocal, corporateaction["description"] .. " - REVERSAL", rate, "corporateaction:" .. corporateactionid, timestamp, timestampms) \
                if retval[1] == 1 then \
                  return retval \
                end \
              end \
              totresidue = totresidue + stubcash \
           end \
         end \
        end \
      end \
    end \
    return {0, tostring(totsharesdue), tostring(totresidue)} \
  ';

  /*
  * cascripissue()
  * script to apply a scrip issue
  * params: brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms, mode (1=test,2=apply,3=reverse)
  * note: exdate is actually exdate - 1 in "YYYYMMDD" format, exdatems is millisecond representation of time at the end of exdate - 1
  * returns: 0, total sharesdue, total residue if ok, else 1 + an error message if unsuccessful
  */
  exports.cascripissue = geteodprice + getpositionsbysymbolbydate + getsharesdue + getaccount + newpositiontransaction + transactiondividend + '\
    redis.log(redis.LOG_NOTICE, "cascripissue") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local exdate = ARGV[3] \
    local exdatems = ARGV[4] \
    local timestamp = ARGV[5] \
    local timestampms = ARGV[6] \
    local mode = ARGV[7] \
    local totsharesdue = 0 \
    local totresidue = 0 \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
    --[[ there needs to be a shares per share figure for this ca ]] \
    if not corporateaction["sharespershare"] then \
      return {1, 1035} \
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
   --[[ we are assuming GBP dividend - todo: get fx rate if necessary ]] \
    local rate = 1 \
    --[[ get all positions in the stock of the corporate action as at the ex date ]] \
    local positions = getpositionsbysymbolbydate(brokerid, corporateaction["symbolid"], exdatems) \
    for i = 1, #positions do \
      --[[ only interested in long positions ]] \
      if positions[i]["quantity"] > 0 then \
        redis.log(redis.LOG_NOTICE, "accountid: " .. positions[i]["accountid"] .. ", quantity: " .. positions[i]["quantity"]) \
        --[[ get shares due & any remainder ]] \
        local sharesdue = getsharesdue(positions[i]["quantity"], corporateaction["sharespershare"]) \
        if sharesdue[1] > 0 then \
          if mode == 2 then \
            --[[ update the position ]] \
            newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, sharesdue[1], corporateaction["symbolid"], timestamp, timestampms) \
          elseif mode == 3 then \
            --[[ reverse the effect on the position ]] \
            newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, -sharesdue[1], corporateaction["symbolid"], timestamp, timestampms) \
          end \
          totsharesdue = totsharesdue + sharesdue[1] \
        end \
        if sharesdue[2] > 0 then \
          --[[ get the account for the currency ]] \
          local account = getaccount(positions[i]["accountid"], brokerid) \
          --[[ calculate how much cash is due ]] \
          local stubcash = round(sharesdue[2] * eodprice["bid"], 2) \
          if stubcash > 0 then \
            local stubcashlocal = stubcash * rate \
            if mode == 2 then \
              local retval = transactiondividend(positions[i]["accountid"], stubcash, brokerid, account["currencyid"], stubcashlocal, corporateaction["description"], rate, "corporateaction:" .. corporateactionid, timestamp, timestampms) \
              if retval[1] == 1 then \
                return retval \
              end \
            elseif mode == 3 then \
              local retval = transactiondividend(positions[i]["accountid"], -stubcash, brokerid, account["currencyid"], -stubcashlocal, corporateaction["description"] .. " - REVERSAL", rate, "corporateaction:" .. corporateactionid, timestamp, timestampms) \
              if retval[1] == 1 then \
                return retval \
              end \
            end \
            totresidue = totresidue + stubcash \
          end \
        end \
      end \
    end \
    return {0, tostring(totsharesdue), tostring(totresidue)} \
  ';

  /*
  * catakeover()
  * script to test/apply/reverse a cash takeover
  * params: brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms, operatortype, operatorid, mode (1=test,2=apply,3=reverse)
  * returns: 0, total position quantity, total shares due, total residue cash, total cash, number of accounts updated if successful, else 1 + an error message if unsuccessful
  */
  exports.catakeover = getpositionquantitiesbysymbolbydate + getaccount + getclientfromaccount + dividendasshares + dividendascash + '\
    redis.log(redis.LOG_NOTICE, "catakeover") \
    local brokerid = ARGV[1] \
    local corporateactionid = ARGV[2] \
    local exdate = ARGV[3] \
    local exdatems = ARGV[4] \
    local timestamp = ARGV[5] \
    local timestampms = ARGV[6] \
    local operatortype = ARGV[7] \
    local operatorid = ARGV[8] \
    local mode = tonumber(ARGV[9]) \
    local totquantity = 0 \
    local numaccounts = 0 \
    local totsharesdue = 0 \
    local totresiduecash = 0 \
    local totcash = 0 \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction["corporateactionid"] then \
      return {1, 1027} \
    end \
    --[[ we are assuming GBP - todo: get fx rate if necessary ]] \
    local rate = 1 \
    --[[ get all positions in the symbol of the corporate action as at the ex-date ]] \
    local positions = getpositionquantitiesbysymbolbydate(brokerid, corporateaction["symbolid"], exdatems) \
    for i = 1, #positions do \
      redis.log(redis.LOG_NOTICE, "accountid: " .. positions[i]["accountid"] .. ", quantity: " .. positions[i]["quantity"]) \
      --[[ may have a position with no quantity ]] \
      if positions[i]["quantity"] ~= 0 then \
        local account = getaccount(positions[i]["accountid"], brokerid) \
        local clientid = getclientfromaccount(positions[i]["accountid"], brokerid) \
        if mode == 2 then \
          --[[ flatten the position in the stock being taken over ]] \
          newpositiontransaction(positions[i]["accountid"], brokerid, 0, "", corporateactionid, 2, -positions[i]["quantity"], corporateaction["symbolid"], timestamp, timestampms) \
        end \
        --[[ share element ]] \
        if tonumber(corporateaction["sharespershare"]) > 0 then \
          local shares = dividendasshares(brokerid, positions[i], corporateaction, timestamp, timestampms, mode) \
          totsharesdue = totsharesdue + shares[1] \
          totresiduecash = totresiduecash + shares[2] \
        end \
        --[[ cash element ]] \
        if tonumber(corporateaction["cashpershare"]) > 0 then \
          local cash = dividendascash(brokerid, positions[i], corporateaction, timestamp, timestampms, mode) \
          totcash = totcash + cash \
        end \
        totquantity = totquantity + positions[i]["quantity"] \
        numaccounts = numaccounts + 1 \
      end \
    end \
    return {0, tostring(totquantity), tostring(totsharesdue), tostring(totresiduecash), tostring(totcash), numaccounts} \
  ';

  /*
  * scriptgettradesbysettlementstatus
  * script to get trades sorted by settlement status
  * params: minimum trade settlement status, maximum trade settlement status
  * returns: list of trades in JSON format
  * note: this index is system wide - index is <settlestatus>:<brokerid>:<tradeid>
  */
  exports.scriptgettradesbysettlementstatus = gettradesbysettlementstatus + '\
    local trades = gettradesbysettlementstatus(ARGV[1], ARGV[2]) \
    return cjson.encode(trades) \
  ';

  /*
  * scriptgettradesbyconsideration
  * script to get trades sorted by consideration
  * params: brokerid, minimum amount, maximum amount
  * returns: list of trades in JSON format
  */
  exports.scriptgettradesbyconsideration = getrecordsbyfieldindex + '\
    --[[ money, so convert min/max to integer values to match index ]] \
    local min = tonumber(ARGV[2]) * 100 \
    local max = tonumber(ARGV[3]) * 100 \
    local trades = getrecordsbyfieldindex(ARGV[1], "trade", "settlcurramt", min, max) \
    return cjson.encode(trades) \
  ';

  /*
  * scriptgettradesbytimestamp
  * script to get trades sorted by timestamp
  * params: brokerid, minimum timestamp, maximum timestamp
  * returns: list of trades in JSON format
  */
  exports.scriptgettradesbytimestamp = getrecordsbyfieldindex + '\
    local trades = getrecordsbyfieldindex(ARGV[1], "trade", "timestamp", ARGV[2], ARGV[3]) \
    return cjson.encode(trades) \
  ';

  /*
  * scriptgettradesbysymbol
  * script to get trades sorted by symbolid
  * params: brokerid, minimum symbolid, maximum symbolid
  * returns: list of trades in JSON format
  */
  exports.scriptgettradesbysymbol = getrecordsbystringfieldindex + '\
    local trades = getrecordsbystringfieldindex(ARGV[1], "trade", "symbolid", ARGV[2], ARGV[3]) \
    return cjson.encode(trades) \
  ';

  /*
  * scriptgettransactionsbytransactiontype
  * script to get transactions sorted by transaction type
  * params: brokerid, minimum transaction type, maximum transaction type
  * returns: list of transactions in JSON format
  */
  exports.scriptgettransactionsbytransactiontype = getrecordsbystringfieldindex + '\
    local transactions = getrecordsbystringfieldindex(ARGV[1], "transaction", "transactiontypeid", ARGV[2], ARGV[3]) \
    return cjson.encode(transactions) \
  ';

  /*
  * scriptgettransactionsbyamount
  * script to get transactions sorted by amount
  * params: brokerid, minimum amount, maximum amount
  * returns: list of transactions in JSON format
  */
  exports.scriptgettransactionsbyamount = getrecordsbyfieldindex + '\
    --[[ money, so convert min/max to integer values to match index ]] \
    local min = tonumber(ARGV[2]) * 100 \
    local max = tonumber(ARGV[3]) * 100 \
    local transactions = getrecordsbyfieldindex(ARGV[1], "transaction", "amount", min, max) \
    return cjson.encode(transactions) \
  ';

  /*
  * scriptgettransactionsbytimestamp
  * script to get transactions sorted by timestamp
  * params: brokerid, minimum timestamp, maximum timestamp
  * returns: list of transactions in JSON format
  */
  exports.scriptgettransactionsbytimestamp = getrecordsbyfieldindex + '\
    local transactions = getrecordsbyfieldindex(ARGV[1], "transaction", "timestamp", ARGV[2], ARGV[3]) \
    return cjson.encode(transactions) \
  ';

  /*
  * scriptgetpostingsbyaccount
  * script to get postings sorted by account
  * params: brokerid, minimum account, maximum account
  * returns: list of postings in JSON format
  */
  exports.scriptgetpostingsbyaccount = getrecordsbyfieldindex + '\
    local postings = getrecordsbyfieldindex(ARGV[1], "posting", "accountid", ARGV[2], ARGV[3]) \
    return cjson.encode(postings) \
  ';

  /*
  * scriptgetpostingsbyamount
  * script to get postings sorted by amount
  * params: brokerid, minimum amount, maximum amount
  * returns: list of postings in JSON format
  */
  exports.scriptgetpostingsbyamount = getrecordsbyfieldindex + '\
    --[[ money, so convert min/max to integer values to match index ]] \
    local min = tonumber(ARGV[2]) * 100 \
    local max = tonumber(ARGV[3]) * 100 \
    local postings = getrecordsbyfieldindex(ARGV[1], "posting", "amount", min, max) \
    return cjson.encode(postings) \
  ';

  /*
  * newcollectaggregateinvest
  * script to collect client funds into a scheme & invest aggregate & pro-rata amounts
  * params: brokerid
  *         schemeid
  *         cash amount - amount of cash, fixed or %
  *         cashisfixed - 1=fixed amount, 0=%
  *         fundallocations - comma delimitted string i.e. "fundid1, %1, fundid2, %2, ..." 
  *         timestamp
  *         timestamp in milliseconds
  *         operatorid
  *         mode - 1=test, 2=apply, 3=reverse
  * returns: 0, scheme cash available, scheme cash invested, number of clients, array of scheme trades {fund id, price, quantity, consideration} if ok, else 1, errorcode
  */
  exports.newcollectaggregateinvest = split + getclientaccountid + getaccount + newtransaction + newposting + newtrade + '\
    redis.log(redis.LOG_NOTICE, "newcollectaggregateinvest") \
    local brokerid = ARGV[1] \
    local schemeid = ARGV[2] \
    local cashamount = tonumber(ARGV[3]) \
    local cashisfixed = tonumber(ARGV[4]) \
    --[[ split the string of fund allocations ]] \
    local fundallocations = split(ARGV[5], ",") \
    local timestamp = ARGV[6] \
    local timestampms = ARGV[7] \
    local operatorid = ARGV[8] \
    local mode = tonumber(ARGV[9]) \
    local rate = 1 \
     --[[ we need a table to return any trades for the scheme ]] \
    local schemetrades = {} \
    for i = 1, #fundallocations, 2 do \
      local schemetrade = {} \
      schemetrade["symbolid"] = fundallocations[i] \
      schemetrade["quantity"] = 0 \
      schemetrade["settlcurramt"] = 0 \
      table.insert(schemetrades, schemetrade) \
    end \
    --[[ table for return values ]] \
    local ret = {} \
    ret["schemecashavailable"] = 0 \
    ret["schemecashinvested"] = 0 \
    ret["numclients"] = 0 \
    --[[ get scheme details ]] \
    local scheme = gethashvalues("broker:" .. brokerid .. ":scheme:" .. schemeid) \
    if not scheme["schemeid"] then \
      return {1, 1037} \
    end \
    local schemeaccount = getaccount(scheme["accountid"], brokerid) \
    local schemeclientid = redis.call("get", "broker:" .. brokerid .. ":account:" .. scheme["accountid"] .. ":client") \
    --[[ process clients in the scheme ]] \
    local schemeclients = redis.call("smembers", "broker:" .. brokerid .. ":scheme:" .. schemeid .. ":clients") \
    for i = 1, #schemeclients do \
      redis.log(redis.LOG_NOTICE, "client: " .. schemeclients[i]) \
      --[[ get the default account for this client ]] \
      local accountid = getclientaccountid(brokerid, schemeclients[i], 1) \
      local account = getaccount(accountid, brokerid) \
      --[[ calculate how much cash is available ]] \
      local amount \
      if cashisfixed == 1 then \
        amount = cashamount \
      else \
        amount = math.floor(tonumber(account["balance"]) * cashamount / 100) \
      end \
      redis.log(redis.LOG_NOTICE, "cash available: " .. amount) \
      --[[ loop through the fundallocations & determine how much to invest in each ]] \
      local k = 1 \
      for j = 1, #fundallocations, 2 do \
        redis.log(redis.LOG_NOTICE, "fund:" .. fundallocations[j] .. " % " .. fundallocations[j+1]) \
        local symbol = gethashvalues("symbol:" .. fundallocations[j]) \
        if not symbol["symbolid"] then \
          redis.log(redis.LOG_NOTICE, "symbol not found:" .. fundallocations[j]) \
          return {1, 1015} \
        end \
        schemetrades[k]["price"] = tonumber(symbol["ask"]) \
        if schemetrades[k]["price"] == 0 then \
          redis.log(redis.LOG_NOTICE, "symbol: " .. fundallocations[j] .. " does not have a price") \
          return {1, 1040} \
        end \
        --[[ calculate investment for this client & fund ]] \
        local quantity = math.floor(amount * fundallocations[j+1] / 100 / schemetrades[k]["price"]) \
        local settlcurramt = round(schemetrades[k]["price"] * quantity, 2) \
        if quantity > 0 then \
          if mode == 2 then \
            --[[ create client trades ]] \
            newtrade(accountid, brokerid, schemeclients[i], "", fundallocations[j], 1, quantity, symbol["ask"], account["currencyid"], 1, 1, 0, 0, 0, 0, 0, 3, 0, "", "", timestamp, "", "", account["currencyid"], settlcurramt, 1, 0, 0, 2, operatorid, 0, timestampms) \
          end \
          schemetrades[k]["quantity"] = schemetrades[k]["quantity"] + quantity \
          schemetrades[k]["settlcurramt"] = schemetrades[k]["settlcurramt"] + settlcurramt \
        end \
        k = k + 1 \
      end \
      ret["schemecashavailable"] = ret["schemecashavailable"] + amount \
      ret["numclients"] = ret["numclients"] + 1 \
    end \
    for i = 1, #schemetrades do \
      if schemetrades[i]["quantity"] > 0 and mode == 2 then \
        --[[ create a trade for the scheme for this fund ]] \
        newtrade(scheme["accountid"], brokerid, schemeclientid, "", schemetrades[i]["symbolid"], 1, schemetrades[i]["quantity"], schemetrades[i]["price"], schemeaccount["currencyid"], 1, 1, 0, 0, 0, 0, 0, 3, 0, "", "", timestamp, "", "", schemeaccount["currencyid"], schemetrades[i]["settlcurramt"], 1, 0, 0, 2, operatorid, 0, timestampms) \
        ret["schemecashinvested"] = ret["schemecashinvested"] + schemetrades[i]["settlcurramt"] \
      end \
    end \
    ret["schemetrades"] = schemetrades \
    return cjson.encode({0, ret}) \
  ';

  /*
  * scriptdeletetrade()
  * processing to delete a trade
  * params: brokerid, tradeid, timestamp, timestamp in milliseconds
  * returns: 0, CREST message type to send or empty string if successful else 1, error message 
  */
  scriptdeletetrade = deletetrade + '\
  redis.log(redis.LOG_NOTICE, "scriptdeletetrade") \
  local brokerid = ARGV[1] \
  local tradeid = ARGV[2] \
  local timestamp = ARGV[3] \
  local timestampms = ARGV[4] \
  --[[ get the original trade ]] \
  local trade = gethashvalues("broker:" .. brokerid .. ":trade:" .. tradeid) \
  if tonumber(trade["status"]) == 3 then \
    return {1, "Trade already deleted"} \
  end \
  if tonumber(trade.tradesettlestatusid) == 0 then \
    deletetrade(trade, timestamp, timestampms) \
  elseif tonumber(trade.tradesettlestatusid) == 1 or tonumber(trade.tradesettlestatusid) == 2 or tonumber(trade.tradesettlestatusid) == 3 then \
    deletetrade(trade, timestamp, timestampms) \
    return {0, "AXTD"} \
  elseif tonumber(trade.tradesettlestatusid) == 4 then \
    return {1, "Trade already settled. Unable to delete."} \
  end \
  return {0, ""} \
  ';

  scriptedittrade = deletetrade + newtrade + updatetrade + '\
  redis.log(redis.LOG_NOTICE, "scriptedittrade") \
  --[[ these are the updated values ]] \
  local accountid = ARGV[1] \
  local brokerid = ARGV[2] \
  local clientid = ARGV[3] \
  local orderid = ARGV[4] \
  local symbolid = ARGV[5] \
  local side = ARGV[6] \
  local quantity = ARGV[7] \
  local price = ARGV[8] \
  local currencyid = ARGV[9] \
  local currencyratetoorg = ARGV[10] \
  local currencyindtoorg = ARGV[11] \
  local commission = ARGV[12] \
  local ptmlevy = ARGV[13] \
  local stampduty = ARGV[14] \
  local contractcharge = ARGV[15] \
  local counterpartyid = ARGV[16] \
  local counterpartytype = ARGV[17] \
  local markettype = ARGV[18] \
  local externaltradeid = ARGV[19] \
  local futsettdate = ARGV[20] \
  local timestamp = ARGV[21] \
  local lastmkt = ARGV[22] \
  local externalorderid = ARGV[23] \
  local settlcurrencyid = ARGV[24] \
  local settlcurramt = ARGV[25] \
  local settlcurrfxrate = ARGV[26] \
  local settlcurrfxratecalc = ARGV[27] \
  local margin = ARGV[28] \
  local operatortype = ARGV[29] \
  local operatorid = ARGV[30] \
  local finance = ARGV[31] \
  local timestampms = ARGV[32] \
  local tradeid = ARGV[33] \
  --[[ get the original trade ]] \
  local trade = gethashvalues("broker:" .. brokerid .. ":trade:" .. tradeid) \
  if trade.status == 3 then \
    return {1, "Cannot edit a deleted trade"} \
  end \
  --[[ see if one or more of the values relevant to CREST have changed ]] \
  if tonumber(accountid) ~= tonumber(trade.accountid) or tonumber(clientid) ~= tonumber(trade.clientid) or tonumber(commission) ~= tonumber(trade.commission) or currencyid ~= trade.currencyid or tonumber(quantity) ~= tonumber(trade.quantity) or tonumber(price) ~= tonumber(trade.price) or tonumber(ptmlevy) ~= tonumber(trade.ptmlevy) or tonumber(currencyratetoorg) ~= tonumber(trade.currencyratetoorg) or tonumber(currencyindtoorg) ~= tonumber(trade.currencyindtoorg) or tonumber(settlcurramt) ~= tonumber(trade.settlcurramt) or settlcurrencyid ~= trade.settlcurrencyid or tonumber(settlcurrfxrate) ~= tonumber(trade.settlcurrfxrate) or tonumber(side) ~= tonumber(trade.side) or tonumber(stampduty) ~= tonumber(trade.stampduty) or timestamp ~= trade.timestamp or symbolid ~= trade.symbolid then \
    --[[ they have, so what we do depends on CREST status ]] \
    if tonumber(trade.tradesettlestatusid) == 0 then \
      --[[ we can edit but need to reverse the postings of the original trade, so delete & enter a new one ]] \
      deletetrade(trade, timestamp, timestampms) \
      --[[ create a new trade with a combination of the existing & updated values ]] \
      newtrade(accountid, brokerid, clientid, trade.orderid, trade.symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, 0, trade.counterpartyid, trade.counterpartytype, trade.markettype, trade.externaltradeid, trade.futsettdate, timestamp, trade.lastmkt, trade.externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, trade.margin, operatortype, operatorid, trade.finance, timestampms) \
    elseif tonumber(trade.tradesettlestatusid) == 1 then \
      --[[ we can only edit if only stamp duty related values have changed ]] \
      if tonumber(accountid) == tonumber(trade.accountid) and tonumber(clientid) == tonumber(trade.clientid) and tonumber(commission) == tonumber(trade.commission) and currencyid == trade.currencyid and tonumber(quantity) == tonumber(trade.quantity) and tonumber(price) == tonumber(trade.price) and tonumber(ptmlevy) == tonumber(trade.ptmlevy) and tonumber(currencyratetoorg) == tonumber(trade.currencyratetoorg) and tonumber(currencyindtoorg) == tonumber(trade.currencyindtoorg) and tonumber(settlcurrfxrate) == tonumber(trade.settlcurrfxrate) and tonumber(side) == tonumber(trade.side) and timestamp == trade.timestamp and symbolid == trade.symbolid then \
        --[[ remove the original trade ]] \
        deletetrade(trade, timestamp, timestampms) \
        --[[ create a new trade with a combination of the existing & updated values ]] \
        newtrade(accountid, brokerid, clientid, trade.orderid, trade.symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, 0, trade.counterpartyid, trade.counterpartytype, trade.markettype, trade.externaltradeid, trade.futsettdate, timestamp, trade.lastmkt, trade.externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, trade.margin, operatortype, operatorid, trade.finance, timestampms) \
        --[[ need to send a message to CREST ]] \
        return {0, "ATXA"} \
      else \
        return {1, "Changes other than Stamp Duty related cannot be accepted. Delete this trade and enter a new one."} \
      end \
    elseif tonumber(trade.tradesettlestatusid) == 2 or tonumber(trade.tradesettlestatusid) == 3 then \
      return {1, "Matching has already taken place. Changes cannot now be accepted."} \
    elseif tonumber(trade.tradesettlestatusid) == 4 then \
      return {1, "Settlement has already taken place. Changes cannot now be accepted."} \
    end \
  else \
    --[[ only minor changes, so can just update existing trade values ]] \
    updatetrade(trade.accountid, trade.brokerid, trade.clientid, orderid, symbolid, trade.side, trade.quantity, trade.price, trade.currencyid, trade.currencyratetoorg, trade.currencyindtoorg, trade.commission, trade.ptmlevy, trade.stampduty, contractcharge, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, trade.settlcurrencyid, trade.settlcurramt, trade.settlcurrfxrate, trade.settlcurrfxratecalc, margin, trade.operatortype, trade.operatorid, finance, timestampms, trade.tradeid, trade.tradesettlestatusid) \
  end \
  return {0, ""} \
  ';
}
