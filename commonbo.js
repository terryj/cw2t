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
    case 1024:
      desc = "Quote not found";
      break;
    default:
      desc = "Unknown reason";
    }

    return desc;
  }

  exports.getReasonDesc = getReasonDesc;

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

  //
  // get a range of trades from passed ids
  //
  gettrades = '\
  local gettrades = function(trades) \
    local tblresults = {} \
    local fields = {"clientid","orderid","symbolid","side","quantity","price","currencyid","currencyratetoorg","currencyindtoorg","commission","ptmlevy","stampduty","contractcharge","counterpartyid","markettype","externaltradeid","futsettdate","timestamp","lastmkt","externalorderid","tradeid","settlcurrencyid","settlcurramt","settlcurrfxrate","settlcurrfxratecalc","nosettdays","margin","finance"} \
    local vals \
    for index = 1, #trades do \
      vals = redis.call("hmget", "trade:" .. trades[index], unpack(fields)) \
      local brokerclientcode = redis.call("hget", "client:" .. vals[1], "brokerclientcode") \
      table.insert(tblresults, {clientid=vals[1],brokerclientcode=brokerclientcode,orderid=vals[2],symbolid=vals[3],side=vals[4],quantity=vals[5],price=vals[6],currencyid=vals[7],currencyratetoorg=vals[8],currencyindtoorg=vals[9],commission=vals[10],ptmlevy=vals[11],stampduty=vals[12],contractcharge=vals[13],counterpartyid=vals[14],markettype=vals[15],externaltradeid=vals[16],futsettdate=vals[17],timestamp=vals[18],lastmkt=vals[19],externalorderid=vals[20],tradeid=vals[21],settlcurrencyid=vals[22],settlcurramt=vals[23],settlcurrfxrate=vals[24],settlcurrfxratecalc=vals[25],nosettdays=vals[26],margin=vals[27],finance=vals[28]}) \
    end \
    return tblresults \
  end \
  ';

  exports.gettrades = gettrades;

  /*
  * getposition()
  * gets a position
  * params: accountid, brokerid, symbolsettdatekey
  * returns: quantity, cost as an array
  */
  getposition = '\
  local getposition = function(accountid, brokerid, symbolsettdatekey) \
    local fields = {"quantity", "cost"} \
    local position = redis.call("hmget", "broker:" .. brokerid .. ":account:" .. accountid .. ":position:" .. symbolsettdatekey, unpack(fields)) \
    return position \
  end \
  ';

  exports.getposition = getposition;

  /*
  * getpositions()
  * gets all positions for an account
  * params: accountid, brokerid
  * returns: all position fields as a table
  */
  getpositions = '\
  local getpositions = function(accountid, brokerid) \
    redis.log(redis.LOG_DEBUG, "getpositions") \
    local tblresults = {} \
    local fields = {"accountid","brokerid","cost","positionid","quantity","symbolid"} \
    local positions = redis.call("smembers", "broker:" .. brokerid .. ":account:" .. accountid .. "positions") \
    for index = 1, #positions do \
      local vals = redis.call("hmget", "broker:" .. brokerid .. ":position:" .. positions[index], unpack(fields)) \
      table.insert(tblresults, {accountid=vals[1],brokerid=vals[2],cost=vals[3],positionid=vals[4],quantity=vals[5],symbolid=vals[6]}) \
    end \
    return tblresults \
  end \
  ';

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
      if instrumenttypeid ~= "DE" and instrumenttypeid ~= "IE" then \
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

  getunrealisedpandl = round + '\
  local getunrealisedpandl = function(symbolid, quantity, cost) \
    local unrealisedpandl = 0 \
    local price = 0 \
    local qty = tonumber(quantity) \
    if qty > 0 then \
      local bidprice = redis.call("hget", "symbol:" .. symbolid, "bid") \
      if bidprice and tonumber(bidprice) ~= 0 then \
        price = tonumber(bidprice) \
        if price ~= 0 then \
          unrealisedpandl = round(qty * price / 100 - cost, 2) \
        end \
      end \
    else \
      local askprice = redis.call("hget", "symbol:" .. symbolid, "ask") \
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

  gettotalpositions = getpositions + getunrealisedpandl + getmargin + '\
  local gettotalpositions = function(accountid, brokerid) \
    redis.log(redis.LOG_DEBUG, "gettotalpositions") \
    local positions = getpositions(accountid, brokerid) \
    local totalmargin = 0 \
    local totalunrealisedpandl = 0 \
    for index = 1, #positions do \
      local margin = getmargin(positions[index][6], positions[index][5]) \
      totalmargin = totalmargin + margin \
      local unrealisedpandl = getunrealisedpandl(positions[index][6], positions[index][5], positions[index][3]) \
      totalunrealisedpandl = totalunrealisedpandl + unrealisedpandl[1] \
    end \
    return {totalmargin, totalunrealisedpandl} \
  end \
  ';

  exports.gettotalpositions = gettotalpositions;

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
  * returns the first account id found for the account type, else 0
  */
  getclientaccountid = '\
  local getclientaccountid = function(brokerid, clientid, accounttypeid) \
    local acctid = 0 \
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
  * getaccountbalance()
  * params: accountid, brokerid
  * returns: account balance & local currency balance
  */
  getaccountbalance = '\
  local getaccountbalance = function(accountid, brokerid) \
    local fields = {"balance", "localbalance"} \
    local vals = redis.call("hmget", "broker:" .. brokerid .. ":account:" .. accountid, unpack(fields)) \
    return vals \
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
  * calculates free margin for an accountcurrency
  * balance = cash
  * equity = balance + unrealised p&l
  * free margin = equity - margin used to hold positions
  */
  getfreemargin = getaccountbalance + gettotalpositions + '\
  local getfreemargin = function(accountid, brokerid) \
    redis.log(redis.LOG_DEBUG, "getfreemargin") \
    local freemargin = 0 \
    local accountbalance = getaccountbalance(accountid, brokerid) \
    if accountbalance[1] then \
      redis.log(redis.LOG_DEBUG, "accountbalance[1]") \
      redis.log(redis.LOG_DEBUG, accountbalance[1]) \
      local balance = tonumber(accountbalance[1]) \
      local totalpositions = gettotalpositions(accountid, brokerid) \
      redis.log(redis.LOG_DEBUG, "totalpositions[2]") \
      redis.log(redis.LOG_DEBUG, totalpositions[2]) \
      local equity = balance + totalpositions[2] \
      freemargin = equity - totalpositions[1] \
    end \
    return freemargin \
  end \
  ';

  exports.getfreemargin = getfreemargin;

  /*
  * newposting()
  * creates a posting record & updates balances for an account
  */
  newposting = updateaccountbalance + '\
  local newposting = function(accountid, amount, brokerid, localamount, transactionid) \
    local postingid = redis.call("hincrby", "broker:" .. brokerid, "lastpostingid", 1) \
    local brokerkey = "broker:" .. brokerid \
    redis.call("hmset", brokerkey .. ":posting:" .. postingid, "accountid", accountid, "brokerid", brokerid, "amount", amount, "localamount", localamount, "postingid", postingid, "transactionid", transactionid) \
    redis.call("sadd", brokerkey .. ":transaction:" .. transactionid .. ":postings", postingid) \
    --[[ add a sorted set for time based queries ]] \
    --[[redis.call("zadd", brokerkey .. ":account:" .. accountid .. ":postings", unixtimestamp, postingid) ]]\
    updateaccountbalance(accountid, amount, brokerid, localamount) \
  return postingid \
  end \
  ';

  /*
  * newtransaction()
  * creates a transaction record
  */
  newtransaction = '\
  local newtransaction = function(amount, brokerid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid) \
    local transactionid = redis.call("hincrby", "broker:" .. brokerid, "lasttransactionid", 1) \
    redis.call("hmset", "broker:" .. brokerid .. ":transaction:" .. transactionid, "amount", amount, "brokerid", brokerid, "currencyid", currencyid, "localamount", localamount, "note", note, "rate", rate, "reference", reference, "timestamp", timestamp, "transactiontypeid", transactiontypeid, "transactionid", transactionid) \
    return transactionid \
  end \
  ';

  /*
  * newtradeaccounttransaction()
  * create transaction & postings for the cash side of a trade
  * params: amount, brokerid, clientaccountid, currencyid, localamount, nominalaccountid, note, rate, timestamp, tradeid, transactiontype
  */
  newtradeaccounttransaction = newtransaction + newposting + getbrokeraccountid + '\
  local newtradeaccounttransaction = function(amount, brokerid, clientaccountid, currencyid, localamount, nominalaccountid, note, rate, timestamp, tradeid, transactiontype) \
    local clientcontrolaccountid = getbrokeraccountid(brokerid, currencyid, "clientcontrolaccount") \
    local transactionid = newtransaction(amount, brokerid, currencyid, localamount, note, rate, "trade:" .. tradeid, timestamp, transactiontype) \
    if transactiontype == "TR" then \
      --[[ receipt from broker point of view ]] \
      newposting(clientaccountid, -amount, brokerid, -localamount, transactionid) \
      newposting(clientcontrolaccountid, -amount, brokerid, -localamount, transactionid) \
      newposting(nominalaccountid, amount, brokerid, localamount, transactionid) \
    else \
      --[[ pay from broker point of view ]] \
      newposting(clientaccountid, amount, brokerid, localamount, transactionid) \
      newposting(clientcontrolaccountid, amount, brokerid, localamount, transactionid) \
      newposting(nominalaccountid, -amount, brokerid, -localamount, transactionid) \
    end \
  end \
  ';

  /*
  * newtradeaccounttransactions()
  * cash side of a client trade
  * creates a separate transaction for the consideration & each of the cost items
  */
  newtradeaccounttransactions = newtradeaccounttransaction + '\
  local newtradeaccounttransactions = function(consideration, commission, ptmlevy, stampduty, brokerid, clientaccountid, currencyid, localamount, note, rate, timestamp, tradeid, side) \
    local nominaltradeaccountid = getbrokeraccountid(brokerid, currencyid, "nominaltradeaccount") \
    local nominalcommissionaccountid = getbrokeraccountid(brokerid, currencyid, "nominalcommissionaccount") \
    local nominalptmaccountid = getbrokeraccountid(brokerid, currencyid, "nominalptmaccount") \
    local nominalstampdutyaccountid = getbrokeraccountid(brokerid, currencyid, "nominalstampdutyaccount") \
    --[[ side determines pay / receive ]] \
    if tonumber(side) == 1 then \
      --[[ client buy, so cash received from broker point of view ]] \
      newtradeaccounttransaction(consideration, brokerid, clientaccountid, currencyid, localamount, nominaltradeaccountid, note, rate, timestamp, tradeid, "TR") \
    else \
      --[[ cash paid from broker point of view ]] \
      newtradeaccounttransaction(consideration, brokerid, clientaccountid, currencyid, localamount, nominaltradeaccountid, note, rate, timestamp, tradeid, "TP") \
    end \
    --[[ broker always receives costs ]] \
    newtradeaccounttransaction(commission, brokerid, clientaccountid, currencyid, commission, nominalcommissionaccountid, note .. " Commission", rate, timestamp, tradeid, "TR") \
    newtradeaccounttransaction(ptmlevy, brokerid, clientaccountid, currencyid, ptmlevy, nominalptmaccountid, note .. " PTM Levy", rate, timestamp, tradeid, "TR") \
    newtradeaccounttransaction(stampduty, brokerid, clientaccountid, currencyid, stampduty, nominalstampdutyaccountid, note .. " Stamp Duty", rate, timestamp, tradeid, "TR") \
    return 0 \
  end \
  ';

  exports.newtradeaccounttransactions = newtradeaccounttransactions;

  /*** Scripts ***/

  //
  // get holidays for a market, i.e. "L" = London...assume "L" for the time being?
  //
  exports.scriptgetholidays = '\
  local tblresults = {} \
  local holidays = redis.call("smembers", "holidays:" .. ARGV[1]) \
  for index = 1, #holidays do \
    table.insert(tblresults, {holidays[index]}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // get trades, most recent first
  // params: client id
  //
  exports.scriptgettrades = gettrades + '\
  local trades = redis.call("sort", ARGV[1] .. ":trades", "DESC") \
  local tblresults = gettrades(trades) \
  return cjson.encode(tblresults) \
  ';

  //
  // params: client id, positionkey
  //
  exports.scriptgetpostrades = gettrades + '\
  local postrades = redis.call("smembers", ARGV[1] .. ":trades:" .. ARGV[2]) \
  local tblresults = gettrades(postrades) \
  return cjson.encode(tblresults) \
  ';

  //
  // get positions for a client
  // params: client id
  // returns an array of positions
  //
  exports.scriptgetpositions = getunrealisedpandl + getmargin + '\
  local tblresults = {} \
  local positions = redis.call("smembers", ARGV[1] .. ":positions") \
  local fields = {"clientid","symbolid","quantity","cost","currencyid","positionid","futsettdate"} \
  local vals \
  for index = 1, #positions do \
    vals = redis.call("hmget", ARGV[1] .. ":position:" .. positions[index], unpack(fields)) \
    local margin = getmargin(vals[2], vals[3]) \
    --[[ value the position ]] \
    local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
    table.insert(tblresults, {clientid=vals[1],symbolid=vals[2],quantity=vals[3],cost=vals[4],currencyid=vals[5],margin=margin,positionid=vals[6],futsettdate=vals[7],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
  end \
  return tblresults \
  ';

  //
  // get position(s) for a client for a single symbol
  // params: client id, symbol
  // may return none/one or a number of positions as may be more than one settlement date
  //
  exports.scriptgetposition = getunrealisedpandl + getmargin + '\
  local fields = {"clientid","symbolid","quantity","cost","currencyid","positionid","futsettdate"} \
  local tblresults = {} \
  local instrumenttypeid = redis.call("hget", "symbol:" .. ARGV[2], "instrumenttypeid") \
  if instrumenttypeid == "CFD" or instrumenttypeid == "SPB" then \
    local positions = redis.call("smembers", ARGV[1] .. ":positions:" .. ARGV[2]) \
    for index = 1, #positions do \
      local vals = redis.call("hmget", ARGV[1] .. ":position:" .. ARGV[2] .. ":" .. positions[index], unpack(fields)) \
      local margin = getmargin(vals[2], vals[3]) \
      --[[ value the position ]] \
      local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
      table.insert(tblresults, {clientid=vals[1],symbolid=vals[2],quantity=vals[3],cost=vals[4],currencyid=vals[5],margin=margin,positionid=vals[6],futsettdate=vals[7],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
    end \
  else \
    local vals = redis.call("hmget", ARGV[1] .. ":position:" .. ARGV[2], unpack(fields)) \
    if vals[1] then \
      local margin = getmargin(vals[2], vals[3]) \
      --[[ value the position ]] \
      local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
      table.insert(tblresults, {clientid=vals[1],symbolid=vals[2],quantity=vals[3],cost=vals[4],currencyid=vals[5],margin=margin,positionid=vals[6],futsettdate=vals[7],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
    end \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // calculates account p&l, margin & equity for a client
  // assumes there is cash for any currency with positions
  // params: client id
  //
  exports.scriptgetaccountsummary = gettotalpositions + '\
  local tblresults = {} \
  local cash = redis.call("smembers", "client:" .. ARGV[1] .. ":cash") \
  for index = 1, #cash do \
    local amount = redis.call("get", "client:" .. ARGV[1] .. ":cash:" .. cash[index]) \
    local totalpositions = gettotalpositions(ARGV[1], cash[index]) \
    local balance = tonumber(amount) \
    local equity = balance + totalpositions[2] \
    local freemargin = equity - totalpositions[1] \
    table.insert(tblresults, {currencyid=cash[index],cash=amount,balance=balance,unrealisedpandl=totalpositions[2],equity=equity,margin=totalpositions[1],freemargin=freemargin}) \
  end \
  return cjson.encode(tblresults) \
  ';

  //
  // get positions for a client & subscribe client & server to the position symbols
  // params: client id, server id
  //
  /*exports.scriptsubscribepositions = getunrealisedpandl + subscribesymbolnbt + getmargin + '\
  local tblresults = {} \
  local tblsubscribe = {} \
  local positions = redis.call("smembers", "client:" .. ARGV[1] .. ":positions") \
  local fields = {"clientid","symbolid","quantity","cost","currencyid","positionid","futsettdate"} \
  local vals \
  for index = 1, #positions do \
    vals = redis.call("hmget", "client:" .. ARGV[1] .. ":position:" .. positions[index], unpack(fields)) \
    --[[ todo: error msg ]] \
    if vals[1] then \
      local margin = getmargin(vals[2], vals[3]) \
      --[[ value the position ]] \
      local unrealisedpandl = getunrealisedpandl(vals[2], vals[3], vals[4]) \
      table.insert(tblresults, {clientid=vals[1],symbolid=vals[2],quantity=vals[3],cost=vals[4],currencyid=vals[5],margin=margin,positionid=vals[6],futsettdate=vals[7],mktprice=unrealisedpandl[2],unrealisedpandl=unrealisedpandl[1]}) \
      --[[ subscribe to this symbol, so as to get prices to the f/e for p&l calc ]] \
      local subscribe = subscribesymbolnbt(vals[2], ARGV[1], ARGV[2]) \
      if subscribe[1] == 1 then \
        table.insert(tblsubscribe, vals[2]) \
      end \
    end \
  end \
  return {cjson.encode(tblresults), tblsubscribe} \
  ';*/

  /*
  * newClientFundsTransfer
  * script to handle client deposits & withdrawals
  * keys: broker:<brokerid>
  * args: amount, bankaccountid, brokerid, clientaccountid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid
  */
  exports.newClientFundsTransfer = newtransaction + newposting + getbrokeraccountid + '\
    local controlclientaccountid = getbrokeraccountid(ARGV[3], ARGV[5], "controlclient") \
    local amount \
    local localamount \
    if ARGV[11] == "CD" then \
      amount = tonumber(ARGV[1]) \
      localamount = tonumber(ARGV[6]) \
    else \
      amount = -tonumber(ARGV[1]) \
      localamount = -tonumber(ARGV[6]) \
    end \
    local transactionid = newtransaction(ARGV[1], ARGV[3], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11]) \
    --[[ update client account ]] \
    newposting(ARGV[4], amount, ARGV[3], localamount, transactionid) \
    --[[ client control account ]] \
    newposting(controlclientaccountid, amount, ARGV[3], localamount, transactionid) \
    --[[ update bank account ]] \
    newposting(ARGV[2], amount, ARGV[3], localamount, transactionid) \
    return 0 \
  ';

  // todo: this needs to be called from a trade
  exports.scripttesttrade = newtradeaccounttransactions + '\
    newtradeaccounttransactions(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13]) \
    return 0 \
  ';

  /*
  * newTradeSettlementTransaction
  * script to handle settlement of trades
  * params: amount, brokerid, currencyid, fromaccountid, localamount, note, rate, timestamp, toaccountid, tradeid, transactiontypeid
  * returns: 0 if ok, else error message
  */
  exports.newTradeSettlementTransaction = '\
    redis.log(redis.LOG_DEBUG, "newTradeSettlementTransaction") \
    --[[ transactiontypeid may be passed, else derive it ]] \
    local transactiontypeid = ARGV[11] \
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
        return "Invalid account" \
      end \
    end \
    local transactionid = newtransaction(ARGV[1], ARGV[2], ARGV[3], ARGV[5], ARGV[6], ARGV[7], ARGV[10], ARGV[8], transactiontypeid) \
    newPosting(fromaccountgroupid, -tonumber(ARGV[1]), ARGV[2], -tonumber(ARGV[5]), transactionid) \
    newPosting(tobankaccountid, ARGV[1], ARGV[2], ARGV[5], transactionid) \
    return 0 \
  ';

  /*
  * newBrokerFundsTransfer
  * script to handle transfer of funds between broker and supplier
  * params: amount, brokerid, currencyid, brokerbankaccountid, localamount, nominalaccountid, note, rate, reference, supplieraccountid, timestamp, transactiontypeid
  * returns 0
  */
  exports.newBrokerFundsTransfer = '\
    redis.log(redis.LOG_DEBUG, "newBrokerFundsTransfer") \
    local transactionid = newtransaction(ARGV[1], ARGV[2], ARGV[3], ARGV[5], ARGV[7], ARGV[8], ARGV[9], ARGV[11], ARGV[12]) \
    if ARGV[12] == "BP" then \
      newPosting(ARGV[4], -tonumber(ARGV[1]), ARGV[2], -tonumber(ARGV[5]), transactionid) \
      newPosting(ARG[10], ARGV[1], ARGV[2], ARGV[5], transactionid) \
    else \
      newPosting(ARGV[4], ARGV[1], ARGV[2], ARGV[5], transactionid) \
      newPosting(ARG[10], -tonumber(ARGV[1]), ARGV[2], -tonumber(ARGV[5]), transactionid) \
    end \
    return 0 \
  ';
}
