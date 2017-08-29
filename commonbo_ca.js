/******************
* commonbo.js
* Common back-office functions
* Cantwaittotrade Limited
* Terry Johnston
* June 2015
* Changes:
* reverseca() used to reverse the ca, no need to handle mode == 3 in any ca
*  1 Sep 2016 - modified corporate action functions to take account of default client decisions - added dividendasshares(), dividendascash()
*  1 Sep 2016 - modified corporate action functions to use account rather than symbol for currency of transactions
* 21 Oct 2016 - added getorderbook(), getorderbooktop(), getorderbooktopall()
*             - added scriptgetorderbook(), scriptgetorderbooktop(), scriptgetorderbooktopall()
* 23 Oct 2016 - added orderbookchannel
* 07 Nov 2016 - modified getpostingsbydate() to use posting note rather than transaction note
* 10 Dec 2016 - added fixseqnum to newtrade()
* 20 Dec 2016 - added scriptErrorLog(), errorlog() & publisherror()
* 21 Dec 2016 - updated publishposition() & publishtrade() to use a specific channel
* 31 Dec 2016 - added caconversion()
* 20 Jan 2017 - added electedquantityasshares to carightspaydate()
* 14 Feb 2017 - added getpositionvaluesbysymbol(), getpositionvaluesbysymbolbydate(), scriptgetpositionvaluesbysymbol() & scriptgetpositionvaluesbysymbolbydate()
* 14 Feb 2017 - added valueposition(), valuepositiondate() & modified getunrealisedpandl() & getunrealisedpandlvaluedate() to enable summary positions to be valued
* 15 Feb 2017 - amended getcurrencyrate() to use revised format currency pair symbolid i.e. "USDGBP"
* 21 Feb 2017 - added getaccountsummarydate() & gettotalpositionvaluedate()
* 24 Feb 2017 - replaced getcurrencyrate() with getcurrencyratedate() in valuepositiondate()
* 28 Feb 2017 - renamed isholiday() to isExchangeClosed()
* 02 Mar 2017 - modified getSettDate() to use exchangeid instead of holidays list to calculate settlement date
* 13 Mar 2017 - added getdefaultsettletype()
* 15 Mar 2017 - added isValidTradingDay() and getPreviousTradingDay()
* 22 Mar 2017 - modified scriptedittrade
* 23 Mar 2017 - added updatetradeindexes and modified newtrade
* 24 Mar 2017 - modified round
* 14 Apr 2017 - added getorderfromorderbook()
*             - removed broker parameter from getorderbook(), getorderbooktop(), getorderbooktopall()
*             - updated scriptgetorderbook, scriptgetorderbooktop, scriptgetorderbooktopall
*  8 May 2017 - updated getclientaccountid() to include currencyid & use getaccountsbyclient()
*  8 May 2017 - added applybenefits()
*  8 May 2017 - replaced dividendascash() with benefitascash()
*  8 May 2017 - replace dividendasshares() with benefitasshares()
* 22 May 2017 - Added getcaclientoption()
* 22 May 2017 - Removed dividendasshares() dividendascash()
* 22 May 2017 - modified getsharesdue()
* 22 May 2017 - renamed getclientfromaccount() to getclientidfromaccount()
* 22 May 2017 - added getclientfromaccount()
* 24 May 2017 - removed cacashdividend(), cadividendscrip(), cadividendoption(), carightsexdate()
* 24 May 2017 - added caapply()
* 24 May 2017 - added cavalidate()
* 24 May 2017 - removed carightspaydate()
* 24 May 2017 - added benefitasshares()
* 25 May 2017 - updated convertsharesascash()
* 25 May 2017 - removed cacapitalrepayment(), castocksplit(), catakeover(), caconversion()
* 30 May 2017 - fixed getaccount() call in getclientaccountid()
* 30 May 2017 - modified caapply(), applybenefits(), benefitascash(), benefitasshares()
*  1 Jun 2017 - added benefit percentage parameter to getsharesdue()
*  2 Jun 2017 - updated caapply(), benefitasshares(), benefitascash(), applybenefits()
*             - removed convertsharesasshares()
*             - added getaccountforcurrency()
*  3 Jun 2017 - updated convertsharesascash()
*  4 Jun 2017 - updated applybenefits()
*  8 Jun 2017 - added getpostingdetails() and updated getpostingdescription(), getpostingsbydate(), scriptgetstatement() & scriptsgethistory()
*  9 Jun 2017 - removed getcaclientoption()
*             - amended getcaclientdecision() to use accountid
*             - amended applybenefits() to use getcaclientdecision()
*             - added valuebenefit(), valuebenefitascash() valuebenefitasshares()
* 10 Jun 2017 - added updatebenefit(), updatebenefitascash(), updatebenefitasshares()
*             - modified applybenefits() to use separate value & update routines
* 12 Jun 2017 - tidied caapply()
* 23 Jun 2017 - operatortimestamp removed - newtrade()
*             - localtimestamp removed - scriptedittrade(), scriptdeletetrade(), deletetrade(), newtradetransaction(), deletetradetransaction(), newcollectaggregateinvest(), newtransaction() and newposting()
* 04 Jul 2017 - added electedquantityasshares to getsharesdue() to cope with client elected quantity
*             - updated valuebenefitascash() to take account of electedquantityasshares
*             - added decision parameter to valuebenefit(), valuebenefitasshares(), valuebenefitascash()
**********************/
var utils = require('./utils.js');

utils.utils();

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
  exports.priceserverchannel_int = 12;
  exports.errorchannel = 13;
  exports.orderbookchannel = 14;
  exports.systemmonitorchannel = 15;

  exports.holidays = [];
  /*** Functions ***/

  round = '\
  local round = function(num, dp) \
    return string.format("%0." .. dp .. "f", num) \
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
      desc = "No closing price for symbol found as at ex date";
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
    case 1042:
      desc = "Insufficient scheme cash";
      break;
    case 1043:
      desc = "No orders found";
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
    local temp = {} \
    temp.vals = {} \
    temp.rawvals = redis.call("hgetall", key) \
    for index = 1, #temp.rawvals, 2 do \
      temp.vals[temp.rawvals[index]] = temp.rawvals[index + 1] \
    end \
    return temp.vals \
  end \
  ';

  exports.gethashvalues = gethashvalues;

  /*
  * getsettlementdetails()
  * params: KEYS[1] = symbolid
  * function returns exchangeid and defaultsettledays
  */
  getsettlementdetails = '\
    local defaultsettledays = redis.call("hget", "config", "defaultsettledays") \
    local exchangeid = redis.call("hget", "symbol:" .. KEYS[1], "exchangeid") \
    return {exchangeid, defaultsettledays} \
  ';

  exports.getsettlementdetails = getsettlementdetails;

 /*
  * getdefaultsettletype()
  * gets a default settlement type based on the default number of settlement days
  * returns: default settlement type
  * note: using a 'standard' settlement type of 0 does not always return a settlement date from the market via NBTrader FIX 4.2,
  *       depending on the market-maker, so this function attempts to set the default type
  */
  getdefaultsettletype = '\
  local getdefaultsettletype = function() \
    local defaultsettledays = redis.call("hget", "config", "defaultsettledays") \
    return tonumber(defaultsettledays) + 1 \
  end \
  ';

  exports.getdefaultsettletype = getdefaultsettletype;
  /*
  * isExchangeClosed()
  * checks whether a date is a holiday
  * params: date object, exchangeid
  * returns: true if the date is a holiday else false
  */
  function isExchangeClosed(datetocheck, exchangeid, cb) {
    var datetocheckstr = getUTCDateString(datetocheck);
    db.sismember('exchangesclosed:'+ exchangeid, datetocheckstr, function(err, isExchangeClosed) {
      if (err) {
        console.log("isExchangeClosed", err);
        return cb(err);
      }
      if (isExchangeClosed) {
        cb(null, true);
      } else {
        cb(null, false);
      }
    });
  }

 /*
  * calculateSettDate()
  * returns valid trading day as date object, taking into account weekends and holidays
  * params: date object, number of settlement days
  * returns: settlement date object
  * note: this is in javascript as the lua date library is not available from redis
  */
  function calculateSettDate(dt, exchangeid, nosettdays, cb) {
    var days = 0;
    if (nosettdays > 0) {
      getSettlmentDate();
    } else {
      cb(null, dt);
    }
    function getSettlmentDate() {
      // add a day
      dt.setDate(dt.getDate() + 1);
      // promise used to hold the process until isExchangeClosed is fetched
      var promise = new Promise(function(resolve, reject) {
        if (dt.getDay() == 6 || dt.getDay() == 0) {
          reject();
        } else {
          isExchangeClosed(dt, exchangeid, function(err, isExchangeClosed) {
            if (err) {
              reject();
            } else {
              if (isExchangeClosed) {
                reject();
              } else {
                resolve();
              }
            }
          });
        }
      });
      promise.then(function(isExchangeClosed) {
        // add to days & check to see if we are there
        days++;
        if (days >= nosettdays) {
          cb(null, dt);
        } else {
          getSettlmentDate();
        }
      }).catch(function(err) {
        getSettlmentDate();
      });
    }
  }

  exports.calculateSettDate = calculateSettDate;

 /*
  * getSettDate()
  * returns valid trading day as date object, taking into account weekends and holidays
  * params: date object, symbolid
  * returns: settlement date object
  */
  function getSettDate(tradedate, symbolid, cb) {
    var days = 0;
    db.eval(getsettlementdetails, 1, symbolid, function(err, ret) {
      if (err) {
        console.log('getSettDate', err);
      } else {
        calculateSettDate(tradedate, ret[0], parseInt(ret[1]), cb);
      }
    });
  }

  exports.getSettDate = getSettDate;

  /*
   * isValidTradingDay()
   * params: date object, exchangeid
   * returns true if the date is valid trading day else false
  */
  function isValidTradingDay(datetocheck, exchangeid, cb) {
    // Promise is used for carrying out synchronous operation
    var promise = new Promise(function(resolve, reject) {
      if (datetocheck.getDay() === 6 || datetocheck.getDay() === 0) {
        resolve(false);
      } else if (true) {
        isExchangeClosed(datetocheck, exchangeid, function(err, isExchangeClosed) {
          if (err) {
            // In error condition, the function returns false
            resolve(false);
          } else {
            if (isExchangeClosed) {
              resolve(false);
            } else {
              resolve(true);
            }
          }
        });
      }
    });
    promise.then(function(isValidTradingDay) {
      cb(null, isValidTradingDay);
    });
  }

  exports.isValidTradingDay = isValidTradingDay;

  /*
   * getPreviousTradingDay()
   * params: date object, exchangeid
   * returns previous valid trading date object
   */
  function getPreviousTradingDay(datetocheck, exchangeid, cb) {
    // getTradindDay is self-invoking function
    (function getTradindDay() {
      datetocheck.setDate(datetocheck.getDate() - 1);
      isValidTradingDay(datetocheck, exchangeid, function(err, isValidDate) {
        if (err) {
          console.log('Error in getting previous trading day');
          cb(null, datetocheck);
        } else if (isValidDate) {
          cb(null, datetocheck);
        } else {
          getTradindDay();
        }
      });
    })();
  }

  exports.getPreviousTradingDay = getPreviousTradingDay;

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
    local temp = {} \
    temp.symbolkey = symbolid \
    --[[ add settlement date to symbol key for devivs ]] \
    if futsettdate ~= "" then \
      temp.instrumenttypeid = redis.call("hget", "symbol:" .. symbolid, "instrumenttypeid") \
      if temp.instrumenttypeid == "CFD" or temp.instrumenttypeid == "SPD" or temp.instrumenttypeid == "CCFD" then \
        temp.symbolkey = temp.symbolkey .. ":" .. futsettdate \
      end \
    end \
    redis.call("set", "broker:" .. brokerid .. ":account:" .. accountid .. ":symbol:" .. temp.symbolkey, positionid) \
    redis.call("sadd", "broker:" .. brokerid .. ":symbol:" .. temp.symbolkey .. ":positions", positionid) \
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

  exports.rejectorder = rejectorder;

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

  exports.geteodprice = geteodprice;

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
      midprice = redis.call("hget", "symbol:" .. currencyid1 .. currencyid2, "midprice") \
      if not midprice then \
        midprice = 0 \
      end \
    end \
    return midprice \
  end \
  ';

  /*
  * getcurrencyratedate()
  * gets current mid price between two currencies as a date
  * params: currency id 1, currency id 2, eoddate
  * returns: midprice if symbol is found, else 0
  */
  getcurrencyratedate = geteodprice + '\
  local getcurrencyratedate = function(currencyid1, currencyid2, eoddate) \
    local midprice = 1 \
    if currencyid1 ~= currencyid2 then \
      local symbolid = currencyid1 .. currencyid2 \
      midprice = geteodprice(eoddate, symbolid) \
      if not midprice then \
        midprice = 0 \
      end \
    end \
    return midprice \
  end \
  ';

  /* valueposition()
  * calculate the unrealised profit/loss for a position
  * params: quantity, symbolid, valuecurrencyid, cost
  * returns: table as follows:
  * price = price of stock in account currency
  * value = value of positon in account currency
  * unrealisedpandl = unrealised p&l in account currency
  * symbolcurrencyid = currency of symbol
  * symbolcurrencyprice = price in currency of symbol
  * currencyrate - currency rate used to convert price
  */
  valueposition = getcurrencyrate + round + '\
  local valueposition = function(quantity, symbolid, valuecurrencyid, cost) \
    local ret = {} \
    ret.price = 0 \
    ret.value = 0 \
    ret.unrealisedpandl = 0 \
    ret.symbolcurrencyid = redis.call("hget", "symbol:" .. symbolid, "currencyid") \
    ret.symbolcurrencyprice = 0 \
    ret.currencyrate = 1 \
    local qty = tonumber(quantity) \
    if qty > 0 then \
      --[[ position is long, so we would sell, so use the bid price ]] \
      local bidprice = redis.call("hget", "symbol:" .. symbolid, "bid") \
      if bidprice and tonumber(bidprice) ~= 0 then \
        ret.symbolcurrencyprice = tonumber(bidprice) \
        if ret.symbolcurrencyid ~= valuecurrencyid then \
          --[[ get currency rate & adjust price ]] \
          ret.currencyrate = getcurrencyrate(ret.symbolcurrencyid, valuecurrencyid) \
          ret.price = ret.symbolcurrencyprice * ret.currencyrate \
        else \
          ret.price = ret.symbolcurrencyprice \
        end \
        ret.value = qty * ret.price \
        ret.unrealisedpandl = round(ret.value - cost, 2) \
      end \
    elseif qty < 0 then \
      --[[ position is short, so we would buy, so use the ask price ]] \
      local askprice = redis.call("hget", "symbol:" .. symbolid, "ask") \
      if askprice and tonumber(askprice) ~= 0 then \
        ret.symbolcurrencyprice = tonumber(askprice) \
        if ret.symbolcurrencyid ~= valuecurrencyid then \
          --[[ get currency rate & adjust price ]] \
          ret.currencyrate = getcurrencyrate(ret.symbolcurrencyid, valuecurrencyid) \
          ret.price = ret.symbolcurrencyprice * ret.currencyrate \
        else \
          ret.price = ret.symbolcurrencyprice \
        end \
        ret.value = qty * ret.price \
        ret.unrealisedpandl = round(ret.value + cost, 2) \
      end \
    end \
    return ret \
  end \
  ';

  /*
  * valuepositiondate()
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
  valuepositiondate = getcurrencyratedate + round + '\
  local valuepositiondate = function(quantity, symbolid, valuecurrencyid, cost, valuedate) \
    local ret = {} \
    ret.price = 0 \
    ret.value = 0 \
    ret.unrealisedpandl = 0 \
    ret.symbolcurrencyid = redis.call("hget", "symbol:" .. symbolid, "currencyid") \
    ret.symbolcurrencyprice = 0 \
    ret.currencyrate = 1 \
    --[[ get the end of day price for the valuedate ]] \
    local eodprice = geteodprice(valuedate, symbolid) \
    local qty = tonumber(quantity) \
    if qty > 0 then \
      --[[ position is long, so we would sell, so use the bid price ]] \
      if eodprice.bid and tonumber(eodprice.bid) ~= 0 then \
        ret.symbolcurrencyprice = tonumber(eodprice.bid) \
        if ret.symbolcurrencyid ~= valuecurrencyid then \
          --[[ get currency rate at value date & adjust price ]] \
          ret.currencyrate = getcurrencyratedate(ret.symbolcurrencyid, valuecurrencyid, valuedate) \
          ret.price = ret.symbolcurrencyprice * ret.currencyrate \
        else \
          ret.price = ret.symbolcurrencyprice \
        end \
        ret.value = qty * ret.price \
        ret.unrealisedpandl = round(ret.value - cost, 2) \
      end \
    elseif qty < 0 then \
      --[[ position is short, so we would buy, so use the ask price ]] \
      if eodprice.ask and tonumber(eodprice.ask) ~= 0 then \
        ret.symbolcurrencyprice = tonumber(eodprice.ask) \
        if ret.symbolcurrencyid ~= valuecurrencyid then \
          --[[ get currency rate at value date & adjust price ]] \
          ret.currencyrate = getcurrencyratedate(ret.symbolcurrencyid, valuecurrencyid, valuedate) \
          ret.price = ret.symbolcurrencyprice * ret.currencyrate \
        else \
          ret.price = ret.symbolcurrencyprice \
        end \
        ret.value = qty * ret.price \
        ret.unrealisedpandl = round(ret.value + cost, 2) \
      end \
    end \
    return ret \
  end \
  ';

  /*
  * getunrealisedpandl()
  * calculate the unrealised profit/loss for a position
  * params: position
  * returns: see valueposition()
  */
  getunrealisedpandl = valueposition + '\
  local getunrealisedpandl = function(position) \
    --[[ get the account currency as the symbol may be priced in a different currency ]] \
    local accountcurrencyid = redis.call("hget", "broker:" .. position.brokerid .. ":account:" .. position["accountid"], "currencyid") \
    local ret = valueposition(position.quantity, position.symbolid, accountcurrencyid, position.cost) \
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
  getunrealisedpandlvaluedate = valuepositiondate + '\
  local getunrealisedpandlvaluedate = function(position, valuedate) \
    redis.log(redis.LOG_NOTICE, "getunrealisedpandlvaluedate") \
    --[[ get the account currency & symbol currency as may be different ]] \
    local accountcurrencyid = redis.call("hget", "broker:" .. position.brokerid .. ":account:" .. position.accountid, "currencyid") \
    local ret = valuepositiondate(position.quantity, position.symbolid, accountcurrencyid, position.cost, valuedate) \
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
    local marginpercent = redis.call("hget", "broker:" .. brokerid .. ":brokersymbol:" .. symbolid, "marginpercent") \
    if not marginpercent then marginpercent = 100 end \
    local initialmargin = tonumber(consid) * tonumber(marginpercent) / 100 \
    return initialmargin \
  end \
  ';
  exports.getinitialmargin = getinitialmargin;

  /*
  * getaccountbalance()
  * params: accountid, brokerid
  * returns: cleared & local currency cleared balances
  */
  getaccountbalance = '\
  local getaccountbalance = function(accountid, brokerid) \
    return redis.call("hmget", "broker:" .. brokerid .. ":account:" .. accountid, "balance", "localbalance") \
  end \
  ';

  /*
  * getaccountbalanceuncleared()
  * params: accountid, brokerid
  * returns: uncleared & local currency uncleared balances
  */
  getaccountbalanceuncleared = '\
  local getaccountbalanceuncleared = function(accountid, brokerid) \
    return redis.call("hmget", "broker:" .. brokerid .. ":account:" .. accountid, "balanceuncleared", "localbalanceuncleared") \
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
  updateaccountbalance = round + getaccountbalance + '\
  local updateaccountbalance = function(accountid, amount, brokerid, localamount) \
    local temp = {} \
    temp.vals = getaccountbalance(accountid, brokerid) \
    if not temp.vals[1] then return end \
    temp.balance = round(tonumber(temp.vals[1]) + tonumber(amount), 2) \
    temp.localbalance = round(tonumber(temp.vals[2]) + tonumber(localamount), 2) \
    redis.call("hmset", "broker:" .. brokerid .. ":account:" .. accountid, "balance", tostring(temp.balance), "localbalance", tostring(temp.localbalance)) \
  end \
  ';

  /*
  * updateaccountbalanceuncleared()
  * updates uncleared account balance & local currency balance
  * note: amount & localamount can be -ve
  */
  updateaccountbalanceuncleared = getaccountbalanceuncleared + round + '\
  local updateaccountbalanceuncleared = function(accountid, amount, brokerid, localamount) \
    local temp = {} \
    temp.vals = getaccountbalanceuncleared(accountid, brokerid) \
    if not temp.vals[1] then return end \
    temp.balanceuncleared = round((tonumber(temp.vals[1]) + tonumber(amount)), 2) \
    temp.localbalanceuncleared = round((tonumber(temp.vals[2]) + tonumber(localamount)), 2) \
    redis.call("hmset", "broker:" .. brokerid .. ":account:" .. accountid, "balanceuncleared", tostring(temp.balanceuncleared), "localbalanceuncleared", tostring(temp.localbalanceuncleared)) \
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
    local updatetradesettlestatusindex = function(brokerid, tradeid, oldtradesettlestatusid, newtradesettlestatusid) \
      if oldtradesettlestatusid ~= "" then \
        redis.call("zrem", "trade:tradesettlestatus", 0, oldtradesettlestatusid .. ":" .. brokerid .. ":" .. tradeid) \
      end \
      redis.call("zadd", "trade:tradesettlestatus", 0, newtradesettlestatusid .. ":" .. brokerid .. ":" .. tradeid) \
    end \
  ';

  exports.updatetradesettlestatusindex = updatetradesettlestatusindex + '\
    updatetradesettlestatusindex(ARGV[1], ARGV[2], ARGV[3], ARGV[4]) \
    return 0 \
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
    local gettradesbysettlementstatus = function(mintradesettlementstatusid, brokerid) \
      local tbltrades = {} \
      redis.log(redis.LOG_NOTICE, "gettradesbysettlementstatus") \
      --[[ get the character beyond the maximum requested to mark the end of the search ]] \
      --[[ local nextchar = string.char(string.byte(maxtradesettlementstatusid) + 1) ]] \
      --[[ calculate the next brokerid to the currently requested broker(id) to mark the end of the search ]] \
      local nextbroker = tonumber(brokerid) + 1 \
      local trades = redis.call("zrangebylex", "trade:tradesettlestatus", "[" .. mintradesettlementstatusid .. ":" .. brokerid, "(" .. mintradesettlementstatusid .. ":" .. nextbroker) \
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
  * params: accountid, amount, brokerid, localamount, transactionid, timestamp in milliseconds, timestamp, linktype, linkid
  */
  newposting = '\
  local newposting = function(accountid, amount, brokerid, localamount, note, transactionid, timestampms, timestamp, linktype, linkid) \
    redis.log(redis.LOG_NOTICE, "newposting") \
    local temp = {} \
    temp.brokerkey = "broker:" .. brokerid \
    temp.postingid = redis.call("hincrby", temp.brokerkey, "lastpostingid", 1) \
    temp.stramount = round(amount, 2) \
    temp.strlocalamount = round(localamount, 2) \
    redis.call("hmset", temp.brokerkey .. ":posting:" .. temp.postingid, "accountid", accountid, "brokerid", brokerid, "amount", temp.stramount, "localamount", temp.strlocalamount, "postingid", temp.postingid, "note", note, "transactionid", transactionid) \
    redis.call("sadd", temp.brokerkey .. ":transaction:" .. transactionid .. ":postings", temp.postingid) \
    redis.call("sadd", temp.brokerkey .. ":account:" .. accountid .. ":postings", temp.postingid) \
    --[[ add a sorted set for time based queries by account ]] \
    redis.call("zadd", temp.brokerkey .. ":account:" .. accountid .. ":postingsbydate", timestampms, temp.postingid) \
    redis.call("sadd", temp.brokerkey .. ":postings", temp.postingid) \
    createaddtionalindex(brokerid, "postings", temp.postingid, linktype, linkid) \
    redis.call("sadd", temp.brokerkey .. ":postingid", "posting:" .. temp.postingid) \
    temp.searchvalue = temp.postingid .. ":" .. transactionid .. ":" .. tostring(temp.strlocalamount) .. ":" .. tostring(temp.stramount) .. ":" .. lowercase(trim(gettimestampindex(timestamp))) \
    --[[ redis.call("zadd", temp.brokerkey .. ":postings:search_index", temp.postingid, temp.searchvalue) ]] \
    createsearchindex(brokerid, "postings:search_index", temp.postingid, temp.searchvalue, linktype, linkid) \
    --[[ add sorted sets for columns that require sorting capability ]] \
    --[[ temp.indexamount = tonumber(temp.stramount) * 100 ]] \
    --[[ temp.fieldscorekeys = {"amount", indexamount, temp.postingid, "accountid", accountid, temp.postingid} ]] \
    --[[ updatefieldindexes(brokerid, "posting", fieldscorekeys) ]] \
    --[[ if linktype and linkid then ]] \
      --[[ updateaddtionalindex(brokerid, "postings", temp.postingid, linktype, linkid, "posting:search_index", temp.searchvalue) ]] \
    --[[ end ]] \
    return temp.postingid \
  end \
  ';
 /*
  * newtransaction()
  * creates a transaction record
  * params: amount, brokerid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid, timestamp in milliseconds, linktype, linkid
  */
  newtransaction = utils.lowercase + utils.trim + utils.replace + utils.getindexkey + utils.gettimestampindex + utils.createaddtionalindex + utils.createsearchindex + '\
  local newtransaction = function(amount, brokerid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid, timestampms, linktype, linkid) \
    redis.log(redis.LOG_NOTICE, "newtransaction") \
    local temp = {} \
    temp.transactionid = redis.call("hincrby", "broker:" .. brokerid, "lasttransactionid", 1) \
    temp.brokerkey = "broker:" .. brokerid \
    temp.stramount = round(amount, 2) \
    temp.strlocalamount = round(localamount, 2) \
    redis.call("hmset", temp.brokerkey .. ":transaction:" .. temp.transactionid, "amount", temp.stramount, "brokerid", brokerid, "currencyid", currencyid, "localamount", temp.strlocalamount, "note", note, "rate", rate, "reference", reference, "timestamp", timestamp, "transactiontypeid", transactiontypeid, "transactionid", temp.transactionid) \
    redis.call("sadd", temp.brokerkey .. ":transactions", temp.transactionid) \
    createaddtionalindex(brokerid, "transactions", temp.transactionid, linktype, linkid) \
    redis.call("sadd", temp.brokerkey .. ":transactionid", "transaction:" .. temp.transactionid) \
    --[[ the following index is used for table search using localamount, amount, currencyid and etc.. ]] \
    temp.searchvalue = temp.transactionid .. ":" .. lowercase(trim(transactiontypeid)) .. ":" .. lowercase(trim(currencyid)) .. ":" .. tostring(temp.strlocalamount) .. ":" .. tostring(temp.stramount) .. ":" .. lowercase(trim(replace(reference, ":", "-"))) .. ":" .. lowercase(trim(note) .. ":" .. lowercase(trim(gettimestampindex(timestamp)))) \
    --[[ redis.call("zadd", temp.brokerkey .. ":transaction:search_index", temp.transactionid, temp.searchvalue) ]] \
    createsearchindex(brokerid, "transaction:search_index", temp.transactionid, temp.searchvalue, linktype, linkid) \
    --[[ add sorted sets for columns that require sorting capability ]] \
    --[[ temp.indexamount = tonumber(temp.stramount) * 100 ]] \
    --[[ temp.fieldscorekeys = {"amount", indexamount, temp.transactionid, "timestamp", timestampms, temp.transactionid, "transactiontypeid", 0, transactiontypeid .. ":" .. temp.transactionid} ]] \
    --[[ updatefieldindexes(brokerid, "transaction", fieldscorekeys) ]] \
    --[[ if linktype and linkid then ]] \
     --[[ updateaddtionalindex(brokerid, "transactions", temp.transactionid, linktype, linkid, "transaction:search_index", temp.searchvalue) ]] \
    --[[ end ]] \
    return temp.transactionid \
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
  * getbrokerlist()
  * get all brokers id
  */
  getbrokerlist = '\
  local getbrokerlist = function() \
    return redis.call("smembers", "brokers") \
  end \
  ';
  exports.getbrokerlist = getbrokerlist + '\
    return getbrokerlist() \
  ';

  /*
  * gettradedescription()
  * get a description of a trade for a client statement
  * params: trade reference i.e. broker:1:trade:1
  * returns: a description of the trade or empty string if not found
  */
  gettradedescription = gethashvalues + utils.getdateindex + '\
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
      desc = desc .. tonumber(trade["quantity"]) .. " " .. symbol["shortname"] .. " @ " .. trade["price"] .. " to settle " .. getdateindex(trade["futsettdate"]) \
    end \
    return desc \
  end \
  ';
  /*
   * getpostingdescription
   * This script used to get posting description
   * params: brokerid, transactionid
   */
   getpostingdescription = utils.stringmatch + gettradedescription + '\
     local getpostingdescription = function(brokerid, transactionid, postingnote) \
       redis.log(redis.LOG_NOTICE, "getpostingdescription") \
       local transaction = redis.call("hmget", "broker:" .. brokerid .. ":transaction:" .. transactionid, "reference", "transactiontypeid", "note") \
       if stringmatch(transaction[1], "trade:") and transaction[2] ~= "TRR" and transaction[2] ~= "TPR" then \
         return gettradedescription("broker:" .. ARGV[2] .. ":" .. transaction[1]) \
       elseif transaction[2] == "JE" then \
         return postingnote \
       else \
         return transaction[3] \
       end \
     end \
   ';
  /*
   * getpostingdetails
   * returns posting and transaction details
   * params: brokerid, postingid
   * returns: posting detail
   */
  getpostingdetails = gethashvalues + gettransaction + getpostingdescription + '\
    local getpostingdetails = function(brokerid, postingid) \
      --[[ get the posting ]] \
      local posting = gethashvalues("broker:" .. brokerid .. ":posting:" .. postingid) \
      --[[ get additional details from the transaction ]] \
      local transaction = gettransaction(brokerid, posting.transactionid) \
      posting.description = getpostingdescription(brokerid, posting.transactionid, posting.note) \
      posting.reference = transaction.reference \
      posting.timestamp = transaction.timestamp \
      posting.transactiontypeid = transaction.transactiontypeid \
      return posting \
    end \
  ';
  /*
  * getpostingsbydate()
  * gets postings for an account between two dates, sorted by datetime
  * params: accountid, brokerid, start of period, end of period - (datetimes expressed in milliseconds) and view
  * returns: array of postings
  */
  getpostingsbydate = getpostingdetails + '\
  local getpostingsbydate = function(accountid, brokerid, startmilliseconds, endmilliseconds, view) \
    redis.log(redis.LOG_NOTICE, "getpostingsbydate") \
    local tblpostings = {} \
    local posting = {} \
    local brokerkey = "broker:" .. brokerid \
    local postings = redis.call("zrangebyscore", brokerkey .. ":account:" .. accountid .. ":postingsbydate", startmilliseconds, endmilliseconds) \
    local hiddenpostings = redis.call("sort", brokerkey .. ":account:" .. accountid .. ":hidepostings") \
    if view == "client" then \
      postings = filter(postings, hiddenpostings) \
    end \
    for i = 1, #postings do \
      posting = getpostingdetails(brokerid, postings[i]) \
      if view == "user" then \
        if find(hiddenpostings, postings[i]) == true then \
          posting.hide = 1 \
        else \
          posting.hide = 0 \
        end \
      end \
      table.insert(tblpostings, posting) \
    end \
    return tblpostings \
  end \
  ';

  /*
  * getclientidfromaccount()
  * get a clientid from an accountid
  * params: accountid, brokerid
  * returns: clientid
  */
  getclientidfromaccount = '\
  local getclientidfromaccount = function(accountid, brokerid) \
    local clientid = redis.call("get", "broker:" .. brokerid .. ":account:" .. accountid .. ":client") \
    return clientid \
  end \
  ';

  /*
  * getclientfromaccount()
  * get a client from an accountid
  * params: accountid, brokerid
  * returns: client
  */
  getclientfromaccount = '\
  local getclientfromaccount = function(accountid, brokerid) \
    local clientid = redis.call("get", "broker:" .. brokerid .. ":account:" .. accountid .. ":client") \
    local client = gethashvalues("broker:" .. brokerid .. ":client:" .. clientid) \
    return client \
  end \
  ';

  /*
  * addunclearedcashlistitem()
  * add an item to the uncleared cash list
  */
  addunclearedcashlistitem = utils.getclientnamebyaccount + utils.lowercase + utils.trim + '\
  local addunclearedcashlistitem = function(brokerid, clearancedate, clientaccountid, transactionid, timestamp, timestampms, paymenttypeid, currencyid, linktype, linkid) \
    redis.log(redis.LOG_NOTICE, "addunclearedcashlistitem") \
    local brokerkey = "broker:" .. brokerid \
    local unclearedcashlistid = redis.call("hincrby", brokerkey, "lastunclearedcashlistid", 1) \
    local clientdetails = getclientnamebyaccount(brokerid, clientaccountid) \
    redis.call("hmset", brokerkey .. ":unclearedcashlist:" .. unclearedcashlistid, "clientaccountid", clientaccountid, "brokerid", brokerid, "clearancedate", clearancedate, "transactionid", transactionid, "unclearedcashlistid", unclearedcashlistid, "currencyid", currencyid, "paymenttypeid", paymenttypeid, "timestamp", timestamp) \
    redis.call("sadd", brokerkey .. ":unclearedcashlist", unclearedcashlistid) \
    createaddtionalindex(brokerid, "unclearedcashlist", unclearedcashlistid, linktype, linkid) \
    redis.call("sadd", brokerkey .. ":unclearedcashlistid", "unclearedcashlist:" .. unclearedcashlistid) \
    redis.call("zadd", brokerkey .. ":account:" .. clientaccountid .. ":unclearedcashbreakdown", timestampms, "unclearedcash:" .. unclearedcashlistid) \
    --[[ redis.call("zadd", brokerkey .. ":unclearedcashlist:unclearedcashlistbydate", clearancedate, unclearedcashlistid) ]] \
    createsearchindex(brokerid, "unclearedcashlist:unclearedcashlistbydate", clearancedate, unclearedcashlistid, linktype, linkid) \
    --[[ redis.call("zadd", brokerkey .. ":unclearedcashlist:search_index", unclearedcashlistid, lowercase(trim(clientdetails[1])).. ":" .. unclearedcashlistid) ]] \
    local searchvalue = lowercase(trim(clientdetails[1])).. ":" .. unclearedcashlistid \
    createsearchindex(brokerid, "unclearedcashlist:search_index", unclearedcashlistid, searchvalue, linktype, linkid) \
    return {0, unclearedcashlistid} \
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
      position["isin"] = redis.call("hget", "symbol:" .. position["symbolid"], "isin") \
    end \
    return position \
  end \
  ';

  /*
  * publishposition()
  * publish a position
  * here 10 refer to the positionchannel
  * params: brokerid, positionid
  */
  publishposition = getposition + '\
  local publishposition = function(brokerid, positionid) \
    redis.call("publish", 10, "{" .. cjson.encode("position") .. ":" .. cjson.encode(getposition(brokerid, positionid)) .. "}") \
  end \
  ';

  /*
  * newposition()
  * create a new position
  * params: accountid, brokerid, cost, futsettdate, quantity, symbolid
  * returns: positionid
  */
  newposition = setsymbolkey + publishposition + utils.getparentlinkdetails + utils.updateaddtionalindex + utils.getclientnamebyaccount + trim + lowercase + '\
  local newposition = function(accountid, brokerid, cost, futsettdate, quantity, symbolid) \
    redis.log(redis.LOG_NOTICE, "newposition") \
    local fields = {} \
    fields.linkdetails = getparentlinkdetails(brokerid, accountid) \
    fields.positionid = redis.call("hincrby", "broker:" .. brokerid, "lastpositionid", 1) \
    redis.call("hmset", "broker:" .. brokerid .. ":position:" .. fields.positionid, "brokerid", brokerid, "accountid", accountid, "symbolid", symbolid, "quantity", quantity, "cost", tostring(cost), "positionid", fields.positionid, "futsettdate", futsettdate) \
    setsymbolkey(accountid, brokerid, futsettdate, fields.positionid, symbolid) \
    redis.call("sadd", "broker:" .. brokerid .. ":positions", fields.positionid) \
    redis.call("sadd", "broker:" .. brokerid .. ":account:" .. accountid .. ":positions", fields.positionid) \
    fields.client = getclientnamebyaccount(brokerid, accountid) \
    redis.call("zadd", "broker:" .. brokerid .. ":position:search_index", fields.positionid, fields.client[2] .. ":" .. accountid .. ":" .. lowercase(trim(fields.client[1])) .. ":" .. lowercase(trim(symbolid))) \
    if tonumber(quantity) ~= 0 then \
      redis.call("sadd", "broker:" .. brokerid .. ":account:" .. accountid .. ":nonzeropositions", fields.positionid) \
      redis.call("sadd", "broker:" .. brokerid .. ":position:nonzerosymbols", symbolid) \
    end \
    redis.call("sadd", "broker:" .. brokerid .. ":positionid", "position:" .. fields.positionid) \
    updateaddtionalindex(brokerid, "positions", fields.positionid, fields.linkdetails[1], fields.linkdetails[2], "", "") \
    publishposition(brokerid, fields.positionid) \
    return fields.positionid \
  end \
  ';

  exports.getposition = getposition;

  /*
  * updateposition()
  * update an existing position
  * quantity & cost can be +ve/-ve
  */
  updateposition = round + '\
  local updateposition = function(accountid, brokerid, cost, futsettdate, positionid, quantity, symbolid) \
    redis.log(redis.LOG_NOTICE, "updateposition") \
    local temp = {} \
    temp.positionkey = "broker:" .. brokerid .. ":position:" .. positionid \
    temp.position = gethashvalues(temp.positionkey) \
    temp.newquantity = tonumber(temp.position["quantity"]) + tonumber(quantity) \
    if temp.newquantity == 0 then \
      temp.newcost = 0 \
      --[[ when quantity becomes zero, need to remove from nonzeropositions ]] \
      redis.call("srem", "broker:" .. brokerid .. ":account:" .. accountid .. ":nonzeropositions", positionid) \
    elseif tonumber(quantity) > 0 then \
      --[[ when quantity greater than zero, need to add in nonzeropositions ]] \
      redis.call("sadd", "broker:" .. brokerid .. ":account:" .. accountid .. ":nonzeropositions", positionid) \
      temp.newcost = tonumber(temp.position["cost"]) + tonumber(cost) \
    else \
      temp.newcost = round(temp.newquantity / tonumber(temp.position["quantity"]) * tonumber(temp.position["cost"]), 2) \
    end \
    redis.call("hmset", temp.positionkey, "accountid", accountid, "symbolid", symbolid, "quantity", temp.newquantity, "cost", tostring(temp.newcost), "futsettdate", futsettdate) \
    if symbolid ~= temp.position["symbolid"] or (futsettdate ~= "" and futsettdate ~= temp.position["futsettdate"]) then \
      setsymbolkey(accountid, brokerid, futsettdate, positionid, symbolid) \
    end \
    publishposition(brokerid, positionid) \
  end \
  ';

  /*
  * newpositionposting()
  * creates a positionposting for a position
  * **dependency function declared in main function
  */
  newpositionposting = '\
  local newpositionposting = function(brokerid, cost, linkid, positionid, positionpostingtypeid, quantity, timestamp, milliseconds) \
    redis.log(redis.LOG_NOTICE, "newpositionposting") \
    local temp = {} \
    temp.brokerkey = "broker:" .. brokerid \
    temp.accountid = redis.call("hget", temp.brokerkey .. ":position:" .. positionid, "accountid") \
    temp.linkdetails = getparentlinkdetails(brokerid, temp.accountid) \
    temp.positionpostingid = redis.call("hincrby", temp.brokerkey, "lastpositionpostingid", 1) \
    redis.call("hmset", temp.brokerkey .. ":positionposting:" .. temp.positionpostingid, "brokerid", brokerid, "cost", tostring(cost), "linkid", linkid, "positionid", positionid, "positionpostingid", temp.positionpostingid, "positionpostingtypeid", positionpostingtypeid, "quantity", quantity, "timestamp", timestamp) \
    redis.call("sadd", temp.brokerkey .. ":position:" .. positionid .. ":positionpostings", temp.positionpostingid) \
    redis.call("zadd", temp.brokerkey .. ":position:" .. positionid .. ":positionpostingsbydate", milliseconds, temp.positionpostingid) \
    redis.call("sadd", temp.brokerkey .. ":positionpostings", temp.positionpostingid) \
    redis.call("sadd", temp.brokerkey .. ":positionpostingid", "positionposting:" .. temp.positionpostingid) \
    updateaddtionalindex(brokerid, "positionpostings", temp.positionpostingid, temp.linkdetails[1], temp.linkdetails[2], "", "") \
    return temp.positionpostingid \
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
      positionposting["quantity"] = tonumber(positionposting["quantity"]) \
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
    return redis.call("get", "broker:" .. brokerid .. ":account:" .. accountid .. ":symbol:" .. getsymbolkey(futsettdate, symbolid)) \
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
    local positionid = getpositionid(accountid, brokerid, symbolid, futsettdate) \
    if positionid then \
      return getposition(brokerid, positionid) \
    else \
      return \
    end \
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
      vals["quantity"] = tonumber(vals["quantity"]) \
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
  getpositionvalues = getpositionvalue + utils.getsymbolshortname + '\
  local getpositionvalues = function(accountid, brokerid) \
    redis.log(redis.LOG_NOTICE, "getpositionvalues") \
    local tblresults = {} \
    local positionids = redis.call("smembers", "broker:" .. brokerid .. ":account:" .. accountid .. ":positions") \
    for index = 1, #positionids do \
      local vals = getpositionvalue(brokerid, positionids[index]) \
      vals["quantity"] = tonumber(vals["quantity"]) \
      local symbolshortname = getsymbolshortname(vals["symbolid"]) \
      vals["symbolshortname"] = symbolshortname[1] \
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
    if #positionpostings > 0 then \
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
  getpositionvaluebydate = getpositionbydate + getunrealisedpandlvaluedate + gethashvalues + '\
  local getpositionvaluebydate = function(brokerid, positionid, valuedate, valuedatems) \
    redis.log(redis.LOG_NOTICE, "getpositionvaluebydate") \
    --[[ get the position at the value date]] \
    local position = getpositionbydate(brokerid, positionid, valuedatems) \
    if tonumber(position["quantity"]) > 0 then \
      --[[ value the position ]] \
      local upandl = getunrealisedpandlvaluedate(position, valuedate) \
      position.price = upandl.price \
      position.value = upandl.value \
      position.unrealisedpandl = upandl.unrealisedpandl \
      position.symbolcurrencyid = upandl.symbolcurrencyid \
      position.symbolcurrencyprice = upandl.symbolcurrencyprice \
      position.currencyrate = upandl.currencyrate \
      local symbol = gethashvalues("symbol:" .. position.symbolid) \
      position.isin = symbol.isin \
      position.symbollongname = symbol.longname \
      position.symbolcountrycodeid = symbol.countrycodeid \
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
          if trade["tradeid"] and trade["tradesettlestatusid"] ~= "YYI" then \
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
  getpositionsbysymbol = getposition + utils.getclientnamebyaccount + utils.getaccountname + '\
  local getpositionsbysymbol = function(brokerid, symbolid) \
    redis.log(redis.LOG_NOTICE, "getpositionsbysymbol") \
    local tblresults = {} \
    local positions = redis.call("smembers", "broker:" .. brokerid .. ":symbol:" .. symbolid .. ":positions") \
    for index = 1, #positions do \
      local vals = getposition(brokerid, positions[index]) \
      vals["quantity"] = tonumber(vals["quantity"]) \
      local clientname = getclientnamebyaccount(brokerid, vals["accountid"]) \
      local accountname = getaccountname(brokerid, vals["accountid"]) \
      vals["clientname"] = clientname[1] \
      vals["clientid"] = clientname[2] \
      vals["accountname"] = accountname[2] \
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
  getpositionsbysymbolbydate = getpositionbydate + utils.getclientnamebyaccount + utils.getaccountname + '\
  local getpositionsbysymbolbydate = function(brokerid, symbolid, milliseconds) \
    redis.log(redis.LOG_NOTICE, "getpositionsbysymbolbydate") \
    local tblresults = {} \
    --[[ get position ids ]] \
    local positionids = redis.call("smembers", "broker:" .. brokerid .. ":symbol:" .. symbolid .. ":positions") \
    for i = 1, #positionids do \
      local position = getpositionbydate(brokerid, positionids[i], milliseconds) \
      --[[ only interested in non-zero positions ]] \
      if tonumber(position["quantity"]) ~= 0 then \
        local clientname = getclientnamebyaccount(brokerid, position["accountid"]) \
        local accountname = getaccountname(brokerid, position["accountid"]) \
        position["clientname"] = clientname[1] \
        position["clientid"] = clientname[2] \
        position["accountname"] = accountname[2] \
      end \
      table.insert(tblresults, position) \
    end \
    return tblresults \
  end \
  ';

  /*
  * getpositionvaluesbysymbol()
  * gets all position values for a symbol
  * params: brokerid, symbolid
  * returns: a table of position values
  */
  getpositionvaluesbysymbol = getpositionvalue + utils.getclientnamebyaccount + utils.getaccountname + '\
  local getpositionvaluesbysymbol = function(brokerid, symbolid) \
    redis.log(redis.LOG_NOTICE, "getpositionvaluesbysymbol") \
    local tblresults = {} \
    local positions = redis.call("smembers", "broker:" .. brokerid .. ":symbol:" .. symbolid .. ":positions") \
    for index = 1, #positions do \
      local vals = getpositionvalue(brokerid, positions[index]) \
      if tonumber(vals["quantity"]) > 0 then \
        local clientname = getclientnamebyaccount(brokerid, vals["accountid"]) \
        local accountname = getaccountname(brokerid, vals["accountid"]) \
        vals["clientname"] = clientname[1] \
        vals["clientid"] = clientname[2] \
        vals["accountname"] = accountname[2] \
        table.insert(tblresults, vals) \
      end \
    end \
    return tblresults \
  end \
  ';

  /*
  * getpositionvaluesbysymbolbydate()
  * gets all position values for a symbol as at a date
  * params: brokerid, symbolid, valuedate, date in milliseconds
  * returns: a table of position values
  */
  getpositionvaluesbysymbolbydate = getpositionvaluebydate + utils.getclientnamebyaccount + utils.getaccountname + '\
  local getpositionvaluesbysymbolbydate = function(brokerid, symbolid, valuedate, milliseconds) \
    redis.log(redis.LOG_NOTICE, "getpositionvaluesbysymbolbydate") \
    local tblresults = {} \
    --[[ get position ids ]] \
    local positionids = redis.call("smembers", "broker:" .. brokerid .. ":symbol:" .. symbolid .. ":positions") \
    for i = 1, #positionids do \
      local position = getpositionvaluebydate(brokerid, positionids[i], valuedate, milliseconds) \
      --[[ only interested in non-zero positions ]] \
      if position["quantity"] ~= 0 then \
        local clientname = getclientnamebyaccount(brokerid, position["accountid"]) \
        local accountname = getaccountname(brokerid, position["accountid"]) \
        position["clientname"] = clientname[1] \
        position["clientid"] = clientname[2] \
        position["accountname"] = accountname[2] \
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
 * gettotalpositionvaluedate()
 * gets sum of p&l for all positions for an account as a date
 * params: accountid, brokerid, valuedate, valuedate as milliseconds
 * returns: total cost, total value, total unrealisedpandl as a table
 */
 gettotalpositionvaluedate = getpositionvaluebydate + '\
 local gettotalpositionvaluedate = function(accountid, brokerid, valuedate, valuedatems) \
   redis.log(redis.LOG_NOTICE, "gettotalpositionvaluedate") \
   local totalpositionvalue = {} \
   totalpositionvalue.cost = 0 \
   totalpositionvalue.value = 0 \
   totalpositionvalue.unrealisedpandl = 0 \
   --[[ get the ids for the positions held by this account ]] \
   local positionids = redis.call("smembers", "broker:" .. brokerid .. ":account:" .. accountid .. ":positions") \
   --[[ calculate each position together with value as at the value date ]] \
   for index = 1, #positionids do \
     local position = getpositionvaluebydate(brokerid, positionids[index], valuedate, valuedatems) \
     totalpositionvalue.cost = totalpositionvalue.cost + tonumber(position.cost) \
     totalpositionvalue.value = totalpositionvalue.value + tonumber(position.value) \
     totalpositionvalue.unrealisedpandl = totalpositionvalue.unrealisedpandl + tonumber(position.unrealisedpandl) \
   end \
   return totalpositionvalue \
 end \
 ';

exports.gettotalpositionvaluedate = gettotalpositionvaluedate;

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
  * returns: account cash balances, currency, unrealised p&l, equity, free margin
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
      local currencyid = redis.call("hget", "broker:" .. brokerid .. ":account:" .. accountid , "currencyid") \
      accountsummary["currencyid"] = currencyid \
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
  * getaccountsummarydate()
  * calculates account p&l, margin & equity for a client account at a date
  * params: accountid, brokerid, valuedate, valuedate in milliseconds
  * returns: account cash balances, unrealised p&l, equity, free margin
  */
  getaccountsummarydate = getaccount + gettotalpositionvaluedate + '\
  local getaccountsummarydate = function(accountid, brokerid, valuedate, valuedatems) \
    redis.log(redis.LOG_NOTICE, "getaccountsummarydate") \
    local accountsummary = {} \
    local account = getaccount(accountid, brokerid) \
    if account["balance"] then \
      local totalpositionvalue = gettotalpositionvaluedate(accountid, brokerid, valuedate, valuedatems) \
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
    local broker = {} \
    broker.key = "broker:" .. brokerid \
    broker.accountsmapid = redis.call("get", broker.key .. ":" .. name .. ":" .. currencyid) \
    if broker.accountsmapid then \
      return redis.call("hget", broker.key .. ":brokeraccountsmap:" .. broker.accountsmapid, "accountid") \
    else \
      return 0 \
    end \
  end \
  ';

  exports.getbrokeraccountsmapid = getbrokeraccountsmapid;

 /*
  * getclientaccountid()
  * gets the account of a designated type and currency for a client
  * params: brokerid, clientid, accounttypeid, currencyid
  * returns the account id if found, else 0
  */
  getclientaccountid = getaccount + '\
  local getclientaccountid = function(brokerid, clientid, accounttypeid, currencyid) \
    local clientaccounts = redis.call("smembers", "broker:" .. brokerid .. ":client:" .. clientid .. ":clientaccounts") \
    for i = 1, #clientaccounts do \
      local account = getaccount(clientaccounts[i], brokerid) \
      if tonumber(account.accounttypeid) == tonumber(accounttypeid) and account.currencyid == currencyid then \
        return clientaccounts[i] \
      end \
    end \
    return 0 \
  end \
  ';

  exports.getclientaccountid = getclientaccountid;

  /*
  * getorderfromorderbook()
  * get an order from a brokerid/orderid string key
  * params: orderbook key (i.e. 1:1)
  * returns 0, errorcode, orderid if error else order
  */
  getorderfromorderbook = gethashvalues + '\
  local getorderfromorderbook = function(orderbookkey) \
    local brokerorderid = split(orderbookkey, ":") \
    local order = gethashvalues("broker:" .. brokerorderid[1] .. ":order:" .. brokerorderid[2]) \
    if not order["orderid"] then \
      return {0, 1009, brokerorderid[2]} \
    end \
    order.symbolshortname = redis.call("hget", "symbol:" .. order.symbolid, "shortname") \
    return {1, order} \
  end \
  ';

  /*
  * getorderbook()
  * get the orderbook buy or sell orders for a symbol
  * params: symbolid, side
  * returns: 0, errorcode, orderid if error else 1, table of orders
  */
  getorderbook = getorderfromorderbook + '\
  local getorderbook = function(symbolid, side) \
    local orders = {} \
    local lowerbound \
    local upperbound \
    --[[ buy orders are -ve ]] \
    if tonumber(side) == 1 then \
      lowerbound = "-inf" \
      upperbound = 0 \
    else \
      lowerbound = 0 \
      upperbound = "inf" \
    end \
    --[[ get the orders ]] \
    local symbolshortname = redis.call("hget", "symbol:" .. symbolid, "shortname") \
    local ordersbyside = redis.call("zrangebyscore", "orderbook:" .. symbolid, lowerbound, upperbound) \
    for i = 1, #ordersbyside do \
      local order = getorderfromorderbook(ordersbyside[i]) \
      if order[1] == 0 then \
        return order \
      end \
      order[2].symbolshortname = symbolshortname \
      table.insert(orders, order[2]) \
    end \
    return {1, orders, symbolid, side} \
  end \
  ';

  exports.getorderbook = getorderbook;

  /*
  * getorderbooktop()
  * get the lowest buy or highest sell priced order on the orderbook
  * params: symbolid, side
  * returns: 0, errorcode if error, else 1, order
  */
  getorderbooktop = getorderfromorderbook + '\
  local getorderbooktop = function(symbolid, side) \
    local lowerbound \
    local upperbound \
    --[[ buy orders are -ve ]] \
    if tonumber(side) == 1 then \
      lowerbound = "-inf" \
      upperbound = 0 \
    else \
      lowerbound = 0 \
      upperbound = "inf" \
    end \
    --[[ get the first order ]] \
    local orderid = redis.call("zrangebyscore", "orderbook:" .. symbolid, lowerbound, upperbound, "limit", 0, 1) \
    if #orderid == 0 then \
      return {0, 1043, symbolid, side} \
    end \
    local order = getorderfromorderbook(orderid[1]) \
    order[2].symbolshortname = redis.call("hget", "symbol:" .. symbolid, "shortname") \
    return {order[1], order[2], symbolid, side} \
  end \
  ';

  exports.getorderbooktop = getorderbooktop;

  /*
  * getorderbooktopall()
  * get the lowest buy or highest sell orders on the orderbook for all symbols
  * params: side
  * returns: list of orders
  */
  getorderbooktopall = getorderbooktop + '\
  local getorderbooktopall = function(side) \
    local orders = {} \
    --[[ get the symbols that have orderbooks ]] \
    local orderbooks = redis.call("smembers", "orderbooks") \
    for i = 1, #orderbooks do \
      local order = getorderbooktop(orderbooks[i], side) \
      if order[1] == 1 then \
        table.insert(orders, order[2]) \
      end \
    end \
    return orders \
  end \
  ';

  /*
  * creditcheck()
  * credit checks an order or trade
  * params: accountid, brokerid, orderid, clientid, symbolid, side, quantity, price, settlcurramt, currencyid, futsettdate, totalcost, instrumenttypeid
  * returns: 0=succeed, inialmargin/1=fail, error code
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
            return {0, initialmargin} \
          end \
          if side == 2 and (instrumenttypeid == "DE" or instrumenttypeid == "IE") then \
              --[[ equity, so cannot sell more than we have ]] \
              rejectorder(brokerid, orderid, 0, "Quantity greater than position quantity") \
              return {1, 1019} \
          end \
          --[[ we are trying to close a quantity greater than current position, so need to check we can open a new position ]] \
          local freemargin = getfreemargin(accountid, brokerid) \
          --[[ add the margin returned by closing the position ]] \
          local margin = getmargin(symbolid, math.abs(posqty)) \
          --[[ get initial margin for remaining quantity after closing position ]] \
          initialmargin = getinitialmargin(brokerid, symbolid, (quantity - math.abs(posqty)) * price, totalcost) \
          if initialmargin + totalcost > freemargin + margin then \
            rejectorder(brokerid, orderid, 0, "Insufficient free margin") \
            return {1, 1020} \
          end \
          --[[ closing trade or enough margin to close & open a new position, so ok ]] \
          return {0, initialmargin} \
        end \
      end \
      if instrumenttypeid == "DE" or instrumenttypeid == "IE" then \
        if side == 1 then \
          local account = getaccount(accountid, brokerid) \
          if (tonumber(account["balance"]) + tonumber(account["balanceuncleared"]) + tonumber(account["creditlimit"])) == 0 or (tonumber(account["balance"]) + tonumber(account["balanceuncleared"]) + tonumber(account["creditlimit"])) < settlcurramt + totalcost then \
            rejectorder(brokerid, orderid, 0, "Insufficient funds") \
            return {1, 1041} \
          end \
        else \
          --[[ allow ifa certificated equity sells ]] \
          if instrumenttypeid == "DE" then \
            if redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "clienttypeid") == "3" then \
              return {0, initialmargin} \
            end \
          end \
          --[[ check there is a position ]] \
          if not position then \
            rejectorder(brokerid, orderid, 0, "No position held in this instrument") \
            return {1, 1003} \
          end \
          --[[ check the position is of sufficient size ]] \
          local posqty = tonumber(position["quantity"]) \
          if posqty < 0 or quantity > posqty then \
            rejectorder(brokerid, orderid, 0, "Insufficient position size in this instrument") \
            return {1, 1004} \
          end \
        end \
      elseif instrumenttypeid == "CFD" or instrumenttypeid == "SPB" or instrumenttypeid == "CCFD" then \
        --[[ check free margin for all derivative trades ]] \
        local freemargin = getfreemargin(accountid, brokerid) \
        if initialmargin + totalcost > freemargin then \
          rejectorder(brokerid, orderid, 0, "Insufficient free margin") \
          return {1, 1020} \
        end \
      end \
      return {0, initialmargin} \
    end \
  ';
  exports.creditcheck = creditcheck;
 /*
  * getpostingnote()
  * get a note of a trade for a trade transaction
  * params: brokerid, tradeid, isbrokernote
  * returns: a note of the trade posting
  * currently we are not using this function ***
  */
  getpostingnote = utils.getdateindex + utils.getclientnamebyaccount + utils.firsttoupper + '\
    local getpostingnote = function(brokerid, tradeid, isbrokernote) \
      local temp = {} \
      local trade = {} \
      temp.trade = redis.call("hmget", "broker:" .. brokerid .. ":trade:" .. tradeid, "side", "quantity", "price", "futsettdate", "symbolid", "accountid") \
      trade.side = temp.trade[1] \
      trade.quantity = temp.trade[2] \
      trade.price = temp.trade[3] \
      trade.futsettdate = temp.trade[4] \
      trade.symbolid = temp.trade[5] \
      trade.accountid = temp.trade[6] \
      trade.symbolshortname = redis.call("hget", "symbol:" .. trade.symbolid, "shortname") \
      if trade.accountid then \
        trade.clientname = getclientnamebyaccount(brokerid, trade.accountid) \
      end \
      if tonumber(trade.side) == 1 then \
        if isbrokernote == 1 and trade.clientname then \
          temp.desc = firsttoupper(trade.clientname[1]) .. " A/c: " .. trade.accountid .. "  bought" \
        else \
          temp.desc = "Bought" \
        end \
      else \
        if isbrokernote == 1 and trade.clientname then \
          temp.desc = firsttoupper(trade.clientname[1]) .. " A/c: " .. trade.accountid .. "  sold" \
        else \
          temp.desc = "Sold" \
        end \
      end \
      return temp.desc .. " " .. trade.quantity .. " " .. trade.symbolshortname .. " @ " .. trade.price .. " to settle " .. getdateindex(trade.futsettdate) \
    end \
  ';
  /*
  * newtradetransaction()
  * cash side of a client trade
  * args: consideration, commission, ptmlevy, stampduty, contractcharge, brokerid, clientaccountid, currencyid, rate, timestamp, tradeid, side, timestampms, linktype, linkid
  */
  newtradetransaction = getbrokeraccountsmapid + newtransaction + newposting + getaccountbalance + updateaccountbalanceuncleared + updateaccountbalance + '\
  local newtradetransaction = function(consideration, commission, ptmlevy, stampduty, contractcharge, brokerid, clientaccountid, currencyid, rate, timestamp, tradeid, side, timestampms, linktype, linkid) \
    redis.log(redis.LOG_NOTICE, "newtradetransaction") \
    local fields = {} \
    --[[ get broker accounts ]] \
    fields.considerationaccountid = getbrokeraccountsmapid(brokerid, currencyid, "Stock B/S") \
    fields.commissionaccountid = getbrokeraccountsmapid(brokerid, currencyid, "Commission") \
    fields.ptmaccountid = getbrokeraccountsmapid(brokerid, currencyid, "PTM levy") \
    fields.sdrtaccountid = getbrokeraccountsmapid(brokerid, currencyid, "SDRT") \
    --[[ calculate amounts in broker currency ]] \
    consideration = tonumber(consideration) \
    fields.considerationlocalamount = consideration * rate \
    fields.commissionlocalamount = commission * rate \
    fields.ptmlevylocalamount = ptmlevy * rate \
    fields.transactionid = "" \
    if tonumber(side) == 1 then \
      --[[ buy includes all costs ]] \
      fields.totalamount = consideration + commission + stampduty + ptmlevy \
      --[[ calculate amounts in local currency ]] \
      fields.localamount = fields.totalamount * rate \
      fields.stampdutylocalamount = stampduty * rate \
      fields.note = "Trade receipt" \
      --[[ create new transaction, "" - localtimestamp ]] \
      fields.transactionid = newtransaction(fields.totalamount, brokerid, currencyid, fields.localamount, fields.note, rate, "trade:" .. tradeid, timestamp, "TRC", timestampms, linktype, linkid) \
      --[[ create client account posting - note: update cleared balance ]] \
      newposting(clientaccountid, -fields.totalamount, brokerid, -fields.localamount, "", fields.transactionid, timestampms, timestamp, linktype, linkid) \
      updateaccountbalance(clientaccountid, -fields.totalamount, brokerid, -fields.localamount) \
      --[[ create consideration posting ]] \
      newposting(fields.considerationaccountid, consideration, brokerid, fields.considerationlocalamount, "", fields.transactionid, timestampms, timestamp, linktype, linkid) \
      updateaccountbalance(fields.considerationaccountid, consideration, brokerid, fields.considerationlocalamount) \
      --[[ create commission posting ]] \
      if tonumber(commission) ~= 0 then \
        newposting(fields.commissionaccountid, commission, brokerid, fields.commissionlocalamount, "", fields.transactionid, timestampms, timestamp, linktype, linkid) \
        updateaccountbalance(fields.commissionaccountid, commission, brokerid, fields.commissionlocalamount) \
      end \
      --[[ create ptm levy posting ]] \
      if tonumber(ptmlevy) ~= 0 then \
        newposting(fields.ptmaccountid, ptmlevy, brokerid, fields.ptmlevylocalamount, "", fields.transactionid, timestampms, timestamp, linktype, linkid) \
        updateaccountbalance(fields.ptmaccountid, ptmlevy, brokerid, fields.ptmlevylocalamount) \
      end \
      --[[ create sdrt posting ]] \
      if tonumber(stampduty) ~= 0 then \
        newposting(fields.sdrtaccountid, stampduty, brokerid, fields.stampdutylocalamount, "", fields.transactionid, timestampms, timestamp, linktype, linkid) \
        updateaccountbalance(fields.sdrtaccountid, stampduty, brokerid, fields.stampdutylocalamount) \
      end \
    else \
      --[[ we are selling so only commission & PTM apply ]] \
      fields.totalamount = consideration - commission - ptmlevy \
      fields.localamount = fields.totalamount * rate \
      fields.note = "Trade payment" \
      --[[ create new transaction ]] \
      fields.transactionid = newtransaction(fields.totalamount, brokerid, currencyid, fields.localamount, fields.note, rate, "trade:" .. tradeid, timestamp, "TPC", timestampms, linktype, linkid) \
      --[[ create client account posting - note: update uncleared balance ]] \
      newposting(clientaccountid, fields.totalamount, brokerid, fields.localamount, "", fields.transactionid, timestampms, timestamp, linktype, linkid) \
      updateaccountbalanceuncleared(clientaccountid, fields.totalamount, brokerid, fields.localamount) \
      --[[ create consideration posting ]] \
      newposting(fields.considerationaccountid, -consideration, brokerid, -fields.considerationlocalamount, "", fields.transactionid, timestampms, timestamp, linktype, linkid) \
      updateaccountbalance(fields.considerationaccountid, -consideration, brokerid, -fields.considerationlocalamount) \
      --[[ create commission posting ]] \
      if tonumber(commission) ~= 0 then \
        newposting(fields.commissionaccountid, commission, brokerid, fields.commissionlocalamount, "", fields.transactionid, timestampms, timestamp, linktype, linkid) \
        updateaccountbalance(fields.commissionaccountid, commission, brokerid, fields.commissionlocalamount) \
      end \
      --[[ create ptm levy posting ]] \
      if tonumber(ptmlevy) ~= 0 then \
        newposting(fields.ptmaccountid, ptmlevy, brokerid, fields.ptmlevylocalamount, "", fields.transactionid, timestampms, timestamp, linktype, linkid) \
        updateaccountbalance(fields.ptmaccountid, ptmlevy, brokerid, fields.ptmlevylocalamount) \
      end \
   end \
   --[[ add transactionid index to the trade ]] \
   redis.call("set", "broker:" .. brokerid .. ":trade:" .. tradeid .. ":transactionid", fields.transactionid) \
  end \
  ';

 /*
  * deletetradetransaction()
  * params: consideration, commission, ptmlevy, stampduty, brokerid, clientaccountid, currencyid, rate, timestamp, tradeid, side, timestampms
  * cash side of deleting a client trade
  */
  deletetradetransaction = getbrokeraccountsmapid + newtransaction + newposting + updateaccountbalanceuncleared + updateaccountbalance + utils.getparentlinkdetails + '\
    local deletetradetransaction = function(consideration, commission, ptmlevy, stampduty, brokerid, clientaccountid, currencyid, rate, timestamp, tradeid, side, timestampms) \
      redis.log(redis.LOG_NOTICE, "deletetradetransaction") \
      --[[ get broker accounts ]] \
      local accounts = {} \
      local trade = {} \
      accounts.considerationid = getbrokeraccountsmapid(brokerid, currencyid, "Stock B/S") \
      accounts.commissionid = getbrokeraccountsmapid(brokerid, currencyid, "Commission") \
      accounts.ptmid = getbrokeraccountsmapid(brokerid, currencyid, "PTM levy") \
      accounts.sdrtid = getbrokeraccountsmapid(brokerid, currencyid, "SDRT") \
      --[[ get reverse amounts because we are deleting ]] \
      consideration = -tonumber(consideration) \
      commission = -tonumber(commission) \
      stampduty = -tonumber(stampduty) \
      ptmlevy = -tonumber(ptmlevy) \
      --[[ calculate amounts in broker currency ]] \
      rate = tonumber(rate) \
      trade.considerationlocalamount = consideration * rate \
      trade.commissionlocalamount = commission * rate \
      trade.ptmlevylocalamount = ptmlevy * rate \
      trade.linkdetails = getparentlinkdetails(brokerid, clientaccountid) \
      trade.linktype = tonumber(trade.linkdetails[1]) \
      trade.linkid = tonumber(trade.linkdetails[2]) \
      if tonumber(side) == 1 then \
        --[[ buy includes all costs ]] \
        trade.totalamount = consideration + commission + stampduty + ptmlevy \
        --[[ calculate amounts in local currency ]] \
        trade.localamount = trade.totalamount * rate \
        trade.stampdutylocalamount = stampduty * rate \
        --[[ the transaction ]] \
        trade.transactionid = newtransaction(trade.totalamount, brokerid, currencyid, trade.localamount, "", rate, "trade:" .. tradeid, timestamp, "TRR", timestampms, trade.linktype, trade.linkid) \
        --[[ client account posting - note: update cleared balance ]] \
        newposting(clientaccountid, -trade.totalamount, brokerid, -trade.localamount, "", trade.transactionid, timestampms, timestamp, trade.linktype, trade.linkid) \
        updateaccountbalance(clientaccountid, -trade.totalamount, brokerid, -trade.localamount) \
        --[[ consideration posting ]] \
        newposting(accounts.considerationid, consideration, brokerid, trade.considerationlocalamount, "", trade.transactionid, timestampms, timestamp, trade.linktype, trade.linkid) \
        updateaccountbalance(accounts.considerationid, consideration, brokerid, trade.considerationlocalamount) \
        --[[ commission posting ]] \
        if commission ~= 0 then \
          newposting(accounts.commissionid, commission, brokerid, trade.commissionlocalamount, "", trade.transactionid, timestampms, timestamp, trade.linktype, trade.linkid) \
          updateaccountbalance(accounts.commissionid, commission, brokerid, trade.commissionlocalamount) \
        end \
        --[[ ptm levy posting ]] \
        if ptmlevy ~= 0 then \
          newposting(accounts.ptmid, ptmlevy, brokerid, trade.ptmlevylocalamount, "", trade.transactionid, timestampms, timestamp, trade.linktype, trade.linkid) \
          updateaccountbalance(accounts.ptmid, ptmlevy, brokerid, trade.ptmlevylocalamount) \
        end \
        --[[ sdrt posting ]] \
        if stampduty ~= 0 then \
          newposting(accounts.sdrtid, stampduty, brokerid, trade.stampdutylocalamount, "", trade.transactionid, timestampms, timestamp, trade.linktype, trade.linkid) \
          updateaccountbalance(accounts.sdrtid, stampduty, brokerid, trade.stampdutylocalamount) \
        end \
      else \
        --[[ we are selling so only commission & PTM apply ]] \
        trade.totalamount = consideration - commission - ptmlevy \
        trade.localamount = trade.totalamount * rate \
        --[[ the transaction ]] \
        trade.transactionid = newtransaction(trade.totalamount, brokerid, currencyid, trade.localamount, "", rate, "trade:" .. tradeid, timestamp, "TPR", timestampms, trade.linktype, trade.linkid) \
        --[[ client account posting - note: update uncleared balance ]] \
        newposting(clientaccountid, trade.totalamount, brokerid, trade.localamount, "", trade.transactionid, timestampms, timestamp, trade.linktype, trade.linkid) \
        updateaccountbalanceuncleared(clientaccountid, trade.totalamount, brokerid, trade.localamount) \
        --[[ consideration posting ]] \
        newposting(accounts.considerationid, -consideration, brokerid, -trade.considerationlocalamount, "", trade.transactionid, timestampms, timestamp, trade.linktype, trade.linkid) \
        updateaccountbalance(accounts.considerationid, -consideration, brokerid, -trade.considerationlocalamount) \
        --[[ commission posting ]] \
        if commission ~= 0 then \
          newposting(accounts.commissionid, commission, brokerid, trade.commissionlocalamount, "", trade.transactionid, timestampms, timestamp, trade.linktype, trade.linkid) \
          updateaccountbalance(accounts.commissionid, commission, brokerid, trade.commissionlocalamount) \
        end \
         --[[ ptm levy posting ]] \
        if ptmlevy ~= 0 then \
          newposting(accounts.ptmid, ptmlevy, brokerid, trade.ptmlevylocalamount, "", trade.transactionid, timestampms, timestamp, trade.linktype, trade.linkid) \
          updateaccountbalance(accounts.ptmid, ptmlevy, brokerid, trade.ptmlevylocalamount) \
        end \
      end \
    end \
  ';
  /*
  * newpositiontransaction()
  * a transaction to either create or update a position and create a position posting
  */
  newpositiontransaction = utils.removefromnonzerosymbolindex + utils.addtononzerosymbolindex + getpositionid + newposition + updateposition + newpositionposting + '\
    local newpositiontransaction = function(accountid, brokerid, cost, futsettdate, linkid, positionpostingtypeid, quantity, symbolid, timestamp, milliseconds) \
      redis.log(redis.LOG_NOTICE, "newpositiontransaction") \
      local temp = {} \
      temp.positionid = getpositionid(accountid, brokerid, symbolid, futsettdate) \
      quantity = tonumber(quantity) \
      if not temp.positionid then \
        --[[ no position, so create a new one ]] \
        temp.positionid = newposition(accountid, brokerid, cost, futsettdate, quantity, symbolid) \
      else \
        --[[ just update it ]] \
        temp.oldquantity = redis.call("hget", "broker:" .. brokerid .. ":position:" .. temp.positionid, "quantity") \
        temp.newquantity = tonumber(temp.oldquantity + quantity) \
        updateposition(accountid, brokerid, cost, futsettdate, temp.positionid, quantity, symbolid) \
        if tonumber(temp.oldquantity) == 0 and tonumber(temp.newquantity) > 0 then \
          addtononzerosymbolindex(brokerid, symbolid) \
        elseif tonumber(temp.newquantity) == 0 then \
          removefromnonzerosymbolindex(brokerid, symbolid) \
        end \
      end \
      if tonumber(quantity) ~= 0 then \
        temp.positionpostingid = newpositionposting(brokerid, cost, linkid, temp.positionid, positionpostingtypeid, quantity, timestamp, milliseconds) \
        if tonumber(positionpostingtypeid) == 2 then \
          redis.call("sadd", "corporateaction:" .. linkid .. ":positionpostings", brokerid .. ":" .. temp.positionpostingid) \
        end \
      end \
      return {temp.positionid, temp.positionpostingid} \
    end \
  ';

  exports.newpositiontransaction = newpositiontransaction + '\
    return newpositiontransaction(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10]) \
  ';

  updateunclearedcashbreakdownindex = '\
    local updateunclearedcashbreakdownindex = function(brokerid, tradeid, accountid) \
      redis.log(redis.LOG_NOTICE, "updateunclearedcashbreakdownindex") \
      redis.call("zrem", "broker:" .. brokerid .. ":account:" .. accountid .. ":unclearedcashbreakdown", "trade:" .. tradeid) \
      return \
    end \
  ';

  exports.updateunclearedcashbreakdownindex = updateunclearedcashbreakdownindex + '\
    return updateunclearedcashbreakdownindex(ARGV[1], ARGV[2], ARGV[3]) \
  ';
 /*
  * deletetrade()
  * function to handle processing for deleting a trade
  * params: trade, timestamp, timestampms, localtimestamp
  */
  deletetrade = gethashvalues + round + updateunclearedcashbreakdownindex + deletetradetransaction + newpositiontransaction + utils.settradestatus + removetradesettlestatusindex + '\
    local deletetrade = function(trade, timestamp, timestampms) \
      local temp = {} \
      temp.rate = 1 \
      if tonumber(trade["side"]) == 1 then \
        temp.quantity = trade["quantity"] \
        temp.cost = trade["settlcurramt"] \
      else \
        temp.quantity = -tonumber(trade["quantity"]) \
        temp.cost = -tonumber(trade["settlcurramt"]) \
        updateunclearedcashbreakdownindex(trade["brokerid"], trade["tradeid"], trade["accountid"]) \
      end \
      deletetradetransaction(trade["settlcurramt"], trade["commission"], trade["ptmlevy"], trade["stampduty"], trade["brokerid"], trade["accountid"], trade["currencyid"], temp.rate, timestamp, trade["tradeid"], trade["side"], timestampms) \
      newpositiontransaction(trade["accountid"], trade["brokerid"], -temp.cost, trade["futsettdate"], trade["tradeid"], 5, -temp.quantity, trade["symbolid"], timestamp, timestampms) \
      settradestatus(trade["brokerid"], trade["tradeid"], 3) \
      --[[ ensure trade is removed from CREST list ]] \
      removetradesettlestatusindex(trade["brokerid"], trade["tradeid"], trade["tradesettlestatusid"]) \
      --[[ remove from list for sending contract notes ]] \
      redis.call("lrem", "contractnotes", 1, trade["brokerid"] .. ":" .. trade["tradeid"]) \
    end \
  ';

  /*
  * publishtrade()
  * here 6 refer to tradechannel
  * publish a trade
  */
  publishtrade = gethashvalues + '\
  local publishtrade = function(brokerid, tradeid) \
    redis.log(redis.LOG_NOTICE, "publishtrade") \
    redis.call("publish", 6, "{" .. cjson.encode("trade") .. ":" .. cjson.encode(gethashvalues("broker:" .. brokerid .. ":trade:" .. tradeid)) .. "}") \
  end \
  ';
  generatetradeid = '\
    local generatetradeid = function(brokerid, tradeid) \
      while #tradeid < 8 do \
        tradeid = "0" .. tradeid \
      end \
      local prefix = redis.call("hget", "broker:" .. brokerid, "tradeidprefix") \
      if prefix then \
        tradeid = prefix .. tradeid \
      end \
      return tradeid \
    end \
  ';

  exports.generatetradeid = generatetradeid + '\
    return generatetradeid(ARGV[1], ARGV[2]) \
  ';
  /*
  * newtrade()
  * args: accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, contractcharge, stampdutyid, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, operatortype, operatorid, finance, timestampms, tradesettlestatusid, linkedfromtrade, linkedtotrade, fixseqnumid, lastcrestmessagestatus, markettimestamp, tradesettlestatustime
  * stores a trade & updates cash & position
  */
  newtrade = generatetradeid + newtradetransaction + newpositiontransaction + updatetradesettlestatusindex + updatefieldindexes + publishtrade + utils.getparentlinkdetails + utils.updateaddtionalindex +  utils.updateaddtionaltimeindex + '\
  local newtrade = function(accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, contractcharge, stampdutyid, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, operatortype, operatorid, finance, timestampms, tradesettlestatusid, linkedfromtrade, linkedtotrade, fixseqnumid, lastcrestmessagestatus, markettimestamp, tradesettlestatustime) \
    redis.log(redis.LOG_NOTICE, "newtrade") \
    local trade = {} \
    trade.brokerkey = "broker:" .. brokerid \
    quantity = tonumber(quantity) \
    trade.tradeid = redis.call("hincrby", trade.brokerkey, "lasttradeid", 1) \
    trade.linkdetails = getparentlinkdetails(brokerid, accountid) \
    trade.linktype = trade.linkdetails[1] \
    trade.linkid = trade.linkdetails[2] \
    if not trade.tradeid then return 0 end \
    trade.gentradeid = generatetradeid(brokerid, tostring(trade.tradeid)) \
    trade.tradeindexid = trade.gentradeid \
    if #trade.gentradeid > 8 then \
      trade.tradeindexid = substring(trade.gentradeid, 3) \
    end \
    trade.strsettlcurramt = tostring(settlcurramt) \
    if not tradesettlestatustime then \
      tradesettlestatustime = "" \
    end \
    redis.call("hmset", trade.brokerkey .. ":trade:" .. trade.gentradeid, "accountid", accountid, "brokerid", brokerid, "clientid", clientid, "orderid", orderid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "commission", round(commission, 2), "ptmlevy", round(ptmlevy, 2), "stampduty", round(stampduty, 2), "contractcharge", round(contractcharge, 2), "stampdutyid", stampdutyid, "counterpartyid", counterpartyid, "counterpartytype", counterpartytype, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", trade.gentradeid, "settlcurrencyid", settlcurrencyid, "settlcurramt", trade.strsettlcurramt, "settlcurrfxrate", round(settlcurrfxrate, 6), "settlcurrfxratecalc", settlcurrfxratecalc, "margin", margin, "operatortype", operatortype, "operatorid", operatorid, "finance", finance, "tradesettlestatusid", tradesettlestatusid, "linkedfromtrade", linkedfromtrade, "linkedtotrade", linkedtotrade, "status", 1, "fixseqnumid", fixseqnumid, "lastcrestmessagestatus", lastcrestmessagestatus, "markettimestamp", markettimestamp, "tradesettlestatustime", tradesettlestatustime) \
    redis.call("sadd", trade.brokerkey .. ":tradeid", "trade:" .. trade.gentradeid) \
    redis.call("sadd", trade.brokerkey .. ":trades", trade.gentradeid) \
    redis.call("sadd", trade.brokerkey .. ":account:" .. accountid .. ":trades", trade.gentradeid) \
    --[[ add to a system wide list of items for sending contract notes ]] \
    redis.call("rpush", "contractnotes", brokerid .. ":" .. trade.gentradeid) \
    --[[ add to a system wide index of trades by settlementstatus for CREST ]] \
    updatetradesettlestatusindex(brokerid, trade.gentradeid, "", "000") \
    updateaddtionalindex(brokerid, "trades", trade.gentradeid, trade.linktype, trade.linkid, "", "") \
    if orderid ~= "" then \
      redis.call("sadd", trade.brokerkey .. ":order:" .. orderid .. ":trades", trade.gentradeid) \
    end \
    redis.call("zadd", trade.brokerkey .. ":account:" .. accountid .. ":tradesbydate", timestampms, trade.gentradeid) \
    trade.searchKey = clientid .. ":" .. accountid .. ":" .. lowercase(trade.gentradeid) .. ":" .. lowercase(trim(symbolid)) .. ":" .. lowercase(getdateindex(futsettdate)) .. ":" .. lowercase(gettimestampindex(timestamp)) \
    redis.call("zadd", "broker:" .. brokerid .. ":trade:search_index", trade.tradeindexid, trade.searchKey) \
    updateaddtionaltimeindex(brokerid, "trade:search_index", trade.searchKey, trade.linktype, trade.linkid, trade.tradeindexid) \
    --[[ add sorted sets for columns that require sorting capability ]] \
    trade.indexsettlcurramt = tonumber(trade.strsettlcurramt) * 100 \
    trade.fieldscorekeys = {"symbolid", 0, symbolid .. ":" .. trade.gentradeid, "timestamp", timestampms, trade.gentradeid, "settlcurramt", trade.indexsettlcurramt, trade.gentradeid, "settlementdate", futsettdate, trade.gentradeid,  "tradesettlementstatus", 0, tradesettlestatusid .. ":" .. trade.gentradeid} \
    updatefieldindexes(brokerid, "trade", trade.fieldscorekeys) \
    updateaddtionaltimeindex(brokerid, "trade:timestamp", trade.gentradeid, trade.linktype, trade.linkid, timestampms) \
    updateaddtionaltimeindex(brokerid, "trade:settlementdate", trade.gentradeid, trade.linktype, trade.linkid, futsettdate) \
    updateaddtionaltimeindex(brokerid, "trade:tradesettlementstatus", tradesettlestatusid .. ":" .. trade.gentradeid, trade.linktype, trade.linkid, 0) \
    updateaddtionaltimeindex(brokerid, "trade:symbolid", symbolid .. ":" .. trade.gentradeid, trade.linktype, trade.linkid, 0) \
    updateaddtionaltimeindex(brokerid, "trade:settlcurramt", trade.gentradeid, trade.linktype, trade.linkid, trade.indexsettlcurramt) \
    if tonumber(side) == 1 then \
      trade.cost = settlcurramt \
    else \
      quantity = -tonumber(quantity) \
      trade.cost = -tonumber(settlcurramt) \
      redis.call("zadd", trade.brokerkey .. ":account:" .. accountid .. ":unclearedcashbreakdown", timestampms, "trade:" .. trade.gentradeid) \
    end \
    newtradetransaction(settlcurramt, commission, ptmlevy, stampduty, contractcharge, brokerid, accountid, settlcurrencyid, 1, timestamp, trade.gentradeid, side, timestampms, trade.linktype, trade.linkid) \
    newpositiontransaction(accountid, brokerid, trade.cost, futsettdate, trade.gentradeid, 1, quantity, symbolid, timestamp, timestampms) \
    publishtrade(brokerid, trade.gentradeid, operatortype) \
    return trade.gentradeid \
  end \
  ';

  exports.newtrade = newtrade;
 /*
  * updateorderindex()
  * update order index of the trade
  */
  updateorderindex = '\
    local updateorderindex = function(brokerid, oldorderid, neworderid, tradeid) \
      redis.log(redis.LOG_NOTICE, "updateorderindex") \
      redis.call("srem", "broker:" .. brokerid .. ":order:" .. oldorderid .. ":trades", tradeid) \
      if neworderid ~= "" then \
        redis.call("sadd", "broker:" .. brokerid .. ":order:" .. neworderid .. ":trades", tradeid) \
      end \
    end \
  ';
 /*
  * updatesettlementdateindex()
  * update settlement date index of the trade
  */
  updatesettlementdateindex = '\
    local updatesettlementdateindex = function(brokerid, oldtradeid, newtradeid, futsettdate) \
      redis.log(redis.LOG_NOTICE, "updatesettlementdateindex") \
      redis.call("zrem", "broker:" .. brokerid .. ":trade:settlementdate", oldtradeid) \
      redis.call("zadd", "broker:" .. brokerid .. ":trade:settlementdate", futsettdate, newtradeid) \
    end \
  ';
 /*
  * updatetrade()
  * script used to update the trade values
  * accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, contractcharge, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, operatortype, operatorid, finance, timestampms, tradeid, tradesettlestatusid, markettimestamp
  */
  updatetrade = updateorderindex + updatesettlementdateindex + updatetradesettlestatusindex + '\
    local updatetrade = function(accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, contractcharge, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, operatortype, operatorid, finance, timestampms, tradeid, tradesettlestatusid, markettimestamp) \
      redis.log(redis.LOG_NOTICE, "updatetrade") \
      local trade = {} \
      trade.key = "broker:" .. brokerid .. ":trade:" .. tradeid \
      trade.old = redis.call("hmget", trade.key, "orderid", "futsettdate") \
      local oldtradesettlestatus = "" \
      if tradesettlestatusid and tradesettlestatusid ~= "" then \
        oldtradesettlestatus = redis.call("hget", "broker:" .. brokerid .. ":trade:" .. tradeid, "tradesettlestatusid") \
      end \
      redis.call("hmset", trade.key, "accountid", accountid, "brokerid", brokerid, "clientid", clientid, "orderid", orderid, "symbolid", symbolid, "side", side, "quantity", quantity, "price", price, "currencyid", currencyid, "currencyratetoorg", currencyratetoorg, "currencyindtoorg", currencyindtoorg, "commission", tostring(commission), "ptmlevy", tostring(ptmlevy), "stampduty", tostring(stampduty), "contractcharge", tostring(contractcharge), "counterpartyid", counterpartyid, "counterpartytype", counterpartytype, "markettype", markettype, "externaltradeid", externaltradeid, "futsettdate", futsettdate, "timestamp", timestamp, "lastmkt", lastmkt, "externalorderid", externalorderid, "tradeid", tradeid, "settlcurrencyid", settlcurrencyid, "settlcurramt", settlcurramt, "settlcurrfxrate", settlcurrfxrate, "settlcurrfxratecalc", settlcurrfxratecalc, "margin", margin, "finance", finance, "tradesettlestatusid", tradesettlestatusid, "markettimestamp", markettimestamp) \
      if orderid ~= trade.old[1] then \
        updateorderindex(brokerid, trade.old[1], orderid, tradeid) \
      end \
      if futsettdate ~= trade.old[2] then \
        updatesettlementdateindex(brokerid, trade.old[2], tradeid, futsettdate) \
      end \
      if oldtradesettlestatus and oldtradesettlestatus ~= "" then \
        updatetradesettlestatusindex(brokerid, tradeid, oldtradesettlestatus, tradesettlestatusid) \
        --[[ updating broker level trade index(by trade settlement status) for report ]] \
        redis.call("zrem", "broker:" .. brokerid .. ":trade:tradesettlementstatus", oldtradesettlestatus .. ":" .. tradeid) \
        redis.call("zadd", "broker:" .. brokerid .. ":trade:tradesettlementstatus", 0, tradesettlestatusid .. ":" .. tradeid) \
      end \
    end \
  ';
  /*
   * updatetradenote()
   * This script used to update trade note.
   * params: brokerid, accountid, tradeid
   */
  updatetradenote = getpostingnote + '\
    local updatetradenote = function(brokerid, tradeid) \
      redis.log(redis.LOG_WARNING, "updatetradenote") \
      local fields = {} \
      fields.brokerkey = "broker:" .. brokerid \
      fields.transactionid = redis.call("get", fields.brokerkey .. ":trade:" .. tradeid .. ":transactionid") \
      if fields.transactionid then \
        fields.postingids = redis.call("smembers", fields.brokerkey .. ":transaction:" .. fields.transactionid .. ":postings") \
        if #fields.postingids > 0 then \
          for i = 1, #fields.postingids do \
            fields.accountid = redis.call("hget", fields.brokerkey .. ":posting:" .. fields.postingids[i], "accountid") \
            fields.clientid = redis.call("get", fields.brokerkey .. ":account:" .. fields.accountid .. ":client") \
            if fields.clientid then \
              fields.note = getpostingnote(brokerid, tradeid, 0) \
            else \
              fields.note = getpostingnote(brokerid, tradeid, 1) \
            end \
            redis.call("hset", fields.brokerkey .. ":posting:" .. fields.postingids[i], "note", fields.note) \
          end \
        end \
      end \
    end \
  ';
  /*
  * getcaclientdecision()
  * get a corporate action client decision
  * params: accountid, brokerid, corporateactionid
  * returns: 0, corporate action client decision as a table if found, else 1
  */
  getcaclientdecision = '\
    local getcaclientdecision = function(accountid, brokerid, corporateactionid) \
      redis.log(redis.LOG_NOTICE, "getcaclientdecision") \
      local brokerkey = "broker:" .. brokerid \
      local cadecision = {} \
      local cadecisionid = redis.call("get", brokerkey .. ":account:" .. accountid .. ":corporateaction:" .. corporateactionid .. ":corporateactiondecision") \
      if cadecisionid then \
        cadecision = gethashvalues(brokerkey .. ":corporateactiondecision:" .. cadecisionid) \
      else \
        return {1} \
      end \
      return {0, cadecision} \
    end \
  ';
  /*
  * getsharesdue()
  * get the whole number of shares due & any remainder for a quantity & ratio
  * params: position quantity, shares per share ratio, percentage of position quantity to apply ratio to, client elected quantity as shares
  * returns: whole number of shares due, remainder
  */
  getsharesdue = round + '\
  local getsharesdue = function(posqty, sharespershare, qtypercentage, electedquantityasshares) \
    redis.log(redis.LOG_NOTICE, "getsharesdue") \
    local sharesdue = {} \
    local numberofshares = round(tonumber(posqty) * (tonumber(qtypercentage) / 100) * tonumber(sharespershare), 2) \
    if tonumber(electedquantityasshares) ~= 0 then \
      sharesdue.wholenumber = tonumber(electedquantityasshares) \
      sharesdue.remainder = 0 \
    else \
      sharesdue.wholenumber = math.floor(numberofshares) \
      sharesdue.remainder = numberofshares - sharesdue.wholenumber \
    end \
    return sharesdue \
  end \
  ';

  /*
  * transactiondividend()
  * cash dividend transaction
  * params: clientaccountid, dividend, unsettled dividend, brokerid, currencyid, dividend in local currency, unsettled dividend in local currency, description, rate, reference, timestamp, transactiontypeid, timestampms, corporateactionid, linktype, linkid
  * returns: 0 if ok, else 1 followed by error message
  */
  transactiondividend = getbrokeraccountsmapid + newtransaction + newposting + updateaccountbalance + '\
  local transactiondividend = function(clientaccountid, dividend, unsettleddividend, brokerid, currencyid, dividendlocal, unsettleddividendlocal, description, rate, reference, timestamp, transactiontypeid, timestampms, corporateactionid, linktype, linkid) \
    redis.log(redis.LOG_NOTICE, "transactiondividend") \
    local accounts = {} \
    --[[ get the relevant broker accounts ]] \
    accounts.clientfundsaccount = getbrokeraccountsmapid(brokerid, currencyid, "Client funds") \
    if not accounts.clientfundsaccount then \
      return {1, 1027} \
    end \
    accounts.clientsettlementaccount = getbrokeraccountsmapid(brokerid, currencyid, "Client settlement") \
    if not accounts.clientsettlementaccount then \
      return {1, 1027} \
    end \
    accounts.cmaaccount = getbrokeraccountsmapid(brokerid, currencyid, "CMA") \
    if not accounts.cmaaccount then \
      return {1, 1027} \
    end \
    --[[ create the transaction ]] \
    local transactionid = newtransaction(dividend, brokerid, currencyid, dividendlocal, description, rate, reference, timestamp, transactiontypeid, timestampms, linktype, linkid) \
    redis.call("sadd", "corporateaction:" .. corporateactionid .. ":transactions", brokerid .. ":" .. transactionid) \
    --[[ client posting ]] \
    newposting(clientaccountid, dividend, brokerid, dividendlocal, "", transactionid, timestampms, timestamp, linktype, linkid) \
    updateaccountbalance(clientaccountid, dividend, brokerid, dividendlocal) \
    --[[ broker side of client posting ]] \
    newposting(accounts.clientfundsaccount, -dividend, brokerid, -dividendlocal, "", transactionid, timestampms, timestamp, linktype, linkid) \
    updateaccountbalance(accounts.clientfundsaccount, -dividend, brokerid, -dividendlocal) \
    if unsettleddividend ~= 0 then \
      --[[ unsettled transaction ]] \
      local unsettledtransactionid = newtransaction(unsettleddividend, brokerid, currencyid, unsettleddividendlocal, description, rate, reference, timestamp, transactiontypeid, timestampms, linktype, linkid) \
      redis.call("sadd", "corporateaction:" .. corporateactionid .. ":transactions", brokerid .. ":" .. unsettledtransactionid) \
      newposting(accounts.cmaaccount, unsettleddividend, brokerid, unsettleddividendlocal, "", unsettledtransactionid, timestampms, timestamp, linktype, linkid) \
      updateaccountbalance(accounts. cmaaccount, unsettleddividend, brokerid, unsettleddividendlocal) \
      newposting(accounts.clientsettlementaccount, -unsettleddividend, brokerid, -unsettleddividendlocal, "", unsettledtransactionid, timestampms, timestamp, linktype, linkid) \
      updateaccountbalance(accounts.clientsettlementaccount, -unsettleddividend, brokerid, -unsettleddividendlocal) \
    end \
    return {0} \
  end \
  ';

  /*
  * convertsharesascash()
  * calculate remainder shares as cash
  * params: symbolid, quantity, exdate
  * returns: 0, amount of cash if ok, else 1, error message
  */
  convertsharesascash = geteodprice + round + getdateindex + '\
  local convertsharesascash = function(symbolid, quantity, exdate) \
    redis.log(redis.LOG_NOTICE, "convertsharesascash") \
    --[[ get the closing price at the ex-date ]] \
    local eodprice = geteodprice(exdate, symbolid) \
    if not eodprice.bid then \
      return {1, "There needs to be an end of day price for " .. symbolid .. " as at " .. getdateindex(exdate)} \
    end \
    --[[ calculate how much cash is due ]] \
    local stubcash = {} \
    stubcash.amount = tonumber(round(tonumber(quantity) * tonumber(eodprice.bid), 2)) \
    --[[ we are assuming GBP dividend - todo: get fx rate if necessary ]] \
    stubcash.rate = 1 \
    stubcash.amountlocal = tonumber(round(stubcash.amount * stubcash.rate, 2)) \
    return {0, stubcash} \
  end \
  ';

 /*
  * getaccountforcurrency()
  * params: account, brokerid, currencyid
  * returns: an accountid for this client and currency
  */
  getaccountforcurrency = getclientidfromaccount + gethashvalues + getclientaccountid + newaccount + '\
    local getaccountforcurrency = function(account, brokerid, currencyid) \
      --[[ get the client ]] \
      local clientid = getclientidfromaccount(account.accountid, brokerid) \
      local client = gethashvalues("broker:" .. brokerid .. ":client:" .. clientid) \
      --[[ see if an account already exists for this client in this currency, if not create one ]] \
      local newaccountid = getclientaccountid(brokerid, clientid, account.accounttypeid, currencyid) \
      if newaccountid == 0 then \
        local name = client.clientid .. " " .. client.name .. " " .. currencyid \
        --[[ todo: sort ]] \
        local exdiffdate = "05-May-2015" \
        --[[ set default(0) value for following fields balance, balanceuncleared, creditlimit, debitlimit, exdiff, exdiffuncleared, localbalance, localbalanceuncleared ]] \
        newaccountid = newaccount(account.accountgroupid, account.accounttaxtypeid, account.accounttypeid, 0, 0, brokerid, 0, currencyid, 0, 0, 0, exdiffdate, 0, 0, name, account.active, clientid) \
        if newaccountid[1] == 1 then \
          newaccountid = "" \
        else \
          newaccountid = newaccountid[2] \
        end \
      end \
      return newaccountid \
    end \
  ';

  /*
  * valuebenefitascash()
  * value a cash benefit
  * params: benefit, position, decision
  * returns: cash amounts and rate as a table
  */
  valuebenefitascash = round + '\
  local valuebenefitascash = function(benefit, position, decision) \
    redis.call("RPUSH", "cadebug", "valuebenefitascash") \
    local cash = {} \
    --[[ we are assuming rate = 1 - todo: get fx rate if necessary ]] \
    cash.rate = 1 \
    if benefit.debitorcredit == "D" then \
      --[[ allow for a client elected quantity ]] \
      if decision.electedquantityasshares and tonumber(decision.electedquantityasshares) ~= 0 then \
        cash.dividend = -tonumber(round(tonumber(benefit.benefitratio) * tonumber(decision.electedquantityasshares), 2)) \
      else \
        cash.dividend = -tonumber(round(tonumber(benefit.benefitratio) * tonumber(position.quantity), 2)) \
      end \
    else \
      cash.dividend = tonumber(round(tonumber(benefit.benefitratio) * tonumber(position.quantity), 2)) \
    end \
    cash.dividendlocal = cash.dividend * cash.rate \
    redis.call("RPUSH", "cadebug", "end valuebenefitascash") \
    return cash \
  end \
  ';

  /*
  * valuebenefitasshares()
  * value a shares benefit
  * params: benefit, corporateaction, position, decision
  * returns: share amounts and any residual cash
  */
  valuebenefitasshares = getsharesdue + convertsharesascash + '\
  local valuebenefitasshares = function(benefit, corporateaction, position, decision) \
    redis.call("RPUSH", "cadebug", "valuebenefitasshares") \
    local sharesdue = getsharesdue(position.quantity, benefit.benefitratio, benefit.benefitpercentage, decision.electedquantityasshares) \
    --[[ amount of shares due may be a debit or credit ]] \
    if benefit.debitorcredit == "D" then \
      sharesdue.wholenumber = -sharesdue.wholenumber \
      sharesdue.remainder = -sharesdue.remainder \
    end \
    sharesdue.residuecash = 0 \
    sharesdue.rate = 1 \
    sharesdue.residuecashlocal = 0 \
    if sharesdue.remainder ~= 0 and tonumber(corporateaction.fractionspaidbyissuer) == 1 then \
      local stubcash = convertsharesascash(benefit.symbolid, sharesdue.remainder, corporateaction.exdate) \
      if stubcash[1] == 1 then \
        return stubcash \
      end \
      sharesdue.residuecash = stubcash[2].amount \
      sharesdue.rate = stubcash[2].rate \
      sharesdue.residuecashlocal = stubcash[2].amountlocal \
    end \
    redis.call("RPUSH", "cadebug", "end valuebenefitasshares") \
    return {0, sharesdue} \
  end \
  ';

  /*
  * valuebenefit()
  * values a benefit
  * params: benefit, corporateaction, position, decision
  * returns: 0, benefit value else 1, error message
  */
  valuebenefit = valuebenefitascash + valuebenefitasshares + '\
  local valuebenefit = function(benefit, corporateaction, position, decision) \
    redis.call("RPUSH", "cadebug", "valuebenefit") \
    local benefitvalue = {} \
    if benefit.benefittypeid == "CASH" then \
      local cash = valuebenefitascash(benefit, position, decision) \
      benefitvalue.cash = cash.dividend \
      benefitvalue.cashlocal = cash.dividendlocal \
      benefitvalue.rate = cash.rate \
      benefitvalue.shares = 0 \
      benefitvalue.residuecash = 0 \
      benefitvalue.residuecashlocal = 0 \
    elseif benefit.benefittypeid == "SECU" then \
      local shares = valuebenefitasshares(benefit, corporateaction, position, decision) \
      if shares[1] == 1 then \
        return shares \
      end \
      benefitvalue.shares = shares[2].wholenumber \
      benefitvalue.residuecash = shares[2].residuecash\
      benefitvalue.residuecashlocal = shares[2].residuecashlocal \
      benefitvalue.rate = shares[2].rate \
      benefitvalue.cash = 0 \
      benefitvalue.cashlocal = 0 \
    end \
    redis.call("RPUSH", "cadebug", "end valuebenefit") \
    return {0, benefitvalue} \
  end \
  ';

  /*
  * updatebenefitascash()
  * processing for benefit taken as cash
  * params: brokerid, position, corporateaction, benefit, timestamp, timestampms
  * returns: 0, else 1, error message
  */
  updatebenefitascash = transactiondividend + getclientfromaccount + getaccount + getaccountforcurrency + '\
  local updatebenefitascash = function(brokerid, position, corporateaction, benefit, timestamp, timestampms) \
    redis.log(redis.LOG_NOTICE, "updatebenefitascash") \
    redis.call("RPUSH", "cadebug", "updatebenefitascash") \
    if benefit.cash ~= 0 then \
      --[[ get account currency ]] \
      local account = getaccount(position.accountid, brokerid) \
      local accountid \
      local currencyid \
      --[[ if the benefit currency does not match the position currency, we need to get an account in the benefit currency ]] \
      if benefit.currencyid and benefit.currencyid ~= account.currencyid then \
      accountid = getaccountforcurrency(account, brokerid, benefit.currencyid) \
        currencyid = benefit.currencyid \
      else \
        accountid = position.accountid \
        currencyid = account.currencyid \
      end \
      local client = getclientfromaccount(position.accountid, brokerid) \
      local retval = transactiondividend(accountid, benefit.cash, 0, brokerid, currencyid, benefit.cashlocal, 0, corporateaction.description, benefit.rate, "corporateaction:" .. corporateaction.corporateactionid, timestamp, "DVP", timestampms, corporateaction.corporateactionid, client.linktype, client.linkid) \
      if retval[1] == 1 then \
        return retval \
      end \
    end \
    redis.call("RPUSH", "cadebug", "end updatebenefitascash") \
    return {0} \
  end \
  ';

 /*
  * updatebenefitasshares()
  * processing for benefit taken as shares
  * params: brokerid, position, corporateaction, benefit, timestamp, timestampms
  * returns: 0, else 1, error message
  */
  updatebenefitasshares = newpositiontransaction + getaccount + getclientfromaccount + transactiondividend + getaccountforcurrency + '\
  local updatebenefitasshares = function(brokerid, position, corporateaction, benefit, timestamp, timestampms) \
    redis.log(redis.LOG_NOTICE, "updatebenefitasshares") \
    redis.call("RPUSH", "cadebug", "updatebenefitasshares") \
    --[[ add any shares to the position ]] \
    if benefit.shares ~= 0 then \
      --[[ calculate cost of shares ]] \
      local costofshares = 0 \
      if benefit.price and tonumber(benefit.price) ~= 0 then \
        costofshares = benefit.shares * tonumber(benefit.price) \
      end \
      newpositiontransaction(position.accountid, brokerid, costofshares, "", corporateaction.corporateactionid, 2, benefit.shares, benefit.symbolid, timestamp, timestampms) \
    end \
    --[[ add any residual cash ]] \
    if benefit.residuecash ~= 0 then \
    --[[ get account currency ]] \
      local account = getaccount(position.accountid, brokerid) \
      local accountid \
      local currencyid \
      --[[ if the benefit currency does not match the position currency, we need to get an account in the benefit currency ]] \
      if benefit.currencyid and benefit.currencyid ~= account.currencyid then \
        accountid = getaccountforcurrency(account, brokerid, benefit.currencyid) \
        currencyid = benefit.currencyid \
      else \
        accountid = position.accountid \
        currencyid = account.currencyid \
      end \
      --[[ get the client ]] \
      local client = getclientfromaccount(position.accountid, brokerid) \
      --[[ apply transactions & postings ]] \
      local retval = transactiondividend(accountid, benefit.residuecash, 0, brokerid, currencyid, benefit.residuecashlocal, 0, corporateaction.description, benefit.rate, "corporateaction:" .. corporateaction.corporateactionid, timestamp, "DVP", timestampms, corporateaction.corporateactionid, client.linktype, client.linkid) \
      if retval[1] == 1 then \
        return retval \
      end \
    end \
    redis.call("RPUSH", "cadebug", "end updatebenefitasshares") \
    return {0} \
  end \
  ';

  /*
  * updatebenefit()
  * updates positions, transactions & postings for a benefit
  * params: brokerid, position, corporateaction, benefit, timestamp, timestampms
  * returns: 0, else 1, error message
  */
  updatebenefit = updatebenefitascash + updatebenefitasshares + '\
  local updatebenefit = function(brokerid, position, corporateaction, benefit, timestamp, timestampms) \
    local ret \
    if benefit.benefittypeid == "CASH" then \
      ret = updatebenefitascash(brokerid, position, corporateaction, benefit, timestamp, timestampms) \
    elseif benefit.benefittypeid == "SECU" then \
      ret = updatebenefitasshares(brokerid, position, corporateaction, benefit, timestamp, timestampms) \
    end \
    return ret \
  end \
  ';

  /*
  * applybenefits()
  * apply the benefits of a corporate action
  * params: brokerid, position, corporateaction, timestamp, timestampms, mode
  * returns: 0, table of benefits for this position if successful, else 1, error message
  */
  applybenefits = getcaclientdecision + gethashvalues + valuebenefit + updatebenefit + '\
  local applybenefits = function(brokerid, position, corporateaction, timestamp, timestampms, mode) \
    redis.call("RPUSH", "cadebug", "applybenefits") \
    local benefits = {} \
    local decision = {} \
    --[[ get the client option for a voluntary ca, default as per ca, usually 1 ]] \
    local optionid = corporateaction.defaultoptionid \
    if corporateaction.mandatoryorvoluntaryind ~= "M" then \
      local caclientdecision = getcaclientdecision(position.accountid, brokerid, corporateaction.corporateactionid) \
      if caclientdecision[1] == 0 then \
        optionid = caclientdecision[2].optionid \
        decision = caclientdecision[2] \
      end \
    end \
    local benefitids = redis.call("smembers", "corporateaction:" .. corporateaction.corporateactionid .. ":benefits") \
    for i = 1, #benefitids do \
      local benefit = gethashvalues("corporateaction:" .. corporateaction.corporateactionid .. ":benefit:" .. benefitids[i]) \
      if tonumber(benefit.optionid) == tonumber(optionid) then \
        local benefitvalue = valuebenefit(benefit, corporateaction, position, decision) \
        if benefitvalue[1] == 1 then \
          return benefitvalue \
        end \
        benefit.cash = benefitvalue[2].cash \
        benefit.cashlocal = benefitvalue[2].cashlocal \
        benefit.shares = benefitvalue[2].shares \
        benefit.residuecash = benefitvalue[2].residuecash \
        benefit.residuecashlocal = benefitvalue[2].residuecashlocal \
        benefit.rate = benefitvalue[2].rate \
        table.insert(benefits, benefit) \
        if mode == 2 then \
          local ret = updatebenefit(brokerid, position, corporateaction, benefit, timestamp, timestampms) \
          if ret[1] == 1 then \
            return ret \
          end \
        end \
      end \
    end \
    redis.call("RPUSH", "cadebug", "end applybenefits") \
    return {0, benefits} \
  end \
  ';

  /*
  * publisherror()
  * publish an error
  * here 13 refers to errorchannel
  * params: errorlogid
  */
  publisherror = gethashvalues + '\
  local publisherror = function(errorlogid) \
    local errorlog = gethashvalues("errorlog:" .. errorlogid) \
    redis.call("publish", 13, "{" .. cjson.encode("errorlog") .. ":" .. cjson.encode(errorlog) .. "}") \
  end \
  ';

  /*
  * errorlog()
  * store details of errors
  * params: 1=brokerid, 2=businessobjectid, 3=businessobjecttypeid, 4=errortypeid, 5=messageid, 6=messagetypeid, 7=module, 8=rejectreasonid, 9=text, 10=timestamp
  * returns:
  */
  errorlog = publisherror + '\
    local errorlog = function(brokerid, businessobjectid, businessobjecttypeid, errortypeid, messageid, messagetypeid, module, rejectreasonid, text, timestamp) \
      redis.log(redis.LOG_NOTICE, "errorlog") \
      local errorlogid = redis.call("hincrby", "config", "lasterrorlogid", 1) \
      redis.call("hmset", "errorlog:" .. errorlogid, "brokerid", brokerid, "businessobjectid", businessobjectid, "businessobjecttypeid", businessobjecttypeid, "errorlogid", errorlogid, "errortypeid", errortypeid, "messageid", messageid, "messagetypeid", messagetypeid, "module", module, "rejectreasonid", rejectreasonid, "text", text, "timestamp", timestamp) \
      redis.call("sadd", "errorlog:errorlogs", errorlogid) \
      publisherror(errorlogid) \
    end \
  ';

  exports.errorlog = errorlog;

  /*** Scripts ***/

  /*
  * scripterrorlog
  * store errors
  * params: brokerid, businessobjectid, businessobjecttypeid, errortypeid, messageid, messagetypeid, module, rejectreasonid, text, timestamp
  * returns:
  */
  exports.scripterrorlog = errorlog + '\
    errorlog(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10]) \
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
  * get an orderbook for a symbol & side
  * params: symbolid, side
  * returns: array of orders as JSON
  */
  exports.scriptgetorderbook = getorderbook + '\
    return cjson.encode(getorderbook(ARGV[1], ARGV[2]) \
  ';

  /*
  * get the lowest buy or highest sell order on an orderbook for a symbol
  * params: symbolid, side
  * returns: array of orders as JSON
  */
  exports.scriptgetorderbooktop = getorderbooktop + '\
    return cjson.encode(getorderbooktop(ARGV[1], ARGV[2])) \
  ';

  /*
  * get the lowest buy or highest sell orders on the orderbook for all symbols
  * params: side
  * returns: array of orders as JSON
  */
  exports.scriptgetorderbooktopall = getorderbooktopall + '\
    return cjson.encode(getorderbooktopall(ARGV[1])) \
  ';

  /*
  * get positions for an account
  * params: accountid, brokerid
  * returns an array of positions as JSON
  */
  exports.scriptgetpositions = getpositions + '\
    return cjson.encode(getpositions(ARGV[1], ARGV[2])) \
  ';

  /*
  * scriptgetpositionvalues
  * get positions for an account
  * params: accountid, brokerid
  * returns: positions with their values
  */
  exports.scriptgetpositionvalues = getpositionvalues + '\
    return cjson.encode(getpositionvalues(ARGV[1], ARGV[2])) \
  ';

  /*
  * scriptgetpositionsbysymbol
  * get positions for a symbol
  * params: brokerid, symbolid
  * returns: a table of positions
  */
  exports.scriptgetpositionsbysymbol = getpositionsbysymbol + '\
    redis.log(redis.LOG_NOTICE, "scriptgetpositionsbysymbol") \
    local issymbolid = redis.call("hget", "symbol:" ..  ARGV[2], "symbolid") \
    if issymbolid ~=  ARGV[2] then \
      return{1, "symbol not found"} \
    end \
    return {0, cjson.encode(getpositionsbysymbol(ARGV[1], ARGV[2]))} \
  ';

  /*
  * scriptgetpositionsbysymbolbydate
  * get positions as at a date
  * params: brokerid, symbolid, millisecond representation of the date
  * returns: a table of positions
  */
  exports.scriptgetpositionsbysymbolbydate = getpositionsbysymbolbydate + '\
    redis.log(redis.LOG_NOTICE, "scriptgetpositionsbysymbol") \
    local issymbolid = redis.call("sismember", "symbol:_indicies:symbolid", ARGV[2]) \
    if issymbolid == 0 then \
      return{1, "symbol not found"} \
    end \
    return {0, cjson.encode(getpositionsbysymbolbydate(ARGV[1], ARGV[2], ARGV[3]))} \
  ';

  /*
  * scriptgetpositionvaluesbysymbol
  * get position values for a symbol
  * params: brokerid, symbolid
  * returns: a table of position values
  */
  exports.scriptgetpositionvaluesbysymbol = getpositionvaluesbysymbol + '\
    local issymbolid = redis.call("sismember", "symbol:_indicies:symbolid", ARGV[2]) \
    if issymbolid == 0 then \
      return{1, "symbol not found"} \
    end \
    return {0, cjson.encode(getpositionvaluesbysymbol(ARGV[1], ARGV[2]))} \
  ';

  /*
  * getpositionvaluesbysymbolbydate
  * get position values for a symbol as at a date
  * params: brokerid, symbolid, value date, millisecond representation of date
  * returns: a table of position values
  */
  exports.scriptgetpositionvaluesbysymbolbydate = getpositionvaluesbysymbolbydate + '\
    local issymbolid = redis.call("sismember", "symbol:_indicies:symbolid", ARGV[2]) \
    if issymbolid == 0 then \
      return{1, "symbol not found"} \
    end \
    return {0, cjson.encode(getpositionvaluesbysymbolbydate(ARGV[1], ARGV[2], ARGV[3], ARGV[4]))} \
  ';

  /*
  * scriptgetpositionsbybroker
  * get positions for a broker
  * params: brokerid
  * returns: a table of positions
  */
  exports.scriptgetpositionsbybroker = getpositionsbybroker + '\
    return cjson.encode(getpositionsbybroker(ARGV[1])) \
  ';

  /*
  * scriptgetpositionpostings
  * params: brokerid, positionid, startmilliseconds, endmilliseconds
  * returns: array of postings
  */
  exports.scriptgetpositionpostings = getpositionpostingsbydate + '\
    return cjson.encode(getpositionpostingsbydate(ARGV[1], ARGV[2], ARGV[3], ARGV[4])) \
  ';

  /*
  * scriptgetaccountsummary
  * calculates account p&l, margin & equity for a client
  * params: accountid, brokerid
  * returns: array of values as JSON string
  */
  exports.scriptgetaccountsummary = getaccountsummary + '\
    return cjson.encode(getaccountsummary(ARGV[1], ARGV[2])) \
  ';

 /*
  * scriptvaluation
  * value a portfolio as at a date
  * params: accountid, brokerid, value date, end of value date in milliseconds
  * returns: array of positions with their associated values in JSON
  */
  exports.scriptvaluation = getpositionvaluebydate + getsymbollongname + '\
    redis.log(redis.LOG_NOTICE, "scriptvaluation") \
    local accountid = ARGV[1] \
    local brokerid = ARGV[2] \
    local valuedate = ARGV[3] \
    local valuedatems = ARGV[4] \
    local tblvaluation = {} \
    --[[ get the ids for the positions held by this account ]] \
    local positionids = redis.call("smembers", "broker:" .. brokerid .. ":account:" .. accountid .. ":positions") \
    --[[ calculate each position together with value as at the value date ]] \
    for index = 1, #positionids do \
      local position = getpositionvaluebydate(brokerid, positionids[index], valuedate, valuedatems) \
      if position["quantity"] ~= 0 then \
        position.symbollongname = getsymbollongname(position.symbolid) \
        position.cost = tonumber(round(tonumber(position.cost), 2)) \
        position.symbolcountrycodeid = redis.call("hget", "symbol:" .. position.symbolid, "countrycodeid" ) \
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
  exports.newclientfundstransfer = round + newtransaction + newposting + updateaccountbalanceuncleared + updateaccountbalance + getbrokeraccountsmapid + addunclearedcashlistitem + creditcheckwithdrawal + utils.getparentlinkdetails + utils.createaddtionalindex + '\
    redis.log(redis.LOG_NOTICE, "newclientfundstransfer") \
    local action = tonumber(ARGV[1]) \
    local paymenttypeid = ARGV[8] \
    local transactionid \
    local linkdetails = getparentlinkdetails(ARGV[3], ARGV[4])\
    if paymenttypeid == "DCR" then \
      if action == 1 then \
        transactionid = newtransaction(ARGV[2], ARGV[3], ARGV[5], ARGV[6], ARGV[7], ARGV[9], ARGV[10], ARGV[11], "CRR", ARGV[12], linkdetails[1], linkdetails[2]) \
        newposting(ARGV[4], ARGV[2], ARGV[3], ARGV[6], "", transactionid, ARGV[12], ARGV[11], linkdetails[1], linkdetails[2]) \
        updateaccountbalanceuncleared(ARGV[4], ARGV[2], ARGV[3], ARGV[6]) \
        local clientsettlementaccountid = getbrokeraccountsmapid(ARGV[3], ARGV[5], "Client settlement") \
        newposting(clientsettlementaccountid, -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6]), "", transactionid, ARGV[12], ARGV[11], linkdetails[1], linkdetails[2]) \
        updateaccountbalance(clientsettlementaccountid, -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6])) \
        addunclearedcashlistitem(ARGV[3], ARGV[13], ARGV[4], transactionid, ARGV[11], ARGV[12], ARGV[8], ARGV[5], linkdetails[1], linkdetails[2]) \
      end \
    elseif paymenttypeid == "BAC" or paymenttypeid == "FP" or paymenttypeid == "CHAPS" or paymenttypeid == "CHQ" then \
      if action == 1 then \
        transactionid = newtransaction(ARGV[2], ARGV[3], ARGV[5], ARGV[6], ARGV[7], ARGV[9], ARGV[10], ARGV[11], "CAR", ARGV[12], linkdetails[1], linkdetails[2]) \
        newposting(ARGV[4], ARGV[2], ARGV[3], ARGV[6], "", transactionid, ARGV[12], ARGV[11], linkdetails[1], linkdetails[2]) \
        updateaccountbalance(ARGV[4], ARGV[2], ARGV[3], ARGV[6]) \
        local clientfundsaccount = getbrokeraccountsmapid(ARGV[3], ARGV[5], "Client funds") \
        newposting(clientfundsaccount, -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6]), "", transactionid, ARGV[12], ARGV[11], linkdetails[1], linkdetails[2]) \
        updateaccountbalance(clientfundsaccount, -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6])) \
      else \
        if creditcheckwithdrawal(ARGV[3], ARGV[4], ARGV[2]) == 1 then \
          return {1, "Insufficient cleared funds"} \
        end \
        transactionid = newtransaction(ARGV[2], ARGV[3], ARGV[5], ARGV[6], ARGV[7], ARGV[9], ARGV[10], ARGV[11], "CAP", ARGV[12], linkdetails[1], linkdetails[2]) \
        newposting(ARGV[4], -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6]), "", transactionid, ARGV[12], ARGV[11], linkdetails[1], linkdetails[2]) \
        updateaccountbalance(ARGV[4], -tonumber(ARGV[2]), ARGV[3], -tonumber(ARGV[6])) \
        local clientfundsaccount = getbrokeraccountsmapid(ARGV[3], ARGV[5], "Client funds") \
        newposting(clientfundsaccount, ARGV[2], ARGV[3], ARGV[6], "", transactionid, ARGV[12], ARGV[11], linkdetails[1], linkdetails[2]) \
        updateaccountbalance(clientfundsaccount, ARGV[2], ARGV[3], ARGV[6]) \
      end \
    end \
    if transactionid then \
      redis.call("sadd", "broker:" .. ARGV[3] .. ":transaction:clientfundtransfers", transactionid) \
      createaddtionalindex(ARGV[3], "transaction:clientfundtransfers", transactionid, linkdetails[1], linkdetails[2]) \
      local newreference = ARGV[10] \
      if  newreference == "" then \
        if action == 1 then \
          newreference = paymenttypeid .. " IN " .. transactionid \
        else \
          newreference = paymenttypeid .. " OUT " .. transactionid \
        end \
        redis.call("hset", "broker:" .. ARGV[3] .. ":transaction:" .. transactionid, "reference", newreference) \
      end \
    end \
   return {0, transactionid} \
  ';
  /*
  * scriptgetstatement
  * prepares a statement for an account between two dates
  * params: accountid, brokerid, start date, end date
  */
  exports.scriptgetstatement = utils.filter + getpostingsbydate + utils.sorttransaction + getclientidbyaccount + utils.getlimitopeningbalance + '\
    redis.log(redis.LOG_NOTICE, "scriptgetstatement") \
    local tblresults = {} \
    local tbloutput = {} \
    local balance = 0 \
    local clientdata \
    local posting = {} \
    --[[ get all the postings up to the end date ]] \
    local postings = getpostingsbydate(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[6]) \
    table.sort(postings, sorttransaction) \
    balance = getlimitopeningbalance(ARGV[2], ARGV[1], ARGV[3], ARGV[6]) \
    tbloutput.openingbalance = balance \
    --[[ go through all the postings, adjusting the cleared/uncleared balances as we go ]] \
    for index = 1, #postings do \
      posting = postings[index] \
      balance = balance + tonumber(posting.amount) \
      posting.clientid = getclientidbyaccount(ARGV[2], posting.accountid) \
      clientdata = redis.call("hmget", "broker:" .. ARGV[2] .. ":client:" .. posting.clientid, "name", "address") \
      posting.clientname = clientdata[1] \
      posting.clientaddress = clientdata[2] \
      posting.date = ARGV[5] \
      posting.balance = balance \
      table.insert(tblresults, posting) \
    end \
    tbloutput.closingbalance = balance \
    tbloutput.data = tblresults \
    return cjson.encode(tbloutput) \
  ';
  /*
  * scriptgethistory
  * prepares a history for an account from beginning to now
  * params: accountid, brokerid
  */
  exports.scriptsgethistory = utils.filter + getpostingsbydate + utils.sorttransaction + utils.getlimitopeningbalance + '\
    redis.log(redis.LOG_NOTICE, "scriptsgethistory") \
    local tblresults = {} \
    local tbloutput = {} \
    local balance = 0 \
    local posting = {} \
    --[[ get all the postings up to the end date ]] \
    local postings = getpostingsbydate(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[6]) \
    table.sort(postings, sorttransaction) \
    balance = getlimitopeningbalance(ARGV[2], ARGV[1], ARGV[3], ARGV[6]) \
    tbloutput["openingbalance"] = balance \
    --[[ go through all the postings, adjusting the cleared/uncleared balances as we go ]] \
    for index = 1, #postings do \
      posting = postings[index] \
      balance = balance + tonumber(posting.amount) \
      posting.balance = balance \
      table.insert(tblresults, posting) \
    end \
    tbloutput["closingbalance"] = balance \
    tbloutput["data"] = tblresults \
    return cjson.encode(tbloutput) \
  ';

 /*
  * cavalidate
  * validate a corporate action
  * params: corporateaction
  * returns: 0 if ok, else 1, error message if invalid
  */
  cavalidate = gethashvalues + geteodprice + getdateindex + '\
  local cavalidate = function(corporateaction) \
    if corporateaction.processed == 1 then \
      return {1, "Corporate action already applied"} \
    end \
    --[[ validate the symbol ]] \
    local symbol = gethashvalues("symbol:" .. corporateaction.symbolid) \
    if not symbol.symbolid then \
      return {1, "Symbol: " .. symbol.symbolid .. " not found"} \
    end \
    --[[ validate the benefits ]] \
    local benefit = {} \
    local benefits = redis.call("smembers", "corporateaction:" .. corporateaction.corporateactionid .. ":benefits") \
    if #benefits == 0 then \
      return {1, "No benefits added to apply"} \
    end \
    for i = 1, #benefits do \
      benefit = gethashvalues("corporateaction:" .. corporateaction.corporateactionid .. ":benefit:" .. benefits[i]) \
      if benefit.benefittypeid == "SECU" then \
        --[[ validate the benefit symbol ]] \
        local symbol = gethashvalues("symbol:" .. benefit.symbolid) \
        if not symbol.symbolid then \
          return {1, "Symbol: " .. benefit.symbolid .. " not found"} \
        end \
        --[[ get the closing price at the ex-date ]] \
        local eodprice = geteodprice(corporateaction.exdate, benefit.symbolid) \
        if not eodprice["bid"] then \
          return {1, "There needs to be an end of day price for " .. benefit.symbolid .. " as at " .. getdateindex(corporateaction.exdate)} \
        end \
      end \
    end \
    return {0} \
  end \
  ';
  /*
   * updatecaaccountindex()
   * update index of corporateaction into broker level
   * params: corporateactionid, symbolid, exdatems
   */
  updatecaaccountindex = getpositionsbysymbolbydate + '\
    local updatecaaccountindex = function(corporateactionid, symbolid, exdatems) \
      local positions \
      local brokers = redis.call("sort", "brokers") \
      for i = 2, #brokers do \
        positions = getpositionsbysymbolbydate(brokers[i], symbolid, exdatems) \
        for j = 1, #positions do \
          if tonumber(positions[j].quantity) > 0 then \
            redis.call("sadd", "broker:" .. brokers[i] .. ":account:" .. positions[j].accountid .. ":corporateactions", corporateactionid) \
            redis.call("sadd", "corporateaction:" .. corporateactionid .. ":beneficiaries", brokers[i] .. ":" .. positions[j].accountid) \
          end \
        end \
      end \
    end \
  ';
  exports.updatecaaccountindex = updatecaaccountindex;
  /*
   * removecaaccountindex()
   * remove index of corporateaction from broker level
   * params: corporateactionid
   */
  removecaaccountindex = split + '\
    local removecaaccountindex = function(corporateactionid) \
      local beneficiaries = redis.call("smembers", "corporateaction:" .. corporateactionid .. ":beneficiaries") \
      local ids \
      for i = 1, #beneficiaries do \
        ids = split(beneficiaries[i], ":") \
        redis.call("srem", "broker:" .. ids[1] .. ":account:" .. ids[2] .. ":corporateactions", corporateactionid) \
      end \
      redis.call("del", "corporateaction:" .. corporateactionid .. ":beneficiaries") \
    end \
  ';
  exports.removecaaccountindex = removecaaccountindex;
  /*
  * caapply()
  * script to apply a corporate action
  * params: 1=corporateactionid, 2=exdatems, 3=timestamp, 4=timestampms, 5=mode (1=test,2=apply)
  * returns: 0, total position quantity, unsettled quantity, number of accounts, number of brokers, total benefits if successful, else 1 + an error message if unsuccessful
  */
  exports.caapply = cavalidate + getpositionquantitiesbysymbolbydate + applybenefits + newpositiontransaction + utils.updatecastatus + updatecaaccountindex + removecaaccountindex + '\
    redis.log(redis.LOG_NOTICE, "caapply") \
    redis.call("RPUSH", "cadebug", "caapply") \
    local corporateactionid = ARGV[1] \
    local exdatems = ARGV[2] \
    local timestamp = ARGV[3] \
    local timestampms = ARGV[4] \
    local mode = tonumber(ARGV[5]) \
    local totquantity = 0 \
    local totunsettledquantity = 0 \
    local numaccounts = 0 \
    local totalbenefits = {} \
    --[[ get the corporate action ]] \
    local corporateaction = gethashvalues("corporateaction:" .. corporateactionid) \
    if not corporateaction.corporateactionid then \
      return {1, "Corporate action not found"} \
    end \
    --[[ validate it ]] \
    local ret = cavalidate(corporateaction) \
    if ret[1] == 1 then \
      return ret \
    end \
    --[[ we are applying across all positions, so need to interate through all brokers ]] \
    local brokers = redis.call("smembers", "brokers") \
    for j = 1, #brokers do \
      --[[ get all positions in the symbol of the corporate action as at the ex-date for this broker ]] \
      local positions = getpositionquantitiesbysymbolbydate(brokers[j], corporateaction.symbolid, exdatems) \
      for i = 1, #positions do \
        --[[ may have a position with no quantity ]] \
        if tonumber(positions[i].quantity) ~= 0 then \
          --[[ apply the benefits ]] \
          local benefits = applybenefits(brokers[j], positions[i], corporateaction, timestamp, timestampms, mode) \
          if benefits[1] == 1 then \
            return benefits \
          end \
          for k = 1, #benefits[2] do \
            local isbenefitintotal = 0 \
            --[[ add the benefit to the total ]] \
            for l = 1, #totalbenefits do \
              if tonumber(totalbenefits[l].benefitid) == tonumber(benefits[2][k].benefitid) then \
                if totalbenefits[l].benefittypeid == "CASH" then \
                  totalbenefits[l].cash = totalbenefits[l].cash + benefits[2][k].cash \
                elseif totalbenefits[l].benefittypeid == "SECU" then \
                  totalbenefits[l].shares = totalbenefits[l].shares + benefits[2][k].shares \
                  totalbenefits[l].residuecash = totalbenefits[l].residuecash + benefits[2][k].residuecash \
                end \
                isbenefitintotal = 1 \
                break \
              end \
            end \
            if isbenefitintotal == 0 then \
              table.insert(totalbenefits, benefits[2][k]) \
            end \
          end \
          totquantity = totquantity + tonumber(positions[i].quantity) \
          totunsettledquantity = totunsettledquantity + tonumber(positions[i].unsettledquantity) \
          numaccounts = numaccounts + 1 \
        end \
      end \
    end \
    if totquantity == 0 then \
      return {1, "No position found to apply corporateaction"} \
    end \
    if mode == 2 then \
      updatecastatus(corporateactionid, 1) \
    end \
    --[[ delete old account level index and create new index, this will update the postion which is created after corporate action created  ]] \
    removecaaccountindex(corporateactionid) \
    updatecaaccountindex(corporateactionid, corporateaction.symbolid, exdatems) \
    redis.call("RPUSH", "cadebug", "end caapply") \
    return {0, tostring(totquantity), tostring(totunsettledquantity), tostring(numaccounts), #brokers - 1, cjson.encode(totalbenefits)} \
  ';

 /*
  * cavalidateclients()
  * script is used to validate list of all clients from the list
  * params: brokerid, corporateactionid, exdate, exdatems, timestamp, timestampms, mode (1=test,2=apply,3=reverse), localtimestamp
  * returns: 0, total position quantity, total shares due, total residue cash if ok, else 1 + an error message if unsuccessful
  */
  cavalidateclients = getclientidfromaccount + '\
    local cavalidateclients = function(symbolid) \
      redis.log(redis.LOG_NOTICE, "cavalidateclients") \
      local brokers = redis.call("smembers", "brokers") \
      for j = 1, #brokers do \
        local positionids = redis.call("smembers", "broker:" .. brokers[j] .. ":symbol:" .. symbolid .. ":positions") \
        for i = 1, #positionids do \
          local position = redis.call("hmget", "broker:" .. brokers[j] .. ":position:" .. positionids[i], "quantity", "accountid") \
          if tonumber(position[1]) > 0 then \
            local clientid = getclientidfromaccount(position[2], brokers[j]) \
            if not clientid then \
              return {1, 1017} \
            end \
          end \
        end \
      end \
      return {0} \
    end \
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
  * scriptcheckschemebalance
  * script to check available scheme balance
  * params: brokerid, schemebalance, schemeclients, cashisfixed, cashamount
  * returns: 0 if ok, else 1 + an error message if unsuccessful
  */
  scriptcheckschemebalance = getclientaccountid + '\
    local scriptcheckschemebalance = function(brokerid, schemebalance, schemeclients, cashisfixed, cashamount) \
      redis.log(redis.LOG_NOTICE, "scriptcheckschemebalance") \
      local amount = 0 \
      if cashisfixed == 1 then \
        amount = cashamount * #schemeclients \
      else \
        for i = 1, #schemeclients do \
          local accountid = getclientaccountid(brokerid, schemeclients[i], 1) \
          local accountbalance = redis.call("hget", "broker:" .. brokerid .. ":account:" .. accountid, "balance") \
          amount = amount + math.floor(tonumber(accountbalance) * cashamount / 100) \
        end \
      end \
      if schemebalance >= amount then \
        return {0} \
      else \
        return {1, 1042} \
      end \
    end \
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
  *         futsettdate
  *         localtimestamp
  * returns: 0, scheme cash available, scheme cash invested, number of clients, array of scheme trades {fund id, price, quantity, consideration} if ok, else 1, errorcode
  */
  exports.newcollectaggregateinvest = split + getclientaccountid + getaccount + newtrade + scriptcheckschemebalance + '\
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
    local futsettdate = ARGV[10] \
    local localtimestamp = ARGV[11] \
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
    local checkschemecash = scriptcheckschemebalance(brokerid, tonumber(schemeaccount["balance"]), schemeclients, cashisfixed, cashamount) \
    if checkschemecash[1] == 1 then \
      return checkschemecash \
    end \
    for i = 1, #schemeclients do \
      redis.log(redis.LOG_NOTICE, "client: " .. schemeclients[i]) \
      --[[ get the default account for this client ]] \
      local accountid = getclientaccountid(brokerid, schemeclients[i], 1) \
      local account = getaccount(accountid, brokerid) \
      --[[ calculate how much cash is available ]] \
      local amount \
      if cashisfixed == 1 then \
        amount = 0 \
        if tonumber(account["balance"]) >= cashamount then \
          amount = cashamount \
        end \
      else \
        amount = math.floor(tonumber(account["balance"]) * cashamount / 100) \
      end \
      redis.log(redis.LOG_NOTICE, "cash available: " .. amount) \
      --[[ loop through the fundallocations & determine how much to invest in each ]] \
      if amount > 0 then \
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
            newtrade(accountid, brokerid, schemeclients[i], "", fundallocations[j], 1, quantity, symbol["ask"], account["currencyid"], 1, 1, 0, 0, 0, 0, 0, 0, 3, 0, "", futsettdate, timestamp, "", "", account["currencyid"], settlcurramt, 1, 0, 0, 2, operatorid, 0, timestampms, "000", "", "", "", "000", timestamp) \
          end \
          schemetrades[k]["quantity"] = schemetrades[k]["quantity"] + quantity \
          schemetrades[k]["settlcurramt"] = schemetrades[k]["settlcurramt"] + settlcurramt \
        end \
        k = k + 1 \
      end \
      ret["schemecashavailable"] = ret["schemecashavailable"] + amount \
      ret["numclients"] = ret["numclients"] + 1 \
    end \
    end \
    for i = 1, #schemetrades do \
      if schemetrades[i]["quantity"] > 0 and mode == 2 then \
        --[[ create a trade for the scheme for this fund ]] \
        newtrade(scheme["accountid"], brokerid, schemeclientid, "", schemetrades[i]["symbolid"], 1, schemetrades[i]["quantity"], schemetrades[i]["price"], schemeaccount["currencyid"], 1, 1, 0, 0, 0, 0, 0, 0, 3, 0, "", futsettdate, timestamp, "", "", schemeaccount["currencyid"], schemetrades[i]["settlcurramt"], 1, 0, 0, 2, operatorid, 0, timestampms, "000", "", "", "", "000", timestamp) \
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
  exports.scriptdeletetrade = deletetrade + '\
    redis.log(redis.LOG_NOTICE, "scriptdeletetrade") \
    local fields = {} \
    fields.brokerid = ARGV[1] \
    fields.tradeid = ARGV[2] \
    fields.timestamp = ARGV[3] \
    fields.timestampms = ARGV[4] \
    --[[ get the original trade ]] \
    local trade = gethashvalues("broker:" .. fields.brokerid .. ":trade:" .. fields.tradeid) \
    deletetrade(trade, fields.timestamp, fields.timestampms) \
    return {0, "success"} \
  ';
  /*
  * updatetradeindexes()
  * processing to update the trade indexes
  * params: 1=brokerid, 2=tradeid, 3=oldclientid, 4=clientid, 5=oldaccountid, 6=accountid, 7=oldsymbolid, 8=symbolid, 9=oldfutsettdate, 10=futsettdate,  11=oldtimestamp, 12=timestamp, 13=timestampms, 14=newside, 15=existingside
  * returns: 0, CREST message type to send or empty string if successful else 1, error message
  */
  updatetradeindexes =  utils.getparentlinkdetails + utils.lowercase + utils.trim + utils.getdateindex + utils.gettimestampindex + utils.updateaddtionaltimeindex + utils.deleteaddtionaltimeindex + '\
    local updatetradeindexes = function(brokerid, tradeid, oldclientid, clientid, oldaccountid, accountid, oldsymbolid, symbolid, oldfutsettdate, futsettdate,  oldtimestamp, timestamp, timestampms, newside, existingside) \
      redis.log(redis.LOG_NOTICE, "updatetradeindexes") \
      if oldclientid ~= clientid or oldaccountid ~= accountid or oldsymbolid ~= symbolid or oldfutsettdate ~= futsettdate or oldtimestamp ~= timestamp then \
        local brokerkey = "broker:" .. brokerid \
        local tradekey = tradeid \
        local oldlinkdetails = {} \
        local newlinkdetails = getparentlinkdetails(brokerid, accountid) \
        if #tradeid > 8 then \
          tradekey = substring(tradeid, 3) \
        end \
        local oldsearhkey = oldclientid .. ":" .. oldaccountid .. ":" .. lowercase(tradeid) .. ":" .. lowercase(trim(oldsymbolid)) .. ":" .. lowercase(getdateindex(oldfutsettdate)) .. ":" .. lowercase(gettimestampindex(oldtimestamp)) \
        local searchkey = clientid .. ":" .. accountid .. ":" .. lowercase(tradeid) .. ":" .. lowercase(trim(symbolid)) .. ":" .. lowercase(getdateindex(futsettdate)) .. ":" .. lowercase(gettimestampindex(timestamp)) \
        redis.call("zrem", brokerkey .. ":trade:search_index", tradekey, oldsearhkey) \
        redis.call("zadd", brokerkey .. ":trade:search_index", tradekey, searchkey) \
        if oldaccountid ~= accountid then \
          oldlinkdetails = getparentlinkdetails(brokerid, oldaccountid) \
          deleteaddtionaltimeindex(brokerid, "trade:search_index", "", oldsearhkey, oldlinkdetails[1], oldlinkdetails[2]) \
        else \
          deleteaddtionaltimeindex(brokerid, "trade:search_index", "", oldsearhkey, newlinkdetails[1], newlinkdetails[2]) \
        end \
        updateaddtionaltimeindex(brokerid, "trade:search_index", searchkey, newlinkdetails[1], newlinkdetails[2], tradekey) \
        if oldaccountid ~= accountid or oldtimestamp ~= timestamp or oldfutsettdate ~= futsettdate then \
          --[[ when accountid changed, trade index removed from old account and get parentdetails of the old account ]] \
          if oldaccountid ~= accountid then \
            redis.call("zrem", brokerkey .. ":account:" .. oldaccountid .. ":tradesbydate", tradeid) \
          end \
          --[[ when accountid or timestamp changed, tradesbydate index will be updated ]] \
          if oldaccountid ~= accountid or oldtimestamp ~= timestamp then \
            redis.call("zadd", brokerkey .. ":account:" .. accountid .. ":tradesbydate", timestampms, tradeid) \
          end \
          --[[ when timestamp changed, timestamp index will be updated ]] \
          if oldtimestamp ~= timestamp then \
            redis.call("zadd", brokerkey .. ":trade:timestamp", timestampms, tradeid) \
            if oldaccountid ~= accountid then \
              deleteaddtionaltimeindex(brokerid, "trade:timestamp", "", tradeid, oldlinkdetails[1], oldlinkdetails[2]) \
            end \
            updateaddtionaltimeindex(brokerid, "trade:timestamp", tradeid, newlinkdetails[1], newlinkdetails[2], timestampms) \
          end \
          --[[ when settlementdate changed, settlementdate index will be updated ]] \
          if oldfutsettdate ~= futsettdate then \
            redis.call("zadd", brokerkey .. ":trade:settlementdate", futsettdate, tradeid) \
            if oldaccountid ~= accountid then \
              deleteaddtionaltimeindex(brokerid, "trade:settlementdate", "", tradeid, oldlinkdetails[1], oldlinkdetails[2]) \
            end \
            updateaddtionaltimeindex(brokerid, "trade:settlementdate", tradeid, newlinkdetails[1], newlinkdetails[2], futsettdate) \
          end \
          if tonumber(newside) ~= tonumber(existingside) then \
            if tonumber(newside) == 2 then \
              redis.call("zadd", brokerkey .. ":account:" .. accountid .. ":unclearedcashbreakdown", timestampms, "trade:" .. tradeid) \
            else \
              redis.call("zrem", brokerkey .. ":account:" .. accountid .. ":unclearedcashbreakdown", "trade:" .. tradeid) \
            end \
          end \
        end \
      end \
      return {0} \
    end \
  ';
  exports.updatetradeindexes = updatetradeindexes + '\
    return updatetradeindexes(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13], ARGV[14], ARGV[15]) \
  ';
  /*
  * scriptedittrade()
  * processing to edit a trade
  * params: 1=accountid, 2=brokerid, 3=clientid, 4=orderid, 5=symbolid, 6=side, 7=quantity, 8=price, 9=currencyid, 10=currencyratetoorg, 11=currencyindtoorg, 12=commission, 13=ptmlevy, 14=stampduty, 15=contractcharge, 16=stampduty, 17=counterpartyid, 18=counterpartytype, 19=markettype, 20=externaltradeid, 21=futsettdate, 22=timestamp, 23=lastmkt, 24=externalorderid, 25=settlcurrencyid, 26=settlcurramt, 27=settlcurrfxrate, 28=settlcurrfxratecalc, 29=margin, 30=operatortype, 31=operatorid, 32=finance, 33=timestampms, 34=tradeid, 35=tradesettlestatusid, 36=linkedfromtrade, 37=linkedtotrade, 38=now, 39=nowms, 40=flag, 41=markettimestamp)
  * returns: 0, CREST message type to send or empty string if successful, else 1, error message
  */
  scriptedittrade = round + trim + lowercase + updatetrade + updatetradenote + deletetradetransaction + newpositiontransaction + newtradetransaction + utils.settradestatus + '\
    local scriptedittrade = function(accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, contractcharge, stampdutyid, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, operatortype, operatorid, finance, timestampms, tradeid, tradesettlestatusid, linkedfromtrade, linkedtotrade, now, nowms, flag, markettimestamp) \
      redis.log(redis.LOG_NOTICE, "scriptedittrade") \
      local rate = 1 \
      --[[ get the original trade ]] \
      local trade = gethashvalues("broker:" .. brokerid .. ":trade:" .. tradeid) \
      if trade.status == 3 then \
        return {1, "Cannot edit a deleted trade"} \
      end \
      if flag == "newtrade" then \
        --[[ we can edit but need to reverse the postings of the original trade, so update the trade and related values ]] \
        local oldtrade = {} \
        local newtrade = {} \
        --[[ existing trade going to reverse ]] \
        if tonumber(trade.side) == 2 then \
          oldtrade.quantity = trade.quantity \
          oldtrade.cost = trade.settlcurramt \
        else \
          oldtrade.quantity = -tonumber(trade.quantity) \
          oldtrade.cost = -tonumber(trade.settlcurramt) \
        end \
        if tonumber(side) == 1 then \
          newtrade.quantity = quantity \
          newtrade.cost = settlcurramt \
        else \
          newtrade.quantity = -tonumber(quantity) \
          newtrade.cost = -tonumber(settlcurramt) \
        end \
        local linkdetails = getparentlinkdetails(brokerid, accountid) \
        deletetradetransaction(trade.settlcurramt, trade.commission, trade.ptmlevy, trade.stampduty, brokerid, trade.accountid, trade.settlcurrencyid, rate, now, tradeid, trade.side, nowms) \
        newpositiontransaction(trade.accountid, trade.brokerid, oldtrade.cost, trade.futsettdate, tradeid, 5, oldtrade.quantity, trade.symbolid, now, nowms) \
        newtradetransaction(settlcurramt, commission, ptmlevy, stampduty, contractcharge, brokerid, accountid, settlcurrencyid, rate, now, tradeid, side, nowms, linkdetails[1], linkdetails[2]) \
        newpositiontransaction(accountid, brokerid, newtrade.cost, futsettdate, tradeid, 1, newtrade.quantity, symbolid, now, nowms) \
        updatetrade(accountid, brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, contractcharge, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, operatortype, operatorid, finance, nowms, tradeid, tradesettlestatusid, markettimestamp) \
        settradestatus(brokerid, tradeid, 2) \
        return {0, tradeid} \
      else \
        --[[ only minor changes, so can just update existing trade values ]] \
        if orderid and orderid ~= "" then \
          local order = redis.call("hget", "broker:" .. brokerid .. ":order:" .. orderid, "orderid") \
          if not order then \
            return {1, "Invalid orderid"} \
          end \
        end \
        updatetrade(accountid, trade.brokerid, clientid, orderid, symbolid, side, quantity, price, currencyid, currencyratetoorg, currencyindtoorg, commission, ptmlevy, stampduty, contractcharge, counterpartyid, counterpartytype, markettype, externaltradeid, futsettdate, timestamp, lastmkt, externalorderid, settlcurrencyid, settlcurramt, settlcurrfxrate, settlcurrfxratecalc, margin, trade.operatortype, trade.operatorid, finance, timestampms, trade.tradeid, tradesettlestatusid, markettimestamp) \
        if trade.futsettdate ~= futsettdate then \
          updatetradenote(brokerid, tradeid) \
        end \
        return {0, "success"} \
      end \
    end \
  ';
  /*
  * scriptedittrade()
  * params: 1=accountid, 2=brokerid, 3=clientid, 4=orderid, 5=symbolid, 6=side, 7=quantity, 8=price, 9=currencyid, 10=currencyratetoorg, 11=currencyindtoorg, 12=commission, 13=ptmlevy, 14=stampduty, 15=contractcharge, 16=stampduty, 17=counterpartyid, 18=counterpartytype, 19=markettype, 20=externaltradeid, 21=futsettdate, 22=timestamp, 23=lastmkt, 24=externalorderid, 25=settlcurrencyid, 26=settlcurramt, 27=settlcurrfxrate, 28=settlcurrfxratecalc, 29=margin, 30=operatortype, 31=operatorid, 32=finance, 33=timestampms, 34=tradeid, 35=tradesettlestatusid, 36=linkedfromtrade, 37=linkedtotrade, 38=now, 39=nowms, 40=flag, 41=markettimestamp
  */
  exports.scriptedittrade = scriptedittrade  + '\
    return scriptedittrade(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13], ARGV[14], ARGV[15], ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20], ARGV[21], ARGV[22], ARGV[23], ARGV[24], ARGV[25], ARGV[26], ARGV[27], ARGV[28], ARGV[29], ARGV[30], ARGV[31], ARGV[32], ARGV[33], ARGV[34], ARGV[35], ARGV[36], ARGV[37], ARGV[38], ARGV[39], ARGV[40], ARGV[41]) \
  ';

  /*
  * publishsystemmonitorlog()
  * publish system monitor log
  * here 15 refers to systemmonitorchannel
  * params: systemmonitorid
  */
  exports.publishsystemmonitorlog = '\
    redis.call("publish", 15, ARGV[1]) \
  ';

  /*
  * sendtomonitor()
  * store details of system monitor log
  * params: 1=timestamp, 2=timestampms, 3=message
  * returns: 0 + success message if logged successfully
  */
  sendtomonitor = '\
    local sendtomonitor = function(timestamp, timestampms, message) \
      redis.log(redis.LOG_NOTICE, "sendtomonitor") \
      local systemmonitorid = redis.call("incr", "systemmonitor:lastindex") \
      redis.call("hmset", "systemmonitor:" .. systemmonitorid, "id", systemmonitorid, "message", message, "timestamp", timestamp) \
      redis.call("sadd", "systemmonitor:systemmonitors", systemmonitorid) \
      redis.call("zadd", "systemmonitor:systemmonitorbydate", timestampms, systemmonitorid) \
      return {0, systemmonitorid} \
    end \
  ';

  exports.sendtomonitor = sendtomonitor + '\
    return sendtomonitor(ARGV[1], ARGV[2], ARGV[3]) \
  ';

  /*
  * getsystemmonitorlogs()
  * get details of system monitor log
  * params: 1=page, 2=limit, 3=starttimestamp, 4=endtimestamp
  * returns: system monitor logs in json format
  */
  getsystemmonitorlogs = gethashvalues + '\
    local getsystemmonitorlogs = function(page, limit, starttimestamp, endtimestamp, sortorder) \
      redis.log(redis.LOG_NOTICE, "getsystemmonitorlogs") \
      local tblsystemmonitorlogs = {} \
      local systemmonitorlogids = {} \
      local from = (tonumber(page) * tonumber(limit)) - tonumber(limit) \
      if sortorder and sortorder == "asc" then \
        systemmonitorlogids = redis.call("zrangebyscore", "systemmonitor:systemmonitorbydate", starttimestamp, endtimestamp, "limit", from, limit) \
      else \
        systemmonitorlogids = redis.call("zrevrangebyscore", "systemmonitor:systemmonitorbydate", endtimestamp, starttimestamp, "limit", from, limit) \
      end \
      if #systemmonitorlogids > 0 then \
        for i = 1, #systemmonitorlogids do \
          local systemmonitorlog = gethashvalues("systemmonitor:" .. systemmonitorlogids[i]) \
          table.insert(tblsystemmonitorlogs, systemmonitorlog) \
        end \
      end \
      local systemmonitorlogs = {} \
      systemmonitorlogs.totalrecords = redis.call("scard", "systemmonitor:systemmonitors") \
      systemmonitorlogs.data = tblsystemmonitorlogs \
      return cjson.encode(systemmonitorlogs) \
    end \
  ';

  exports.getsystemmonitorlogs = getsystemmonitorlogs + '\
    return getsystemmonitorlogs(ARGV[1], ARGV[2], ARGV[3], ARGV[4]) \
  ';

  /*
  * clearallsystemmonitorlogs()
  * clearall system logs
  */
  clearallsystemmonitorlogs = '\
    local clearallsystemmonitorlogs = function() \
      redis.log(redis.LOG_NOTICE, "clearallsystemmonitorlogs") \
      local systemmonitorlogs = redis.call("smembers", "systemmonitor:systemmonitors") \
      for i = 1, #systemmonitorlogs do \
        redis.call("del", "systemmonitor:" .. systemmonitorlogs[i]) \
      end \
      redis.call("del", "systemmonitor:systemmonitorbydate") \
      redis.call("del", "systemmonitor:systemmonitors") \
      redis.call("del", "systemmonitor:lastindex") \
    end \
  ';

  exports.clearallsystemmonitorlogs = clearallsystemmonitorlogs + '\
    return clearallsystemmonitorlogs() \
  ';
}
