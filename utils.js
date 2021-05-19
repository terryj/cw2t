/****************
* utils.js
* Following function are only used in commonbo and tradeserver files.
* - getTimezone(), getOffset(), convertTimeUTCtoLocal() and convertToLocalTime()
* Changes:
* 24 Mar 2017 - modified round
* 10 Aug 2018 - changed round() to use math functions and return a numeric value
*             - added formattostring() to format a number
* 17 Aug 2018 - added brokerid parameter to updatecastatus()
****************/
var Moment = require('moment');
var async = require('async');

exports.utils = function () {

 /*
  * getTimezone()
  * used to get timezone details
  * params: brokerid, userid, cb
  * returns: timezone
  */

  function getTimezone(brokerid, orderid, cb) {
    db.eval(gettimezone, 0, brokerid, orderid, function(err, result) {
      if (err) {
        console.log(err);
        return;
      }
      if (result[0] === 1) {
        cb(result[1]);
      } else {
        cb(null, JSON.parse(result[1]));
      }
    });
  }

  exports.getTimezone = getTimezone;

 /*
  * getOperatorTimezone()
  * used to get timezone details
  * params: operatortype, brokerid, operatorid, cb
  * returns: timezone
  */
  function getOperatorTimezone(operatorType, brokerId, operatorId, cb) {
    db.eval(getoperatortimezone, 0, operatorType, brokerId, operatorId, function(err, result) {
      if (err) {
        console.log(err);
        return cb(err);
      }
      if (result[0] === 1) {
        return cb(result[1]);
      } else {
        cb(null, JSON.parse(result[1]));
      }
    });
  }

  exports.getOperatorTimezone = getOperatorTimezone;

 /*
  * getOffset()
  * used to get users timezone offset difference from UTC
  * params: timzone object, timestamp
  * returns: utcoffset
  */

  function getOffset(timezone, timestamp) {
    var possibleDateFormats = ['YYYYMMDD', 'DD-MMM-YYYY', 'YYYYMMDD HH:mm:ss'];
    if (timezone.dstfrom && timezone.dstto) {
      if ((Moment(timestamp, possibleDateFormats).isSame(Moment(timezone.dstfrom, possibleDateFormats)) ||
        Moment(timestamp, possibleDateFormats).isAfter(Moment(timezone.dstfrom, possibleDateFormats))) &&
        (Moment(timestamp, possibleDateFormats).isBefore(Moment(timezone.dstto, possibleDateFormats)) ||
          Moment(timestamp, possibleDateFormats).isSame(Moment(timezone.dstto, possibleDateFormats)))) {
        return timezone.utcdstoffset.replace(':', '');
      } else {
        return timezone.utcoffset.replace(':', '');
      }
    } else {
      return timezone.utcoffset.replace(':', '');
    }
  }

  exports.getOffset = getOffset;

 /*
  * convertTimeUTCtoLocal()
  * used to convert UTC timestamp to local timestamp
  * params: timzone object, utctimestamp
  * returns: userlocaltimestamp
  */

  function convertTimeUTCtoLocal(timezone, timestamp) {
    var date;
    // check timestamp in this(YYYYMMDD-HH:mm:ss) format or not.
    if (Moment(timestamp, 'YYYYMMDD-HH:mm:ss', true).isValid()) {
      date = new Moment(timestamp, 'YYYYMMDD-HH:mm:ss');
    } else {
      date = new Moment(new Date(timestamp));
    }
    // apply user timezone offset to UTC time
    date.utcOffset(getOffset(timezone, date.format('DD-MMM-YYYY')));
    return date.format('YYYYMMDD-HH:mm:ss');
  }

  exports.convertTimeUTCtoLocal = convertTimeUTCtoLocal;

 /*
  * convertToLocalTime()
  * used to apply local timezone convertion for timestamp
  * params: brokerid, userid, timestamp, cb
  * returns: userlocaltimestamp
  */

  function convertToLocalTime(brokerid, orderid, timestamp, cb) {
    getTimezone(brokerid, orderid, function(err, timezone) {
      if (err) {
        return cb(err);
      }
      if (Object.prototype.toString.call(timestamp) === '[object Array]') {
        for (i = 0; i < timestamp.length; i++) {
          timestamp[i] = convertTimeUTCtoLocal(timezone, timestamp[i]);
        }
        cb(null, timestamp);
      } else {
        cb(null, convertTimeUTCtoLocal(timezone, timestamp));
      }
    });
  }

  exports.convertToLocalTime = convertToLocalTime;

 /*
  * convertTimestamp()
  * used to convert UTC timestamp to operator timezone
  * params: timzone object, utctimestamp
  * returns: operator local timestamp
  */
  function convertTimestamp(timezone, timestamp) {
    var dateTime;
    // check timestamp in this(YYYYMMDD-HH:mm:ss) format or not.
    if (new Moment(timestamp, 'YYYYMMDD-HH:mm:ss', true).isValid()) {
      dateTime = new Moment(timestamp, 'YYYYMMDD-HH:mm:ss');
    } else {
      dateTime = new Moment(new Date(timestamp));
    }
    // apply user timezone offset to UTC time
    dateTime.utcOffset(getOffset(timezone, dateTime.format('DD-MMM-YYYY')));
    return {
      operatorTimestamp: dateTime.format('YYYYMMDD-HH:mm:ss'),
      timestampms: new Date(dateTime.format('DD-MMM-YYYY HH:mm:ss')).getTime()
    };
  }

  exports.convertTimestamp = convertTimestamp;

  function convertToOperatorTimezone(operatorType, brokerId, operatorId, timestamps, callback) {
    async.waterfall([
      function(cb) {
        getOperatorTimezone(operatorType, brokerId, operatorId, function(err, operatorTimezone) {
          if (err) {
            return callback(err);
          }
          cb(null, operatorTimezone);
        });
      },
      function(operatorTimezone, cb) {
        var result = [];
        async.eachSeries(timestamps, function iterator(timestamp, next) {
          result.push(convertTimestamp(operatorTimezone, timestamp));
          next();
        }, function done(err) {
          if (err) {
            return callback(err);
          } else {
            callback(null, result);
          }
        });
      }
    ]);
  }

  exports.convertToOperatorTimezone = convertToOperatorTimezone;
  /*
   * find()
   * function is used to find given values is exists or not in index list
   * params: index list, value
   * return: if success return true, else return false
   */
  find = '\
    local find = function(list, item) \
      for i = 1, #list do \
        if list[i] == item then \
          return true \
        end \
      end \
      return false \
    end \
  ';
  /*
   * filter()
   * function is used to filter indexes based on removed list
   * params: index list, removed index
   * return: filtered index list
   */
  filter = find + '\
    local filter = function(list, removedlist) \
      local filteredList = {} \
      for i  = 1, #list do \
        if find(removedlist, list[i]) == false then \
          table.insert(filteredList, list[i]) \
        end \
      end \
      return filteredList \
    end \
  ';
  exports.filter = filter;
 /*
  * converttoobject()
  * Thi script used to convert result array to object
  * params: resultarray
  * return: object
  */
  converttoobject = '\
    local converttoobject = function(fields, rawvals) \
      local vals = {} \
      for index = 1, #fields do \
        vals[fields[index]] = rawvals[index] \
      end \
      return vals \
    end \
  ';
 /*
  * updatestaticfield()
  * params: id, name
  */
  updatestaticfield = '\
    local updatestaticfield = function(table, field, value) \
      redis.log(redis.LOG_NOTICE, "updatestaticfield") \
      local isdataexist = redis.call("hget", table, field) \
      if isdataexist then \
        redis.call("hmset", table, field, value) \
        return {0} \
      end \
      return {1, "record not found"} \
    end \
  ';

  exports.updatestaticfield = updatestaticfield + '\
    return updatestaticfield(ARGV[1], ARGV[2], ARGV[3]) \
  ';

  /*
   * formatotstring()
   * converts a number to a string format
   * params: number, number of decimal places
   * returns: string value
   */
  formattostring = '\
  local formattostring = function(num, dp) \
    return string.format("%0." .. dp .. "f", num) \
  end \
  ';

  exports.formattostring = formattostring;

  /*
   * round()
   * round a number
   * params: number, number of decimal points
   * returns: rounded number
   */
  round = '\
  local round = function(val, n) \
    if (n) then \
      return math.floor((val * 10^n) + 0.5) / (10^n) \
    else \
      return math.floor(val + 0.5) \
    end \
  end \
  ';

  exports.round = round;

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
   * gettimezone()
   * This script used to get of timezone by order
   * params: brokerid, orderid
   * returns: 0 + user timezone details if ok, else 1 + an error message if unsuccessful
   */
  gettimezone = gethashvalues + '\
    redis.log(redis.LOG_NOTICE, "gettimezone") \
    local temp = {} \
    local order \
    temp.brokerid = ARGV[1] \
    temp.orderid = ARGV[2] \
    if not temp.orderid then \
      return {1, "orderid should not be empty"} \
    else \
      order = redis.call("hmget", "broker:" .. temp.brokerid .. ":order:" .. temp.orderid, "orderid", "operatortype", "operatorid") \
      if not order[1] then \
        return {1, "Invaild orderid"} \
      else \
        if tonumber(order[2]) == 1 then \
          temp.usertimezoneid = redis.call("hget", "broker:" .. temp.brokerid .. ":client:" .. order[3], "timezoneid") \
        elseif tonumber(order[2]) == 2 then \
          temp.usertimezoneid = redis.call("hget", "broker:" .. temp.brokerid .. ":user:" .. order[3], "timezoneid") \
        else \
          return {1, "Invalid operator"} \
        end \
        if not temp.usertimezoneid then \
          return {1, "Timezone not found for the operator/user: " .. order[2] .. " / " .. order[3]} \
        end \
        return {0, cjson.encode(gethashvalues("timezones:" .. temp.usertimezoneid))} \
      end \
    end \
  ';

  /*
   * getoperatortimezone()
   * This script used to get of operator timezone
   * params: operatortype, brokerid, operatorid
   * returns: 0 + operator timezone details if ok, else 1 + an error message if unsuccessful
   */
  getoperatortimezone = gethashvalues + '\
    redis.log(redis.LOG_NOTICE, "getoperatortimezone") \
    local temp = {} \
    temp.operatortype = ARGV[1] \
    temp.brokerid = ARGV[2] \
    temp.operatorid = ARGV[3] \
    temp.broker = redis.call("hget", "broker:" .. temp.brokerid, "brokerid") \
    if not temp.broker then \
      return {0, "broker not found"} \
    end \
    if tonumber(temp.operatortype) == 1 then \
      temp.client = redis.call("hmget", "broker:" .. temp.brokerid .. ":client:" .. temp.operatorid, "clientid", "timezoneid") \
      if not temp.client[1] then \
        return {1, "operator not found. operatortype: " .. temp.operatortype .. ". operatorid: " .. temp.operatorid} \
      end \
      temp.timezoneid =  temp.client[2] \
    elseif tonumber(temp.operatortype) == 2 then \
      temp.user = redis.call("hmget", "broker:" .. temp.brokerid .. ":user:" .. temp.operatorid, "userid", "timezoneid") \
      if not temp.user[1] then \
        return {1, "operator not found. operatortype: " .. temp.operatortype .. ". operatorid: " .. temp.operatorid} \
      end \
      temp.timezoneid =  temp.user[2] \
    end \
    return {0, cjson.encode(gethashvalues("timezones:" .. temp.timezoneid))} \
  ';
 /*
  * getbrokeridlist()
  * get all brokers id
  */
  getbrokeridlist = '\
    local getbrokeridlist = function() \
      return redis.call("smembers", "brokers") \
    end \
  ';
  /*
  * isuservalid()
  * This script is used to validate userid
  * params: brokerid, userid
  * returns: true if success else false
  */
  isuservalid = '\
    local isuservalid = function(brokerid, userid) \
      local value = redis.call("hget", "broker:" .. brokerid .. ":user:" .. userid, "userid") \
      redis.log(redis.LOG_NOTICE, value) \
      if value then \
        return true \
      else \
        return false \
      end \
    end \
  ';

  exports.isuservalid = isuservalid + '\
    return isuservalid(ARGV[1], ARGV[2]) \
  ';
  /*
   * count()
   * This script used to count the model by key where key is the modelid name
   * params: modelidname
   * returns: model key count
   */
  count = '\
    local count = function(modelkey) \
      redis.log(redis.LOG_NOTICE, modelkey) \
      local countexec = redis.call("scard", modelkey) \
      return countexec \
    end \
  ';

  exports.count = count + '\
    return count(ARGV[1]) \
  ';
  /*
   * countbylimit()
   * This script used to count the model by key with date limit
   * params: modelidname
   * returns: model key count
   */
  countbylimit = '\
    local countbylimit = function(key, from, to) \
      redis.log(redis.LOG_NOTICE, key) \
      local countexec = redis.call("zrangebyscore", key, from, to) \
      return #countexec \
    end \
  ';
  exports.countbylimit = countbylimit + '\
    return countbylimit(ARGV[1], ARGV[2], ARGV[3]) \
  ';
  /*
   * numbersort()
   * This script used to sort number array
   * params: unsortedarray
   * returns: sorted array in ascending order
   */
  numbersort = '\
    local numbersort = function(a, b) \
      return tonumber(a) < tonumber(b) \
    end \
  ';
  /*
   * descendingorder()
   * This script used to sort number array
   * params: unsortedarray
   * returns: sorted array in descending order
   */
  descendingorder = '\
    local descendingorder = function(a, b) \
      return tonumber(a) > tonumber(b) \
    end \
  ';
  exports.descendingorder = descendingorder;
  /*
   * replace()
   * This script used to replace the char from the string
   * params: string
   * return: replaced string
   */
  replace = '\
    local replace = function(value, oldchar, newchar) \
      return string.gsub(value, oldchar, newchar) \
    end \
  ';
  exports.replace = replace;
  /*
   * sortclients()
   * This script used to sort client array
   * params: unsortedarray
   * returns: sorted array
   */
  sortclients = '\
    local sortclients = function(a, b) \
      if tonumber(a.brokerid) == tonumber(b.brokerid) then \
        return tonumber(a.clientid) < tonumber(b.clientid) \
      else \
        return tonumber(a.brokerid) < tonumber(b.brokerid) \
      end \
    end \
  ';
  exports.sortclients = sortclients;
  /*
   * sorttransaction()
   * This script used to sort transaction array
   * params: unsortedarray
   * returns: sorted array
   */
  sorttransaction = replace + '\
    local sorttransaction = function(a, b) \
      local time1 = replace(replace(a["timestamp"], "-", ""), ":", "") \
      time1 = tonumber(time1) \
      local time2 = replace(replace(b["timestamp"], "-", ""), ":", "") \
      time2 = tonumber(time2) \
      if time1 == time2 then \
        return tonumber(a["postingid"]) < tonumber(b["postingid"]) \
      else \
        return time1 < time2 \
      end \
    end \
  ';
  exports.sorttransaction = sorttransaction;
  /*
   * trim()
   * This script used to remove spaces from string
   * params: string
   * returns: string without space
   */
  trim = '\
    local trim = function(string) \
      return string:gsub("%s", "") \
    end \
  ';
  exports.trim = trim;
  /*
   * lowercase()
   * This script used to convert string to lowercase
   * params: string
   * returns: lowercase string
   */
  lowercase = '\
    local lowercase = function(string) \
      return string.lower(string) \
    end \
  ';
  exports.lowercase = lowercase;
  /*
   * uppercase()
   * This script used to convert string to uppercase
   * params: string
   * returns: uppercase string
   */
  uppercase = '\
    local uppercase = function(string) \
      return string.upper(string) \
    end \
  ';
  /*
   * firsttoupper()
   * This script used to convert first letter of the string
   * params: string
   * returns: first letter of the string changed to upper case
   */
  firsttoupper = '\
    local firsttoupper = function(string) \
      return string:gsub("^%l", string.upper) \
    end \
  ';
  exports.firsttoupper = firsttoupper;
  /*
   * substring()
   * This substring used to get substring by stringindex
   * params: string
   * returns: substring
   */
  substring = '\
    local substring = function(value, from, to) \
      return string.sub(value, from, to) \
    end \
  ';
  exports.substring = substring;
  /*
   * split()
   * This script used to split the string by given key
   * params: string
   * returns: string array
   */
  split = '\
    local split = function(value, key) \
      local str = {} \
      for word in string.gmatch(value, "([^" .. key .. "]+)") do \
        table.insert(str, word) \
      end \
      return str \
    end \
  ';
  exports.split = split;
  /*
   * getdateindex()
   * This script used to convert date into index format
   * params: date(YYYYMMDD)
   * returns: formateddate(DD-MMM-YYYY)
   */
  getdateindex = substring + '\
    local getdateindex = function(date) \
      redis.log(redis.LOG_NOTICE, "getdateindex") \
      local tempdate = {} \
      if date ~= "" then \
        tempdate.monthname = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"} \
        tempdate.year = substring(date, 1, 4) \
        tempdate.month = tonumber(substring(date, 5, 6)) \
        tempdate.day = substring(date, 7, 8) \
        return tempdate.day .. "-" .. tempdate.monthname[tempdate.month] .. "-" .. tempdate.year \
      else \
        return "" \
      end \
    end \
  ';
  exports.getdateindex = getdateindex;
  /*
   * gettimestampindex()
   * This script used to convert timestamp into index format
   * params: timestamp(YYYYMMDD-HH:mm:SS)
   * returns: formateddate(DD-MMM-YYYY-HH-mm-SS)
   */
  gettimestampindex = getdateindex + split + replace + '\
    local gettimestampindex = function(timestamp) \
      redis.log(redis.LOG_NOTICE, "gettimestampindex") \
      local temptimestamp = {} \
      temptimestamp.splitdate = split(timestamp, "-") \
      temptimestamp.date = getdateindex(temptimestamp.splitdate[1]) \
      return temptimestamp.date .. "-" .. replace(temptimestamp.splitdate[2], ":", "-") \
    end \
  ';
  exports.gettimestampindex = gettimestampindex;
  /*
   * contains()
   * This script used to check given string isexists or not
   * params: array, val
   * returns: if success return true, else return false
   */
  contains = '\
    local contains = function(array, val) \
      for i = 1, #array do \
        if array[i] == val then \
          return true \
        end \
      end \
      return false \
    end \
  ';
  /*
   * stringmatch()
   * This script used to check sub string isexists or not in given string
   * params: string, substring
   * returns: if success return true, else return false
   */
  stringmatch = '\
    local stringmatch = function(string, substring) \
      if string.match(string, substring) then \
        return true \
      else \
        return false \
      end \
    end \
  ';
  exports.stringmatch = stringmatch;
  /*
   * getaccountname()
   * script to get account name by accountid
   * params : brokerid, accountid
   * returns: 0 + account object if ok, else 1 + an error message if unsuccessful
   */
  getaccountname = '\
    local getaccountname = function(brokerid, accountid) \
      local account = redis.call("hmget", "broker:" .. brokerid .. ":account:" .. accountid, "accountid", "name") \
      if not account[1] then \
        return {1, "Account not found"} \
      else \
        return {0, account[2]} \
      end \
    end \
  ';
  exports.getaccountname = getaccountname;
  /*
   * getclientname()
   * script to get client name by clientid.
   * params : brokerid, clientid
   * returns: return clientname
   */
  getclientname = '\
    local getclientname = function(brokerid, clientid) \
      return redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "name") \
    end \
  ';
  exports.getclientname = getclientname;
  /*
   * getclientnamebyaccount()
   * script to get client name by accountid.
   * params : brokerid, accountid
   * returns: 0 + account object if ok, else 1 + an error message if unsuccessful
   */
  getclientnamebyaccount = getclientname + '\
    local getclientnamebyaccount = function(brokerid, accountid) \
      local clientid = redis.call("get", "broker:" .. brokerid .. ":account:" .. accountid .. ":client") \
      if not clientid then \
        return {nil} \
      else \
        local result = {} \
        table.insert(result, getclientname(brokerid, clientid)) \
        table.insert(result, clientid) \
        return result \
      end \
    end \
  ';
  exports.getclientnamebyaccount = getclientnamebyaccount;
  /*
   * getsymbolshortname
   * get a symbol short name
   * params: symbolid
   * returns: symbolname if ok, else nil
   */
  getsymbolshortname = '\
    local getsymbolshortname = function(symbolid) \
      local shortname = redis.call("hmget", "symbol:" .. symbolid, "shortname" ) \
      if not shortname[1] then \
        return {nil} \
      end \
      return shortname \
    end \
  ';
  exports.getsymbolshortname = getsymbolshortname;
  /*
   * getrecordbyid()
   * This script used to get record by recordid.
   * params: tblname, recordid
   * returns: 0 + record object if ok, else 1 + an error message if unsuccessful
   */
  getrecordbyid = gethashvalues + '\
    local getrecordbyid = function(tblname, recordid) \
      redis.log(redis.LOG_NOTICE, "getrecordbyid") \
      local record = gethashvalues(tblname .. ":" .. recordid) \
      return {0, record} \
    end \
  ';
  exports.getrecordbyid = getrecordbyid + '\
    local record = getrecordbyid(ARGV[1], ARGV[2]) \
    return cjson.encode(record) \
  ';
  /*
   * getclientidbyaccount()
   * script to get clientid by accountid.
   * params : brokerid, accountid
   * returns: clientid
   */
  getclientidbyaccount = '\
    local getclientidbyaccount = function(brokerid, accountid) \
      redis.log(redis.LOG_NOTICE, "getclientidbyaccount") \
      return redis.call("get", "broker:" .. brokerid .. ":account:" .. accountid .. ":client") \
    end \
  ';
  exports.getclientidbyaccount = getclientidbyaccount;
  /*
   * getparentlinkdetails()
   * script used to get parent link details client by clientaccountid.
   * params : brokerid, clientaccountid
   * returns: parentlinkdetails
   */
  getparentlinkdetails = getclientidbyaccount + '\
    local getparentlinkdetails = function(brokerid, accountid) \
      local clientid = getclientidbyaccount(brokerid, accountid) \
      if clientid then \
        return redis.call("hmget", "broker:" .. brokerid .. ":client:" .. clientid, "linktype", "linkid") \
      else \
        return \
      end \
    end \
  ';
  exports.getparentlinkdetails = getparentlinkdetails;
 /*
  * getmodelname()
  * This script used to get model name of linktype
  * params: linktype
  */
  getmodelname = '\
    local getmodelname = function(linktype) \
      if tonumber(linktype) == 2 then \
        return "businessgetter" \
      elseif tonumber(linktype) == 3 then \
        return "ifa" \
      end \
    end \
  ';
  exports.getmodelname = getmodelname;
  /*
   * getindexkey
   * This script used to get indexkey based on usertype
   * params: brokerid, linktype, linkid, indexkey
   * returns: userbasedindexkey
   */
  getindexkey = '\
    local getindexkey = function(brokerid, linktype, linkid, modelkey) \
      redis.log(redis.LOG_NOTICE, "getindexkey") \
      local indexkey = "broker:" .. brokerid \
      if tonumber(linktype) == 3 then \
        indexkey = indexkey .. ":ifa:" .. linkid \
      elseif tonumber(linktype) == 2 then \
        indexkey = indexkey .. ":businessgetter:" .. linkid \
      end \
      if modelkey ~= "" then \
        indexkey = indexkey .. ":" .. modelkey \
      end \
      redis.log(redis.LOG_NOTICE, indexkey) \
      return indexkey \
    end \
  ';
  exports.getindexkey = getindexkey;
 /*
  * addtoaddtionalindex()
  * This script used to add ids to addtional indexes
  * params: brokerid, primarykey, primaryid, linktype, linkid, searchkey, searchvalue
  */
  addtoaddtionalindex = getindexkey + '\
    local addtoaddtionalindex = function(brokerid, primarykey, primaryid, linktype, linkid, searchkey, searchvalue) \
      redis.call("sadd", getindexkey(brokerid, linktype, linkid, primarykey), primaryid) \
      if searchkey and searchkey ~= "" then \
        redis.call("zadd", getindexkey(brokerid, linktype, linkid, searchkey), primaryid, searchvalue) \
      end \
    end \
  ';
 /*
  * updateaddtionalindex()
  * This script used to create addtional indexes
  * params: brokerid, primarykey, primaryid, linktype(2=businessgetter, 3=ifa), linkid, searchkey, searchvalue
  */
  updateaddtionalindex = addtoaddtionalindex + '\
    local updateaddtionalindex = function(brokerid, primarykey, primaryid, linktype, linkid, searchkey, searchvalue) \
      redis.log(redis.LOG_NOTICE, "updateaddtionalindex") \
      if tonumber(linktype) == 2 or tonumber(linktype) == 3 then \
        if tonumber(linktype) == 2 then \
          local parentdetails = redis.call("hmget", getindexkey(brokerid, linktype, linkid, ""), "linktype", "linkid") \
          if tonumber(parentdetails[1]) == 3 then \
            addtoaddtionalindex(brokerid, primarykey, primaryid, parentdetails[1], parentdetails[2], searchkey, searchvalue) \
          end \
        end \
        addtoaddtionalindex(brokerid, primarykey, primaryid, linktype, linkid, searchkey, searchvalue) \
      end \
    end \
  ';
  exports.updateaddtionalindex = updateaddtionalindex;
  /*
   * createaddtionalindex()
   * This script used to create addtional primary indexes in ifa and bg level
   * params: brokerid, primarykey, primaryid, linktype(2=businessgetter, 3=ifa), linkid
   */
  createaddtionalindex = getindexkey + '\
    local createaddtionalindex = function(brokerid, primarykey, primaryid, linktype, linkid) \
      redis.log(redis.LOG_NOTICE, "createaddtionalindex") \
      if tonumber(linktype) == 2 or tonumber(linktype) == 3 then \
        if tonumber(linktype) == 2 then \
          local parentdetails = redis.call("hmget", getindexkey(brokerid, linktype, linkid, ""), "linktype", "linkid") \
          if tonumber(parentdetails[1]) == 3 then \
            redis.call("sadd", getindexkey(brokerid, parentdetails[1], parentdetails[2], primarykey), primaryid) \
          end \
        end \
        redis.call("sadd", getindexkey(brokerid, linktype, linkid, primarykey), primaryid) \
      end \
    end \
  ';
  exports.createaddtionalindex = createaddtionalindex;
  /*
   * removeaddtionlaindex()
   * This script used to remove addtional primary indexes in ifa and bg level
   * params: brokerid, primarykey, primaryid, linktype(2=businessgetter, 3=ifa), linkid
   * this function need to rename as deleteaddtionlaindex
   */
  removeaddtionlaindex = '\
    local removeaddtionlaindex = function(brokerid, primarykey, primaryid, linktype, linkid) \
      redis.log(redis.LOG_NOTICE, "removeaddtionlaindex") \
      if tonumber(linktype) == 2 or tonumber(linktype) == 3 then \
        if tonumber(linktype) == 2 then \
          local parentdetails = redis.call("hmget", getindexkey(brokerid, linktype, linkid, ""), "linktype", "linkid") \
          if tonumber(parentdetails[1]) == 3 then \
            redis.call("srem", getindexkey(brokerid, parentdetails[1], parentdetails[2], primarykey), primaryid) \
          end \
        end \
        redis.call("srem", getindexkey(brokerid, linktype, linkid, primarykey), primaryid) \
      end \
    end \
  ';
  exports.removeaddtionlaindex = removeaddtionlaindex;
  /*
   * createsearchindex function splited to used in trade settlement transaction searchindex update
   */
  addtionalsearchindex = '\
    local addtionalsearchindex = function(brokerid, searchkey, score, value, linktype, linkid) \
      if linktype and linkid then \
        if linktype ~= "" and linkid ~= "" then \
          if tonumber(linktype) == 2 or tonumber(linktype) == 3 then \
            if tonumber(linktype) == 2 then \
              local parentdetails = redis.call("hmget", getindexkey(brokerid, linktype, linkid, ""), "linktype", "linkid") \
              if tonumber(parentdetails[1]) == 3 then \
                --[[ create searchindex in ifa level if the bg belongs to ifa ]] \
                redis.call("zadd", getindexkey(brokerid, parentdetails[1], parentdetails[2], searchkey), score, value) \
              end \
            end \
            --[[ create searchindex in ifa or bg level ]] \
            redis.call("zadd", getindexkey(brokerid, linktype, linkid, searchkey), score, value) \
          end \
        end \
      end \
    end \
  ';
  /*
   * createsearchindex()
   * This script used to create searchindex in all level(broker, bg & ifa)
   * params: brokerid, searchkey, score, value, linktype, linkid
   * this function going to replace updateaddtionalindex and addtoaddtionalindex
   */
  createsearchindex = addtionalsearchindex + '\
    local createsearchindex = function(brokerid, searchkey, score, value, linktype, linkid) \
      redis.log(redis.LOG_NOTICE, "createsearchindex") \
      --[[ create searchindex in broker level ]] \
      redis.call("zadd", "broker:" .. brokerid .. ":" .. searchkey, score, value) \
      addtionalsearchindex(brokerid, searchkey, score, value, linktype, linkid) \
    end \
  ';
  exports.createsearchindex = createsearchindex;
  /*
   * deletesearchindex()
   * This script used to remove searchindex in all level(broker, bg & ifa)
   * params: brokerid, searchkey, value, linktype, linkid
   * this function going to replace deleteaddtionalindex and removefromaddtionalindex
   */
  deletesearchindex = '\
    local deletesearchindex = function(brokerid, searchkey, value, linktype, linkid) \
      redis.log(redis.LOG_NOTICE, "deletesearchindex") \
      --[[ remove searchindex in broker level ]] \
      redis.call("zrem", "broker:" .. brokerid .. ":" .. searchkey, value) \
      if linktype and linkid then \
        if linktype ~= "" and linkid ~= "" then \
          if tonumber(linktype) == 2 or tonumber(linktype) == 3 then \
            if tonumber(linktype) == 2 then \
              local parentdetails = redis.call("hmget", getindexkey(brokerid, linktype, linkid, ""), "linktype", "linkid") \
              if tonumber(parentdetails[1]) == 3 then \
                --[[ remove searchindex in ifa level if the bg belongs to ifa ]] \
                redis.call("zrem", getindexkey(brokerid, parentdetails[1], parentdetails[2], searchkey), value) \
              end \
            end \
            --[[ remove searchindex in ifa or bg level ]] \
            redis.call("zrem", getindexkey(brokerid, linktype, linkid, searchkey), value) \
          end \
        end \
      end \
    end \
  ';
  exports.deletesearchindex = deletesearchindex;
 /*
  * removefromaddtionalindex()
  * This script used to remove ids from addtional indexes
  * params: brokerid, primarykey, primaryid, linktype, linkid, searchkey, searchvalue
  */
  removefromaddtionalindex = getindexkey + '\
    local removefromaddtionalindex = function(brokerid, primarykey, primaryid, linktype, linkid, searchkey, searchvalue) \
      redis.call("srem", getindexkey(brokerid, linktype, linkid, primarykey), primaryid) \
      if searchkey and searchkey ~= "" then \
        redis.call("zrem", getindexkey(brokerid, linktype, linkid, searchkey), searchvalue) \
      end \
    end \
  ';
 /*
  * deleteaddtionalindex()
  * This script used to delete addtional indexes
  * params: brokerid, primarykey, primaryid, linktype, linkid, searchkey, searchvalue
  */
  deleteaddtionalindex = getindexkey + removefromaddtionalindex + '\
    local deleteaddtionalindex = function(brokerid, primarykey, primaryid, linktype, linkid, searchkey, searchvalue) \
      redis.log(redis.LOG_NOTICE, "removefromaddtionalindex") \
      if tonumber(linktype) == 2 or tonumber(linktype) == 3 then \
        if tonumber(linktype) == 2 then \
          local parentdetails = redis.call("hmget", getindexkey(brokerid, linktype, linkid, ""), "linktype", "linkid") \
          if tonumber(parentdetails[1]) == 3 then \
            removefromaddtionalindex(brokerid, primarykey, primaryid, parentdetails[1], parentdetails[2], searchkey, searchvalue) \
          end \
        end \
        removefromaddtionalindex(brokerid, primarykey, primaryid, linktype, linkid, searchkey, searchvalue) \
      end \
    end \
  ';
  exports.deleteaddtionalindex = deleteaddtionalindex;
  /*
   * getpaginatedrecordsbymodalindex
   * returns paginated records
   * params: page, limit, sortorder, modalindex, tblname and isintegerindex
   * returns: 0 + result objects if ok, else 1 + an error message if unsuccessful
   */
  getpaginatedrecordsbymodalindex = gethashvalues + '\
    local getpaginatedrecordsbymodalindex = function(page, limit, sortorder, modalindex, tblname, isintegerindex) \
      redis.log(redis.LOG_NOTICE, "getpaginatedrecordsbymodalindex") \
      local records = {} \
      local tblresults = {} \
      local from = (tonumber(page) * tonumber(limit)) - tonumber(limit) \
      local recordids \
      if isintegerindex == "true" then \
        recordids = redis.call("sort", modalindex, "limit", from, limit, sortorder) \
      else \
        recordids = redis.call("sort", modalindex, "limit", from, limit, "alpha", sortorder) \
      end \
      if #recordids >= 1 then \
        for index = 1, #recordids do \
          local record \
          if isintegerindex == "true" then \
            record = gethashvalues(tblname .. ":" .. recordids[index]) \
          else \
            record = gethashvalues(recordids[index]) \
          end \
          table.insert(records, record) \
        end \
      end \
      tblresults.data = records \
      tblresults.totalrecords = redis.call("scard", modalindex) \
      return cjson.encode(tblresults) \
    end \
  ';
  exports.getpaginatedrecordsbymodalindex = getpaginatedrecordsbymodalindex + '\
    return getpaginatedrecordsbymodalindex(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]) \
  ';
  /*
   * getallrecordsbymodalindex()
   * This script used to get all records by modal index.
   * params: modalindex, tblname, sortorder and isintegerindex
   * returns: 0 + records objects if ok, else 1 + an error message if unsuccessful
   */
  exports.getallrecordsbymodalindex = count + getpaginatedrecordsbymodalindex + '\
    redis.log(redis.LOG_NOTICE, "getallrecordsbymodalindex") \
    local recordscount = count(ARGV[1]) \
    return getpaginatedrecordsbymodalindex(1, recordscount, ARGV[3], ARGV[1], ARGV[2], ARGV[4]) \
  ';

  /*
  * updateaddtionaltimeindex()
  * This script used to add ids to trade addtional indexes
  * params: brokerid, indexkey, value, linktype, linkid, score
  */
  updateaddtionaltimeindex = getindexkey + '\
    local updateaddtionaltimeindex = function(brokerid, indexkey, value, linktype, linkid, score) \
      redis.log(redis.LOG_NOTICE, "updateaddtionaltimeindex") \
      if tonumber(linktype) == 2 or tonumber(linktype) == 3 then \
        if tonumber(linktype) == 2 then \
          local parentdetails = redis.call("hmget", getindexkey(brokerid, linktype, linkid, ""), "linktype", "linkid") \
          if tonumber(parentdetails[1]) == 3 then \
            redis.call("zadd", getindexkey(brokerid, parentdetails[1], parentdetails[2], indexkey), score, value) \
          end \
        end \
        redis.call("zadd", getindexkey(brokerid, linktype, linkid, indexkey), score, value) \
      end \
    end \
  ';
  exports.updateaddtionaltimeindex = updateaddtionaltimeindex;

  /*
  * deleteaddtionaltimeindex()
  * This script used to delete ids to trade addtional indexes
  * params: brokerid, primarykey, primaryid, linktype, linkid
  */
  deleteaddtionaltimeindex = getindexkey + '\
    redis.log(redis.LOG_NOTICE, "deleteaddtionaltimeindex") \
    local deleteaddtionaltimeindex = function(brokerid, indexkey, primaryindexkey, primaryid, linktype, linkid) \
      if tonumber(linktype) == 2 or tonumber(linktype) == 3 then \
        if tonumber(linktype) == 2 then \
          local parentdetails = redis.call("hmget", getindexkey(brokerid, linktype, linkid, ""), "linktype", "linkid") \
          if tonumber(parentdetails[1]) == 3 then \
            if primaryindexkey and primaryindexkey ~= "" then \
              redis.call("srem", getindexkey(brokerid, parentdetails[1], parentdetails[2], primaryindexkey), primaryid) \
            end \
            redis.call("zrem", getindexkey(brokerid, parentdetails[1], parentdetails[2], indexkey), primaryid) \
          end \
        end \
        if primaryindexkey and primaryindexkey ~= "" then \
          redis.call("srem", getindexkey(brokerid, linktype, linkid, primaryindexkey), primaryid) \
        end \
        redis.call("zrem", getindexkey(brokerid, linktype, linkid, indexkey), primaryid) \
      end \
    end \
  ';
  exports.deleteaddtionaltimeindex = deleteaddtionaltimeindex;
  /*
   * getcommissionstructure
   * This script used to get commission by commissionId
   * params: brokerid, commissionid
   * returns: 0 + commission if ok, else 1 + an error message if unsuccessful
   */
  getcommissionstructure = gethashvalues + '\
    local getcommissionstructure = function(brokerid, commissionid) \
      redis.log(redis.LOG_NOTICE, "getcommissionstructure") \
      local temp = {} \
      temp.tblresults = {} \
      temp.brokerkey = "broker:" .. brokerid \
      temp.commission = gethashvalues(temp.brokerkey .. ":commission:" .. commissionid) \
      temp.defaultcommissionid = redis.call("get",  temp.brokerkey .. ":commission:default") \
      if temp.defaultcommissionid == commissionid then \
        temp.commission.defaultcommission = 1 \
      end \
      if not temp.commission["commissionid"] then \
        return {1, "commission not found"} \
      end \
      temp.commissionranges = redis.call("sort", temp.brokerkey .. ":commission:".. commissionid .. ":commissionranges") \
      if #temp.commissionranges >= 1 then \
        for index = 1, #temp.commissionranges do \
          table.insert(temp.tblresults, gethashvalues(temp.brokerkey .. ":commissionrange:" .. temp.commissionranges[index])) \
        end \
      end \
      temp.commission.commissionrange = temp.tblresults \
      return {0, temp.commission} \
    end \
  ';
  exports.getcommissionstructure = getcommissionstructure;
  /*
   * getcommission
   * This script used to get commission for the trade
   * params: brokerid, accountid, consideration
   * returns: commission if ok, else 1 + an error message if unsuccessful
   */
  getcommission = getcommissionstructure + round + '\
    local getcommission = function(brokerid, accountid, consideration) \
      redis.log(redis.LOG_NOTICE, "getcommission") \
      local commission = {} \
      commission.brokerkey = "broker:" .. brokerid \
      commission.commissionid = redis.call("hget", commission.brokerkey .. ":account:" .. accountid, "commissionid") \
      commission.result = getcommissionstructure(brokerid, commission.commissionid) \
      if commission.result[1] == 1 then \
        return commission.result \
      end \
      commission.structure = commission.result[2] \
      commission.range = commission.structure["commissionrange"] \
      commission.min = tonumber(commission.structure["commissionmin"]) \
      commission.max = tonumber(commission.structure["commissionmax"]) \
      commission.percent = 0 \
      for i = 1, #commission.range do \
        commission.rangefrom = tonumber(commission.range[i]["rangefrom"]) \
        commission.rangeupto = tonumber(commission.range[i]["rangeupto"]) \
        if consideration > commission.rangefrom and consideration < commission.rangeupto then \
          commission.percent = tonumber(commission.range[i]["rangepercent"]) \
          break \
        elseif commission.rangeupto == 0.0 then \
          commission.percent = tonumber(commission.range[i]["rangepercent"]) \
          break \
        end \
      end \
      commission.amount = round(consideration * commission.percent / 100, 2) \
      if commission.amount < commission.min then \
        commission.amount = commission.min \
      elseif ommission.amount > commission.max then \
        commission.amount = commission.max \
      end \
      return {0, commission.amount} \
    end \
  ';
  exports.getcommission = getcommission;
  /*
   * gettransactionstampstatus
   * This script used to get transaction stamp status by countrycode
   * params: countrycode
   * returns: transactionstampstatus if ok, else 1 + an error message if unsuccessful
   */
  gettransactionstampstatus = '\
    local gettransactionstampstatus = function(countrycode) \
      return redis.call("get", "countrycode:" .. countrycode .. ":transactionstampstatus")  \
    end \
  ';
  exports.gettransactionstampstatus = gettransactionstampstatus;
  /*
   * getstampduty
   * This script used to get transaction stampduty details by symbolid & settlementamount and side
   * params: symbolid, consideration, side, issymbol
   * returns: stampduty & stampdutyid if ok, else 1 + an error message if unsuccessful
   */
  getstampduty = gettransactionstampstatus + round + '\
    local getstampduty = function(symbolid, consideration, side, issymbol) \
      local stampduty = {} \
      stampduty.id = 0 \
      stampduty.amount = 0 \
      if side == 1 then \
        if tonumber(issymbol) == 1 then \
          stampduty.isin = redis.call("hget", "symbol:" .. symbolid, "isin") \
          stampduty.countrycode = string.sub(stampduty.isin, 1, 2) \
          stampduty.transactionstampstatusid = gettransactionstampstatus(stampduty.countrycode) \
        else \
          stampduty.transactionstampstatusid = symbolid \
        end \
        if stampduty.transactionstampstatusid and stampduty.transactionstampstatusid ~= "" then \
          stampduty.transactionstampstatus = gethashvalues("transactionstampstatus:" .. stampduty.transactionstampstatusid) \
          stampduty.id = stampduty.transactionstampstatus["stampdutyid"] \
          stampduty.rate = tonumber(stampduty.transactionstampstatus["stampdutyrate"]) / 100 \
          stampduty.amount = round(consideration * stampduty.rate, 2) \
        end \
      end \
      return {stampduty.amount, stampduty.id} \
    end \
  ';
  exports.getstampduty = getstampduty;
  /*
   * getptmlevy
   * This script used to get transaction ptmlevy by symbolid & settlementamount
   * params: symbolid, consideration
   * returns: ptmlevy if ok, else 1 + an error message if unsuccessful
   */
  getptmlevy = '\
    local getptmlevy = function(symbolid, consideration) \
      local ptm = {} \
      ptm.levy = 0 \
      ptm.exempt = redis.call("hget", "symbol:" .. symbolid, "ptmexempt") \
      if ptm.exempt and tonumber(ptm.exempt) ~= 1 then \
        ptm.limit = redis.call("hget", "config", "ptmlevylimit") \
        if consideration > tonumber(ptm.limit) then \
          ptm.levy = 1 \
        end \
      end \
      return tostring(ptm.levy) \
    end \
  ';
  exports.getptmlevy = getptmlevy;
  /*
   * getsymbollongname
   * get a symbol long name
   * params: symbolid
   * returns: 0 + result objects if ok, else 1 + an error message if unsuccessful
   */
  getsymbollongname = '\
    local getsymbollongname = function(symbolid) \
      redis.log(redis.LOG_NOTICE, "getsymbollongname") \
      local symbollongname = redis.call("hget", "symbol:" .. symbolid, "longname") \
      if not symbollongname then \
        return nil \
      end \
      return symbollongname \
    end \
  ';
  exports.getsymbollongname = getsymbollongname;
  /*
   * convertarraytoobject()
   * To convert array of static list to object
   * params: records, primaryid, name
   */
  convertarraytoobject = '\
    local convertarraytoobject = function(records, primaryid, name) \
      redis.log(redis.LOG_NOTICE, "convertarraytoobject") \
      local object = {} \
      for i = 1, #records do \
        object[records[i][primaryid]] = records[i][name] \
      end \
      return object \
    end \
  ';
  /*
   * getaccountgroups()
   * To get all accountgroups
   */
  getaccountgroups = convertarraytoobject + gethashvalues + '\
    local getaccountgroups = function() \
      redis.log(redis.LOG_NOTICE, "getaccountgroup") \
      local tblresults = {} \
      local accountgroupkey = "accountgroups:_indicies:accountgroupid" \
      local accountgroupids = redis.call("sort", accountgroupkey) \
      if #accountgroupids >= 1 then \
        for index = 1, #accountgroupids do \
          local accountgroup = gethashvalues("accountgroups:" .. accountgroupids[index]) \
          table.insert(tblresults, accountgroup) \
        end \
      end \
      return tblresults \
    end \
  ';
  /*
   * getaccounttypes()
   * To get all accounttypes
   */
  getaccounttypes = convertarraytoobject + gethashvalues + '\
    local getaccounttypes = function() \
      redis.log(redis.LOG_NOTICE, "getaccounttypes") \
      local tblresults = {} \
      local accounttypekey = "accounttypes:_indicies:accounttypeid" \
      local accounttypeids = redis.call("sort", accounttypekey) \
      if #accounttypeids >= 1 then \
        for index = 1, #accounttypeids do \
          local accounttype = gethashvalues("accounttypes:" .. accounttypeids[index]) \
          table.insert(tblresults, accounttype) \
        end \
      end \
      return tblresults \
    end \
  ';
  /*
   * sortaccountnameascending()
   * script to sort account by name.
   * returns: sorted array in ascending order
   */
  sortaccountnameascending = '\
    local sortaccountnameascending = function(a, b) \
      if a["currencyid"] == b["currencyid"] then \
        return string.lower(a["name"]) < string.lower(b["name"]) \
      else \
        return a["currencyid"] < b["currencyid"] \
      end \
    end \
  ';
  /*
   * sortaccountnamedescending()
   * script to sort account name in descending order.
   * returns: account name in descending order.
   */
  sortaccountnamedescending = '\
    local sortaccountnamedescending = function(a, b) \
      if a["currencyid"] == b["currencyid"] then \
        return string.lower(a["name"]) > string.lower(b["name"]) \
      else \
        return a["currencyid"] < b["currencyid"] \
      end \
    end \
  ';
  /*
   * numericsort()
   * script to sort numbers in provided sortorder.
   * returns: sorted array.
   */
  numericsort = '\
    local numericsort = function(sortfield, sortorder, data) \
      if sortorder == "asc" then \
        table.sort(data, function(a, b) return tonumber(a[sortfield]) < tonumber(b[sortfield]) end) \
      else \
        table.sort(data, function(a, b) return tonumber(a[sortfield]) > tonumber(b[sortfield]) end) \
      end \
      return data \
    end \
  ';
  /*
   * alphabeticsort()
   * script to sort alphabets in provided sortorder.
   * returns: sorted array.
   */
  alphabeticsort = '\
    local alphabeticsort = function(sortfield, sortorder, data) \
      if sortorder == "asc" then \
        table.sort(data, function(a, b) return string.lower(a[sortfield]) < string.lower(b[sortfield]) end) \
      else \
        table.sort(data, function(a, b) return string.lower(a[sortfield]) > string.lower(b[sortfield]) end) \
      end \
      return data \
    end \
  ';
 /*
  * updatecastatus()
  * script to update status(apply/reverse) of corporate action
  * params: brokerid, corporateactionid, status (1=applied,0=reverse)
  */
  updatecastatus = '\
    local updatecastatus = function(brokerid, corporateactionid, status) \
      redis.log(redis.LOG_NOTICE, "updatecastatus") \
      redis.call("hset", "broker:" .. brokerid .. ":corporateaction:" .. corporateactionid, "processed", status) \
    end \
  ';
  exports.updatecastatus = updatecastatus;
 /*
  * updatecastagestatus()
  * script to update ca right processed status (0=reverse, 1=by exdate, 2=by paydate)
  * params: corporateactionid, stage
  * returns: 0 if ok, else 1 + an error message if unsuccessful
  */
  updatecastagestatus = '\
    local updatecastagestatus = function(corporateactionid, stage) \
      redis.log(redis.LOG_NOTICE, "updatecastagestatus") \
      redis.call("hset", "corporateaction:" .. corporateactionid, "stage", stage) \
    end \
  ';
  exports.updatecastagestatus = updatecastagestatus;
  /*
   * settradestatus()
   * To sets trade status
   * params: brokerid, tradeid, status
   */
  settradestatus = '\
    local settradestatus = function(brokerid, tradeid, status) \
      redis.log(redis.LOG_NOTICE, "settradestatus") \
      redis.call("hset", "broker:" .. brokerid .. ":trade:" .. tradeid, "status", status) \
      return tradeid \
    end \
  ';
  exports.settradestatus = settradestatus;
  /*
   * getlimitopeningbalance()
   * To get opening balance of the limit
   * params: brokerid, accountid, startmilliseconds, view
   * return: openingbalance
   */
  getlimitopeningbalance = '\
    local getlimitopeningbalance = function(brokerid, accountid, startmilliseconds, view) \
      local openingbalance = 0 \
      local balance = 0 \
      local postingids = redis.call("zrangebyscore", "broker:" .. brokerid .. ":account:" .. accountid .. ":postingsbydate", "-inf", startmilliseconds) \
      if view == "client" then \
        local hiddenpostings = redis.call("sort", "broker:" .. brokerid .. ":account:" .. accountid .. ":hidepostings") \
        postingids = filter(postingids, hiddenpostings) \
      end \
      if #postingids > 0 then \
        for i = 1, #postingids do \
          balance = redis.call("hget", "broker:" .. brokerid .. ":posting:" .. postingids[i], "amount") \
          openingbalance = openingbalance + tonumber(balance) \
        end \
      end \
      return openingbalance \
    end \
  ';
  exports.getlimitopeningbalance = getlimitopeningbalance;
  /*
   * getpageopeningbalance()
   * To get opening balance of the page
   * params: brokerid, postings, query, upto, startmilliseconds and view
   * return: openingbalance
   */
  getpageopeningbalance = getlimitopeningbalance + '\
    local getpageopeningbalance = function(brokerid, postings, accountid, upto, startmilliseconds, view) \
      local openingbalance = getlimitopeningbalance(brokerid, accountid, startmilliseconds, view) \
      if upto > 0 then \
        for i = 1, upto do \
          local balance = redis.call("hget", "broker:" .. brokerid .. ":posting:" .. postings[i], "amount") \
          openingbalance = openingbalance + tonumber(balance) \
        end \
      end \
      return openingbalance \
    end \
  ';
  exports.getpageopeningbalance = getpageopeningbalance;
    /*
  * checknonzerosymbol()
  * params: brokerid, symbolid
  * return: 0 or 1
  */
  checknonzerosymbol = '\
    local checknonzerosymbol = function(brokerid, symbolid) \
      redis.log(redis.LOG_NOTICE, "checknonzerosymbol") \
      local temp = {} \
      temp.brokerkey = "broker:" .. brokerid \
      if redis.call("SISMEMBER", temp.brokerkey .. ":position:nonzerosymbols", symbolid) == 0 then \
        return 0 \
      else \
        temp.positions = redis.call("smembers", temp.brokerkey .. ":symbol:" .. symbolid .. ":positions") \
        if #temp.positions == 0 then \
          return 0 \
        else \
          for i = 1, #temp.positions do \
            if tonumber(redis.call("hget", temp.brokerkey .. ":position:" .. temp.positions[i], "quantity")) > 0 then \
              return 1 \
            end \
          end \
          return 0 \
        end \
      end \
    end \
  ';
  exports.checknonzerosymbol = checknonzerosymbol;
  /*
  * addtononzerosymbolindex()
  * params: brokerid, symbolid
  * return: 0
  */
  addtononzerosymbolindex = checknonzerosymbol + '\
    local addtononzerosymbolindex = function(brokerid, symbolid) \
      redis.log(redis.LOG_NOTICE, "addtononzerosymbolindex") \
      if checknonzerosymbol(brokerid, symbolid) == 0 then \
        redis.call("sadd", "broker:" .. brokerid .. ":position:nonzerosymbols", symbolid) \
      end \
    end \
  ';
  exports.addtononzerosymbolindex= addtononzerosymbolindex;
  /*
  * removefromnonzerosymbolindex()
  * params: brokerid, symbolid
  */
  removefromnonzerosymbolindex = checknonzerosymbol + '\
    local removefromnonzerosymbolindex = function(brokerid, symbolid) \
      redis.log(redis.LOG_NOTICE, "removefromnonzerosymbolindex") \
      if checknonzerosymbol(brokerid, symbolid) == 0 then \
        redis.call("srem", "broker:" .. brokerid .. ":position:nonzerosymbols", symbolid) \
      end \
    end \
  ';
  exports.removefromnonzerosymbolindex = removefromnonzerosymbolindex;
  /*
   * isholiday
   * params: date
   * returns: 0 if not holiday, else 1
   */
  isholiday = '\
    local isholiday = function(exchangeid, date) \
      redis.log(redis.LOG_NOTICE, "isholiday") \
      return redis.call("sismember", "exchangesclosed:" .. exchangeid, date) \
    end \
  ';
  exports.isholiday = isholiday + '\
    return isholiday(ARGV[1], ARGV[2]) \
  ';
  /*
   * validateuseremail
   * This script used to check email already used or not
   * params: brokerid, email
   * returns: 0 if ok, else 1 + an error message if unsuccessful
   */
  validateuseremail = '\
    local validateuseremail = function(brokerid, email) \
      redis.log(redis.LOG_NOTICE, "validateuseremail") \
      local userid = redis.call("get", "broker:" .. brokerid .. ":user:" .. email) \
      if userid then \
        return {1, userid} \
      else \
        return {0} \
      end \
    end \
  ';
  exports.validateuseremail = validateuseremail + '\
    return validateuseremail(ARGV[1], ARGV[2]) \
  ';
  /*
   * updateuseremail
   * This script used to update user email address of the user
   * params: brokerid, email
   * returns: 0 if ok, else 1 + an error message if unsuccessful
   */
  updateuseremail = '\
    local updateuseremail = function(brokerid, userid, email) \
      redis.log(redis.LOG_NOTICE, "updateuseremail") \
      local brokerkey = "broker:" .. brokerid \
      local oldemail = redis.call("hget", brokerkey .. ":user:" .. userid, "email") \
      redis.call("del", brokerkey .. ":user:" .. oldemail) \
      redis.call("hset", brokerkey .. ":user:" .. userid, "email", email) \
      redis.call("set", brokerkey .. ":user:" .. email, userid) \
      return 0 \
    end \
  ';
  /*
  * scriptgetholidays
  * get holidays
  * note: assumes UK holidays
  */
  exports.scriptgetholidays = '\
    local tblresults = {} \
    local holidays = redis.call("smembers", "exchangesclosed:" .. ARGV[1]) \
    for index = 1, #holidays do \
      table.insert(tblresults, holidays[index]) \
    end \
    return cjson.encode(tblresults) \
  ';
  /*
  * validateclient
  * validate client for trading process
  * params: brokerid, clientid
  */
  validateclient = '\
    local validateclient = function(brokerid, clientid) \
      redis.log(redis.LOG_NOTICE, "validateclient") \
      local client = redis.call("hmget", "broker:" .. brokerid .. ":client:" .. clientid, "clientid", "commissionid") \
      if not client[1] then \
        return {1, "Invalid clientid"} \
      end \
      if not client[2] then \
        return {1, "Invalid commissionid"} \
      end \
      if client[2] == "" then \
        return {1, "Please assign a commission to the selected client to perform trades"} \
      end \
      return {0} \
    end \
  ';
  exports.validateclient = validateclient;
  /*
  * validatecommission
  * validate client for trading process
  * params: brokerid, clientid
  */
  validatecommission = getclientidbyaccount + '\
    local validatecommission = function(brokerid, accountid) \
      redis.log(redis.LOG_NOTICE, "validatecommission") \
      local clientid = getclientidbyaccount(brokerid, accountid) \
      local isactive = redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "statusid") \
      if not isactive or isactive == "" or tonumber(isactive) == 1 then \
        return {1, "Given client is not active, please update the selected client detail"} \
      end \
      local commission = redis.call("hget", "broker:" .. brokerid .. ":account:" .. accountid, "commissionid") \
      if not commission or commission == "" or tonumber(commission) == 0 then \
        return {1, "Please assign a commission to the selected client account to perform trades"} \
      end \
      return {0} \
    end \
  ';
  exports.validatecommission = validatecommission + '\
    return validatecommission(ARGV[1], ARGV[2]) \
  ';
  /*
   * validateaccount()
   * script to validate the account details before create or update
   * params : brokerid, clientid, accountgroupid, accounttaxtypeid, accounttypeid, currencyid
   * returns: 0 if ok, else 1 + an error message if invalid references
   */
  validateaccount = '\
    local validateaccount = function(brokerid, clientid, accountgroupid, accounttaxtypeid, accounttypeid, currencyid) \
      redis.log(redis.LOG_NOTICE, "validateaccount") \
      local errors = {} \
      local accountgroup = redis.call("hget", "accountgroups:" .. accountgroupid, "accountgroupid") \
      if not accountgroup then \
        table.insert(errors, "account group") \
      end \
      local accounttype = redis.call("hget", "accounttypes:" .. accounttypeid, "accounttypeid") \
      if not accounttype then \
        table.insert(errors, "account type") \
      end \
      local currency = redis.call("hget", "broker:" .. brokerid .. ":currency:" .. currencyid, "currencyid") \
      if not currency then \
        table.insert(errors, "currency") \
      end \
      if tonumber(accountgroupid) == 3 then \
        local accounttaxtype = redis.call("hget", "accounttaxtypes:" .. accounttaxtypeid, "accounttaxtypeid") \
        if not accounttaxtype then \
          table.insert(errors, "account tax type") \
        end \
        local client = redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "clientid") \
        if not client then \
          table.insert(errors, "client") \
        end \
      end \
      if #errors >= 1 then \
        local error = "Invalid " .. errors[1] \
        for index = 2, #errors do \
          error = error .. ", " .. errors[index] \
        end \
        return {1, error .. " given"} \
      else \
        return {0} \
      end \
    end \
  ';
  /*
   * updateclientdetails()
   * This script used to update connection between client and account.
   * params: brokerid, accountid, clientid
   * returns: 0 if ok, else 1 + an error message if unsuccessful
   */
  updateclientdetails = '\
    local updateclientdetails = function(brokerid, accountid, clientid) \
      redis.log(redis.LOG_WARNING, "updateclientdetails") \
      redis.call("sadd", "broker:" .. brokerid .. ":client:" .. clientid .. ":clientaccounts", accountid) \
      redis.call("set", "broker:" .. brokerid .. ":account:" .. accountid .. ":client", clientid) \
      return {0} \
    end \
  ';
  /*
   * createaccount()
   * This script used for create account
   * params : accountgroupid, accounttaxtypeid, accounttypeid, balance, balanceuncleared, brokerid, creditlimit, currencyid, debitlimit, exdiff, exdiffuncleared, exdiffdate, localbalance, localbalanceuncleared, name, active
   * returns: accountid if ok, else 1 + an error message if unsuccessful
   */
  createaccount = '\
    local createaccount = function(accountgroupid, accounttaxtypeid, accounttypeid, balance, balanceuncleared, brokerid, creditlimit, currencyid, debitlimit, exdiff, exdiffuncleared, exdiffdate, localbalance, localbalanceuncleared, name, active, commissionid, defaultbankaccountid) \
      redis.log(redis.LOG_NOTICE, "createaccount") \
      local brokerkey = "broker:" .. brokerid \
      local accountid = redis.call("hincrby", brokerkey, "lastaccountid", 1) \
      redis.call("hmset", brokerkey .. ":account:" .. accountid, "accountid", accountid, "brokerid" , brokerid, "currencyid", currencyid, "name", name, "accountgroupid", accountgroupid, "accounttypeid", accounttypeid, "balance", balance, "balanceuncleared", balanceuncleared, "creditlimit", creditlimit, "debitlimit", debitlimit, "exdiff", exdiff, "exdiffuncleared", exdiffuncleared, "exdiffdate", exdiffdate, "localbalance", localbalance, "localbalanceuncleared", localbalanceuncleared, "active", active, "accounttaxtypeid", accounttaxtypeid, "commissionid", commissionid, "defaultbankaccountid", defaultbankaccountid) \
      redis.call("sadd", brokerkey .. ":accounts", accountid) \
      redis.call("sadd", brokerkey.. ":accountid", "account:" .. accountid) \
      if accountgroupid == "3" then \
        redis.call("sadd", brokerkey .. ":clientaccounts", accountid) \
      else \
        redis.call("sadd", brokerkey .. ":brokeraccounts", accountid) \
        redis.call("sadd", brokerkey .. ":" .. currencyid .. ":brokeraccounts", accountid) \
        if accountgroupid == "7" then \
          redis.call("sadd", brokerkey .. ":account:" .. "nomineeaccounts", accountid) \
        elseif accountgroupid == "8" then \
          redis.call("sadd", brokerkey .. ":account:" .. "groupaccounts", accountid) \
        end \
      end \
      return accountid \
    end \
  ';
  /*
   * getclientlinkdetails()
   * This script used to get client link details
   * params : brokerid, clientid
   * returns: 0 if ok, else 1 + an error message if unsuccessful
   */
  getclientlinkdetails = '\
    local getclientlinkdetails = function(brokerid, clientid) \
      local temp = redis.call("hmget", "broker:" .. brokerid .. ":client:" .. clientid, "linktype", "linkid") \
      local link = {} \
      link.type = temp[1] \
      link.id = temp[2] \
      return link \
    end \
  ';
  /*
   * newaccount()
   * This script used for create new account
   * params : accountgroupid, accounttaxtypeid, accounttypeid, balance, balanceuncleared, brokerid, creditlimit, currencyid, debitlimit, exdiff, exdiffuncleared, exdiffdate, localbalance, localbalanceuncleared, name, active, clientid, commissionid, defaultbankaccountid
   * returns: 0 if ok, else 1 + an error message if unsuccessful
   */
  newaccount = validateaccount + updateclientdetails + createaccount + createaddtionalindex + getclientlinkdetails + '\
    local newaccount = function(accountgroupid, accounttaxtypeid, accounttypeid, balance, balanceuncleared, brokerid, creditlimit, currencyid, debitlimit, exdiff, exdiffuncleared, exdiffdate, localbalance, localbalanceuncleared, name, active, clientid, commissionid, defaultbankaccountid) \
      redis.call("RPUSH", "newaccount", currencyid) \
      local result = validateaccount(brokerid, clientid, accountgroupid, accounttaxtypeid, accounttypeid, currencyid) \
      if result[1] == 1 then \
        return result \
      end \
      local accountid = createaccount(accountgroupid, accounttaxtypeid, accounttypeid, balance, balanceuncleared, brokerid, creditlimit, currencyid, debitlimit, exdiff, exdiffuncleared, exdiffdate, localbalance, localbalanceuncleared, name, active, commissionid, defaultbankaccountid) \
      if tonumber(accountgroupid) == 3 then \
        updateclientdetails(brokerid, accountid, clientid) \
        local link = getclientlinkdetails(brokerid, clientid) \
        createaddtionalindex(brokerid, "clientaccounts", accountid, link.type, link.id) \
      end \
      return {0, accountid} \
    end \
  ';
  exports.newaccount = newaccount;
  /*
   * validateposting
   * params: brokerid, accountid and postingid
   * returns 0 + amount if success else returns 1
   */
  validateposting = '\
    local validateposting = function(brokerid, accountid, postingid) \
      local brokerkey = "broker:" .. brokerid \
      local isexists = redis.call("sismember", brokerkey .. ":accounts", accountid) \
      if isexists == 0 then \
        return {1, "Invalid account"} \
      end \
      isexists = redis.call("sismember", brokerkey .. ":postings", postingid) \
      if isexists == 0 then \
        return {1, "Invalid posting"} \
      end \
      local account = redis.call("hmget", brokerkey .. ":posting:" .. postingid, "accountid", "amount") \
      if account[1] ~= accountid then \
        return {1, "posting is not belongs to selected account"} \
      end \
      return {0, account[2]} \
    end \
  ';
  /*
   * hideposting()
   * This script used to add postingid to hideindex
   * params: brokerid, accountid, positionid
   */
  hideposting = validateposting + formattostring + '\
    local hideposting = function(brokerid, accountid, postingid) \
      local result = validateposting(brokerid, accountid, postingid) \
      if result[1] == 1 then \
        return result \
      end \
      local total = result[2] \
      redis.call("sadd", "broker:" .. brokerid .. ":account:" .. accountid .. ":hidepostings", postingid) \
      local totalbalance = redis.call("get", "broker:" .. brokerid .. ":account:" .. accountid .. ":hidepostings:balance") \
      if totalbalance then \
        total = formattostring(tonumber(total) + tonumber(totalbalance), 2) \
      end \
      redis.call("set", "broker:" .. brokerid .. ":account:" .. accountid .. ":hidepostings:balance", total) \
      if tonumber(total) ~= 0 then \
        return {2, "Warning! Hidden postings do NOT balance by: " .. total} \
      end \
      return {0, "Posting hidden"} \
    end \
  ';
  /*
   * unhideposting()
   * This script used to remove postingid to hideindex
   * params: brokerid, accountid, positionid
   */
  unhideposting = validateposting + formattostring + '\
    local unhideposting = function(brokerid, accountid, postingid) \
      local result = validateposting(brokerid, accountid, postingid) \
      if result[1] == 1 then \
        return result \
      end \
      local balancekey = "broker:" .. brokerid .. ":account:" .. accountid .. ":hidepostings:balance" \
      local totalbalance = redis.call("get", balancekey) \
      if not totalbalance then \
        totalbalance = 0 \
      end \
      redis.call("srem", "broker:" .. brokerid .. ":account:" .. accountid .. ":hidepostings", postingid) \
      totalbalance = formattostring(tonumber(totalbalance) - tonumber(result[2]), 2) \
      if tonumber(totalbalance) == 0 then \
        redis.call("del", balancekey) \
      else \
        redis.call("set", balancekey, totalbalance) \
        if tonumber(totalbalance) ~= 0 then \
          return {2, "Warning! Hidden postings do NOT balance by: " .. totalbalance} \
        end \
      end \
      return {0, "Posting visible"} \
    end \
  ';
  /*
   * isclientexist
   * This function validates whether the clientid is exist or not
   * params: brokerid, clientid, email
   * returns: 0 + clientid if exist, else 1 + an error message if not exist
   */
  isclientexist = '\
    local isclientexist = function(brokerid, clientid, emailid) \
      local clientid = redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "clientid") \
      if not clientid then \
        return {1, 1017} \
      else \
        if emailid then \
          local existclientid = redis.call("get", "broker:" .. brokerid .. ":client:" .. emailid) \
          if existclientid and existclientid ~= clientid then \
            return {1, 1023} \
          end \
        end \
        return {0, clientid} \
      end \
    end \
  ';
  /*
   * getclientbalance()
   * script to get client total case balance by clientid.
   * params : brokerid, clientid
   * returns: total cash balance
   */
  getclientbalance = formattostring + '\
    local getclientbalance = function(brokerid, clientid) \
      local clientaccounts = redis.call("sort", "broker:" .. brokerid .. ":client:" .. clientid .. ":clientaccounts") \
      local totalbalance = 0 \
      local balance = 0 \
      for i = 1, #clientaccounts do \
        balance = redis.call("hmget", "broker:" .. brokerid .. ":account:" .. clientaccounts[i], "balance", "balanceuncleared") \
        totalbalance = totalbalance + tonumber(balance[1]) + tonumber(balance[2]) \
      end \
      return formattostring(totalbalance, 2) \
    end \
  ';
  exports.getclientbalance = getclientbalance + '\
    return getclientbalance(ARGV[1], ARGV[2]) \
  ';
  /*
   * getclient()
   * This script used to get client details.
   * params: brokerid, clientid
   * returns: 0 + client object if ok, else 1 + an error message if unsuccessful
   */
  getclient = gethashvalues + getclientbalance + '\
    local getclient = function (brokerid, clientid) \
      redis.log(redis.LOG_NOTICE, "getclient") \
      local temp = {} \
      temp.brokerkey = "broker:" .. brokerid \
      temp.client = gethashvalues(temp.brokerkey .. ":client:" .. clientid) \
      temp.client.isaccountactive = 0 \
      temp.instrumenttypes = redis.call("smembers", temp.brokerkey .. ":client:" .. clientid .. ":instrumenttypes") \
      if #temp.instrumenttypes >= 1 then \
        temp.client.instrumenttypes = temp.instrumenttypes \
      end \
      temp.ismailsent = redis.call("get", temp.brokerkey .. ":client:" .. clientid .. ":activated") \
      if temp.ismailsent == nil or tonumber(temp.ismailsent) == 1 then \
        temp.client.isaccountactive = 1 \
      end \
      temp.client.password = nil \
      temp.client.balance = getclientbalance(brokerid, clientid) \
      return temp.client \
    end \
  ';
  exports.getclient = getclient;
  /**
   * isemailenabled
   * params ARGV[1] = brokerid
   * returns true or flase
   */
  exports.isemailenabled = '\
    redis.log(redis.LOG_NOTICE, "isemailenabled") \
    local isemailenabled = redis.call("get", "broker:0:emailenabled") \
    if tonumber(isemailenabled) == 1 then \
      return redis.call("get", "broker:" .. ARGV[1] .. ":emailenabled") \
    end \
    return 0 \
  ';
}
