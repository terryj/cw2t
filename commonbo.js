/****************
* commonbo.js
* Common back-office functions
* Cantwaittotrade Limited
* Terry Johnston
* June 2015
****************/

exports.registerScripts = function () {
  /*
  * getaccountbalance()
  * returns account balance & local currency balance
  */
  getaccountbalance = '\
  local getaccountbalance(accountid, brokerid) \
    local accountkey = "broker:" .. brokerid .. ":account:" .. accountid \
    local fields = {"balance", "localbalance"} \
    local vals = redis.call("hmget", accountkey, unpack(fields)) \
    return vals[1], vals[2] \
  end \
  ';
 
  /*
  * updateaccountbalance()
  * updates account balance & local currency balance
  * amount & localamount can be -ve
  */
  updateaccountbalance = '\
  local updateaccountbalance(accountid, brokerid, amount, localamount) \
    local accountkey = "broker:" .. brokerid .. ":account:" .. accountid \
    hincrbyfloat(accountkey, "balance", amount) \
    hincrbyfloat(accountkey, "localbalance", localamount) \
  end \
  ';

/* alternative method - todo: remove one or other
    local vals = getaccountbalance(brokerid, accountid) \
    redis.call("hmset", accountkey, "balance", tonumber(vals[1]) + tonumber(amount), "localbalance", tonumber(vals[2]) + tonumber(localamount)) \
*/

  /*
  * newposting()
  * creates a posting record & updates balances for an account
  */
  newposting = updateaccountbalance + '\
  local newposting(accountid, brokerid, amount, localamount, transactionid) \
    local postingid = redis.call("hincrby", "broker:" .. brokerid, "lastpostingid", 1) \
    redis.call("hmset", "broker:" .. brokerid .. ":posting:" .. postingid, "accountid", accountid, "brokerid", brokerid, "amount", amount, "localamount", localamount, "postingid", postingid, "transactionid", transactionid) \
    redis.call("sadd", "broker:" .. brokerid .. ":account:" .. accountid .. ":postings", postingid) \
    updateaccountbalance(accountid, brokerid, amount, localamount) \
  end \
  ';
};
