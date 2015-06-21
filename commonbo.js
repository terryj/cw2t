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

  /*
  * newtransaction()
  * creates a transaction record
  */
  newtransaction = '\
  local newtransaction(amount, brokerid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid) \
    local transactionid = redis.call("hincrby", "broker:" .. brokerid, "lasttransactionid", 1) \
    redis.call("hmset", "broker:" .. brokerid .. ":transaction:" .. transactionid, "amount", amount, "brokerid", brokerid, "currencyid", currencyid, "localamount", localamount, "note", note, "rate", rate, "reference", reference, "timestamp", timestamp, "transactiontypeid", transactiontypeid, "transactionid", transactionid) \
    return transactionid \
  end \
  ';

  /*
  * scriptnewclientfundstransfer
  * script to handle bank receipts & payments
  * parameters - amount, brokerid, currencyid, fromaccountid, localamount, nominalaccountid, note, rate, reference, timestamp, toaccountid, transactiontypeid
  */
  scriptnewclientfundstransfer = newtransaction + newposting + '\
    local transactionid = newtransaction(ARGV[1], ARGV[2], ARGV[3], ARGV[5], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[12]) \
    local clientcontrolaccountid = redis.call("hget", "broker:" .. brokerid, "clientcontrolaccountid") \
    local bankcontrolaccountid = redis.call("hget", "broker:" .. brokerid, "bankcontrolaccountid") \
    if ARGV[12] == "BR" then \
      newposting(ARGV[4], ARGV[2], ARGV[1], ARGV[5], transactionid) \
      newposting(clientcontrolaccountid, ARGV[2], ARGV[1], ARGV[5], transactionid) \
      newposting(ARGV[11], ARGV[2], ARGV[1], ARGV[5], transactionid) \
      newposting(bankcontrolaccountid, ARGV[2], ARGV[1], ARGV[5], transactionid) \
    else \
      newposting(ARGV[4], ARGV[2], -ARGV[1], -ARGV[5], transactionid) \
      newposting(clientcontrolaccountid, ARGV[2], -ARGV[1], -ARGV[5], transactionid) \
      newposting(ARGV[11], ARGV[2], -ARGV[1], -ARGV[5], transactionid) \
      newposting(bankcontrolaccountid, ARGV[2], -ARGV[1], -ARGV[5], transactionid) \
    end \
  ';
};
