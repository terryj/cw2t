/****************
* commonbo.js
* Common back-office functions
* Cantwaittotrade Limited
* Terry Johnston
* June 2015
****************/

exports.registerScripts = function () {
  /*** Functions ***/

  /*
  * getaccountbalance()
  * returns account balance & local currency balance
  */
  getaccountbalance = '\
  local getaccountbalance = function(accountid, brokerid) \
    local fields = {"balance", "localbalance"} \
    local vals = redis.call("hmget", "broker:" .. brokerid .. ":account:" .. accountid, unpack(fields)) \
    return vals[1], vals[2] \
  end \
  ';
 
  /*
  * updateaccountbalance()
  * updates account balance & local currency balance
  * amount & localamount can be -ve
  */
  updateaccountbalance = '\
  local updateaccountbalance = function(accountid, amount, brokerid, localamount) \
    local accountkey = "broker:" .. brokerid .. ":account:" .. accountid \
    redis.call("hincrbyfloat", accountkey, "balance", amount) \
    redis.call("hincrbyfloat", accountkey, "localbalance", localamount) \
  end \
  ';

  /*
  * newposting()
  * creates a posting record & updates balances for an account
  */
  newposting = updateaccountbalance + '\
  local newposting = function(accountid, amount, brokerid, localamount, transactionid) \
    local postingid = redis.call("hincrby", "broker:" .. brokerid, "lastpostingid", 1) \
    redis.call("hmset", "broker:" .. brokerid .. ":posting:" .. postingid, "accountid", accountid, "brokerid", brokerid, "amount", amount, "localamount", localamount, "postingid", postingid, "transactionid", transactionid) \
    redis.call("sadd", "broker:" .. brokerid .. ":transaction:" .. transactionid .. ":postings", postingid) \
    --[[ todo: add date based key, will need to pass timestamp ]] \
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
  newtradeaccounttransaction = newtransaction + newposting + '\
  local newtradeaccounttransaction = function(amount, brokerid, clientaccountid, currencyid, localamount, nominalaccountid, note, rate, timestamp, tradeid, transactiontype) \
    local clientcontrolaccountid = redis.call("hget", "broker:" .. brokerid, "clientcontrolaccountid") \
    local transactionid = newtransaction(amount, brokerid, currencyid, localamount, note, rate, "trade:" .. tradeid, timestamp, transactiontype) \
    if transactiontype == "TR" then \
      --[[ receipt from broker point of view ]] \
      newposting(clientaccountid, brokerid, -amount, -localamount, transactionid) \
      newposting(clientcontrolaccountid, brokerid, -amount, -localamount, transactionid) \
      newposting(nominalaccountid, brokerid, amount, localamount, transactionid) \
    else \
      --[[ pay from broker point of view ]] \
      newposting(clientaccountid, brokerid, amount, localamount, transactionid) \
      newposting(clientcontrolaccountid, brokerid, amount, localamount, transactionid) \
      newposting(nominalaccountid, brokerid, -amount, -localamount, transactionid) \
    end \
  end \
  ';

  /*
  * newtradeaccounttransactions()
  * cash side of a client trade
  * creates a separate transaction for the consideration & each of the cost items
  * todo: costs as a table?
  */
  newtradeaccounttransactions = newtradeaccounttransaction + '\
  local newtradeaccounttransactions = function(consideration, commission, ptmlevy, stampduty, brokerid, clientaccountid, currencyid, localamount, note, rate, timestamp, tradeid, side) \
    local brokerkey = "broker:" .. brokerid \
    local nominaltradeaccountid = redis.call("hget", brokerkey, "nominaltradeaccountid") \
    local nominalcommissionaccountid = redis.call("hget", brokerkey, "nominalcommissionaccountid") \
    local nominalptmaccountid = redis.call("hget", brokerkey, "nominalptmaccountid") \
    local nominalstampdutyaccountid = redis.call("hget", brokerkey, "nominalstampdutyaccountid") \
    --[[ side determines pay / receive ]] \
    if tonumber(side) == 1 then \
      --[[ client buy, so cash received from broker point of view ]] \
      newtradeaccounttransaction(consideration, brokerid, clientaccountid, currencyid, localamount, nominaltradeaccountid, note, rate, timestamp, tradeid, "TR") \
    else \
      --[[ cash paid from broker point of view ]] \
      newtradeaccounttransaction(consideration, brokerid, clientaccountid, currencyid, localamount, nominaltradeaccountid, note, rate, timestamp, tradeid, "TP") \
    end \
    --[[ broker always receives costs ]] \
    newtradeaccounttransaction(commission, brokerid, clientaccountid, currencyid, localamount, nominalcommissionaccountid, note .. " Commission", rate, timestamp, tradeid, "TR") \
    newtradeaccounttransaction(ptmlevy, brokerid, clientaccountid, currencyid, localamount, nominalptmaccountid, note .. " PTM Levy", rate, timestamp, tradeid, "TR") \
    newtradeaccounttransaction(stampduty, brokerid, clientaccountid, currencyid, localamount, nominalstampdutyaccountid, note .. " Stamp Duty", rate, timestamp, tradeid, "TR") \
  end \
  ';

  newtradesettlementtransaction = newtransaction + newposting + '\
  local newtradesettlementtransaction = function(amount, brokerid, currencyid, frombankaccountid, localamount, nominalaccountid, note, rate, timestamp, tobankaccountid, tradeid, transactiontype) \
    local transactionid = newtransaction(amount, brokerid, currencyid, localamount, note, rate, "trade:" .. tradeid, timestamp, transactiontype) \
    if transactiontype == "BP" then \
      newposting(nominalaccountid, brokerid, amount, localamount, transactionid) \
    else \
      newposting(nominalaccountid, brokerid, -amount, -localamount, transactionid) \
    end \
    newPosting(frombankaccountid, -amount, brokerid, -localamount, transactionid) \
    newPosting(tobankaccountid, amount, brokerid, localamount, transactionid) \
  end \
  ';

  /*** Scripts ***/

  /*
  * scriptnewclientfundstransfer
  * script to handle client deposits & withdrawals
  * keys: broker:<brokerid>
  * args: amount, bankaccountid, brokerid, clientaccountid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid
  */
  exports.scriptnewclientfundstransfer = newtransaction + newposting + '\
    local clientcontrolaccountid = redis.call("hget", KEYS[1], "clientcontrolaccountid") \
    local bankcontrolaccountid = redis.call("hget", KEYS[1], "bankcontrolaccountid") \
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
    newposting(ARGV[4], amount, ARGV[3], localamount, transactionid) \
    newposting(clientcontrolaccountid, amount, ARGV[3], localamount, transactionid) \
    newposting(ARGV[2], amount, ARGV[3], localamount, transactionid) \
    newposting(bankcontrolaccountid, amount, ARGV[3], localamount, transactionid) \
    return 0 \
  ';

  // todo: this needs to be called from a trade
  exports.scripttesttrade = newtradeaccounttransactions + '\
    newtradeaccounttransactions(ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11], ARGV[12], ARGV[13]) \
    return 0 \
  ';

  /*
  * scriptnewtradesettlement
  * script to handle settlement of trades
  * params: amount, brokerid, currencyid, localamount, note, rate, timestamp, tradeid, transactiontype
  */
  exports.scriptnewtradesettlement = newtradesettlementtransaction + '\
    local fields = {"nominaltradeaccountid", "nominalcommissionaccountid", "nominalptmaccountid", "nominalstampdutyaccountid", "bankaccountbrokerfunds", "bankaccountclientfunds"} \
    local vals = redis.call("hmget", "broker:" .. ARGV[2], unpack(fields)) \
    newtradesettlementtransaction = function(amount, brokerid, currencyid, vals[5], localamount, vals[1], note, rate, timestamp, vals[6], tradeid, transactiontype) \
  ';
}
