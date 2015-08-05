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
    local clientcontrolaccountid = getbrokeraccountid(brokerid, currencyid, "clientcontrolaccount") \
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

  /*
  * getbrokeraccountid()
  * gets a broker accountid for a default broker account
  * params: brokerid, currencyid, account name
  * returns: accountid
  */
  getbrokeraccountid = '\
  local getbrokeraccountid = function(brokerid, currencyid, name) \
    local brokerkey = "broker:" .. brokerid \
    local brokeraccountsmapid = redis.call("get", brokerkey .. ":" .. name .. ":" .. currencyid) \
    local accountid = redis.call("hget", brokerkey .. ":brokeraccountsmap:" .. brokeraccountsmapid, "accountid") \
    return accountid \
  end \
  ';

  /*
  * getclientaccount()
  * gets the account of a designated type for a client
  * params: brokerid, clientid, accounttypeid
  * returns the first account id found for the account type, else 0
  */
  getclientaccount = '\
  local getclientaccount = function(brokerid, clientid, accounttypeid) \
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

  exports.getclientaccount = getclientaccount;

  /*** Scripts ***/

  /*
  * scriptnewclientfundstransfer
  * script to handle client deposits & withdrawals
  * keys: broker:<brokerid>
  * args: amount, bankaccountid, brokerid, clientaccountid, currencyid, localamount, note, rate, reference, timestamp, transactiontypeid
  */
  exports.scriptnewclientfundstransfer = newtransaction + newposting + getbrokeraccountid + '\
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
    --[[ update client account & client control account ]] \
    newposting(ARGV[4], amount, ARGV[3], localamount, transactionid) \
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
  * scriptnewtradesettlement
  * script to handle settlement of trades
  * params: amount, brokerid, currencyid, localamount, note, rate, timestamp, tradeid, transactiontype
  */
  /*exports.scriptnewtradesettlement = newtradesettlementtransaction + '\
    local nominaltradeaccountid = getbrokeraccountid(brokerid, currencyid, "nominaltradeaccount") \
    local bankbrokeraccountid = getbrokeraccountid(brokerid, currencyid, "bankbrokerfunds") \
    local bankclientaccountid = getbrokeraccountid(brokerid, currencyid, "bankclientfunds") \
    newtradesettlementtransaction = function(amount, brokerid, currencyid, bankbrokeraccountid, localamount, nominaltradeaccountid, note, rate, timestamp, bankclientaccountid, tradeid, transactiontype) \
  ';*/
}
