/****************
* commonfo.js
* Front-office common server functions
* Cantwaittotrade Limited
* Terry Johnston
* December 2013
* Changes:
* 24 Mar 2017 - modified round
****************/

exports.registerScripts = function () {
  var round;

  round = '\
  local round = function(num, dp) \
    return string.format("%0." .. dp .. "f", num) \
  end \
  ';

  //
  // watchlistnew()
  // create a new watchlist
  // params: brokerid, clientid
  // returns: watchlistid
  //
  watchlistnew = round + '\
  local watchlistnew = function(brokerid, clientid) \
    local watchlistid = redis.call("hincrby", "config", "lastwatchlistid", 1) \
    redis.call("hset", "broker:" .. brokerid .. ":client:" .. clientid, "watchlistid", watchlistid) \
    redis.call("set", "watchlist:" .. watchlistid .. ":client", brokerid .. ":" .. clientid) \
    return watchlistid \
  end \
  ';

  //
  // subscribe a symbol to a watchlist
  // params: symbolid, watchlistid
  //
  subscribesymbol = '\
  local subscribesymbol = function(symbolid, watchlistid) \
    local nbtsymbol = redis.call("hget", "symbol:" .. symbolid, "nbtsymbol") \
    local exchangeid = redis.call("hget", "symbol:" .. symbolid, "exchangeid") \
    --[[ subscribe to this symbol, either UK or international price feed ]] \
    if exchangeid == "L" then \
      redis.call("publish", 7, "{" .. cjson.encode("pricerequest") .. ":" .. cjson.encode("rp:" .. nbtsymbol) .. "}") \
    else \
      redis.call("publish", 12, "{" .. cjson.encode("pricerequest") .. ":" .. cjson.encode("rp:" .. nbtsymbol) .. "}") \
    end \
    --[[ add the symbol to the watchlist ]] \
    redis.call("sadd", "watchlist:" .. watchlistid, symbolid) \
    --[[ add this watchlist to the set watching this symbol ]] \
    redis.call("sadd", "symbol:" .. symbolid .. ":watchlists", watchlistid) \
  end \
  ';

  //
  // unsubscribe a symbol from a watchlist
  // params: symbolid, watchlistid
  //
  unsubscribesymbol = '\
  local unsubscribesymbol = function(symbolid, watchlistid) \
    --[[ remove symbol from watchlist ]] \
    redis.call("srem", "watchlist:" .. watchlistid, symbolid) \
    --[[ remove this watchlist from the set watching this symbol ]] \
    redis.call("srem", "symbol:" .. symbolid .. ":watchlists", watchlistid) \
    --[[ if there are no watchlists subscribed to this symbol, unsubscribe from this symbol ]] \
    if redis.call("scard", "symbol:" .. symbolid .. ":watchlists") == 0 then \
      local nbtsymbol = redis.call("hget", "symbol:" .. symbolid, "nbtsymbol") \
      local exchangeid = redis.call("hget", "symbol:" .. symbolid, "exchangeid") \
      --[[ unsubscribe this symbol from either the UK or international price feed ]] \
      if exchangeid == "L" then \
        redis.call("publish", 7, "{" .. cjson.encode("pricerequest") .. ":" .. cjson.encode("halt:" .. nbtsymbol) .. "}") \
      else \
        redis.call("publish", 12, "{" .. cjson.encode("pricerequest") .. ":" .. cjson.encode("halt:" .. nbtsymbol) .. "}") \
      end \
    end \
  end \
  ';

  //
  // script to subscribe a client to a symbol
  // params: brokerid, clientid, symbolid
  //
  exports.scriptsubscribesymbol = watchlistnew + subscribesymbol + '\
  local brokerid = ARGV[1] \
  local clientid = ARGV[2] \
  local symbolid = ARGV[3] \
  --[[ get the watchlist for this client ]] \
  local watchlistid = redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "watchlistid") \
  --[[ create if one does not exist ]] \
  if not watchlistid or watchlistid == "" then \
    watchlistid = watchlistnew(brokerid, clientid) \
  end \
  subscribesymbol(symbolid, watchlistid) \
  return \
  ';

  //
  // script to unsubscribe a client from a symbol
  // params: brokerid, clientid, symbolid
  // returns: 0 if successful else 1, error message
  //
  exports.scriptunsubscribesymbol = unsubscribesymbol + '\
  local brokerid = ARGV[1] \
  local clientid = ARGV[2] \
  local symbolid = ARGV[3] \
  --[[ get the watchlist for this client ]] \
  local watchlistid = redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "watchlistid") \
  if not watchlistid or watchlistid == "" then \
    return {1, "Watchlist not found"} \
  end \
  unsubscribesymbol(symbolid, watchlistid) \
  return {0} \
  ';

  //
  // scriptgetwatchlist
  // get a watchlist for a client & subscribe to all symbols in the watchlist
  // params: broker id, client id
  // returns: list of symbols in the watchlist for this client
  //
  exports.scriptgetwatchlist = watchlistnew + subscribesymbol + '\
    local brokerid = ARGV[1] \
    local clientid = ARGV[2] \
    local tblresults = {} \
    --[[ get the watchlist for this client ]] \
    local watchlistid = redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "watchlistid") \
    --[[ create if one does not exist ]] \
    if not watchlistid or watchlistid == "" then \
      watchlistid = watchlistnew(brokerid, clientid) \
    end \
    --[[ subscribe to the symbols in the watchlist ]] \
    local watchlist = redis.call("smembers", "watchlist:" .. watchlistid) \
    for i = 1, #watchlist do \
      subscribesymbol(watchlist[i], watchlistid) \
      table.insert(tblresults, {symbolid=watchlist[i]}) \
    end \
    return {cjson.encode(tblresults)} \
  ';

  //
  // scriptunwatchlist
  // unsubscribe all symbols from watchlist for this client
  // params: broker id, client id
  //
  exports.scriptunwatchlist = unsubscribesymbol + '\
  local brokerid = ARGV[1] \
  local clientid = ARGV[2] \
  --[[ get the watchlist for this client ]] \
  local watchlistid = redis.call("hget", "broker:" .. brokerid .. ":client:" .. clientid, "watchlistid") \
  if watchlistid and watchlistid ~= "" then \
    local watchlist = redis.call("smembers", "watchlist:" .. watchlistid) \
    --[[ unsubscribe each symbol ]] \
    for i = 1, #watchlist do \
      unsubscribesymbol(watchlist[i], watchlistid) \
    end \
  end \
  return \
  ';

  //
  // scriptgetsubscriptions
  // get a list of clients who are subscribed to a symbol
  // params: symbol id
  // returns: a list of broker:client combinations
  //
  exports.scriptgetsubscriptions = '\
  local symbolid = ARGV[1] \
  local subscriptions = {} \
  --[[ get the watchlists with this symbol ]] \
  local watchlists = redis.call("sadd", "symbol:" .. symbolid .. ":watchlists") \
  for i = 1, #watchlists do \
    --[[ get the broker:client who owns this watchlist ]] \
    local brokerclient = redis.call("get", "watchlist:" .. watchlists[i] .. ":client") \
    table.insert(subscriptions, brokerclient) \
    end \
  end \
  return cjson.encode(subscriptions) \
  ';

  //
  // update & publish the latest price, together with the change, used for variation margin calculation
  // params: 1=nbtsymbol, 2=timestamp, 3=bid, 4=offer(ask), 5=calcmidnetchg, 6=calcmidpctchg
  // any symbols related to this nbtsymbol will be updated
  //
  scriptpriceupdate = round + '\
    local nbtsymbol = ARGV[1] \
    local bid = tonumber(ARGV[3]) \
    local ask = tonumber(ARGV[4]) \
    local symbols = redis.call("smembers", "nbtsymbol:" .. nbtsymbol .. ":symbols") \
    if #symbols >= 1 then \
      for index = 1, #symbols do \
        --[[ adjust prices from pence to pounds if currency is GBP ]] \
        local currencyid = redis.call("hget", "symbol:" .. symbols[index], "currencyid") \
        if currencyid == "GBP" then \
          if bid and bid ~= "" then bid = bid / 100 end \
          if ask and ask ~= "" then ask = ask / 100 end \
        end \
        local pricemsg = "{" .. cjson.encode("price") .. ":{" .. cjson.encode("symbolid") .. ":" .. cjson.encode(symbols[index]) \
        --[[ may get all or none of params ]] \
        local publish = false \
        if bid and bid ~= "" then \
          local oldbid = redis.call("hget", "symbol:" .. symbols[index], "bid") \
          if not oldbid then oldbid = 0 end \
          local bidchange = round(bid - tonumber(oldbid), 4) \
          pricemsg = pricemsg .. "," .. cjson.encode("bid") .. ":" .. tostring(bid) .. "," .. cjson.encode("bidchange") .. ":" .. bidchange \
          redis.call("hset", "symbol:" .. symbols[index], "bid", tostring(bid)) \
          publish = true \
        end \
        if ask and ask ~= "" then \
          local oldask = redis.call("hget", "symbol:" .. symbols[index], "ask") \
          if not oldask then oldask = 0 end \
          local askchange = round(ask - tonumber(oldask), 4) \
          pricemsg = pricemsg .. "," .. cjson.encode("ask") .. ":" .. tostring(ask) .. "," .. cjson.encode("askchange") .. ":" .. askchange \
          redis.call("hset", "symbol:" .. symbols[index], "ask", tostring(ask)) \
          publish = true \
        end \
        if ARGV[5] ~= "" then \
          pricemsg = pricemsg .. "," .. cjson.encode("midnetchg") .. ":" .. ARGV[5] \
          redis.call("hset", "symbol:" .. symbols[index], "midnetchg", ARGV[5]) \
          publish = true \
        end \
        if ARGV[6] ~= "" then \
          pricemsg = pricemsg .. "," .. cjson.encode("midpctchg") .. ":" .. cjson.encode(ARGV[6]) \
          redis.call("hset", "symbol:" .. symbols[index], "midpctchg", ARGV[6]) \
          publish = true \
        end \
        if publish then \
          --[[ updating timestamp ]] \
          redis.call("hset", "symbol:" .. symbols[index], "timestamp", ARGV[2]) \
          --[[ publish the message to the price channel ]] \
          pricemsg = pricemsg .. "}}" \
          redis.call("publish", 9, pricemsg) \
        end \
      end \
    end \
  ';

  exports.scriptpriceupdate = scriptpriceupdate;
};
