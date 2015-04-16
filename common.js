/****************
* common.js
* Common server functions
* Cantwaittotrade Limited
* Terry Johnston
* April 2015
****************/

function getErrorcode(errorcode) {
  var desc;

  switch (parseInt(errorcode)) {
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
  default:
    desc = "Unknown reason";
  }

  return desc;
}

exports.getErrorcode = getErrorcode;

exports.registerCommonScripts = function () {
  //
  // function to split a string into an array of substrings, based on a character
  // parameters are the string & character
  // i.e. stringsplit("abc,def,hgi", ",") = ["abc", "def", "hgi"]
  //
  stringsplit = '\
  local stringsplit = function(str, inSplitPattern) \
    local outResults = {} \
    local theStart = 1 \
    local theSplitStart, theSplitEnd = string.find(str, inSplitPattern, theStart) \
    while theSplitStart do \
      table.insert(outResults, string.sub(str, theStart, theSplitStart-1)) \
      theStart = theSplitEnd + 1 \
      theSplitStart, theSplitEnd = string.find(str, inSplitPattern, theStart) \
    end \
    table.insert(outResults, string.sub(str, theStart)) \
    return outResults \
  end \
  ';

  /*
  * scriptnewclient
  * params: brokerid, name, email, mobile, address, ifaid, clienttype, insttypes, hedge, brokerclientcode, commissionpercent, active
  * returns: {errorcode, clientid}, errorcode=0 is ok, otherwise error
  */
  exports.scriptnewclient = stringsplit + '\
  --[[ check email is unique ]] \
  local emailexists = redis.call("get", "client:" .. ARGV[3]) \
  if emailexists then return {1023} end \
  local clientid = redis.call("incr", "clientid") \
  if not clientid then return {1005} end \
  --[[ store the client ]] \
  redis.call("hmset", "client:" .. clientid, "clientid", clientid, "brokerid", ARGV[1], "name", ARGV[2], "email", ARGV[3], "password", ARGV[3], "mobile", ARGV[4], "address", ARGV[5], "ifaid", ARGV[6], "clienttype", ARGV[7], "hedge", ARGV[9], "brokerclientcode", ARGV[10], "commissionpercent", ARGV[11], "active", ARGV[12]) \
  --[[ add to set of clients ]] \
  redis.call("sadd", "clients", clientid) \
  --[[ add route to find client from email ]] \
  redis.call("set", "client:" .. ARGV[3], clientid) \
  --[[ add tradeable instrument types ]] \
  if ARGV[8] ~= "" then \
    local insttypes = stringsplit(ARGV[8], ",") \
    for i = 1, #insttypes do \
      redis.call("sadd", "client:" .. clientid .. ":instrumenttypes", insttypes[i]) \
    end \
  end \
  return {0, clientid} \
  ';

  /*
  * scriptupdateclient
  * params: clientid, brokerid, name, email, mobile, address, ifaid, clienttype, insttypes, hedge, brokerclientcode, commissionpercent, active
  * returns: errorcode, 0=ok, otherwise error
  */
  exports.scriptupdateclient = stringsplit + '\
  local clientkey = "client:" .. ARGV[1] \
  --[[ get existing email, in case we need to change email->client link ]] \
  local email = redis.call("hget", clientkey, "email") \
  if not email then return 1017 end \
  --[[ update client ]] \
  redis.call("hmset", clientkey, "brokerid", ARGV[2], "name", ARGV[3], "email", ARGV[4], "mobile", ARGV[5], "address", ARGV[6], "ifaid", ARGV[7], "clienttype", ARGV[8], "hedge", ARGV[10], "brokerclientcode", ARGV[11], "commissionpercent", ARGV[12], "active", ARGV[13]) \
  --[[ remove old email link and add new one ]] \
  if ARGV[4] ~= email then \
    redis.call("del", "client:" .. email) \
    redis.call("set", "client:" .. ARGV[4], ARGV[1]) \
  end \
  --[[ add/remove tradeable instrument types ]] \
  local insttypes = redis.call("smembers", "instrumenttypes") \
  local clientinsttypes = stringsplit(ARGV[9], ",") \
  for i = 1, #insttypes do \
    local found = false \
    for j = 1, #clientinsttypes do \
      if clientinsttypes[j] == insttypes[i] then \
        redis.call("sadd", clientkey .. ":instrumenttypes", insttypes[i]) \
        found = true \
        break \
      end \
    end \
    if not found then \
      redis.call("srem", clientkey .. ":instrumenttypes", insttypes[i]) \
    end \
  end \
  return 0 \
  ';
};
