/****************
* common.js
* Front-office common server functions
* Cantwaittotrade Limited
* Terry Johnston
* December 2013
****************/

exports.registerCommonScripts = function () {
	var subscribeinstrument;
	var unsubscribeinstrument;

	subscribeinstrument = '\
  	local subscribeinstrument = function(symbol, id, servertype) \
    	local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    	local marketext = redis.call("hget", servertype .. ":" .. id, "marketext") \
    	if marketext then \
      		topic = topic .. marketext \
    	end \
    	redis.call("sadd", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols", symbol) \
    	redis.call("sadd", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype, id) \
      	if redis.call("scard", "topic:" .. topic .. ":" .. servertype) == 0 then \
      		redis.call("publish", "proquote", "subscribe:" .. topic) \
     		redis.call("sadd", "topic:" .. topic .. ":" .. servertype, id) \
    		redis.call("sadd", servertype .. ":" .. id .. ":topics", topic) \
    	end \
    	local needtosubscribe = 0 \
    	if redis.call("sismember", "topic:" .. topic .. ":servers", servertype) == 0 then \
      		redis.call("sadd", "topic:" .. topic .. ":servers", servertype) \
      		redis.call("sadd", "server:" .. servertype .. ":topics", topic) \
      		needtosubscribe = 1 \
    	end \
    	return {needtosubscribe, topic} \
  	end \
  	';

  	// params: symbol, id, servertype
  	exports.scriptsubscribeinstrument = subscribeinstrument + '\
  	redis.call("sadd", "orderbook:" .. KEYS[1] .. ":" .. KEYS[3], KEYS[2]) \
  	redis.call("sadd", KEYS[3] .. ":" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  	local ret = subscribeinstrument(KEYS[1], KEYS[2], KEYS[3]) \
  	return ret \
  	';

  	unsubscribeinstrument = '\
  	local unsubscribeinstrument = function(symbol, id, servertype) \
    	local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    	local marketext = redis.call("hget", servertype .. ":" .. id, "marketext") \
    	if marketext then \
      		topic = topic .. marketext \
    	end \
    	redis.call("srem", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype, id) \
    	redis.call("srem", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols", symbol) \
    	local needtounsubscribe = 0 \
    	if redis.call("scard", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols") == 0 then \
      		redis.call("srem", servertype .. ":" .. id .. ":topics", topic) \
      		redis.call("srem", "topic:" .. topic .. ":" .. servertype, id) \
      		if redis.call("scard", "topic:" .. topic .. ":" .. servertype) == 0 then \
        		redis.call("publish", "proquote", "unsubscribe:" .. topic) \
        		redis.call("srem", "topic:" .. topic .. ":servers", servertype) \
      			redis.call("srem", "server:" .. servertype .. ":topics", topic) \
      			if redis.call("scard", "topic:" .. topic .. ":servers") == 0 then \
        			needtounsubscribe = 1 \
        		end \
      		end \
    	end \
    	return {needtounsubscribe, topic} \
  	end \
  	';

  	// params: symbol, userid
  	exports.scriptunsubscribeinstrument = unsubscribeinstrument + '\
  	redis.call("srem", "orderbook:" .. KEYS[1] .. ":" .. KEYS[3], KEYS[2]) \
  	redis.call("srem", KEYS[3] .. ":" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  	local ret = unsubscribeinstrument(KEYS[1], KEYS[2], KEYS[3]) \
  	return ret \
  	';

  	exports.scriptunsubscribeid = unsubscribeinstrument + '\
  	local orderbooks = redis.call("smembers", KEYS[2] .. ":" .. KEYS[1] .. ":orderbooks") \
  	local unsubscribetopics = {} \
  	for i = 1, #orderbooks do \
    	local ret = unsubscribeinstrument(orderbooks[i], KEYS[1], KEYS[2]) \
    	if ret[1] == 1 then \
      		table.insert(unsubscribetopics, ret[2]) \
      	end \
  	end \
  	return unsubscribetopics \
  	';
};
