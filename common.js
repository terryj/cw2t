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
    	redis.call("sadd", "topic:" .. topic .. ":" .. servertype .. ":symbols", symbol) \
    	local needtosubscribe = 0 \
      	if redis.call("scard", "topic:" .. topic .. ":" .. servertype) == 0 then \
      		redis.call("publish", "proquote", "subscribe:" .. topic) \
    		if redis.call("scard", "topic:" .. topic .. ":servers") == 0 then \
    			redis.call("sadd", "topics", topic) \
      			needtosubscribe = 1 \
    		end \
    	end \
      	redis.call("sadd", "topic:" .. topic .. ":servers", servertype) \
      	redis.call("sadd", "server:" .. servertype .. ":topics", topic) \
     	redis.call("sadd", "topic:" .. topic .. ":" .. servertype, id) \
    	redis.call("sadd", servertype .. ":" .. id .. ":topics", topic) \
    	return {needtosubscribe, topic} \
  	end \
  	';

  	unsubscribeinstrument = '\
  	local unsubscribeinstrument = function(symbol, id, servertype) \
    	local topic = redis.call("hget", "symbol:" .. symbol, "topic") \
    	local marketext = redis.call("hget", servertype .. ":" .. id, "marketext") \
    	if marketext then \
      		topic = topic .. marketext \
    	end \
    	local needtounsubscribe = 0 \
     	redis.call("srem", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype, id) \
    	redis.call("srem", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols", symbol) \
    	if redis.call("scard", "topic:" .. topic .. ":symbol:" .. symbol .. ":" .. servertype) == 0 then \
    		redis.call("srem", "topic:" .. topic .. ":" .. servertype .. ":symbols", symbol) \
    	end \
   		if redis.call("scard", "topic:" .. topic .. ":" .. servertype .. ":" .. id .. ":symbols") == 0 then \
      		redis.call("srem", servertype .. ":" .. id .. ":topics", topic) \
      		redis.call("srem", "topic:" .. topic .. ":" .. servertype, id) \
      		if redis.call("scard", "topic:" .. topic .. ":" .. servertype) == 0 then \
        		redis.call("publish", "proquote", "unsubscribe:" .. topic) \
        		redis.call("srem", "topic:" .. topic .. ":servers", servertype) \
      			redis.call("srem", "server:" .. servertype .. ":topics", topic) \
      			if redis.call("scard", "topic:" .. topic .. ":servers") == 0 then \
      				redis.call("srem", "topics", topic) \
        			needtounsubscribe = 1 \
        		end \
      		end \
    	end \
    	return {needtounsubscribe, topic} \
  	end \
  	';

  	//
  	// subscribe to a new instrument
  	// params: symbol, id, servertype
  	//
  	exports.scriptsubscribeinstrument = subscribeinstrument + '\
  	redis.call("sadd", "orderbook:" .. KEYS[1] .. ":" .. KEYS[3], KEYS[2]) \
  	redis.call("sadd", KEYS[3] .. ":" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  	local ret = subscribeinstrument(KEYS[1], KEYS[2], KEYS[3]) \
  	return ret \
  	';

  	//
  	// unsubscribe from an instrument
  	// params: symbol, id, servertype
  	//
  	exports.scriptunsubscribeinstrument = unsubscribeinstrument + '\
  	redis.call("srem", "orderbook:" .. KEYS[1] .. ":" .. KEYS[3], KEYS[2]) \
  	redis.call("srem", KEYS[3] .. ":" .. KEYS[2] .. ":orderbooks", KEYS[1]) \
  	local ret = unsubscribeinstrument(KEYS[1], KEYS[2], KEYS[3]) \
  	return ret \
  	';

  	//
  	// unsubscribe a user/client/other connection
  	// params: servertype, id
  	//
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

  	//
  	// unsubscribe everything for a server type
  	// params: servertype
  	//
  	/*exports.scriptunsubscribeserver = '\
  	local topics = redis.call("smembers", "server:" .. KEYS[1] .. ":topics") \
  	for i = 1, #topics do \
  		local symbols = redis.call("smembers", "topic:" .. topics[i] .. ":" .. KEYS[1] .. ":symbols") \
  		for j = 1, #symbols do \
    		local users = redis.call("smembers", "topic:" .. topics[i] .. ":symbol:" .. symbols[j] .. ":" .. KEYS[1]) \
    		for k = 1, #users do \
     			redis.call("srem", "topic:" .. topics[i] .. ":symbol:" .. symbols[j] .. ":" .. KEYS[1], users[k]) \
    			redis.call("srem", "topic:" .. topics[i] .. ":" .. KEYS[1] .. ":" .. users[k] .. ":symbols", symbols[j]) \
    		end \
    		redis.call("srem", "topic:" .. topics[i] .. ":" .. KEYS[1] .. ":symbols", symbols[j]) \
  		end \
  		local connections = redis.call("smembers", "topic:" .. topics[i] .. ":" .. KEYS[1]) \
  		for j = 1, #connections do \
  			redis.call("srem", "topic:" .. topics[i] .. ":" .. KEYS[1], connections[j]) \
      		redis.call("srem", KEYS[1] .. ":" .. connections[j] .. ":topics", topics[i]) \
  		end \
  		redis.call("srem", "topic:" .. topics[i] .. ":servers", KEYS[1]) \
  		redis.call("srem", "server:" .. KEYS[1] .. ":topics", topics[i]) \
  	end \
  	';

  	exports.scriptunsubscribeserver = '\
  	local connections = redis.call("smembers", "connections:" + KEYS[1]) \
  	for i = 1, #connections do \
  	end \
  	';*/
};