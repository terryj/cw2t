/**************************
 * Cw2tClient.java
 * Proquote client
 * Terry Johnston
 * 27 May 2013
 * Cantwaittotrade Limited
 *
 * This application acts as a client to the Proquote data feed.
 * It creates a Proquote session using the ClientSessionLayer class.
 * The session handles connections and subscriptions to data.
 * Subscriptions can be requested at start-up, either manually or via
 * an .ini file. Two connections to Redis are established using Jedis.
 * One connection is for publishing & one for subscribing.
 * The subscribe connection is run on its own thread as it blocks.
 * Subscriptions are added to the Proquote session when data is received
 * via the subscribe channel.
 * Data received from Proquote is published via the publish channel.
 *******************************************************************/

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

// logging
import org.apache.log4j.Level;

// proquote
import it.itsoftware.itmware.ClientSessionLayer;
import it.itsoftware.itmware.ISessionObserver;
import it.itsoftware.itmware.Peer;
import it.itsoftware.itmware.SLDictionary;
import it.itsoftware.itmware.SLException;
import it.itsoftware.itmware.TLException;
import it.itsoftware.utility.ConfigManager;
import it.itsoftware.utility.ITLogger;
import it.itsoftware.utility.TagSet;
import it.itsoftware.utility.TagValueMap;
import it.itsoftware.utility.Tag;
import it.itsoftware.utility.TagValueIterator;
import it.itsoftware.utility.Value;
import it.itsoftware.utility.ISDictionary;
import it.itsoftware.utility.value.BaseValue;

// jedis
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPool;

// listening class for jedis pubsub
class Cw2tSubscriber extends JedisPubSub {
    private ClientSessionLayer session;
    
    public Cw2tSubscriber(ClientSessionLayer session) {
        this.session = session;        
    }
    
    public void onMessage(String channel, String message) {
        System.out.println("Received message: " + message);
        
        if (message.equals("exit")) {
            unsubscribe();
            return;
        }
        
        /*TagSet s = new TagSet();
        //s.add(Tag.makeTag( "4040"));
        s.add(Tag.makeTag( "4100"));
        s.add(Tag.makeTag( "4101"));
        s.add(Tag.makeTag( "4103"));
        s.add(Tag.makeTag( "4104"));
        s.add(Tag.makeTag( "4106"));
        s.add(Tag.makeTag( "4112"));
        s.add(Tag.makeTag( "4113"));
        //s.add(Tag.makeTag( "4115"));
        s.add(Tag.makeTag( "4120"));
        s.add(Tag.makeTag( "4121"));
        s.add(Tag.makeTag( "4124"));
        //s.add(Tag.makeTag( "4300"));*/
        
        //session.Subscribe(message, SLDictionary.ReqType.REQ_SNAP_REF, s);
        
        TagValueMap tvm = new TagValueMap();
        //tvm.put( ISDictionary.T_AVAILABLE, BaseValue.value(true) );
        //tvm.put( ISDictionary.T_SYMBOL, BaseValue.value( "sym_" + topics[i] ));
        //tvm.put(ISDictionary.T_TYPE, BaseValue.value("DE"));
        
        // to generate a message, use i.e. 'publish proquote ***' on a Redis connection
        if (message.equals("instrument")) {
            // request a product list for specified market
            tvm.put(ISDictionary.T_MARKET, BaseValue.value("L"));
            session.Request(message, "REQ_ANAGRAFICA_MIT_EXT", null, tvm);
        } else if (message.equals("UKX")) {
            // request constituents of specified index
            tvm.put(ISDictionary.T_MARKET, BaseValue.value("L"));
            tvm.put(ISDictionary.T_INDEX_BASKET_ID, BaseValue.value("TIT.UKX.FTS"));
            session.Request(message, "REQ_INDEX_COMPOS_ALL", null, tvm);           
        } else if (message.equals("MCX")) {
            // request constituents of specified index
            tvm.put(ISDictionary.T_MARKET, BaseValue.value("L"));
            tvm.put(ISDictionary.T_INDEX_BASKET_ID, BaseValue.value("TIT.MCX.FTS"));
            session.Request(message, "REQ_INDEX_COMPOS_ALL", null, tvm);           
        } else if (message.substring(0, 9).equals("subscribe")) {
            TagSet s = new TagSet();
            
            // bid/offer price
            s.add(Tag.makeTag("4332/1"));
            s.add(Tag.makeTag("4333/1"));
            
            // last traded price
            //s.add(Tag.makeTag("4300"));
            /*s.add(Tag.makeTag("4332/2"));
            s.add(Tag.makeTag("4333/2"));
            s.add(Tag.makeTag("4332/3"));
            s.add(Tag.makeTag("4333/3"));
            s.add(Tag.makeTag("4332/4"));
            s.add(Tag.makeTag("4333/4"));
            s.add(Tag.makeTag("4332/5"));
            s.add(Tag.makeTag("4333/5"));
            s.add(Tag.makeTag("4332/6"));
            s.add(Tag.makeTag("4333/6"));*/
            
            // bid/offer volume
            /*s.add(Tag.makeTag("4331/1"));
            s.add(Tag.makeTag("4334/1"));
            s.add(Tag.makeTag("4331/2"));
            s.add(Tag.makeTag("4334/2"));
            s.add(Tag.makeTag("4331/3"));
            s.add(Tag.makeTag("4334/3"));
            s.add(Tag.makeTag("4331/4"));
            s.add(Tag.makeTag("4334/4"));
            s.add(Tag.makeTag("4331/5"));
            s.add(Tag.makeTag("4334/5"));
            s.add(Tag.makeTag("4331/6"));
            s.add(Tag.makeTag("4334/6"));*/
            
            //s.add(Tag.makeTag("4220"));
            //s.add(Tag.makeTag("4221"));

            System.out.println("Subscribing to topic: " + message.substring(10));
            session.Subscribe(message.substring(10), SLDictionary.ReqType.REQ_SNAP_REF, s);
        } else if (message.substring(0, 11).equals("unsubscribe")) {
            System.out.println("Unsubscribing topic: " + message.substring(12));
            session.Subscribe(message.substring(12), SLDictionary.ReqType.REQ_FORGET, null);
        }
    }
    
    public void onSubscribe(String channel, int subscribedChannels) {
        System.out.println("Subscribed to channel " + channel);
    }
    
    public void onUnsubscribe(String channel, int subscribedChannels) {
        System.out.println("Unsubscribed to channel " + channel);
    }
    
    public void onPSubscribe(String pattern, int subscribedChannels) {
    }
    
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
    }
    
    public void onPMessage(String pattern, String channel, String message) {
    }
}

// listening thread for jedis pubsub
class Cw2tSubscriberThread extends Thread {
    private Jedis jedissubscriber;
    private Cw2tSubscriber cw2tsubscriber;

    public Cw2tSubscriberThread(Jedis jedissubscriber, Cw2tSubscriber cw2tsubscriber) {
        this.jedissubscriber = jedissubscriber;
        this.cw2tsubscriber = cw2tsubscriber;
    }
    
    public void run() {
        System.out.println("Starting pubsub thread");
        jedissubscriber.subscribe(cw2tsubscriber, "proquote");
        System.out.println( "Exiting pubsub thread");
    }
}

public class Cw2tClient implements ISessionObserver {
	ClientSessionLayer session;
	int status;
    JedisPool jedispool;
    Jedis jedissubscriber;
	
	public static final int st_start = 0;
	public static final int st_connected = 1;
	public static final int st_running = 2;
	public static final int st_disconnected = 3;
	public static final int st_connecting = 4;
	
	HashMap<String, TagSet> subscriptions = new HashMap<String, TagSet>();
	int showEvery = 0;
	int receivedMessages = 0;
    
	Cw2tClient(String[] args) {
		System.out.println("Creating session layer");
		status = st_start;
	}
    
    public void startJedis() {
        System.out.println("Starting Jedis");
        
        // get a connection pool
        jedispool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, 0);
        
        // connection for subscribing
        jedissubscriber = jedispool.getResource();
        
        // instance of listening class
        Cw2tSubscriber cw2tsubscriber = new Cw2tSubscriber(session);
        
        // create a separate thread as connection will be blocked
        new Cw2tSubscriberThread(jedissubscriber, cw2tsubscriber).start();
    }
    
    public void stopJedis() {
        System.out.println("Stopping Jedis");
        
        // send message to subscribe channel to terminate thread
        Jedis jedispublisher = jedispool.getResource();
        jedispublisher.publish("proquote", "exit");
        jedispool.returnResource(jedispublisher);
        
        // tidy jedis
        jedispool.returnResource(jedissubscriber);
        jedispool.destroy();
    }
	
	public void onAck(Peer peer, String tid, boolean bSuccess, String sReason) {
		// TODO Auto-generated method stub
	}

	public String onCloseRequest(Peer peer, String RID) {
		// TODO Auto-generated method stub
		return null;
	}

	public void onGetStatus(Peer peer) {
		// TODO Auto-generated method stub
	}

	public void onHeartbeatTimeout(Peer peer) {
		System.out.println("Remote connection heart beat timeout. Reconnecting...");
		startSession();
	}

	public void onLoadStatus(Peer peer, int totClients, int totTopics, int cliTopics, int loadFactor) {
		// TODO Auto-generated method stub
	}

	public String onLogin(Peer peer, String username, String password) {
		// We don't manage login in this client.
		return null;
	}

	public void onLogout(Peer peer) {
		// We don't manage logout in this client.
        System.out.println("logout");
	}

	public void onReply(Peer peer, String RID, TagValueMap tvm, boolean isLast) {
		System.out.println("Received reply " + RID + (isLast ? "(last) " : "") 
				+ tvm.toString());
        
        if (RID.equals("instrument")) {
            updateProducts(tvm);
        } else if (RID.equals("UKX")) {
            updateIndex("UKX", tvm);            
        } else if (RID.equals("MCX")) {
            updateIndex("MCX", tvm);
        }
	}

	public String onRequest(Peer peer, String request, String RID, String Tpc, TagValueMap params) {
		// We don't manage requests in this client.
		return null;
	}

	public void onStatus(Peer peer, int st, String description) {
		// TODO Auto-generated method stub
		System.out.println("On status " + st + ": " + description);
		
		// signal our status
		synchronized (this) {
			status = (st == SLDictionary.STATUS_CONNECT ? st_connected : st_disconnected);
			notify();
		}
	}
    
	public String onSubscribe(Peer peer, int reqType, String Tpc, TagSet ts) {
		// We don't manage subscriptions in this client.
		return null;
	}

	public void onUpdate(Peer peer, String Tpc, TagValueMap tvm) {
        Map<String, String> fieldmap = new HashMap<String, String>();

		++receivedMessages;
		
		if (showEvery > 0) {
			if (receivedMessages % showEvery == 0) {
				System.out.println("Received " + receivedMessages +  " messages");
			}
		} else {
            // todo: consider adding as an option
			//System.out.println("Received update on " + Tpc + ": " + tvm.toString());
		}
        /*for ( int i = 0; i < topics.length; i ++ ) {
            TagValueMap tvm = new TagValueMap();
            tvm.put( ISDictionary.T_AVAILABLE, BaseValue.value(true) );
            tvm.put( ISDictionary.T_SYMBOL, BaseValue.value( "sym_" + topics[i] ));
            tvm.put( ISDictionary.T_MARKET, BaseValue.value( "Fake Markets Ltd." ));
            cache.put( topics[i], tvm );
        }*/
        
        // get the symbol - todo: keep in a symbol -> topic hash?
        //String symbol = jedispublisher.hget("topic:" + Tpc, "symbol");
        
        // only interested in topics that are recognised symbols
        /*if (symbol == null) {
            System.out.println("Topic not found: " + Tpc);
            return;
        }*/
        //{\"orderbook\":{\"symbol\":\"" + symbol + "\",\"
        
        String jsonmsg = "\"prices\":[";
        boolean firstprice = true;
        String level = "";
        String desc = "";
        
        // iterate through the tag map
        TagValueIterator tvi = tvm.getIterator();
        while (tvi.hasNext()) {
            // get the tag
            int i = tvi.next();
            String tag = Tag.toString(i);
            
            // get the tag description and level
            int slash = tag.indexOf("/");
            if (slash > 0) {
                if (tag.substring(0, slash).equals("4331")) {
                    desc = "bidvol";
                } else if (tag.substring(0, slash).equals("4332")) {
                    desc = "bid";
                } else if (tag.substring(0, slash).equals("4333")) {
                    desc = "offer";
                } else if (tag.substring(0, slash).equals("4334")) {
                    desc = "offervol";
                }
                level = tag.substring(slash+1);
            } else {
                //System.out.println(tag);
                
                /*if (tag.equals("4221") {
                    
                }*/
                continue;
            }
            
            // get the value
            String s = "";
            Value value = tvi.getValue();
            if (value.getType() == value.DOUBLE) {
                double d = value.getDoubleValue();
                
                if (desc.equals("bid") || desc.equals("offer")) {
                    // will produce 2dp - todo: check & vary this?
                    s = String.format("%.2f", d);
                } else {
                    s = Double.toString(d);
                }
            } else if (value.getType() == value.STRING) {
                s = value.getStringValue();
            } else if (value.getType() == value.BOOL) {
                boolean b = value.getBooleanValue();
                s = Boolean.toString(b);
            }
            
            if (!firstprice) {
                jsonmsg = jsonmsg + ",";
            } else {
                firstprice = false;
            }
            
            // add a price level to the message
            jsonmsg = jsonmsg + "{\"level\":" + level +  ",\"" + desc + "\":\"" + s + "\"}";
            
            // add to map
            fieldmap.put(desc+level, s);
        }
        
        if (firstprice) return;
        
        // complete the message
        jsonmsg = jsonmsg + "]";
        
        Jedis jedispublisher = jedispool.getResource();
        
        // publish message to channel
        jedispublisher.publish(Tpc, jsonmsg);
        
        // store prices
        String status = jedispublisher.hmset("topic:" + Tpc, fieldmap);
        
        jedispool.returnResource(jedispublisher);
	}
    
    // old
    
    /*String jsonmsg = "\"prices\":[";
    boolean firstprice = true;
    String level = "";
    String bidoffer = "";
    
    // iterate through the tag map
    TagValueIterator tvi = tvm.getIterator();
    while (tvi.hasNext()) {
        // get the tag
        int i = tvi.next();
        String tag = Tag.toString(i);
        
        // get the tag description and level
        int slash = tag.indexOf("/");
        if (slash > 0) {
            if (tag.substring(0, slash).equals("4332")) {
                bidoffer = "bid";
            } else {
                bidoffer = "offer";
            }
            level = tag.substring(slash+1);
        }
        
        // get the value
        String s = "";
        Value value = tvi.getValue();
        if (value.getType() == value.DOUBLE) {
            double d = value.getDoubleValue();
            
            // will produce 2dp - todo: check & vary this?
            s = String.format("%.2f", d);
            
        } else if (value.getType() == value.STRING) {
            s = value.getStringValue();
        } else if (value.getType() == value.BOOL) {
            boolean b = value.getBooleanValue();
            s = Boolean.toString(b);
        }
        
        if (!firstprice) {
            jsonmsg = jsonmsg + ",";
        } else {
            firstprice = false;
        }
        
        // add a price level to the message
        jsonmsg = jsonmsg + "{\"level\":" + level +  ",\"" + bidoffer + "\":" + s + "}";
        
        // add to map
        fieldmap.put(bidoffer+level, s);
    }
    
    // complete the message
    jsonmsg = jsonmsg + "]";
    //System.out.println(jsonmsg);
    
    // publish message to channel
    jedispublisher.publish(Tpc, jsonmsg);
    
    // store prices
    String status = jedispublisher.hmset("topic:" + Tpc, fieldmap);*/
    
    public void updateIndex(String index, TagValueMap tvm) {
        String symbol = "";
        String market = "";
        
        TagValueIterator tvi = tvm.getIterator();
        while (tvi.hasNext()) {
            // get the tag
            int i = tvi.next();
            int tag = Tag.getTag(i);
        
            // get the value
            String s = "";
            Value value = tvi.getValue();
            if (value.getType() == value.STRING) {
                s = value.getStringValue();
            } else if (value.getType() == value.DOUBLE) {
                double d = value.getDoubleValue();
                s = Double.toString(d);
            } else if (value.getType() == value.BOOL) {
                boolean b = value.getBooleanValue();
                s = Boolean.toString(b);
            }
        
            // get the tag name
            String tagname = getTagName(tag);

            if (tagname.equals("proquotesymbol")) {
                // remove trailing dot if necessary
                if (s.charAt(s.length()-1) == '.') {
                    s = s.substring(0, s.length()-1);
                }
                symbol = s;
            }
            
            if (tagname.equals("market")) {
                market = s;
            }
        }
        
        symbol = symbol + "." + market;
        
        Jedis jedispublisher = jedispool.getResource();
        jedispublisher.sadd("index:" + index, symbol);
        jedispool.returnResource(jedispublisher);
        
        System.out.println("updated index:" + index + " with symbol:" + symbol);
    }
    
    public void updateProducts(TagValueMap tvm) {
        Map<String, String> fieldmap = new HashMap<String, String>();
        Map<String, String> topicfieldmap = new HashMap<String, String>();
        String symbol = "";
        String market = "";
        String insttype = "";
        String topic = "";
        String proquotesymbol = "";
        
        TagValueIterator tvi = tvm.getIterator();
        while (tvi.hasNext()) {
            // get the tag
            int i = tvi.next();
            int tag = Tag.getTag(i);
            
            // get the value
            String s = "";
            Value value = tvi.getValue();
            if (value.getType() == value.STRING) {
                s = value.getStringValue();
            } else if (value.getType() == value.DOUBLE) {
                double d = value.getDoubleValue();
                s = Double.toString(d);
            } else if (value.getType() == value.BOOL) {
                boolean b = value.getBooleanValue();
                s = Boolean.toString(b);
            }
            
            // get the tag name
            String tagname = getTagName(tag);
            
            // ignore unwanted tags
            if (tagname.equals("invalid")) {
                continue;
            }
            
            // just domestic & international equities for the time being
            if (tagname.equals("instrumenttype")) {
                if (s.equals("DE") || s.equals("IE")) {
                    insttype = s;
                } else {
                    System.out.println("ignoring, type:" + s);                
                    return;
                }
            }
            
            // store the topic for reverse link
            if (tagname.equals("topic")) {
                topic = s;
            }
            
            // change 'GBX' to 'GBP'
            if (tagname.equals("currency")) {
                if (s.equals("GBX")) {
                    s = "GBP";
                }
            }
            
            // only products tradable on daily official list
            if (tagname.equals("dol")) {
                if (s.equals("false")) {
                    return;
                } else {
                    continue;
                }
            }
            
            // we are using 'symbol' as our generic key, so need something else
            if (tagname.equals("proquotesymbol")) {
                proquotesymbol = s;
                
                // remove trailing dot if necessary
                if (s.charAt(s.length()-1) == '.') {
                    s = s.substring(0, s.length()-1);
                }
                symbol = s;
            }
            
            if (tagname.equals("market")) {
                market = s;
            }
            
            fieldmap.put(tagname, s);
        }
        
        // make sure there is something in the map
        if (fieldmap.isEmpty()) {
            return;
        }
        
        // ptm levy exempt default
        fieldmap.put("ptmexempt", "0");
        
        // update instruments
        symbol = symbol + "." + market;
        String key = "symbol:" + symbol;
        fieldmap.put("symbol", symbol);
        
        Jedis jedispublisher = jedispool.getResource();        

        String status = jedispublisher.hmset(key, fieldmap);
        jedispublisher.sadd("instruments", symbol);
        jedispublisher.sadd("instrumenttypes", insttype);
        
        // create a way to get from topic to symbol (i.e. 'TIT.VOD.L' -> 'VOD.L')
        topicfieldmap.put("symbol", symbol);
        status = jedispublisher.hmset("topic:" + topic, topicfieldmap);
        jedispublisher.sadd("topic:" + topic + ":symbols", symbol);
        
        // & for delayed (i.e. 'TIT.VOD.LD' -> 'VOD.L')
        status = jedispublisher.hmset("topic:" + topic + "D", topicfieldmap);
        jedispublisher.sadd("topic:" + topic + "D" + ":symbols", symbol);
        
        // initialise stored prices
        initPrices("topic:" + topic);
        initPrices("topic:" + topic + "D");

        // create a way to get from proquote symbol to symbol (i.e. 'VOD' -> 'VOD.L')
        jedispublisher.set("proquotesymbol:" + proquotesymbol, symbol);

        // update instrument types
        String insttypedesc = getInstTypeDesc(insttype);
        jedispublisher.set("instrumenttype:" + insttype, insttypedesc);
        
        jedispool.returnResource(jedispublisher);

        System.out.println("symbol:" + symbol + " added, status:" + status);
    }
    
    public String initPrices(String topickey) {
        Map<String, String> fieldmap = new HashMap<String, String>();
        
        fieldmap.put("bid1", "0");
        fieldmap.put("offer1", "0");
        fieldmap.put("bid2", "0");
        fieldmap.put("offer2", "0");
        fieldmap.put("bid3", "0");
        fieldmap.put("offer3", "0");
        fieldmap.put("bid4", "0");
        fieldmap.put("offer4", "0");
        fieldmap.put("bid5", "0");
        fieldmap.put("offer5", "0");
        fieldmap.put("bid6", "0");
        fieldmap.put("offer6", "0");
        
        Jedis jedispublisher = jedispool.getResource();        
        String status = jedispublisher.hmset(topickey, fieldmap);
        jedispool.returnResource(jedispublisher);

        return status;
    }
    
    public void subscribeCw2tTopics() {
        TagSet s = new TagSet();
        
        s.add(Tag.makeTag("4332/1"));
        s.add(Tag.makeTag("4333/1"));
        /*s.add(Tag.makeTag("4332/2"));
        s.add(Tag.makeTag("4333/2"));
        s.add(Tag.makeTag("4332/3"));
        s.add(Tag.makeTag("4333/3"));
        s.add(Tag.makeTag("4332/4"));
        s.add(Tag.makeTag("4333/4"));
        s.add(Tag.makeTag("4332/5"));
        s.add(Tag.makeTag("4333/5"));
        s.add(Tag.makeTag("4332/6"));
        s.add(Tag.makeTag("4333/6"));*/
        
        // get existing topics
        Set<String> topics = new HashSet<String>();
        
        Jedis jedispublisher = jedispool.getResource();
        topics = jedispublisher.smembers("topics");
        jedispool.returnResource(jedispublisher);
            
        // & subscribe to them
        Iterator<String> iter = topics.iterator();
        while(iter.hasNext()) {
            String topic = iter.next();
            System.out.println("Subscribing to topic: " + topic);
            session.Subscribe(topic, SLDictionary.ReqType.REQ_SNAP_REF, s);
        }
    }
    
	public String onUpdateRequest(Peer peer, String RID, TagValueMap params) {
		// We don't manage requests in this client.
		return null;
	}
    
	public boolean onPeerConnected(Peer peer) {
		// unimplemented by the client.
		return false;
	}
    
	boolean startSession() {
		session = new ClientSessionLayer(this);
		
		try {
			session.Configure("[ETIS_CLIENT]");
			session.startUp();
		} catch (SLException ex) {
			System.err.println("Error initializing session layer: "
                               + ex.toString());
			return false;
		} catch (TLException ex) {
			System.err.println("Error initializing transport layer: "
                               + ex.toString());
			return false;
		}	
		
		synchronized (this) {
			status = st_connecting;
		}
		
		return true;
	}
	
	public void DoSubscriptions() {
		Iterator<String> isub = subscriptions.keySet().iterator();
		while (isub.hasNext()) {
			String Topic = isub.next();
			session.Subscribe(Topic, SLDictionary.ReqType.REQ_SNAP_REF, subscriptions.get(Topic));
		}
		
		// Ok, we're running
		synchronized (this) {
			status = st_running;
		}
	}
	
	public boolean ReadSubscriptions(final String section) {
		Iterator<String> iter = ConfigManager.getSectionProperties(section);
		subscriptions.clear();
		boolean status = true;
		while (iter.hasNext()) {
			String Topic = (String) iter.next();
			try {
				String tags = ConfigManager.getStringProperty(section, Topic);
				if (tags.equals("*")) {
					subscriptions.put(Topic, null);
				} else {
					String taglist[] = tags.split("[,;]");
					TagSet ts = TagSet.fromStrings(taglist);
					subscriptions.put(Topic, ts);
				}
			}
			catch (Exception e) {
				System.out.println("Error while parsing subscription: " + Topic);
				status = false;
			}
		}
		
		if (!status)
			return false;
		
		if (subscriptions.isEmpty()) {
			System.out.println("Empty or not present [Subscription] section " + section);
			return false;
		}
	
		return true;
	}

	public boolean Setup() {
		try {
			String se = ConfigManager.getStringProperty("[ETIS_CLIENT]", "ShowEvery");
		
			if (se != null) {
                showEvery = Integer.parseInt(se);
			}
		}
		catch( Exception e) {
			
		}
		return true;
	}

    public String getTagName(String tag) {
        String tagname = "";
        
        if (tag.equals("4332/1")) {
            tagname = "bid0";
        } else if (tag.equals("4333/1")) {
            tagname = "offer0";
        } else if (tag.equals("4332/2")) {
            tagname = "bid1";
        } else if (tag.equals("4333/2")) {
            tagname = "offer1";
        } else if (tag.equals("4300")) {
            tagname = "lasttradeprice";
        }
        
        return tagname;
    }
    
    public String getTagName(int tag) {
        String tagname;
        
        switch (tag) {
            case 4040:
                tagname = "available";
                break;
            case 4099:
                tagname = "sourcetime";
                break;
            case 4100:
            case 14100:
                tagname = "proquotesymbol";
                break;
            case 4101:
                tagname = "description";
                break;
            case 4103:
                tagname = "market";
                break;
            case 4104:
                tagname = "isin";
                break;
            case 4106:
                tagname = "instrumenttype";
                break;
            case 4112:
                tagname = "category";
                break;
            case 4113:
                tagname = "currency";
                break;
            /*case 4115:
                tagname = "minlotsize";
                break;*/
            case 4120:
                tagname = "sector";
                break;
            case 4121:
                tagname = "sectordesc";
                break;
            case 5174:
                tagname = "sedol";
                break;
            case 24001:
                tagname = "topic";
                break;
            case 5175:
                tagname = "dol";
                break;
            case 14101:
                tagname = "longname";
                break;
            default:
                tagname = "invalid";
                break;
        }
        
        return tagname;
    }
    
    public String getInstTypeDesc(String insttype) {
        String insttypedesc;
        
        if (insttype.equals("AI")) {
            insttypedesc = "Automated Input FacilityNotification";
        } else if (insttype.equals("AL")) {
            insttypedesc = "Allotment Letters";
        } else if (insttype.equals("BD")) {
            insttypedesc = "Bonds";
        } else if (insttype.equals("BG")) {
            insttypedesc = "Bulldogs";
        } else if (insttype.equals("BO")) {
            insttypedesc = "Bond";
        } else if (insttype.equals("CF")) {
            insttypedesc = "Closed Funds";
        } else if (insttype.equals("CN")) {
            insttypedesc = "Convertible";
        } else if (insttype.equals("CP")) {
            insttypedesc = "Commercial Paper";
        } else if (insttype.equals("CW")) {
            insttypedesc = "Covered Warrants";
        } else if (insttype.equals("DB")) {
            insttypedesc = "Debenture";
        } else if (insttype.equals("DE")) {
            insttypedesc = "UK Equity";
        } else if (insttype.equals("DR")) {
            insttypedesc = "Depository Receipts";
        } else if (insttype.equals("EW")) {
            insttypedesc = "Equity Warrants";
        } else if (insttype.equals("FB")) {
            insttypedesc = "Foreign Government Bonds";
        } else if (insttype.equals("FC")) {
            insttypedesc = "Financial Certificates";
        } else if (insttype.equals("FL")) {
            insttypedesc = "Fully Paid Letter";
        } else if (insttype.equals("FR")) {
            insttypedesc = "Floating Rate";
        } else if (insttype.equals("FS")) {
            insttypedesc = "Foreign Shares";
        } else if (insttype.equals("FT")) {
            insttypedesc = "Foreign Unit Trusts";
        } else if (insttype.equals("FU")) {
            insttypedesc = "Fund Units";
        } else if (insttype.equals("FX")) {
            insttypedesc = "Fix Rate";
        } else if (insttype.equals("GT")) {
            insttypedesc = "Gilts";
        } else if (insttype.equals("GW")) {
            insttypedesc = "Gilt Warrants";
        } else if (insttype.equals("IE")) {
            insttypedesc = "International Equities";
        } else if (insttype.equals("IP")) {
            insttypedesc = "Investment Products";
        } else if (insttype.equals("IT")) {
            insttypedesc = "Italian Equities";
        } else if (insttype.equals("KR")) {
            insttypedesc = "Kruger Rand Group";
        } else if (insttype.equals("LC")) {
            insttypedesc = "Leverage Products Bull";
        } else if (insttype.equals("LE")) {
            insttypedesc = "Leverage Products Exotic";
        } else if (insttype.equals("LP")) {
            insttypedesc = "Leverage Products Bear";
        } else if (insttype.equals("LS")) {
            insttypedesc = "Loan Stock";
        } else if (insttype.equals("MC")) {
            insttypedesc = "Multi Coupon";
        } else if (insttype.equals("ML")) {
            insttypedesc = "Medium Term Loans";
        } else if (insttype.equals("NA")) {
            insttypedesc = "News Announcement";
        } else if (insttype.equals("NL")) {
            insttypedesc = "Nil Paid Letter";
        } else if (insttype.equals("OC")) {
            insttypedesc = "One Coupon";
        } else if (insttype.equals("OS")) {
            insttypedesc = "Provisional JSE Ordinary Share";
        } else if (insttype.equals("CF")) {
            insttypedesc = "Primary Capital Certificates";
        } else if (insttype.equals("PN")) {
            insttypedesc = "Partly Paid Letter";
        } else if (insttype.equals("PN")) {
            insttypedesc = "Portfolio Notification";
        } else if (insttype.equals("PR")) {
            insttypedesc = "Preference Shares";
        } else if (insttype.equals("PS")) {
            insttypedesc = "Preference Share";
        } else if (insttype.equals("PU")) {
            insttypedesc = "Package Units";
        } else if (insttype.equals("RG")) {
            insttypedesc = "Rights";
        } else if (insttype.equals("RT")) {
            insttypedesc = "Rights";
        } else if (insttype.equals("RV")) {
            insttypedesc = "Reverse";
        } else if (insttype.equals("SC")) {
            insttypedesc = "Step Coupon";
        } else if (insttype.equals("SH")) {
            insttypedesc = "Share";
        } else if (insttype.equals("SP")) {
            insttypedesc = "Structured Products";
        } else if (insttype.equals("SU")) {
            insttypedesc = "Stapled Unit";
        } else if (insttype.equals("TA")) {
            insttypedesc = "Tradable During Auction";
        } else if (insttype.equals("TC")) {
            insttypedesc = "Tradable Commodities";
        } else if (insttype.equals("TF")) {
            insttypedesc = "Tradable Fund";
        } else if (insttype.equals("TR")) {
            insttypedesc = "Tradable In Regulated Segment";
        } else if (insttype.equals("UT")) {
            insttypedesc = "Unit Trust";
        } else if (insttype.equals("WA")) {
            insttypedesc = "Warrants";
        } else if (insttype.equals("WC")) {
            insttypedesc = "Leveraged Products Covered Warrant Call";
        } else if (insttype.equals("WP")) {
            insttypedesc = "Leveraged Products Covered Warrant Put";
        } else {
            insttypedesc = "unknown instrument type";
        }
        
        return insttypedesc;
    }
	
	/*************************************************************************
	 * Main routine
	 * @param args
	 */
    
	public static void main(String[] args) {
		// ini file?
		if (args.length > 0) {
			// Should we read an ini file?
			try {
				ITLogger.initDefault(args[0]);
				ConfigManager.init(ConfigManager.INI_FILE, args[0]);
				System.out.println("Initializing system");
			} catch (Exception ex) {
				System.err.println("Error initializing configuration manager module:"
								+ ex.toString());
				System.exit(1);
			}
		}
		else {
			Properties props = new Properties();
			
			// create a default property set configuration
	        props.put("ETIS_CLIENT.Transport", "[Remote]");
	        props.put("ETIS_CLIENT.UserID", "apiwaittest1");
	        props.put("ETIS_CLIENT.Password", "test123");
            props.put( "ETIS_CLIENT.HeartBeat", "30");

	        //props.put("Subscriptions.TIT.VOD.LD", "4040");
	        /*props.put("Subscriptions.TIT.VOD.LD", "4040,4100,4101,4300,4301,4302,4330/1,4331/1,4332/1,4333/1,4334/1,4335/1,4330/2,4331/2,4332/2,4333/2,4334/2,4335/2");*/
	        //props.put("Subscriptions.TIT.A%.MTA", "4040,4100,4101,4300,4301,4302,4330/1,4331/1,4332/1,4333/1,4334/1,4335/1,4330/2,4331/2,4332/2,4333/2,4334/2,4335/2");
	        // a long subscription. -- more than 255 tags
	        /*props.put( "Subscriptions.TIT.S??.MTA","4040,4100,4101,4300,4301,4302,4330/1,4331/1,4332/1,4333/1,4334/1,4335/1,4330/2,4331/2,4332/2,4333/2,4334/2,4335/24100,4101,4102,4103,4104,4105,4106,4107,4108,4109,4110,4111,4112,4113,4114,4115,4116,4117,4118,4119,4120,4121,4122,4123,4124,4125,4126,4127,4128,4129,4130,4131,4132,4133,4134,4135,4136,4137,4138,4139,4140,4141,4142,4143,4144,4145,4146,4147,4148,4149,4150,4151,4152,4153,4154,4155,4156,4157,4158,4159,4160,4161,4162,4163,4164,4165,4166,4167,4168,4169,4170,4171,4172,4173,4174,4175,4176,4177,4178,4179,4180,4181,4182,4183,4184,4185,4186,4187,4188,4189,4190,4191,4192,4193,4194,4195,4196,4197,4198,4199,4200,4201,4202,4203,4204,4205,4206,4207,4208,4209,4210,4211,4212,4213,4214,4215,4216,4217,4218,4219,4220,4221,4222,4223,4224,4225,4226,4227,4228,4229,4230,4231,4232,4233,4234,4235,4236,4237,4238,4239,4240,4241,4242,4243,4244,4245,4246,4247,4248,4249,4250,4251,4252,4253,4254,4255,4256,4257,4258,4259,4260,4261,4262,4263,4264,4265,4266,4267,4268,4269,4270,4271,4272,4273,4274,4275,4276,4277,4278,4279,4280,4281,4282,4283,4284,4285,4286,4287,4288,4289,4290,4291,4292,4293,4294,4295,4296,4297,4298,4299,4300,4301,4302,4303,4304,4305,4306,4307,4308,4309,4310,4311,4312,4313,4314,4315,4316,4317,4318,4319,4320,4321,4322,4323,4324,4325,4326,4327,4328,4329,4330,4331,4332,4333,4334,4335,4336,4337,4338,4339,4340,4341,4342,4343,4344,4345,4346,4347,4348,4349" );*/
	        
	        props.put("Remote.Class", "it.itsoftware.itmware.tltcp.TLTCP");
	        props.put("Remote.Connection", "212.239.25.136:1641");
	        //props.put("Remote.Connection", "172.16.1.101:50143");
	        
	        try {
	        	// create a default logger on stdout
				ITLogger.initDefault();
				// and ask not to be so versbose
				ITLogger.getApplicationLogger().getLogger().setLevel(Level.INFO);
				
				// and use the generated properties
				ConfigManager.init(props);
				System.out.println( "Initializing system" );
			} catch (Exception ex) {
				System.err.println("Error initializing configuration manager module: "
								+ ex.toString());
				System.exit(1);
			}
		}
		
		// session layer initialization
		Cw2tClient ts = new Cw2tClient(args);
		if (!ts.ReadSubscriptions("[Subscriptions]")) {
			//System.out.println("Sorry, subscriptions not found or incorrect in [Subscriptions] section");
			//System.exit(1);
		} else {
			System.out.println("Read subscriptions");            
        }

		System.out.println("Starting session layer");
		if (!ts.Setup()) {
			System.out.println("Sorry, error initializing the client");
			System.exit(1);
		}
        
        // wait for a key
		System.out.println("System initialized, press enter to terminate");
		
		try {
			while (System.in.available() == 0 ) {
				// reconnected?
				int status;
				synchronized (ts) {
					ts.wait(500);
					status = ts.status;
				}
				
				// disconnected in the meanwhile?
				switch(status) {
				case Cw2tClient.st_start:
					if(!ts.startSession())
					{
						System.out.println("Can't start session layer");
						System.exit(0);
					}
                    ts.startJedis();
 					break;
					
				case Cw2tClient.st_connecting:
					System.out.println("Connecting...");
					break;
							
				case Cw2tClient.st_connected:
					System.out.println("Connected");
                    ts.subscribeCw2tTopics();
					//ts.DoSubscriptions();
					break;
										
				case Cw2tClient.st_disconnected:
					System.out.println("Disconnected -- reconnecting");
					ts.session.shutdown();
					
					// quite useless to do excessively aggresive tries
					Thread.sleep(1000);
					
					// restart the session
					if( !ts.startSession() )
					{
						System.out.println("Can't start session layer");
						System.exit(0);
					}
					break;
				}
			}
		}
		catch(IOException e) {
			System.out.println("IO exception: " + e);
		}
		catch(InterruptedException e) {
			System.out.println("Idle wait Interrupted: " + e);
		}
		
		System.out.println("Stopping session layer");
        
        ts.stopJedis();
		ts.session.shutdown();
		System.out.println("Terminating");
	}
}
