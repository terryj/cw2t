/****************
* nbtrader.js
* Link to Netbuilder NBTrader Fix API v4.2
* Cantwaittotrade Limited
* Terry Johnston
* August 2014
****************/

// avoids DEPTH_ZERO_SELF_SIGNED_CERT error for self-signed certs
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

var util = require('util');
//var net = require('net');
var tls = require('tls');
var events = require('events');
var fs = require("fs");

var common = require('./commonfo.js');

var fixver = 'FIX.4.2';
var nbhost; // ip address
var nbport; // port
var sendercompid; // cw2t id
var targetcompid; // nbtrader id
var onbehalfofcompid; // who we are trading for - TG, at least in the first instance
var SOH = '\001';
var datainbuf = ''; // data buffer
var connectDelay = 5000; // re-try connection timer delay in milliseconds
var nbconn;
var encryptmethod = '0';
var heartbeattimer = null; // timer used to monitor incoming messages
var testrequesttimer = null;
var restarttimer = null;
var heartbtint = 30; // heart beat interval in seconds
var transmissiontime = 3; // transmission time in seconds
var matchtestreqid;
var messagerecoveryinrequested = false; // flag to show we have requested resend of some incoming messages
var messagerecoveryinstarted = false;	// flag to show we are in the process of recovering incoming messages
var nextseqnumin; // store the next sequence number in when resetting stored value for resend request
var messagerecoveryout = false; // flag to indicate we are in a resend process for outgoing messages
var sequencegapnum = 0; // sequence gap starting message number
var sequencegaptimestamp; // timestamp of first message in a sequence gap
var resendrequestrequired = false; // indicates we need to do our own resend once we have serviced a resend request
var settlmnttyp = '6'; // indicates a settlement date is being used, rather than a number of days
var norelatedsym = '1'; // number of related symbols in a request, always 1
var idsource = '4'; // indicates ISIN is to be used to identify a security
//var securitytype = 'CS'; // common stock
var handinst = '1'; // i.e. no intervention
var connectstatus = 2; // status of connection, 1=connected, 2=disconnected
var connectstatusint = 30;
var connectstatusinterval = 30;
var datareceivedsinceheartbeat = false; // indicates whether data has been received since the last heartbeat

var options = {
  key: fs.readFileSync('key.pem'),
  cert: fs.readFileSync('cert.pem'),
};

function Nbt() {
	if (false === (this instanceof Nbt)) {
        return new Nbt();
    }

    events.EventEmitter.call(this);

   	// initialise
	init(this);
}
util.inherits(Nbt, events.EventEmitter);

Nbt.prototype.connect = function() {
	var self = this;

	tryToConnect(self);
}
exports.Nbt = Nbt;

function tryToConnect(self) {
	console.log("trying to connect to " + nbhost + ":" + nbport);

	restarttimer = null;

	nbconn = tls.connect(nbport, nbhost, options, function() {
		// connected
		self.emit("connected");

		// ascii is fine
		nbconn.setEncoding('ascii');

		// any received data
		nbconn.on('data', function(data) {
			// only log if not connected
			if (connectstatus != 1) {
				console.log("--- received ---");
				console.log(data);
				console.log("----------------");
			}

			// add new data to buffer
			datainbuf += data;

			// parse it
			parseData(self);

    		datareceivedsinceheartbeat = true;
		});

		// connection termination
		nbconn.on('end', function() {
			console.log('disconnected by ' + nbhost);
			disconnect(self);

		});

		logon(false);
	});

	// need to handle error event
	nbconn.on('error', function(err) {
		console.log(err);
		disconnect(self);
	});
}

function restartTimer(self) {
	if (restarttimer != null) {
		// already set
		return;
	}

	// set a timer to re-try
	restarttimer = setTimeout(function() {
		tryToConnect(self);
   	}, connectDelay);
}

function stopHeartbeatTimers() {
	stopHeartBeatTimer();
	stopTestRequestTimer();
}

function publishStatus() {
	// publish to user channel
	db.publish(2, "tsstatus:" + connectstatus);
}

function initFlags() {
	messagerecoveryinrequested = false;
	messagerecoveryinstarted = false;
	messagerecoveryout = false;
	resendrequestrequired = false;
}

function init(self) {
	initFlags();

	// publish status every time period
	connectstatusinterval = setInterval(publishStatus, connectstatusint * 1000);

  	db.get("trading:ipaddress", function(err, ipaddr) {
    	if (err) {
      		console.log(err);
      		return;
    	}

    	if (ipaddr == null) {
      		console.log("ip address not found");
      		return;
    	}

    	nbhost = ipaddr;
    });

    db.get("trading:port", function(err, port) {
      if (err) {
        console.log(err);
        return;
      }

      if (port == null) {
        console.log("port not found");
        return;
      }

      nbport = port;
    });

    db.get("sendercompid", function(err, senderid) {
      if (err) {
        console.log(err);
        return;
      }

      if (senderid == null) {
        console.log("sendercompid not found");
        return;
      }

      sendercompid = senderid;
    });

    db.get("targetcompid", function(err, targetid) {
      if (err) {
        console.log(err);
        return;
      }

      if (targetid == null) {
        console.log("targetcompid not found");
        return;
      }

      targetcompid = targetid;
    });

    db.get("onbehalfofcompid", function(err, onbehalfofid) {
      if (err) {
        console.log(err);
        return;
      }

      if (onbehalfofid == null) {
        console.log("onbehalfofcompid not found");
        return;
      }

      onbehalfofcompid = onbehalfofid;

      // make this last
      self.emit("initialised");
    });

    // if anything is added here, move the initialised event
}

function sendLogon() {
	logon(false);
}

function logon(reset) {
	console.log("sending logon msg");

	var msg = '98=' + encryptmethod + SOH
			+ '108=' + heartbtint + SOH;

	if (reset) {
		// we are returning a logon with the reset sequence number flag set
		msg += '141=' + 'Y' + SOH;
	}

	sendMessage('A', "", "", msg, false, null, null);
}

function sendLogout(text) {
	var msg = '';

	if (text != '') {
		msg = '58=' + text + SOH;
	}

	sendMessage('5', "", "", msg, false, null, null);
}

function sendHeartbeat() {
	sendMessage('0', "", "", "", false, null, null);
}

function sendTestRequest(self) {
	var msg;

	// just in case we already have a test request running
	if (testrequesttimer != null) {
		return;
	}

	// if we have received data since the last heartbeat, no need to do test
	if (datareceivedsinceheartbeat) {
		return;
	}

	console.log('sending test request');

	// string we expect to receive back
	matchtestreqid = getUTCTimeStamp();

	msg = '112=' + matchtestreqid + SOH;

	// send a test request after agreed quiet time period
	sendMessage('1', "", "", msg, false, null, null);

	// need to receive a signed heartbeat within agreed time period, otherwise we are history
	startTestRequestTimer(self);
}

function TestRequestTimeout(self) {
	console.log("test request timeout, quitting...");

	sendLogout("Test Request Timeout");

	// we haven't heard for a while, so quit
	disconnect(self);
}

function disconnect(self) {
	// tell everyone we are disconnected
	setStatus(2);

	// tidy
	nbconn.destroy();

	// stop timers
	stopHeartbeatTimers();

	// re-set flags
	initFlags();

	// prepare to restart
	restartTimer(self);

	var timestamp = common.getUTCTimeStamp(new Date());
  	console.log(timestamp + " - disconnected from NBTrader");
}

function setStatus(status) {
	// tell everyone our status has changed
	connectstatus = status;
	publishStatus();	
}

Nbt.prototype.quoteRequest = function(quoterequest) {
	var msg = '131=' + quoterequest.quotereqid + SOH
		+ '146=' + norelatedsym + SOH
		+ '55=' + quoterequest.mnemonic + SOH
		+ '48=' + quoterequest.isin + SOH
		+ '22=' + idsource + SOH
		//+ '167=' + securitytype + SOH
		+ '63=' + quoterequest.settlmnttyp + SOH
		+ '207=' + quoterequest.exchangeid + SOH
		+ '15=' + quoterequest.currencyid + SOH
		+ '120=' + quoterequest.settlcurrencyid + SOH
		+ '60=' + quoterequest.timestamp + SOH;

	// add settlement date if future settlement type specified
	if (quoterequest.settlmnttyp == 6) {
		msg += '64=' + quoterequest.futsettdate + SOH;
	}

	// qty as shares or cash
	if (quoterequest.quantity != "") {
		msg += '38=' + quoterequest.quantity + SOH;
	} else {
		msg += '152=' + quoterequest.cashorderqty + SOH;			
	}

	// side, if present
	if ('side' in quoterequest && (quoterequest.side == 1 || quoterequest.side == 2)) {
		msg += '54=' + quoterequest.side + SOH;
	}

	// specify a marketmaker
	if ('qbroker' in quoterequest) {
		msg += '6158=' + quoterequest.qbroker + SOH;
	}

	sendMessage('R', onbehalfofcompid, "", msg, false, null, null);
}

Nbt.prototype.newOrder = function(order) {
	var delivertocompid = "";

	var msg = '11=' + order.orderid + SOH
		+ '21=' + handinst + SOH
		+ '55=' + order.mnemonic + SOH
		+ '48=' + order.isin + SOH
		+ '22=' + idsource + SOH
		//+ '167=' + securitytype + SOH
		+ '207=' + order.exchangeid + SOH
		+ '54=' + order.side + SOH
		+ '60=' + order.timestamp + SOH
		+ '63=' + settlmnttyp + SOH
		+ '64=' + order.futsettdate + SOH
		+ '59=' + order.timeinforce + SOH
		+ '120=' + order.settlcurrencyid + SOH
		+ '15=' + order.currencyid + SOH
		+ '40=' + order.ordertype + SOH;

	if (order.ordertype == 'D') { // previously quoted
		// add quote id & who the quote was from
		msg += '117=' + order.externalquoteid + SOH;
		delivertocompid = order.qbroker;
	} else if (order.delivertocompid != "") {
		delivertocompid = order.delivertocompid;
	}

	// quantity as shares or cash
	if (order.quantity != '') {
		msg += '38=' + order.quantity + SOH;
	} else {
		msg += '152=' + order.cashorderqty + SOH;				
	}

	// price required for a limit order
	if (order.price != '') {
		// add price
		msg += '44=' + order.price + SOH;
	}

	// add date & time if timeinforce = "GTD"
	if (order.timeinforce == '6') {
		msg += '432=' + order.expiredate + SOH;
			//+ '126=' + order.expiretime + SOH;
	}

	sendMessage('D', onbehalfofcompid, delivertocompid, msg, false, null, null);
}

Nbt.prototype.orderCancelRequest = function(ocr) {
	var msg = '11=' + ocr.ordercancelreqid + SOH
			+ '41=' + ocr.orderid + SOH
			+ '55=' + ocr.mnemonic + SOH
			+ '48=' + ocr.isin + SOH
			+ '22=' + idsource + SOH
			+ '54=' + ocr.side + SOH
			+ '38=' + ocr.quantity + SOH
			+ '60=' + ocr.timestamp + SOH;

	// todo: need broker id?
	sendMessage('F', onbehalfofcompid, "", msg, false, null, null);
}

//
// resend=false, msgseqnum=null, origtimestamp=null for a new message
// resend=true, msgseqnum=original message number, origtimestamp=original timespamp if a message is being resent
// 
function sendMessage(msgtype, onbehalfofcompid, delivertocompid, body, resend, msgseqnum, origtimestamp) {
	// we need our own fix timestamp - todo: can we use that of message or vice versa?
	var timestamp = getUTCTimeStamp();

	if (resend) {
		console.log('resending message #' + msgseqnum);

		// we are resending, so no need to store, just send
		sendData(msgtype, onbehalfofcompid, delivertocompid, body, resend, msgseqnum, timestamp, origtimestamp);
	} else {
		// get the next outgoing message sequence number for the new message
		db.incr("fixseqnumout", function(err, msgseqnumout) {
			if (err) {
				console.log(err);
				return;
			}

			// store the message in case it needs to be resent later
			// just store the body as the header & footer will be affected by a resend
			//db.hset("fixmessageout:" + msgseqnumout, "body", body);
			//db.hset("fixmessageout:" + msgseqnumout, "msgtype", msgtype);
			//db.hset("fixmessageout:" + msgseqnumout, "timestamp", timestamp);
			db.hmset("fixmessageout:" + msgseqnumout, "body", body, "msgtype", msgtype, "timestamp", timestamp);

			// this is a new message, but if we are in a recovery phase, don't send now
			// instead, the message should be sent by the recovery process itself
			if (!messagerecoveryout) {
				sendData(msgtype, onbehalfofcompid, delivertocompid, body, resend, msgseqnumout, timestamp, origtimestamp);
			}
		});
	}
}

function sendData(msgtype, onbehalfofcompid, delivertocompid, body, resend, msgseqnum, timestamp, origtimestamp) {
	var msg;
	var checksumstr;

	// create the part of the header included in the body length
	msg = '35=' + msgtype + SOH
		+ '49=' + sendercompid + SOH
		+ '56=' + targetcompid + SOH
		+ '34=' + msgseqnum + SOH
		+ '52=' + timestamp + SOH;

	if (onbehalfofcompid != "") {
		msg += '115=' + onbehalfofcompid + SOH;
	}

	if (delivertocompid != "") {
		msg += '128=' + delivertocompid + SOH;
	}

	// add the necessary for when we are resending a message
	if (resend) {
		msg += '43=' + 'Y' + SOH // possible duplicate
		msg + '112=' + origtimestamp + SOH; // original timestamp
	}

	// add the body
	msg += body;

	// add the start of the header, now we can calculate the message length
	msg = '8=' + fixver + SOH
		+ '9=' + msg.length + SOH
		+ msg;

	// calculate the check sum & append it
	checksumstr = '10=' + checksum(msg) + SOH;
	msg += checksumstr;

	// only log the message if not connected
	if (connectstatus != 1) {
		console.log("--- sending ---");
		console.log(msg);
		console.log("---------------");
	}

	// send the message
	//console.log("sending...");
	//console.log(msg);

	nbconn.write(msg, 'ascii'); // todo: need ascii option?
}

function parseData(self) {
	var equals;
	var messagestate = 0;
	var tagvalarr = [];
	var endtag;

	// split the string into tag/value pairs
	var n = datainbuf.split(SOH);

	for (var i = 0; i < n.length; i++) {
		// find the separator
    	equals = n[i].indexOf('=');

    	// build a tag/value pair
    	var tagvalpair = {};
    	tagvalpair.tag = parseInt(n[i].substr(0, equals));
    	tagvalpair.value = n[i].substr(equals + 1);

    	// add it to the array
    	tagvalarr.push(tagvalpair);

    	switch (messagestate) {
    	case 0:
    		// start of message
    		if (tagvalpair.tag == '8') {
    			messagestate = 1;
    		}
    		break;
    	case 1:
    		// end of message
    		if (tagvalpair.tag == '10' && tagvalpair.value.length == 3) {
    			completeMessage(tagvalarr, self);

    			// remove processed message from buffer
    			endtag = datainbuf.indexOf("10=");
    			datainbuf = datainbuf.slice(endtag + 7);

    			// clear array
    			tagvalarr = [];

    			// may be another message, so reset state
    			messagestate = 0;
    		}
    		break;
    	}
	}
}

function startHeartBeatTimer(self) {
	if (heartbeattimer == null) {
		// send a test request after agreed quiet time period
		heartbeattimer = setTimeout(function() {
			sendTestRequest(self);
		}, (heartbtint * 1000) + (transmissiontime * 1000));

		datareceivedsinceheartbeat = false;
	}
}

function stopHeartBeatTimer() {
	if (heartbeattimer != null) {
		// clear the timer
		clearTimeout(heartbeattimer);
		heartbeattimer = null;
	}
}

function completeMessage(tagvalarr, self) {
	var header = {};
	var body = {};

	// build the header & body
	header = getHeader(tagvalarr);
	body = getBody(header.msgtype, tagvalarr);

	// store the message, will overwrite if duplicate
	// todo: not storing as need to store body as string, not object & string is logged anyway
	//db.hset("fixmessagein:" + header.msgseqnum, "msgtype", header.msgtype);
	//db.hset("fixmessagein:" + header.msgseqnum, "body", body);

	if (header.possdupflag == 'Y') {
		if (messagerecoveryinrequested) {
			// this is the start of the recovery process
			// set the stored value to the first value requested & set started flag
			console.log('message recovery started');
			db.set("fixseqnumin", nextseqnumin - 1);
			messagerecoveryinrequested = false;
			messagerecoveryinstarted = true;
		}
	} else {
		if (messagerecoveryinrequested) {
			// we are waiting for the first resend message, so just ignore
			console.log('message recovery in progress, waiting for first possible duplicate, ignore...');
			return;
		}

		if (messagerecoveryinstarted) {
			// we are done recovering
			console.log('message recovery finished');
			messagerecoveryinstarted = false;
		}
	}

	// get the expected incoming message sequence number
	db.incr("fixseqnumin", function(err, msgseqnumin) {
		if (err) {
			console.log(err);
			return;
		}

		// check fix sequence number matches what we expect
		if (parseInt(header.msgseqnum) < parseInt(msgseqnumin)) {
			console.log("incoming sequence number=" + header.msgseqnum + ", expected=" + msgseqnumin);

			// check for sequence reset as this overrides any sequence number processing
			if (header.msgtype == '4') {
				if (!('gapfillflag' in body) || body.gapfillflag == 'N') {
					sequenceReset(body, msgseqnumin, self);
					return;
				}
			}

			if (header.possdupflag == 'Y') {
				// duplicate received but we are catching up, so just ignore
				return;
			} else {
				if (header.msgtype == 'A') {
					if ('resetseqnumflag' in body) {
						// resetting sequence numbers, allow to pass through - todo: check & test this
					} else {
						// normal logon reply & we haven't missed anything, so just set the stored sequence number to that received
						console.log("Resetting incoming sequence number to " + header.msgseqnum);
						db.set("fixseqnumin", header.msgseqnum);
					}
				} else if (header.msgtype == '5') {
					if ('text' in body) {
						console.log("logout received, text=" + body.text);
					}

					// we have been logged out & will be disconnected by the other side, just exit & try again
					return;
				} else {
					// serious error, abort
					disconnect(self);
					return;
				}
			}
		} else if (parseInt(header.msgseqnum) > parseInt(msgseqnumin)) {
			console.log("incoming sequence number=" + header.msgseqnum + ", expected=" + msgseqnumin);

			// check for sequence reset as this overrides any sequence number processing
			if (header.msgtype == '4') {
				if (!('gapfillflag' in body) || body.gapfillflag == 'N') {
					sequenceReset(body, msgseqnumin);
					return;
				}
			}

			if (header.msgtype == '5') {
				// we have been logged out & will be disconnected by the other side, just exit & try again
				if ('text' in body) {
					console.log("logout received, text=" + body.text);
				}
				return;
			}

			if (messagerecoveryinstarted) {
				// we are in the middle of a resend & it has gone wrong, so abort & call
				console.log("message recovery failed, aborting...");
				disconnect(self);
				return;
			}

			if (messagerecoveryinrequested) {
				// already requested a resend, so this should just be a message sent before the resend request
				// so, just ignore, it should be handled as part of the resend
				return;
			} else {
				// store the next sequence number in, as it is used to update the stored value later
				nextseqnumin = msgseqnumin;

				// check the message type before issuing a resend request
				if (header.msgtype == '2') {
					// record that we need to do our own resend request after servicing the requested resend
					resendrequestrequired = true;
				} else if (header.msgtype == 'A') {
					// normal logon reply & we haven't missed anything, so just set the stored sequence number to that received
					console.log("Resetting incoming sequence number to " + header.msgseqnum);
					db.set("fixseqnumin", header.msgseqnum);
				} else {
					// request resend of messages from this point, but wait to adjust the stored value
					// until we know the recovery process has started, as in the first possible duplicate received
					resendRequest(msgseqnumin);

					// we may need to act on this message, once the resend process has finished
					messagerecoveryinrequested = true;
					return;
				}
			}
		}
	
		// message numbers match, so do the processing on our data object
		switch (header.msgtype) {
			case 'S':
				quoteReceived(body, header, self);
				break;
			case '8':
				executionReport(body, self);
				break;
			case '9':
				orderCancelReject(body, self);
				break;
			case 'A':
				logonReplyReceived(body, self);
				break;
			case '0':
				heartbeatReceived(body, self);
				break;
			case '1':
				testRequestReceived(body, self);
				break;
			case '2':
				resendRequestReceived(body, self);
				break;
			case '4':
				sequenceGapReceived(body);
				break;
			case '5':
				logoutReceived(body, self);
				break;
			case 'b':
				quoteAckReceived(body, self);
				break;
			case '3':
				rejectReceived(body, self);
				break;
			case 'j':
				businessRejectReceived(body, self);
				break;
			default:
				console.log("unknown message type");
		}

		// mark our incomimg message as processed
		// todo: ignore until storing rest of message, see above
		//db.hset("fixmessagein:" + header.msgseqnum, "read", "Y");
	});
}

function getHeader(tagvalarr) {
	var header = {};

	var taglen = tagvalarr.length;

	for (var i = 0; i < taglen; i++) {
		switch (tagvalarr[i].tag) {
			case 8:
			case 9:
				// ignore
				break;
			case 35:
				header.msgtype = tagvalarr[i].value;
				break;
			case 49:
				header.sendercompid = tagvalarr[i].value;
				break;
			case 56:
				header.targetcompid = tagvalarr[i].value;
				break;
			case 34:
				header.msgseqnum = tagvalarr[i].value;
				break;
			case 52:
				header.sendingtime = tagvalarr[i].value;
				break;
			case 43:
				header.possdupflag = tagvalarr[i].value;
				break;
			case 97:
				header.possresend = tagvalarr[i].value;
				break;
			case 115:
				header.onbehalfofcompid = tagvalarr[i].value;
				break;
			case 122:
				header.origsendingtime = tagvalarr[i].value;
				break;
			case 128:
				header.delivertocompid = tagvalarr[i].value;
				break;
			default:
		}
	}

	return header;
}

function getBody(msgtype, tagvalarr) {
	var body = {};

	var tagarrlen = tagvalarr.length;

	switch (msgtype) {
	case 'S': // quote
		for (var i = 0; i < tagarrlen; i++) {
			switch (tagvalarr[i].tag) {
			case 131:
				body.quotereqid = tagvalarr[i].value;
				break;
			case 117:
				body.externalquoteid = tagvalarr[i].value;
				break;
			case 55:
			 	body.symbolid = tagvalarr[i].value;
			 	break;
			case 48:
				body.securityid = tagvalarr[i].value;
				break;
			case 22:
				body.idsource = tagvalarr[i].value;
				break;
			case 132:
				body.bidpx = tagvalarr[i].value;
				break;
			case 133:
				body.offerpx = tagvalarr[i].value;
				break;
			case 134:
				body.bidsize = tagvalarr[i].value;
				break;
			case 135:
				body.offersize = tagvalarr[i].value;
				break;
			case 63:
				body.settlmnttyp = tagvalarr[i].value;
				break;
			case 64:
				body.futsettdate = tagvalarr[i].value;
				break;
			case 62:
				body.validuntiltime = tagvalarr[i].value;
				break;
			case 60:
				body.transacttime = tagvalarr[i].value;
				break;
			case 15:
				body.currencyid = tagvalarr[i].value;
				break;
			case 30:
				body.lastmkt + tagvalarr[i].value;
				break;
			case 300:
				body.quoterejectreason = tagvalarr[i].value;
				break;
			case 152:
				body.cashorderqty = tagvalarr[i].value;
				break;
			case 120:
				body.settlcurrencyid = tagvalarr[i].value;
				break;
			case 167:
				body.securitytype = tagvalarr[i].value;
				break;
			case 207:
				body.securityexchange = tagvalarr[i].value;
				break;
			case 6158:
				body.qbroker = tagvalarr[i].value;
				break;
			case 159:
				body.accruedinterest = tagvalarr[i].value;
				break;
			case 5814:
				body.bestbidpx = tagvalarr[i].value;
				break;
			case 5815:
				body.bestofferpx = tagvalarr[i].value;
				break;
			case 5816:
				body.bidquotedepth = tagvalarr[i].value;
				break;
			case 5817:
				body.offerquotedepth = tagvalarr[i].value;
				break;
			case 5818:
				body.maxquotedepth = tagvalarr[i].value;
				break;
			case 5819:
				// list of individual quotes, ignore, at least for time being
				break;
			case 645:
				body.mktbidx = tagvalarr[i].value;
				break;
			case 646:
				body.mktofferpx = tagvalarr[i].value;
				break;
			case 188:
				body.bidspotrate = tagvalarr[i].value;
				break;
			case 190:
				body.offerspotrate = tagvalarr[i].value;
				break;
			case 9007:
				body.settledays = tagvalarr[i].value;
				break;
			case 537:
				body.quotetype = tagvalarr[i].value;
				break;
			default:
				unknownTag(tagvalarr[i].tag);
			}
		}
		break;
	case '8': // executionreport
		for (var i = 0; i < tagarrlen; ++i) {
			switch (tagvalarr[i].tag) {
			case 37:
				body.orderid = tagvalarr[i].value;
				break;
			case 11:
				body.clordid = tagvalarr[i].value;
				break;
			case 17:
				body.execid = tagvalarr[i].value;
				break;
			case 150:
				body.exectype = tagvalarr[i].value;
				break;
			case 39:
				body.ordstatus = tagvalarr[i].value;
				break;
			case 20:
				body.exectranstype = tagvalarr[i].value;
				break;
			case 103:
				body.ordrejreason = tagvalarr[i].value;
				break;
			case 55:
				body.symbolid = tagvalarr[i].value;
				break;
			case 48:
				body.securityid = tagvalarr[i].value;
				break;
			case 22:
				body.idsource = tagvalarr[i].value;
				break;
			case 63:
				body.settlmnttyp = tagvalarr[i].value;
				break;
			case 64:
				body.futsettdate = tagvalarr[i].value;
				break;
			case 54:
				body.side = tagvalarr[i].value;
				break;
			case 38:
				body.orderqty = tagvalarr[i].value;
				break;
			case 152:
				body.cashorderqty = tagvalarr[i].value;
				break;
			case 44:
				body.price = tagvalarr[i].value;
				break;
			case 15:
				body.currencyid = tagvalarr[i].value;
				break;
			case 151:
				body.leavesqty = tagvalarr[i].value;
				break;
			case 14:
				body.cumqty = tagvalarr[i].value;
				break;
			case 6:
				body.avgpx = tagvalarr[i].value;
				break;
			case 60:
				body.transacttime = tagvalarr[i].value;
				break;
			case 119:
				body.settlcurramt = tagvalarr[i].value;
				break;
			case 120:
				body.settlcurrencyid = tagvalarr[i].value;
				break;
			case 32:
				body.lastshares = tagvalarr[i].value;
				break;
			case 31:
				body.lastpx = tagvalarr[i].value;
				break;
			case 30:
				body.lastmkt = tagvalarr[i].value;
				break;
			case 40:
				body.ordtype = tagvalarr[i].value;
				break;
			case 76:
				body.execbroker = tagvalarr[i].value;
				break;
			case 58:
				body.text = tagvalarr[i].value;
				break;
			case 59:
				body.timeinforce = tagvalarr[i].value;
				break;
			case 167:
				body.securitytype = tagvalarr[i].value;
				break;
			case 207:
				body.securityexchange = tagvalarr[i].value;
				break;
			case 155:
				body.settlcurrfxrate = tagvalarr[i].value;
				break;
			case 41:
				body.origclordid = tagvalarr[i].value;
				break;
			case 645:
				body.mktbidx = tagvalarr[i].value;
				break;
			case 646:
				body.mktofferpx = tagvalarr[i].value;
				break;
			case 117:
				body.quoteid = tagvalarr[i].value;
				break;
			case 159:
				body.accruedinterest = tagvalarr[i].value;
				break;
			default:
				unknownTag(tagvalarr[i].tag);
			}
		}
		break;
	case '9': // ordercancelreject
		for (var i = 0; i < tagarrlen; ++i) {
			switch (tagvalarr[i].tag) {
			case 37:
				body.orderid = tagvalarr[i].value;
				break;
			case 11:
				body.clordid = tagvalarr[i].value;
				break;
			case 102:
				body.cxlrejreason = tagvalarr[i].value;
				break;
			case 41:
				body.origclordid = tagvalarr[i].value;
				break;
			case 39:
				body.ordstatus = tagvalarr[i].value;
				break;
			case 434:
				body.cxlresponseto = tagvalarr[i].value;
				break;
			case 58:
				body.text = tagvalarr[i].value;
				break;
			default:
				unknownTag(tagvalarr[i].tag);
			}
		}
		break;
	case 'b': // quote acknowledgement
		for (var i = 0; i < tagarrlen; ++i) {
			switch (tagvalarr[i].tag) {
			case 58:
				body.text = tagvalarr[i].value;
				break;
			case 131:
				body.quotereqid = tagvalarr[i].value;				
				break;
			case 297:
				body.quoteackstatus = tagvalarr[i].value;
				break;
			case 300:
				body.quoterejectreason = tagvalarr[i].value;
				break;
			case 6158:
				body.qbroker = tagvalarr[i].value;
				break;
			default:
				unknownTag(tagvalarr[i].tag);
			}
		}
		break;
	case '3':
		for (var i = 0; i < tagarrlen; ++i) {
			switch (tagvalarr[i].tag) {
			case 45:
				body.refseqnum = tagvalarr[i].value;
				break;
			case 58:
				body.text = tagvalarr[i].value;
				break;
			case 371:
				body.reftagid = tagvalarr[i].value;				
				break;
			case 372:
				body.refmsgtype = tagvalarr[i].value;
				break;
			case 373:
				body.sessionrejectreason = tagvalarr[i].value;
				break;
			default:
				unknownTag(tagvalarr[i].tag);
			}
		}
		break;
	case 'A': // logon
		for (var i = 0; i < tagarrlen; i++) {
			switch (tagvalarr[i].tag) {
			case 141:
				body.resetseqnumflag = true;
				break;
			}
		}
		break;
	case '0': // heartbeat
	case '1': // testrequest
		for (var i = 0; i < tagarrlen; ++i) {
			switch (tagvalarr[i].tag) {
			case 112:
				body.testreqid = tagvalarr[i].value;
				break;
			}
		}
		break;
	case '2': // resendrequest
		for (var i = 0; i < tagarrlen; ++i) {
			switch (tagvalarr[i].tag) {
			case 7:
				body.beginseqno = parseInt(tagvalarr[i].value);
				break;
			case 16:
				body.endseqno = parseInt(tagvalarr[i].value);
				break;
			}
		}
		break;
	case '4': // sequence reset
		for (var i = 0; i < tagarrlen; ++i) {
			switch (tagvalarr[i].tag) {
			case 123:
				body.gapfillflag = tagvalarr[i].value;
				break;
			case 36:
				body.newseqno = parseInt(tagvalarr[i].value);
				break;
			}
		}
		break;
	case '5':
		// logout
		for (var i = 0; i < tagarrlen; ++i) {
			switch (tagvalarr[i].tag) {
			case 58:
				body.text = tagvalarr[i].value;
				break;
			}
		}
		break;
	case 'j':
		for (var i = 0; i < tagarrlen; ++i) {
			switch (tagvalarr[i].tag) {
			case 58:
				body.text = tagvalarr[i].value;
				break;
			case 45:
				body.refseqnum = tagvalarr[i].value;
				break;
			case 372:
				body.refmsgtype = tagvalarr[i].value;
				break;
			case 380:
				body.businessrejectreason = tagvalarr[i].value;
				break;
			}
		}		
		break;
	default:
		console.log("unknown message type");
	}

	return body;
}

function unknownTag(tag) {
	if (tag != '8'
		&& tag != '9'
		&& tag != '35'
		&& tag != '34'
		&& tag != '43'
		&& tag != '49'
		&& tag != '52'
		&& tag != '56'
		&& tag != '115'
		&& tag != '122'
		&& tag != '128'
		&& tag != '10') {
		console.log("unknown tag:" + tag);
	}
}

function rejectReceived(reject, self) {
	console.log(reject);
	self.emit('reject', reject);
}

function logonReplyReceived(logonreply, self) {
	// we are connected
	setStatus(1);

	var timestamp = common.getUTCTimeStamp(new Date());
	console.log(timestamp + ' - logon reply received');

	if (logonreply.resetseqnumflag) {
		// reset both sequence numbers
		resetSequenceNumbers();

		// return a logon message with the reset flag set
		logon(true);
	}
}

function logoutReceived(logout, self) {
	console.log('logout received');

	if ('text' in logout) {
		console.log('logout text=' + logout.text);
	}

	// we are done
	disconnect(self);
}

function resetSequenceNumbers() {
	console.log('resetting sequence numbers');

	// reset next fix sequence numbers
	db.set("fixseqnumin", 0);
	db.set("fixseqnumout", 0);
}

function sequenceReset(body, nextnumin, self) {
	if (body.newseqno < nextnumin) {
		console.log("sequence reset number less than expected sequence number, aborting...");
		disconnect(self);
	}

	// reset the incoming sequence number
	db.set("fixseqnumin", body.newseqno - 1);
}

function resendRequest(beginseqno) {
	var msg;

	console.log("sending resend request, starting from msg. no. " + beginseqno);

	msg = '7=' + beginseqno + SOH
		+ '16=' + '0' + SOH;

	sendMessage('2', "", "", msg, false, null, null);
}

function sequenceGapReceived(seqgap) {
	if ('gapfillflag' in seqgap) {
		if (seqgap.gapfillflag == 'Y') {
			console.log('resetting incoming sequence number');

			// reset the expected incoming sequence number
			db.set("fixseqnumin", seqgap.newseqno - 1);
		}
	}
}

function quoteReceived(quote, header, self) {
	self.emit('quote', quote, header);
}

function quoteAckReceived(quoteack, self) {
	self.emit('quoteack', quoteack);
}

function executionReport(exereport, self) {
	if (exereport.exectype == '0') { // new
		self.emit('orderAck', exereport);
	} else if (exereport.exectype == '1' || exereport.exectype == '2') { // part-fill or fill
		self.emit('orderFill', exereport);
	} else if (exereport.exectype == '4') { // cancel
		self.emit('orderCancel', exereport);
	} else if (exereport.exectype == '5') { // replace
		self.emit('orderReplace', exereport);
	} else if (exereport.exectype == '8') { // reject
		self.emit('orderReject', exereport);
	} else if (exereport.exectype == 'C') { // expired
		self.emit('orderExpired', exereport);
	} else {
		console.log("Unknown execution type received");
	}
}

function orderCancelReject(ordercancelreject, self) {
	self.emit('orderCancelReject', ordercancelreject);
}

function heartbeatReceived(heartbeat, self) {
	stopHeartBeatTimer();

	// if we have a test request running, check the value of the string returned
	if (testrequesttimer != null) {
		//if ('testreqid' in heartbeat) {
			//if (heartbeat.testreqid == matchtestreqid) {
				stopTestRequestTimer();
			//}
		//}
	} else {
		sendHeartbeat();
		startHeartBeatTimer(self);

		// no need to log if connected
		if (connectstatus != 1) {
			// re-set status & tell everyone
			setStatus(1);

			var timestamp = common.getUTCTimeStamp(new Date());
  			console.log(timestamp + " - heartbeat received from NBTrader");
		}
	}
}

function startTestRequestTimer(self) {
	testrequesttimer = setTimeout(function() {
		TestRequestTimeout(self);
	}, (heartbtint * 1000) + (transmissiontime * 1000));
}

function stopTestRequestTimer() {
	clearTimeout(testrequesttimer);
	testrequesttimer = null;	
}

function testRequestReceived(testrequest, self) {
	console.log('test request received');

	testRequestReply(testrequest.testreqid);
}

function testRequestReply(reqid) {	
	var msg = '112=' + reqid + SOH;

	sendMessage('0', "", "", msg, false, null, null);
}

function resendRequestReceived(resendrequest, self) {
	console.log("resendRequestReceived");
	
	// check we have a valid begin & end sequence number
	// end number may be '0' to indicate infinity
	if (resendrequest.beginseqno < 1 || (resendrequest.endseqno < resendrequest.beginseqno && resendrequest.endseqno != 0)) {
		console.log("invalid begin or end sequence number, unable to continue");
		disconnect(self);
		return;
	}

	// set the outgoing message recovery flag
	messagerecoveryout = true;

	// resend the required messages
	resendMessage(resendrequest.beginseqno, resendrequest.endseqno, self);
}

function businessRejectReceived(businessreject, self) {
	self.emit('businessReject', businessreject);
}

function resendMessage(msgno, endseqno, self) {
	// get requested message
	db.hgetall("fixmessageout:" + msgno, function(err, msg) {
		if (err) {
			console.log(err);
			disconnect(self);
			return;
		}

		// resend the message
		if (msg == null || msg.msgtype == '0' || msg.msgtype == '1' || msg.msgtype == '2' || msg.msgtype == '4' || msg.msgtype == '5' || msg.msgtype == 'A' || oldMessage(msg)) {
			// don't resend this message, instead send a sequence gap message
			if (sequencegapnum == 0) {
				// store the first in a possible sequence of admin messages
				sequencegapnum = msgno;
				if (msg == null) {
					sequencegaptimestamp = getUTCTimeStamp();
				} else {
					sequencegaptimestamp = msg.timestamp;
				}
			}
		} else {
			// send any sequence gap
			if (sequencegapnum != 0) {
				sequenceGap(sequencegapnum, msgno, sequencegaptimestamp);
			}

			sendMessage(msg.msgtype, "", "", msg.body, true, msgno, msg.timestamp);
		}

		// check for infinity
		if (endseqno == 0) {
			// get the last outgoing message sequence number
			db.get("fixseqnumout", function(err, msgseqnumout) {
				if (err) {
					console.log(err);
					disconnect(self);
					return;
				}

				// have we reached the last stored message?
				if (msgno == msgseqnumout) {
					// send any sequence gap message
					if (sequencegapnum != 0) {
						sequenceGap(sequencegapnum, parseInt(msgseqnumout) + 1, sequencegaptimestamp);
					}

					// we are done
					doneResending();
					return;
				}

				// keep on going
				resendMessage(msgno + 1, endseqno);
			});
		} else {
			// check for having reached the requested number
			if (msgno == endseqno) {
				// send any sequence gap message
				if (sequencegapnum != 0) {
					// get the last outgoing message sequence number, so we can tell the other side what to expect next
					db.get("fixseqnumout", function(err, msgseqnumout) {
						if (err) {
							console.log(err);
							disconnect(self);
							return;
						}

						sequenceGap(sequencegapnum, parseInt(msgseqnumout) + 1, sequencegaptimestamp);
					});
				}

				// we are done
				doneResending();
				return;
			}

			// keep on going
			resendMessage(msgno + 1, endseqno);
		}
	});

	function doneResending() {
		console.log("doneResending");
		messagerecoveryout = false;

		// we may need to do our own resend request
		if (resendrequestrequired) {
			resendRequest(nextseqnumin);
			messagerecoveryinrequested = true;
			resendrequestrequired = false;
		}
	}
}

function oldMessage(msg) {
	// todo:
	//if (msg.timestamp) {

	//}
	return true;
}


function sequenceGap(beginnum, endnum, timestamp) {
	var msg;

	console.log('sending sequence gap, beginnum=' + beginnum + ', endnum=' + endnum);

	msg = '123=' + 'Y' + SOH
		+ '36=' + endnum + SOH;

	sendMessage('4', "", "", msg, true, beginnum, timestamp);

	sequencegapnum = 0;
}

function checksum(msg) {
    var chksm = 0;
    var checksumstr = '';

    for (var i = 0; i < msg.length; ++i) {
        chksm += msg.charCodeAt(i);
    }

    chksm = chksm % 256;

    if (chksm < 10) {
        checksumstr = '00' + (chksm + '');
    } else if (chksm >= 10 && chksm < 100) {
        checksumstr = '0' + (chksm + '');
    } else {
        checksumstr = '' + (chksm + '');
    }

    return checksumstr;
}

function getUTCTimeStamp() {
    var timestamp = new Date();

    var year = timestamp.getUTCFullYear();
    var month = timestamp.getUTCMonth() + 1; // flip 0-11 -> 1-12
    var day = timestamp.getUTCDate();
    var hours = timestamp.getUTCHours();
    var minutes = timestamp.getUTCMinutes();
    var seconds = timestamp.getUTCSeconds();
    //var millis = timestamp.getUTCMilliseconds();

    if (month < 10) {month = '0' + month;}

    if (day < 10) {day = '0' + day;}

    if (hours < 10) {hours = '0' + hours;}

    if (minutes < 10) {minutes = '0' + minutes;}

    if (seconds < 10) {seconds = '0' + seconds;}

    /*if (millis < 10) {
        millis = '00' + millis;
    } else if (millis < 100) {
        millis = '0' + millis;
    }*/

    //var ts = [year, month, day, '-', hours, ':', minutes, ':', seconds, '.', millis].join('');
    var ts = [year, month, day, '-', hours, ':', minutes, ':', seconds].join('');

    return ts;
}

function getQuoteStatus(status) {
	var desc;

	switch (status) {
	case '0':
		desc = "Accepted";
		break;
	case '1':
		desc = "Canceled for Symbol";
		break;
	case '2':
		desc = "Canceled for Security Type(s)";
		break;
	case '3':
		desc = "Canceled for Underlying";
		break;
	case '4':
		desc = "Canceled All";
		break;
	case '5':
		desc = "Rejected";
		break;
	case '6':
		desc = "Removed from Market";
		break;
	case '7':
		desc = "Expired";
		break;
	case '8':
		desc = "Query";
		break;
	case '9':
		desc = "Quote Not Found";
		break;
	case '10':
		desc = "Pending";
		break;
	case '11':
		desc = "Pass";
		break;
	case '12':
		desc = "Locked Market Warning";
		break;
	case '13':
		desc = "Cross Market Warning";
		break;
	case '14':
		desc = "Canceled due to lock market"
		break;
	case '15':
		desc = "Canceled due to cross market";
		break;
	}

	return desc;
}

Nbt.prototype.getExecTypeDesc = function(exectype) {
	var desc;

	switch (exectype) {
	case '0':
		desc = "New";
		break;
	case '1':
		desc = "Partial fill";
		break;
	case '2':
		desc = "Fill";
		break;
	case '3':
		desc = "Done for day";
		break;
	case '4':
		desc = "Canceled";
		break;
	case '5':
		desc = "Replace";
		break;
	case '6':
		desc = "Pending Cancel";
		break;
	case '7':
		desc = "Stopped";
		break;
	case '8':
		desc = "Rejected";
		break;
	case '9':
		desc = "Suspended";
		break;
	case 'A':
		desc = "Pending New"
		break;
	case 'B':
		desc = "Calculated";
		break;
	case 'C':
		desc = "Expired";
		break;
	case 'D':
		desc = "Restated";
		break;
	case 'E':
		desc = "Pending Replace";
		break;
	}

	return desc;
}

Nbt.prototype.getQuoteRejectReason = function(reason) {
	var desc;

	switch (parseInt(reason)) {
	case 1:
		desc = "Unknown symbol";
		break;
	case 2:
		desc = "Exchange closed";
		break;
	case 3:
		desc = "Quote Request exceeds limit";
		break;
	case 4:
		desc = "Too late to enter";
		break;
	case 5:
		desc = "Unknown Quote";
		break;
	case 6:
		desc = "Duplicate Quote";
		break;
	case 7:
		desc = "Invalid bid/ask spread";
		break;
	case 8:
		desc = "Invalid price";
		break;
	case 9:
		desc = "Not authorized to quote security";
		break;
	}

	return desc;
}

Nbt.prototype.getSessionRejectReason = function(reason) {
	var desc;

	switch (parseInt(reason)) {
	case 0:
		desc = "Invalid tag number";
		break;
	case 1:	
		desc = "Required tag missing";
		break;
	case 2:
		desc = "Tag not defined for this message type"
		break;
	case 3:
		desc = "Undefined Tag";
		break;
	case 4:
		desc = "Tag specified without a value";
		break;
	case 5:
		desc = "Value is incorrect (out of range) for this tag";
		break;
	case 6:
		desc = "Incorrect data format for value";
		break;
	case 7:
		desc = "Decryption problem";
		break;
	case 8:
		desc = "Signature problem";
		break;
	case 9:	
		desc = "CompID problem";
		break;
	case 10:
		desc = "SendingTime accuracy problem";
		break;
	case 11:
		desc = "Invalid MsgType";
		break;
	}

	return desc;
}

Nbt.prototype.getBusinessRejectReason = function(reason) {
	var desc;

	switch (parseInt(reason)) {
		case 0:	
			desc = "Other";
			break;
		case 1:
			desc = "Unknown ID";
			break;
		case 2:
			desc = "Unknown Security";
			break;
		case 3:
			desc = "Unsupported Message Type";
			break;
		case 4:
			desc = "Application not available";
			break;
		case 5:
			desc = "Conditionally Required Field Missing"
			break;
	}

	return desc;
}