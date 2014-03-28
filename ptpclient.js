/****************
* ptpclient.js
* Link to Proquote Trading Platform (PTP) Fix API v1.7
* Cantwaittotrade Limited
* Terry Johnston
* June 2013
****************
*
* Notes
*
***************/

var util = require('util');
var net = require('net');
var events = require('events');
var fixver = 'FIX.4.2';
var pqhost; // ip address
var pqport; // port
var sendercompid; // cw2t id
var targetcompid; // proquote id
var onbehalfofcompid; // who we are trading for - TG, at least in the first instance
var SOH = '\001';
var datainbuf = ''; // data buffer
var connectDelay = 5000; // re-try connection timer delay in milliseconds
var pqconn;
var encryptmethod = '0';
var heartbeatintimer = null; // timer used to monitor incoming messages
var heartbeatouttimer = null; // timer used to monitor outgoing messages
var testrequesttimer = null;
var heartbtint = 30; // heart beat interval in seconds
var transmissiontime = 3; // transmission time in seconds
var matchtestreqid;
var messagerecoveryinrequested = false; // flag to show we have requested resend of some incoming messages
var messagerecoveryinstarted = false;	// flag to show we are in the process of recovering incoming messages
var nextseqnumin; // store the next sequence number in when resetting stored value for resend request
var messagerecoveryout = false; // flag to indicate we are in a resend process for outgoing messages
var sequencegapnum = 0; // sequence gap starting message number
var sequencegaptimestamp; // timestamp of first message in a sequence gap
var logoutinitiated = false; // indicates whether we have initiated a logout
var logoutrequired = false; // indicates a logout should be performed once a resend process has completed
var resendrequestrequired = false; // indicates we need to do our own resend once we have serviced a resend request
var settlmnttyp = '6'; // indicates a settlement date is being used, rather than a number of days
var norelatedsym = '1'; // number of related symbols in a request, always 1
var idsource = '4'; // indicates ISIN is to be used to identify a security
var securitytype = 'CS'; // common stock
var handinst = '1'; // i.e. no intervention

/// performance test ///
//var count = 0;
//var start;
//var first=true;
////////////////////////

function Ptp() {
	if (false === (this instanceof Ptp)) {
        return new Ptp();
    }

    events.EventEmitter.call(this);

   	// initialise
	init(this);
}
util.inherits(Ptp, events.EventEmitter);

Ptp.prototype.connect = function() {
	var self = this;

	tryToConnect(self);
}
exports.Ptp = Ptp;

function tryToConnect(self) {
	console.log("trying to connect to " + pqhost + ":" + pqport);

	pqconn = net.connect({port: pqport, host: pqhost}, function() {
		// connected
		self.emit("connected");

		// ascii is fine
		pqconn.setEncoding('ascii');

		// any received data
		pqconn.on('data', function(data) {
			console.log("----- received --------");
			console.log(data);
			console.log("-----------------------");

			// add new data to buffer
			datainbuf += data;

			/*if (first) {
				start = new Date();
				first = false;
			}*/

			// parse it
			stopHeartBeatIn();
			parseData(self);
			startHeartBeatIn();

    		/*if (count == 20000) {
    			console.log("nummsg="+count);
    			var end = new Date();
    			console.log(end-start);
    		}*/
		});

		// connection termination
		pqconn.on('end', function() {
			console.log('Disconnected from ' + pqhost);
		});

		// todo: do i need this
		/*setTimeout(function() {
			sendLogon();
    	}, 1000);*/

		logon(false);
	});

	// need to handle error event
	pqconn.on('error', function(err) {
		console.log(err);

		// set a timer to re-try - todo: do we need to base this on error?
		setTimeout(function() {
			tryToConnect(self);
    	}, connectDelay);
	});
}

function init(self) {
	messagerecoveryinrequested = false;
	messagerecoveryinstarted = false;
	messagerecoveryout = false;
	logoutinitiated = false;
	logoutrequired = false;
	resendrequestrequired = false;

  	db.get("trading:ipaddress", function(err, ipaddr) {
    	if (err) {
      		console.log(err);
      		return;
    	}

    	if (ipaddr == null) {
      		console.log("ip address not found");
      		return;
    	}

    	pqhost = ipaddr;
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

      pqport = port;
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

	console.log("sending logout msg");

	if (text != '') {
		msg = '58=' + text + SOH;
	}

	sendMessage('5', "", "", msg, false, null, null);

	logoutinitiated = true;
}

function heartbeat() {
	sendMessage('0', "", "", "", false, null, null);
}

function testRequest() {
	var msg;

	console.log('sending test request');

	// just in case we already have a test request running
	if (testrequesttimer != null) {
		return;
	}

	// string we expect to receive back
	matchtestreqid = getUTCTimeStamp();

	msg = '112=' + matchtestreqid + SOH;

	// send a test request after agreed quiet time period
	sendMessage('1', "", "", msg, false, null, null);

	// need to receive a signed heartbeat within agreed time period, otherwise we are history
	testrequesttimer = setTimeout(function() {
		TestRequestTimeout();
	}, (heartbtint * 1000) + (transmissiontime * 1000));
}

function TestRequestTimeout() {
	console.log("test request timeout, quitting...");

	sendLogout("Test Request Timeout");

	// we haven't heard for a while, so quit
	disconnect();
}

function disconnect() {
	console.log("disconnecting");

	// todo: clear timers etc.
	pqconn.destroy();
}

Ptp.prototype.quoteRequest = function(quoterequest) {
	var msg;

	msg = '131=' + quoterequest.quotereqid + SOH
		+ '146=' + norelatedsym + SOH
		+ '55=' + quoterequest.proquotesymbol + SOH
		+ '48=' + quoterequest.isin + SOH
		+ '22=' + idsource + SOH
		+ '167=' + securitytype + SOH
		+ '207=' + quoterequest.exchange + SOH
		+ '63=' + settlmnttyp + SOH
		+ '64=' + quoterequest.futsettdate + SOH
		+ '15=' + quoterequest.currency + SOH
		+ '120=' + quoterequest.settlcurrency + SOH
		+ '60=' + quoterequest.timestamp + SOH;

	// qty as shares or cash
	if (quoterequest.quantity != "") {
		msg += '38=' + quoterequest.quantity + SOH;
	} else {
		msg += '152=' + quoterequest.cashorderqty + SOH;			
	}

	// if price is present, side is required
	if ('price' in quoterequest) {
		msg += '54=' + quoterequest.side + SOH
			+ '44=' + quoterequest.price + SOH;
	}

	// specify a marketmaker
	if ('qbroker' in quoterequest) {
		msg += '6158=' + quoterequest.qbroker + SOH;
	}

	sendMessage('R', onbehalfofcompid, "", msg, false, null, null);
}

Ptp.prototype.newOrder = function(order) {
	var msg;
	var delivertocompid = "";

	msg = '11=' + order.orderid + SOH
		+ '21=' + handinst + SOH
		+ '55=' + order.proquotesymbol + SOH
		+ '48=' + order.isin + SOH
		+ '22=' + idsource + SOH
		+ '167=' + securitytype + SOH
		+ '207=' + order.exchange + SOH
		+ '54=' + order.side + SOH
		+ '60=' + order.timestamp + SOH
		+ '63=' + settlmnttyp + SOH
		+ '64=' + order.futsettdate + SOH
		+ '59=' + order.timeinforce + SOH
		+ '120=' + order.settlcurrency + SOH
		+ '15=' + order.currency + SOH
		+ '40=' + order.ordertype + SOH;

	if (order.ordertype == 'D') { // previously quoted
		// add quote id
		msg += '117=' + order.proquotequoteid + SOH;
		delivertocompid = order.qbroker;
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

Ptp.prototype.orderCancelRequest = function(ocr) {
	var msg = '11=' + ocr.ordercancelreqid + SOH
			+ '41=' + ocr.orderid + SOH
			+ '55=' + ocr.proquotesymbol + SOH
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
			db.hset("messageout:" + msgseqnumout, "body", body);
			db.hset("messageout:" + msgseqnumout, "msgtype", msgtype);
			db.hset("messageout:" + msgseqnumout, "timestamp", timestamp);

			// this is a new message, but if we are in a recovery phase, don't send now
			// instead, the message should be sent by the recovery process itself
			if (!messagerecoveryout) {
				sendData(msgtype, onbehalfofcompid, delivertocompid, body, resend, msgseqnumout, timestamp, origtimestamp);
			}
		});
	}
}

function startHeartbeatTimer() {
	heartbeatouttimer = setTimeout(function() {
		heartbeat();
	}, heartbtint * 1000);
}

function stopHeartbeatTimer() {
	if (heartbeatouttimer != null) {
		clearTimeout(heartbeatouttimer);
		heartbeatouttimer = null;
	}
}

function sendData(msgtype, onbehalfofcompid, delivertocompid, body, resend, msgseqnum, timestamp, origtimestamp) {
	var msg;
	var checksumstr;

	// we are sending, so don't need a heartbeat, so turn the heartbeat timer off
	stopHeartbeatTimer();

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

	// send the message
	console.log("---sending---");
	console.log(msg);
	pqconn.write(msg, 'ascii'); // todo: need ascii option?
	console.log("-------------");

	// start the heartbeat timer
	startHeartbeatTimer();
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

    			//count++;

    			// may be another message, so reset state
    			messagestate = 0;
    		}
    		break;
    	}
	}
}

function startHeartBeatIn() {
	if (heartbeatintimer == null) {
		// send a test request after agreed quiet time period
		heartbeatintimer = setTimeout(function() {
			testRequest();
		}, (heartbtint * 1000) + (transmissiontime * 1000));
	}
}

function stopHeartBeatIn() {
	if (heartbeatintimer != null) {
		// clear the timer
		clearTimeout(heartbeatintimer);
		heartbeatintimer = null;
	}
}

function completeMessage(tagvalarr, self) {
	var header = {};
	var body = {};

	// build the header & body
	header = getHeader(tagvalarr);
	body = getBody(header.msgtype, tagvalarr);

	// store the message, will overwrite if duplicate
	db.hset("messagein:" + header.msgseqnum, "msgtype", header.msgtype);
	db.hset("messagein:" + header.msgseqnum, "body", body);

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
		console.log('fixseqnumin='+msgseqnumin);

		// check fix sequence number matches what we expect
		if (parseInt(header.msgseqnum) < parseInt(msgseqnumin)) {
			console.log("message sequence number received=" + header.msgseqnum + ", number expected=" + msgseqnumin);

			// check for sequence reset as this overrides any sequence number processing
			if (header.msgtype == '4') {
				if (!('gapfillflag' in body) || body.gapfillflag == 'N') {
					sequenceReset(body, msgseqnumin);
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
						db.set("fixseqnumin", header.msgseqnum);
					}
				} else {
					// serious error, abort & call
					console.log("Error: message number received=" + header.msgseqnum + ", number expected=" + msgseqnumin + ", aborting...");
					disconnect();
					return;
				}
			}
		} else if (parseInt(header.msgseqnum) > parseInt(msgseqnumin)) {
			console.log("message sequence number received=" + header.msgseqnum + ", number expected=" + msgseqnumin);

			// check for sequence reset as this overrides any sequence number processing
			if (header.msgtype == '4') {
				if (!('gapfillflag' in body) || body.gapfillflag == 'N') {
					sequenceReset(body, msgseqnumin);
					return;
				}
			}

			if (messagerecoveryinstarted) {
				// we are in the middle of a resend & it has gone wrong, so abort & call
				console.log("Error: message number received=" + header.msgseqnum + ", number expected=" + msgseqnumin + ", aborting...");
				disconnect();
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
				} else {
					if (header.msgtype == '5') {
						// record the fact we need to logout, once we have completed a resend request
						logoutrequired = true; // todo - how do we react to this? - maybe after a time period, check heartbeat?
					}

					// request resend of messages from this point, but wait to adjust the stored value
					// until we know the recovery process has started, as in the first possible duplicate received
					resendRequest(msgseqnumin);

					// we may need to act on this message, once the resend process has finished
					messagerecoveryinrequested = true;
					return;
				}
			}
		}
		console.log('header.msgtype='+header.msgtype);

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
				resendRequestReceived(body);
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
		db.hset("messagein:" + header.msgseqnum, "read", "Y");
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
				body.quoteid = tagvalarr[i].value;
				break;
			case 55:
			 	body.symbol = tagvalarr[i].value;
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
				body.currency = tagvalarr[i].value;
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
				body.settlcurrency = tagvalarr[i].value;
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
				body.symbol = tagvalarr[i].value;
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
				body.currency = tagvalarr[i].value;
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
				body.settlcurrency = tagvalarr[i].value;
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
				body.quotestatus = tagvalarr[i].value; // 'quoteackstatus' in 4.2
				break;
			case 300:
				body.quoterejectreason = tagvalarr[i].value;
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
	self.emit('reject', reject);
}

function logonReplyReceived(logon, self) {
	console.log('logon reply received');

	if (logon.resetseqnumflag) {
		// reset both sequence numbers
		resetSequenceNumbers();

		// return a logon message with the reset flag set
		logon(true);
	}
}

function logoutReceived(logout, self) {
	console.log('logout received');

	if (!logoutinitiated) {
		if ('text' in logout) {
			console.log('logout text=' + logout.text);
		}

		// return a logout
		sendLogout('');
	}

	// we are done
	disconnect();
}

function resetSequenceNumbers() {
	console.log('resetting sequence numbers');

	// reset next fix sequence numbers
	db.set("fixseqnumin", 0);
	db.set("fixseqnumout", 0);
}

function sequenceReset(body, nextnumin) {
	if (body.newseqno < nextnumin) {
		console.log("Sequence reset number less than expected sequence number, aborting...");
		disconnect();
	}

	// reset the incoming sequence number
	db.set("fixseqnumin", body.newseqno - 1);
}

function resendRequest(beginseqno) {
	var msg;

	console.log('sending resend request');

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
	// if we have a test request running, check the value of the string returned
	if (testrequesttimer != null) {
		if ('testreqid' in heartbeat) {
			if (heartbeat.testreqid == matchtestreqid) {
				clearTimeout(testrequesttimer);
				testrequesttimer = null;
			}
		}
	}
}

function testRequestReceived(testrequest, self) {
	console.log(testrequest);
	console.log('test request received');

	stopHeartbeatTimer();
	testRequestReply(testrequest.testreqid);
	startHeartbeatTimer();
}

function testRequestReply(reqid) {	
	var msg = '112=' + reqid + SOH;

	sendMessage('0', "", "", msg, false, null, null);
}

function resendRequestReceived(resendrequest) {
	// check we have a valid begin & end sequence number
	// end number may be '0' to indicate infinity
	if (resendrequest.beginseqno < 1 || (resendrequest.endseqno < resendrequest.beginseqno && resendrequest.endseqno != 0)) {
		console.log("Invalid Begin or End sequence number, unable to continue");
		disconnect();
		return;
	}

	// set the outgoing message recovery flag
	messagerecoveryout = true;

	// resend the required messages
	resendMessage(resendrequest.beginseqno, resendrequest.endseqno);
}

function businessRejectReceived(businessreject, self) {
	self.emit('businessReject', businessreject);
}

function resendMessage(msgno, endseqno) {
	// get requested message
	db.hgetall("messageout:" + msgno, function(err, msg) {
		if (err) {
			console.log(err);
			disconnect();
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
					disconnect();
					console.log(err);
					return;
				}

				// have we reached the last stored message?
				if (msgno == msgseqnumout) {
					// send any sequence gap message
					if (sequencegapnum != 0) {
						sequenceGap(sequencegapnum, msgno, sequencegaptimestamp);
					}

					// we are done
					doneResending();
					return;
				}

				// keep on going
				resendMessage(msgno + 1, endseqno);
			});
		} else {
			// check for having reached the request number
			if (msgno == endseqno) {
				// send any sequence gap message
				if (sequencegapnum != 0) {
					sequenceGap(sequencegapnum, msgno, sequencegaptimestamp);
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

	console.log('seq gap');
	console.log('beginnum='+beginnum);
	console.log('endnum='+endnum);

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

Ptp.prototype.getExecTypeDesc = function(exectype) {
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

Ptp.prototype.getQuoteRejectReason = function(reason) {
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

Ptp.prototype.getSessionRejectReason = function(reason) {
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

Ptp.prototype.getBusinessRejectReason = function(reason) {
	var desc;

	switch (parseInt(reason)) {
		case 0:	
			desc = "Other";
			break;
		case 1:
			desc = "Unkown ID";
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