/****************
* nbtrader.js
* Link to Netbuilder NBTrader Fix API v4.2
* Cantwaittotrade Limited
* Terry Johnston
* August 2014
* Mods:
* Jan 11 2017 - changed fixsequence number handling, ouch!
* ****************/

// avoids DEPTH_ZERO_SELF_SIGNED_CERT error for self-signed certs
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

var util = require('util');
//var net = require('net');
var tls = require('tls');
var events = require('events');
var fs = require("fs");

var commonbo = require('./commonbo.js');

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
var nextseqnumout; // next sequence number out
var messagerecoveryout = false; // flag to indicate we are in a resend process for outgoing messages
var sequencegapnum = 0; // sequence gap starting message number
var resendrequestrequired = false; // indicates we need to do our own resend request once we have serviced a resend request
var resendrequired = false; // indicates we need to service a resend request when we have finished our own resend request
var resendbody = ""; // stores resend request message
//var settlmnttyp = '6'; // indicates a settlement date is being used, rather than a number of days
var norelatedsym = '1'; // number of related symbols in a request, always 1
var idsource = '4'; // indicates ISIN is to be used to identify a security
//var securitytype = 'CS'; // common stock
var handinst = '1'; // i.e. no intervention
var connectstatus = 2; // status of connection, 1=connected, 2=disconnected
var connectstatusint = 30;
var connectstatusinterval = 30;
var datareceivedsinceheartbeat = false; // indicates whether data has been received since the last heartbeat
 
var options = {
  key: fs.readFileSync('scripts/key.pem'),
  cert: fs.readFileSync('scripts/cert.pem'),
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

  //nbconn = net.connect(nbport, nbhost, function() {
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
  resendrequired = false;
}

function init(self) {
  initFlags();
  registerScripts();

  // publish status every time period
  connectstatusinterval = setInterval(publishStatus, connectstatusint * 1000);

  db.hget("config", "nextfixseqnumout", function(err, nextfixseqnumout) {
    if (err) {
      console.log(err);
      return;
    }

    if (nextfixseqnumout == null) {
      console.log("nextfixseqnumout not found");
      return;
    }

    nextseqnumout = nextfixseqnumout;
  });

  db.hget("config", "nextfixseqnumin", function(err, nextfixseqnumin) {
    if (err) {
      console.log(err);
      return;
    }

    if (nextfixseqnumin == null) {
      console.log("nextfixseqnumin not found");
      return;
    }

    nextseqnumin = nextfixseqnumin;
  });

  db.hget("config", "tradingipaddress", function(err, ipaddr) {
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

  db.hget("config", "tradingport", function(err, port) {
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

  db.hget("config", "sendercompid", function(err, senderid) {
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

  db.hget("config", "targetcompid", function(err, targetid) {
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

  db.hget("config", "onbehalfofcompid", function(err, onbehalfofid) {
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

  var msg = '98=' + encryptmethod + SOH + '108=' + heartbtint + SOH;

  if (reset) {
    // we are returning a logon with the reset sequence number flag set
    msg += '141=' + 'Y' + SOH;
  }

  sendMessage('A', "", "", msg, false, null, null, "", "", "");
}

function sendLogout(text) {
  var msg = '';

  if (text != '') {
    msg = '58=' + text + SOH;
  }

  sendMessage('5', "", "", msg, false, null, null, "", "", "");
}

function sendHeartbeat() {
  sendMessage('0', "", "", "", false, null, null, "", "", "");
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
  matchtestreqid = commonbo.getUTCTimeStamp(new Date());

  msg = '112=' + matchtestreqid + SOH;

  // send a test request after agreed quiet time period
  sendMessage('1', "", "", msg, false, null, null, "", "", "");

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

  var timestamp = commonbo.getUTCTimeStamp(new Date());
  console.log(timestamp + " - disconnected from NBTrader");
}

function setStatus(status) {
  // tell everyone our status has changed
  connectstatus = status;
  publishStatus();	
}

Nbt.prototype.quoteRequest = function(quoterequest) {
  var msg = '131=' + quoterequest.brokerid + ":" + quoterequest.quoterequestid + SOH
    + '146=' + norelatedsym + SOH
    + '55=' + quoterequest.mnemonic + SOH
    + '48=' + quoterequest.isin + SOH
    + '22=' + idsource + SOH
    //+ '167=' + securitytype + SOH
    + '63=' + quoterequest.settlmnttypid + SOH
    + '207=' + quoterequest.exchangeid + SOH
    + '15=' + quoterequest.currencyid + SOH
    + '120=' + quoterequest.settlcurrencyid + SOH
    + '60=' + quoterequest.timestamp + SOH;

  // add settlement date if future settlement type specified
  if (quoterequest.settlmnttypid == 6) {
    msg += '64=' + quoterequest.futsettdate + SOH;
  }

  // qty as shares or cash
  if (quoterequest.quantity != "") {
    msg += '38=' + quoterequest.quantity + SOH;
  } else {
    msg += '152=' + quoterequest.cashorderqty + SOH;			
  }

  // side, if present
  if (quoterequest.side != "") {
    msg += '54=' + quoterequest.side + SOH;
  }

  // specify a marketmaker
  if ('quoterid' in quoterequest) {
    msg += '6158=' + quoterequest.quoterid + SOH;
  }

  sendMessage('R', onbehalfofcompid, "", msg, false, null, null, quoterequest.brokerid, quoterequest.quoterequestid, 5);
}

Nbt.prototype.newOrder = function(order) {
  var delivertocompid = "";

  var msg = '11=' + order.brokerid + ":" + order.orderid + SOH
    + '21=' + handinst + SOH
    + '55=' + order.mnemonic + SOH
    + '48=' + order.isin + SOH
    + '22=' + idsource + SOH
    //+ '167=' + securitytype + SOH
    + '207=' + order.exchangeid + SOH
    + '54=' + order.side + SOH
    + '60=' + order.timestamp + SOH
    + '63=' + order.settlmnttypid + SOH
    + '59=' + order.timeinforceid + SOH
    + '120=' + order.settlcurrencyid + SOH
    + '15=' + order.currencyid + SOH
    + '40=' + order.ordertypeid + SOH;

    if (order.ordertypeid == 'D') { // previously quoted
      // add quote id & who the quote was from
      msg += '117=' + order.externalquoteid + SOH;
      delivertocompid = order.quoterid;
    } else if (parseInt(order.ordertypeid) == 1) { // market
      delivertocompid = "BEST";
    } else if (order.delivertocompid != "") {
      delivertocompid = order.delivertocompid;
    }

    // quantity as shares or cash
    if (order.quantity != '') {
      msg += '38=' + order.quantity + SOH;
    } else {
      msg += '152=' + order.cashorderqty + SOH;				
    }

    // add price if specified
    if (order.price != '') {
      msg += '44=' + order.price + SOH;
    }

    // add date & time if timeinforce = "GTD"
    if (order.timeinforceid == '6') {
      msg += '432=' + order.expiredate + SOH;
      //+ '126=' + order.expiretime + SOH;
    }

    if (order.futsettdate != '') {
      msg += '64=' + order.futsettdate + SOH;
    }

    sendMessage('D', onbehalfofcompid, delivertocompid, msg, false, null, null, order.brokerid, order.orderid, 2);
}

Nbt.prototype.orderCancelRequest = function(ocr) {
  var msg = '11=' + ocr.ordercancelreqid + SOH
    + '41=' + ocr.brokerid + ":" + ocr.orderid + SOH
    + '55=' + ocr.mnemonic + SOH
    + '48=' + ocr.isin + SOH
    + '22=' + idsource + SOH
    + '54=' + ocr.side + SOH
    + '38=' + ocr.quantity + SOH
    + '60=' + ocr.timestamp + SOH;

    sendMessage('F', onbehalfofcompid, "", msg, false, null, null, ocr.brokerid, ocr.orderid, 6);
}

//
// resend=false, msgseqnum=null, origtimestamp=null for a new message
// resend=true, msgseqnum=original message number, origtimestamp=original timespamp if a message is being resent
// 
function sendMessage(msgtype, onbehalfofcompid, delivertocompid, body, resend, msgseqnum, origtimestamp, brokerid, businessobjectid, businessobjecttypeid) {
  // we need our own timestamp
  var timestamp = commonbo.getUTCTimeStamp(new Date());

  if (resend) {
    console.log('resending message #' + msgseqnum);

    // we are resending, so use the passed sequence number
    sendData(msgtype, onbehalfofcompid, delivertocompid, body, resend, msgseqnum, timestamp, origtimestamp, brokerid, businessobjectid, businessobjecttypeid);
  } else {
    // if this is a new message & we are in a recovery phase, don't send now
    // instead, the message should be sent by the recovery process itself
    // otherwise, use the next sequence number
    if (!messagerecoveryout) {
      sendData(msgtype, onbehalfofcompid, delivertocompid, body, resend, nextseqnumout, timestamp, origtimestamp, brokerid, businessobjectid, businessobjecttypeid);
    }
  }
}

function sendData(msgtype, onbehalfofcompid, delivertocompid, body, resend, msgseqnum, timestamp, origtimestamp, brokerid, businessobjectid, businessobjecttypeid) {
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

  if (resend) {
    // just send the message
    nbconn.write(msg, 'ascii'); // todo: need ascii option?
  } else {
    // store & send the message
    db.eval(scriptfixmessageout, 0, msgtype, onbehalfofcompid, delivertocompid, msg, timestamp, brokerid, businessobjectid, businessobjecttypeid, nextseqnumout, function(err, ret) {
      if (err) {
        console.log(err);
        return;
      }

      // store the next sequence number
      nextseqnumout = ret;

      // send the message
      nbconn.write(msg, 'ascii'); // todo: need ascii option?
    });
  }
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
        endtag = datainbuf.indexOf("10=");
        completeMessage(tagvalarr, self, endtag);

        // remove processed message from buffer
        //endtag = datainbuf.indexOf("10=");
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

function completeMessage(tagvalarr, self, endtag) {
  // build the header & body
  var header = getHeader(tagvalarr);
  var body = getBody(header.msgtype, tagvalarr);

  // our timestamp
  var timestamp = commonbo.getUTCTimeStamp(new Date());

  // get message to store
  var msg = datainbuf.substring(0, endtag);

  // store the message & get the expected incoming sequence number
  db.eval(scriptfixmessagein, 0, header.msgtype, "", "", msg, timestamp, "", "", "", header.msgseqnum, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }

    nextseqnumin = ret[0];
    var fixseqnumid = ret[1];

    // id created may be used to link to a record
    body.fixseqnumid = fixseqnumid;
 
    if (header.possdupflag == 'Y') {
      if (messagerecoveryinrequested) {
        // this is the start of the recovery process
        console.log('message recovery started');
        messagerecoveryinrequested = false;
        messagerecoveryinstarted = true;
      }
    } else {
      if (messagerecoveryinrequested) {
        // we are waiting for the first resend message, so just ignore
        if (header.msgtype == '2') {
          resendbody = body;
          resendrequired = true; 
        }
        return;
      }

      if (messagerecoveryinstarted) {
        // we are done recovering
        console.log('message recovery finished');
        messagerecoveryinstarted = false;
        if (resendrequired) {
          resendRequestReceived(resendbody, self);
        }
      }
    }

    // check for sequence reset as this overrides any sequence number processing
    if (header.msgtype == '4') {
      sequenceReset(body, self);
      return;
    }

    // check fix sequence number matches what we expect
    if (parseInt(header.msgseqnum) < parseInt(nextseqnumin)) {
      console.log("incoming sequence number: " + header.msgseqnum + " less than expected: " + nextseqnumin);

      if (header.possdupflag == 'Y') {
        // duplicate received but we are catching up, so just ignore
	return;
      } else {
        if (header.msgtype == 'A') {
	  if ('resetseqnumflag' in body) {
	    // resetting sequence numbers, allow to pass through - todo: check & test this
	  } else {
	    // normal logon reply & we haven't missed anything, so just set the stored sequence number to that received
	    console.log("Resetting incoming sequence number to: " + header.msgseqnum);
	    db.hset("config", "nextfixseqnumin", header.msgseqnum);
	  }
        } else if (header.msgtype == '5') {
          if ('text' in body) {
	    console.log("logout text received: " + body.text);
          }

	  // we have been logged out & will be disconnected by the other side, just exit & try again
          return;
        } else {
          // serious error, abort
          disconnect(self);
	  return;
        }
      }
    } else if (parseInt(header.msgseqnum) > parseInt(nextseqnumin)) {
      console.log("incoming sequence number: " + header.msgseqnum + " greater than expected: " + nextseqnumin);

      if (header.msgtype == '5') {
        // we have been logged out & will be disconnected by the other side, just exit & try again
        if ('text' in body) {
          console.log("logout text received: " + body.text);
        }
        return;
      }

      if (messagerecoveryinstarted) {
        // we are in the middle of a resend & it has gone wrong, so abort
        console.log("message recovery failed, aborting...");
        disconnect(self);
        return;
      }

      if (messagerecoveryinrequested) {
        // already requested a resend, so this should just be a message sent before the resend request
        // so, just ignore, it should be handled as part of the resend
        if (header.msgtype == '2') {
          resendbody = body;
          resendrequired = true; 
        }
        return;
      } else {
        // request resend of messages from this point, but wait to adjust the stored value
        // until we know the recovery process has started, as in the first possible duplicate received
        resendRequest(nextseqnumin);

        // we may need to act on this message, once the resend process has finished
        messagerecoveryinrequested = true;
	return;
      }
    }

    // sequence numbers match, so process message
    processMessage(body, header, self);
  });
}

function processMessage(body, header, self) {
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
				var res = tagvalarr[i].value.split(":");
				body.brokerid = res[0];
				body.quoterequestid = res[1];
				break;
			case 117:
				body.externalquoteid = tagvalarr[i].value;
				break;
			case 55:
			 	body.mnemonic = tagvalarr[i].value;
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
				body.settlmnttypid = tagvalarr[i].value;
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
				body.quoterejectreasonid = tagvalarr[i].value;
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
				body.quoterid = tagvalarr[i].value;
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
				var res = tagvalarr[i].value.split(":");
				body.brokerid = res[0];
				body.clordid = res[1];
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
				body.mnemonic = tagvalarr[i].value;
				break;
			case 48:
				body.securityid = tagvalarr[i].value;
				break;
			case 22:
				body.idsource = tagvalarr[i].value;
				break;
			case 63:
				body.settlmnttypid = tagvalarr[i].value;
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
				body.timeinforceid = tagvalarr[i].value;
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
				var res = tagvalarr[i].value.split(":");
				body.brokerid = res[0];
				body.clordid = res[1];
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
				var res = tagvalarr[i].value.split(":");
				body.brokerid = res[0];
				body.quoterequestid = res[1];
				break;
			case 297:
				body.quoteackstatus = tagvalarr[i].value;
				break;
			case 300:
				body.quoterejectreasonid = tagvalarr[i].value;
				break;
			case 6158:
				body.quoterid = tagvalarr[i].value;
				break;
			default:
				unknownTag(tagvalarr[i].tag);
			}
		}
		break;
	case '3': // reject
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
	case 'j': // business message reject
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
    console.log("unknown tag: " + tag);
  }
}

function rejectReceived(reject, self) {
  self.emit('reject', reject);
}

function logonReplyReceived(logonreply, self) {
  // we are connected
  setStatus(1);

  var timestamp = commonbo.getUTCTimeStamp(new Date());
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
    console.log('logout text: ' + logout.text);
  }

  // we are done
  disconnect(self);
}

function resetSequenceNumbers() {
  console.log('resetting sequence numbers');

  db.eval(scriptresetsequencenumbers, 0, function(err, ret) {
    if (err) {
      console.log(err);
      return;
    }
  });
}

function sequenceReset(body, self) {
  console.log("sequence reset");

  if (parseInt(body.newseqno) < nextseqnumin) {
    console.log("sequence reset number:" + body.newseqno + " less than expected sequence number:" + nextseqnumin + ", aborting...");
    disconnect(self);
  }

  // reset the incoming sequence number
  db.hset("config", "nextfixseqnumin", body.newseqno);
}

function resendRequest(beginseqno) {
  var msg;

  console.log("sending resend request, starting from sequence number: " + beginseqno);

  msg = '7=' + beginseqno + SOH
    + '16=' + '0' + SOH;

  sendMessage('2', "", "", msg, false, null, null, "", "", "");
}

function sequenceGapReceived(seqgap) {
  console.log("sequenceGapReceived");

  if ('gapfillflag' in seqgap) {
    console.log('resetting incoming sequence number');

    // reset the expected incoming sequence number
    db.hset("config","nextfixseqnumin", seqgap.newseqno);
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

      var timestamp = commonbo.getUTCTimeStamp(new Date());
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

  sendMessage('0', "", "", msg, false, null, null, "", "", "");
}

function resendRequestReceived(resendrequest, self) {
  console.log("resendRequestReceived, begin:" + resendrequest.beginseqno + ", end: " + resendrequest.endseqno);
	
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
  console.log("resendMessage:" + msgno);

  var sequencegaptimestamp;

  // get requested message
  db.eval(scriptgetfixmessage, 0, msgno, function(err, ret) {
    if (err) {
      console.log(err);
      disconnect(self);
      return;
    }

    var fixmessage = "";

    // in case we are not able to find the message
    if (ret[0] == 1) {
      console.log("unable to find message:" + msgno);
      sequencegaptimestamp = commonbo.getUTCTimeStamp(new Date());
      sequenceGap(sequencegapnum, endseqno, sequencegaptimestamp);

      // we are done
      doneResending();
      return;
    }

    fixmessage = JSON.parse(ret[1]);

    // resend the message
    if (fixmessage == "" || fixmessage.msgtype == '0' || fixmessage.msgtype == '1' || fixmessage.msgtype == '2' || fixmessage.msgtype == '4' || fixmessage.msgtype == '5' || fixmessage.msgtype == 'A' || oldMessage(fixmessage)) {
      // don't resend this message, instead send a sequence gap message
      if (sequencegapnum == 0) {
        // store the first message number in a possible sequence of admin messages
        sequencegapnum = msgno;
        if (fixmessage == "") {
          sequencegaptimestamp = commonbo.getUTCTimeStamp(new Date());
        } else {
          sequencegaptimestamp = fixmessage.timestamp;
        }
      }
    } else {
      // send any sequence gap
      if (sequencegapnum != 0) {
        sequenceGap(sequencegapnum, msgno, sequencegaptimestamp);
      }

      sendMessage(fixmessage.msgtype, "", "", fixmessage.message, true, msgno, fixmessage.timestamp, "", "", "");
    }

    // check for infinity
    if (endseqno == 0) {
      // have we reached the last stored message?
      if (msgno == parseInt(nextseqnumout) - 1) {
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
    } else {
      // check for having reached the requested number
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
    console.log("doneResending");
    messagerecoveryout = false;
    resendrequired = false;

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

	console.log('sending sequence gap, beginnum:' + beginnum + ', endnum=:' + endnum);

        // new sequence number
        var newseqnum = endnum + 1;

	msg = '123=' + 'Y' + SOH
		+ '36=' + newseqnum + SOH;

        // send as a resend so message gets sent as message number beginnum 
	sendMessage('4', "", "", msg, true, beginnum, timestamp, "", "", "");

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

function registerScripts() {
  /*
  * scriptfixmessageout
  * get the outgoing sequence number, store the message & then increment the number
  * params: msgtype, onbehalfofcompid, delivertocompid, message, timestamp, brokerid, businessobjectid, businessobjecttypeid
  * returns: next outgoing sequence number
  * note: we use the current sequnce number to store & then increment as this enables a single call to redis
  */
  scriptfixmessageout = '\
  local fixseqnumout = ARGV[9] \
  local fixseqnumid = redis.call("hincrby", "config", "lastfixseqnumid", 1) \
  redis.call("hmset", "fixmessage:" .. fixseqnumid, "msgtype", ARGV[1], "onbehalfofcompid", ARGV[2], "delivertocompid", ARGV[3], "message", ARGV[4], "timestamp", ARGV[5], "brokerid", ARGV[6], "businessobjectid", ARGV[7], "businessobjecttypeid", ARGV[8], "fixseqnumout", fixseqnumout, "fixseqnumid", fixseqnumid) \
  --[[ set a key so we can get to the stored message from the outgoing fix sequence number ]] \
  redis.call("set", "fixseqnumout:" .. fixseqnumout .. ":fixseqnumid", fixseqnumid) \
  --[[ where possible, link the fix message back to the originating message ]] \
  if ARGV[1] == "R" then \
    redis.call("hset", "broker:" .. ARGV[6] .. ":quoterequest:" ..  ARGV[7], "fixseqnumid", fixseqnumid) \
  elseif ARGV[1] == "D" or tonumber(ARGV[1]) == 1 then \
    redis.call("hset", "broker:" .. ARGV[6] .. ":order:" ..  ARGV[7], "fixseqnumid", fixseqnumid) \
  end \
  local nextfixseqnumout = redis.call("hincrby", "config", "nextfixseqnumout", 1) \
  return nextfixseqnumout \
  ';

  /*
  * scriptfixmessagein
  * store the incoming message & get the next incoming sequence number
  * params: msgtype, onbehalfofcompid, delivertocompid, message, timestamp, brokerid, businessobjectid, businessobjecttypeid
  * returns: nextfixseqnumin - used for fix sequence processing, fixseqnumid - may be used to link to record id yet to be created
  */
  scriptfixmessagein = '\
  local fixseqnumin = ARGV[9] \
  local fixseqnumid = redis.call("hincrby", "config", "lastfixseqnumid", 1) \
  redis.call("hmset", "fixmessage:" .. fixseqnumid, "msgtype", ARGV[1], "onbehalfofcompid", ARGV[2], "delivertocompid", ARGV[3], "message", ARGV[4], "timestamp", ARGV[5], "brokerid", ARGV[6], "businessobjectid", ARGV[7], "businessobjecttypeid", ARGV[8], "fixseqnumid", fixseqnumid, "fixseqnumin", fixseqnumin) \
  local nextfixseqnumin = redis.call("hget", "get", "nextfixseqnumin") \
  if tonumber(fixseqnumin) == tonumber(nextfixseqnumin) then \
    nextfixseqnumin = redis.call("hincrby", "config", "nextfixseqnumin", 1) \
  end \
  return {nextfixseqnumin, fixseqnumid} \
  ';

  /*
  * scriptresetsequencenumbers
  * reset incoming & outgoing sequence numbers
  * params: none
  */
  scriptresetsequencenumbers = '\
  redis.call("hset", "config", "nextfixseqnumin", 1) \
  redis.call("hset", "config", "nextfixseqnumout", 1) \
  ';

  /*
  * scriptgetfixmessage
  * params: fixseqnumout
  * returns: 1 if error, else 0, fixmessage as JSON string
  */
  scriptgetfixmessage = '\
  local fixseqnumid = redis.call("get", "fixseqnumout:" .. ARGV[1] .. ":fixseqnumid") \
  if not fixseqnumid then return {1} end \
  local rawvals = redis.call("hgetall", "fixmessage:" .. fixseqnumid) \
  local fixmessage = {} \
  for index = 1, #rawvals, 2 do \
    fixmessage[rawvals[index]] = rawvals[index + 1] \
  end \
  return {0, cjson.encode(fixmessage)} \
  ';
}
