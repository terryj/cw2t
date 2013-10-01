/****************
* ptpserver.js
* Test server for Proquote Fix API v1.7
* Cantwaittotrade Limited
* Terry Johnston
* June 2013
****************/

var net = require('net');
var redis = require('redis');

var fixver = 'FIX.4.2';
var pqport = 50143;
var sendercompid = 'PTPUAT1';
var targetcompid = 'CWTTUAT1';
var SOH = String.fromCharCode(1);
var datain = '';
var tagarr = [];
var valuearr = [];
var resendrequesttimer = null;
var kludge = true;
var heartbeattimer = null;

db = redis.createClient(6379, "127.0.0.1");
db.on("connect", function(err) {
    if (err) {
      console.log(err);
      return;
    }
    console.log("Connected to Redis");
});

db.on("error", function(err) {
  console.log(err);
});

var server = net.createServer(function(c) {
  console.log('client connected');

  c.setEncoding('ascii');

  c.on('data', function(data) {
  	console.log(data);
  	datain += data;
  	parseData(c);
  });

  c.on('end', function() {
    console.log('client disconnected');
  });
});

server.listen(pqport, function() {
  console.log('Proquote test server listening on port ' + pqport);
});

function parseData(conn) {
	var tag;
	var value;
	var startoftag = 0;
	var startofvalue = 0;
	var messagestarted = false;

	var datalen = datain.length;

	for (var i = 0; i < datalen; ++i) {
		if (datain.charAt(i) == '=') {
			tag = datain.substring(startoftag, i);
			console.log("tag="+tag);
			tagarr.push(tag);
			if (tag == '8') {
				messagestarted = true;
			}
			startofvalue = i + 1;
		} else if (datain.charAt(i) == SOH) {
			value = datain.substring(startofvalue, i);
			valuearr.push(value);
			console.log("value="+value);
			startoftag = i + 1;

			if (tag == '8') {
				messagestarted = true;
			} else if (tag == '10') {
				if (messagestarted) {
					// we have a complete message
					completeMessage(conn);
					messagestarted = false;
				}

				// remove processed message
				datain = datain.slice(i + 1);

				// reset tag markers
				startoftag = 0;
				startofvalue = 0;

				// clear tag & value arrays
				tagarr.splice(0, tagarr.length);
				valuearr.splice(0, valuearr.length);
			}
		}
	}
}

function completeMessage(conn) {
	console.log("complete message");

	var msgtypeindex = tagarr.indexOf('35');

	if (msgtypeindex != -1) {
		switch (valuearr[msgtypeindex]) {
			case 'R':
				quoteRequest(conn);
				break;
			case 'D':
				newOrderSingle(conn);
				break;
			case 'F':
				orderCancelRequest(conn);
				break;
			case 'A':
				logonReceived(conn);
				break;
			case '8':
				break;
			case '2':
				resendRequestReceived(conn);
				break;
		}
	}
}

function logonReceived(conn) {
	console.log("logon received, sending logon reply");

	// send a logon back
	var msg = '98=' + '0' + SOH // unencrypted
			+ '108=' + '30' + SOH; // 30 sec heartbeat

	sendMessage(msg, 'A', conn, false);

	/*if (resendrequesttimer == null) {
		resendrequesttimer = setTimeout(function() {
			resendRequest('1', conn);
		}, 3000);
	}*/
		/*heartbeattimer = setInterval(function() {
			heartbeat(conn, false);
		}, 1000);*/
}

function resendRequestReceived(conn) {
	var beginseqno;
	console.log('resendRequestReceived');

	clearInterval(heartbeattimer);

	for (var i = 0; i < tagarr.length; ++i) {
		switch (tagarr[i]) {
			case '7':
				beginseqno = valuearr[i];
				break;
			default:
		}
	}

	db.get("pqfixseqnumout", function(err, endseqno) {
		db.set("pqfixseqnumout", beginseqno-1);
		x(endseqno, conn);
	});
}

function x(endseqno, conn) {
	db.get("pqfixseqnumout", function(err, msgseqnum) {
		if (msgseqnum < endseqno) {
			console.log('resending hb');
			heartbeat(conn, true);
			x(endseqno, conn);
		} else {
			heartbeat(conn, false);
		}
	});
}

function heartbeat(conn, resend) {
	console.log('sending heartbeat');

	if (kludge) {
		db.get("pqfixseqnumout", function(err, msgseqnum) {
			if (msgseqnum == 10) {
				msgseqnum=15;
				db.set("pqfixseqnumout", msgseqnum);
				kludge=false;
			}
		});
	}

	sendMessage('', '0', conn, resend);
}

function resendRequest(beginno, conn) {
	var msg;

		msg = '7=' + beginno + SOH
			+ '16=' + '0' + SOH;

		sendMessage(msg, '2', conn, false);
}

function quoteRequest(conn) {
	var quote = {};

	console.log('quote request');

	quote.quoteid = '1';
	quote.securityid = '1234';
	quote.idsource = '4';
	quote.bidpx = '1.23';
	quote.bidsize = '100'; // set bidsize & offersize to zero for rejection
	quote.bidquoteid = 'TEST123';
	quote.bidqbroker = 'BROKERABC';
	quote.validuntiltime = getUTCTimeStamp();
	quote.transacttime = getUTCTimeStamp();
	quote.currency = 'GBP';
	quote.lastmkt = 'a';
	quote.quoterejectreason = '0';
	quote.cashorderqty = '100';

	for (var i = 0; i < tagarr.length; ++i) {
		console.log(tagarr[i] + '=' + valuearr[i]);

		switch (tagarr[i]) {
			case '131':
				quote.quotereqid = valuearr[i];
				break;
			case '55':
				quote.symbol = valuearr[i];
				break;
			case '64':
				quote.futsettdate = valuearr[i];
				break;
			case '120':
				quote.settlcurrency = valuearr[i];
				break;
			default:
		}
	}

	// send bid quote
	sendQuote(quote, conn);

	delete quote['bidpx'];
	delete quote['bidsize'];
	delete quote['bidquoteid'];
	delete quote['bidqbroker'];
	quote.offerpx = '1.25';
	quote.offersize = '100';
	quote.offerquoteid = 'TEST456';
	quote.offerqbroker = 'BROKERDEF';

	// send offer quote
	sendQuote(quote, conn);
}

function newOrderSingle(conn) {
	var exereport = {};

	console.log('newOrderSingle');

	exereport.orderid = '1';
	exereport.execid = 'barg1';
	exereport.exectype = '2';
	//exereport.ordrejreason = '101'; // omit if ok
	exereport.securityid = '1234';
	exereport.idsource = '4';
	exereport.futsettdate = '20130115-10:10:10';
	exereport.fundingcharge = '0.01';
	exereport.fundingconsideration = '5';
	exereport.lastmkt = 'a';
	exereport.leavesqty = '0';

	for (var i = 0; i < tagarr.length; ++i) {
		switch (tagarr[i]) {
			case '11':
				exereport.clordid = valuearr[i];
				break;
			case '55':
				exereport.symbol = valuearr[i];
				break;
			case '54':
				exereport.side = valuearr[i];
				break;
			case '38':
				exereport.orderqty = valuearr[i];
				exereport.cumqty = valuearr[i];
				exereport.lastshares = valuearr[i];
				break;
			case '119':
				exereport.cashorderqty = valuearr[i];
				break;
			case '44':
				exereport.price = valuearr[i];
				exereport.avgpx = valuearr[i];
				exereport.lastpx = valuearr[i];
				break;
			case '15':
				exereport.currency = valuearr[i];
				exereport.settlcurrency = valuearr[i];
				break;
			case '60':
				exereport.transacttime = valuearr[i];
				break;
			case '10':
				// checksum - todo: something?
				break;
			case '40': // ordertype
				exereport.ordtype = valuearr[i];
				break;
			default:
				console.log('unknown tag:' + tagarr[i]);
		}
	}

	if (exereport.ordtype == '2') { // limit
		exereport.secondaryorderid = 'barg1';
		exereport.ordstatus = '0'; // new
		//exereport.ordstatus = '8'; // rejected
		exereport.orderqty = '0';
	} else {
		exereport.ordstatus = '2'; // filled
	}

	if (!('price' in exereport)) {
		exereport.price = '1.23';
		exereport.avgpx = '1.23';
		exereport.cashorderqty = '123';
		exereport.lastpx = '1.23';
	}

	exereport.settlcurramt = parseInt(exereport.lastshares) * parseFloat(exereport.lastpx);

	sendExecutionReport(exereport, conn);

			/*setTimeout(function() {
				sendFill();
    		}, 4000);*/

    					/*setTimeout(function() {
				sendFill();
    		}, 8000);*/

    function sendFill() {
		exereport.ordstatus = '2'; // fill
		exereport.orderqty = '100';
		sendExecutionReport(exereport, conn);
    }
}

function orderCancelRequest(conn) {
	var ordercancelreject = {};
	var exereport = {};
	var succeed = true;

	console.log('order cancel request');

	for (var i = 0; i < tagarr.length; ++i) {
		console.log(tagarr[i] + '=' + valuearr[i]);

		switch (tagarr[i]) {
			case '41':
				exereport.orderid = valuearr[i]; // proquote limit ref
				exereport.execid = valuearr[i];
				ordercancelreject.orderid = valuearr[i];
				break;
			case '11':
				exereport.clordid = valuearr[i]; // order cancel request id
				ordercancelreject.clordid = valuearr[i];
				ordercancelreject.origclordid = valuearr[i];
				break;
			case '55':
				exereport.symbol = valuearr[i];
				ordercancelreject.symbol = valuearr[i];
				break;
			default:
		}
	}

	ordercancelreject.cxlrejresponse = '1';
	ordercancelreject.cxlrejreason = '101';

	exereport.exectrantype = '1';
	exereport.exectype = '4';
	exereport.ordstatus = '4';
	exereport.orderqty = '100';
	exereport.ordtype = '2';
	exereport.leavesqty = '0'
	exereport.cumqty = '0';
	exereport.avgpx = '0';

	if (succeed) {
		sendExecutionReport(exereport, conn);
	} else {
		sendOrderCancelReject(ordercancelreject, conn);
	}
}

function sendQuote(quote, conn) {
	var msg = '131=' + quote.quotereqid + SOH
			+ '117=' + quote.quoteid + SOH
			+ '55=' + quote.symbol + SOH
			+ '48=' + quote.securityid + SOH
			+ '22=' + quote.idsource + SOH
			+ '62=' + quote.validuntiltime + SOH
			+ '60=' + quote.transacttime +  SOH
			+ '15=' + quote.currency + SOH
			+ '30=' + quote.lastmkt + SOH
			+ '300=' + quote.quoterejectreason + SOH
			+ '120=' + quote.settlcurrency + SOH
			+ '64=' + quote.futsettdate + SOH
			+ '152=' + quote.cashorderqty + SOH;

	if ('bidpx' in quote) {
		msg += '132=' + quote.bidpx + SOH
			+ '134=' + quote.bidsize + SOH
			+ '117=' + quote.bidquoteid + SOH
			+ '6158=' + quote.bidqbroker + SOH;
	} else {
		msg += '133=' + quote.offerpx + SOH
			+ '135=' + quote.offersize + SOH
			+ '117=' + quote.offerquoteid + SOH
			+ '6158=' + quote.offerqbroker + SOH;
	}

	sendMessage(msg, 'S', conn, false);
}

function sendExecutionReport(exereport, conn) {
	var msg = '37=' + exereport.orderid + SOH
			+ '11=' + exereport.clordid + SOH
			+ '17=' + exereport.execid + SOH
			+ '150=' + exereport.exectype + SOH
			+ '39=' + exereport.ordstatus + SOH
			+ '55=' + exereport.symbol + SOH
			+ '48=' + exereport.securityid + SOH
			+ '22=' + exereport.idsource + SOH
			+ '64=' + exereport.futsettdate + SOH
			+ '54=' + exereport.side + SOH
			+ '38=' + exereport.orderqty + SOH
			+ '152=' + exereport.cashorderqty + SOH
			+ '44=' + exereport.price + SOH
			+ '15=' + exereport.currency + SOH
			+ '151=' + exereport.leavesqty + SOH
			+ '14=' + exereport.cumqty + SOH
			+ '6=' + exereport.avgpx + SOH
			+ '60=' + exereport.transacttime + SOH
			+ '119=' + exereport.settlcurramt + SOH
			+ '120=' + exereport.settlcurrency + SOH
			+ '31=' + exereport.lastpx + SOH
			+ '32=' + exereport.lastshares + SOH
			+ '5063=' + exereport.fundingcharge + SOH
			+ '5064=' + exereport.fundingconsideration + SOH
			+ '30=' + exereport.lastmkt + SOH;

	if ('ordrejreason' in exereport) {
		msg += '103=' + exereport.ordrejreason + SOH;
	}
	if ('secondaryorderid' in exereport) {
		msg += '198=' + exereport.secondaryorderid + SOH;		
	}

	sendMessage(msg, '8', conn, false);
}

function sendOrderCancelReject(ocreject, conn) {
	var msg = '37=' + ocreject.orderid + SOH
			+ '11=' + ocreject.clordid + SOH
			+ '102=' + ocreject.cxlrejreason + SOH;

	sendMessage(msg, '9', conn, false);
}

function sendMessage(body, msgtype, conn, resend) {
	db.incr("pqfixseqnumout", function(err, msgseqnum) {
		if (err) {
			console.log(err);
			return;
		}

		// create the header
		var msg = '8=' + fixver + SOH
				+ '9=' + body.length + SOH
				+ '35=' + msgtype + SOH
				+ '49=' + sendercompid + SOH
				+ '56=' + targetcompid + SOH
				+ '34=' + msgseqnum + SOH
				+ '52=' + getUTCTimeStamp() + SOH;

		if (resend) {
			msg += '43=' + 'Y' + SOH; // possible duplicate
		}

		// add body to header
		msg += body;

		// calculate the check sum & append it
		var checksumstr = '10=' + checksum(msg) + SOH;
		msg += checksumstr;

		console.log('sendmsg');
		console.log(msg);
		console.log('----------');

		// send the message
		//for (var i=0; i<20000; i++) {
			conn.write(msg);
		//}
	});
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
    }
    else if (chksm >= 10 && chksm < 100) {
        checksumstr = '0' + (chksm + '');
    }
    else {
        checksumstr = '' + (chksm + '');
    }

    return checksumstr;
}

function getUTCTimeStamp() {
    var timestamp = new Date();

    var year = timestamp.getUTCFullYear();
    var month = timestamp.getUTCMonth() +1;
    var day = timestamp.getUTCDate();
    var hours = timestamp.getUTCHours();
    var minutes = timestamp.getUTCMinutes();
    var seconds = timestamp.getUTCSeconds();
    var millis = timestamp.getUTCMilliseconds();


    if (month < 10) { month = '0' + month;}

    if (day < 10) { day = '0' + day;}

    if (hours < 10) { hours = '0' + hours;}

    if (minutes < 10) { minutes = '0' + minutes;}

    if (seconds < 10) { seconds = '0' + seconds;}

    if (millis < 10) {
        millis = '00' + millis;
    } else if (millis < 100) {
        millis = '0' + millis;
    }


    var ts = [year, month, day, '-' , hours, ':' , minutes, ':' , seconds, '.' , millis].join('');

    return ts;
}
