/*
 * mmtapp.js
 * market maker server
 * Cantwaittotrade Limited
 * Paul J. Weighell
 * December 2014
 * */

/* node libraries */
var fs = require("fs")

/* external libraries */
var node_static = require('node-static')

/* cw2t library */
var common = require('../common.js')

/* globals */
var static_directory = new node_static.Server(__dirname) /* static files server */

/* redis */
var redis = require('redis')
var redishost = "127.0.0.1"
var redisport = 6379

/* set-up redis client */
db = redis.createClient(redisport, redishost)
db.on("connect", function (err) {
    if (err) {
        var d = new Date()
        console.log("Market maker automation: " + d.toISOString() + " Error: Cannot connect to Redis on host: " + redishost + ", port: " + redisport + ": " + err)
        return
    }
    else {
        var d = new Date()
        console.log("Market maker automation: " + d.toISOString() + " Connected to Redis on host: " + redishost + ", port: " + redisport)
        initialise()
    }
})

db.on("error", function (err) {
    var d = new Date()
    console.log("Market maker automation: " + d.toISOString() + " Db Error: " + err)
})

function initialise() {
    var d = new Date()
    console.log("Market maker automation: " + d.toISOString() + " Registering common scripts...")
    common.registerCommonScripts()
    var d1 = new Date()
    console.log("Market maker automation: " + d1.toISOString() + " Registered common scripts.")
    /*
 *     starttime()
 *         */
    pubsub()
}

/* calls itself at a random interval */
function starttime() {
    var d = new Date()
    console.log("Market maker automation: " + d.toISOString() + " timer...")
    var t = setTimeout(function () { starttime() }, Math.floor((Math.random() * 5000) + 1000))
}

/* set-up pubsub connection */
function pubsub() {

    var d = new Date()
    console.log("Market maker automation: " + d.toISOString() + " Creating Redis client on host: " + redishost + ", port: " + redisport)
    dbsub = redis.createClient(redisport, redishost)

    dbsub.on("subscribe", function (channel, count) {
        var d1 = new Date()
        console.log("Market maker automation: " + d1.toISOString() + " Redis subscribed to channel: " + channel + ", number of channels: " + count)
    })

    dbsub.on("unsubscribe", function (channel, count) {
        var d2 = new Date()
        console.log("Market maker automation: " + d2.toISOString() + " Redis unsubscribed from channel: " + channel + ", number of channels: " + count)
    })

    /* messages any subscribed channels arrive here */
    dbsub.on("message", function (channel, message) {
        /*
 *         var d3 = new Date()
 *                 console.log("Market maker automation: " + d3.toISOString() + " Message received from channel: " + channel + ", Message reads: " + message)
 *                         */
        try {
            var obj = JSON.parse(message)
            if ("quoterequest" in obj) {
                rfqreceived(obj)
            }
        } catch (e) {
            var d4 = new Date()
            console.log("Market maker automation: " + d4.toISOString() + " mmtapp.dbsub.on: Error: " + e)
        }
    })

    /* listen for RFQs */
    dbsub.subscribe(common.quoterequestchannel)
}

/* rfq received */
function rfqreceived(rfq) {
    /*
 *     Dim rfqmessage As String = "{""quoterequest"":""1"",""clientid"":""1"",""symbol"":""LLOY.L.CFD"",""quantity"":""100"",""cashorderqty"":""coq"",""currency"":""GBP"",""settlcurrency"":""GBP"",""nosettdays"":""2"",""futsettdate"":""20141204"",""quotestatus"":""0"",""timestamp"":""20141202-13:50:11"",""quoterejectreason"":""qrr"",""quotereqid"":""857"",""operatortype"":""1"",""operatorid"":""1""}"
 *         */
    /*
 *     var d = new Date()
 *         console.log("Market maker automation: " + d.toISOString() + " RFQ received: quoterequest: " + rfq.quoterequest + ", symbol: " + rfq.symbol + ", quantity: " + rfq.quantity)
 *             */

    /* get symbol record*/
    var symbol = {}
    db.hgetall("symbol:" + rfq.symbol, function (err, obj) {
        if (err) {
            var f = new Date()
            console.log("Market maker automation: " + f.toISOString() + " rfqreceived: Error. Failed to get symbol record: " + err)
            return
        }
        else {
            /* read symbol record*/
            symbol = obj

            /* get position record*/
            var position = {}
            db.hgetall("999999:position:" + rfq.symbol + ":" + rfq.currency, function (err, obj) {
                if (err) {
                    var f1 = new Date()
                    console.log("Market maker automation: " + f1.toISOString() + " rfqreceived: Error. Failed to get position record: " + err)
                    return
                }
                else {
                    /* read position record */
                    position = obj

                    /* get price record */
                    var price = {}
                    db.hgetall("price:" + rfq.symbol, function (err, obj) {
                        if (err) {
                            var f2 = new Date()
                            console.log("Market maker automation: " + f2.toISOString() + " rfqreceived: Error. Failed to get price record: " + err)
                            return
                        }
                        else {
                            /* read price record */
                            price = obj

                            /* get mm limits etc */
                            var mm = {}
                            db.get("mmRFQSizeMax", function (err, obj) {
                                mm.rfqsizemax = Number(obj)
                                db.get("mmSizeAlgoR", function (err, obj) {
                                    mm.sizealgor = 1 + Number(obj)
                                    db.get("mmSpreadMin", function (err, obj) {
                                        mm.spreadmin = Number(obj)
                                        db.get("mmSpreadMax", function (err, obj) {
                                            mm.spreadmax = Number(obj)
                                            /* go make and publish quote */
                                            makequote(rfq, symbol, position, price, mm.rfqsizemax, mm.sizealgor, mm.spreadmin, mm.spreadmax)
                                        })
                                    })
                                })
                            })
                        }
                    })
                }
            })
        }
    })
}

/*
 *     make quote
 *     */
function makequote(rfq, symbol, position, price, rfqsizemax, sizealgor, spreadmin, spreadmax) {
    /*
 *     var dsd = new Date()
 *         console.log("Market maker automation: " + dsd.toISOString() + " Price record: " + JSON.stringify(price))
 *             var dqr = new Date()
 *                 console.log("Market maker automation: " + dqr.toISOString() + " Position returned: " + JSON.stringify(position))
 *                     */
    /*
 *         Dim rfqmessage As String = "{""quoterequest"":""1"",""clientid"":""1"",""symbol"":""LLOY.L.CFD"",""quantity"":""100"",""cashorderqty"":""coq"",""currency"":""GBP"",""settlcurrency"":""GBP"",""nosettdays"":""2"",""futsettdate"":""20141204"",""quotestatus"":""0"",""timestamp"":""20141202-13:50:11"",""quoterejectreason"":""qrr"",""quotereqid"":""857"",""operatortype"":""1"",""operatorid"":""1""}"
 *             */
    /*
 *         ""quotereqid"":""857"",""clientid"":""1"",""quoteid"":""755"",""symbol"":""LLOYL.CFD"",""bestbid"":""75.97"",""bestoffer"":""76"",""bidpx"":""0.7251"",""offerpx"":"""",""bidquantity"":""100"",""offerquantity"":"""",""bidsize"":""15000"",""offersize"":"""",""validuntiltime"":""20141202-13:50:44"",""transacttime"":""20141202-13:50:14"",""currency"":""GBP"",""settlcurrency"":""GBP"",""qbroker"":""WNTSGB2LBIC"",""nosettdays"":""2"",""futsettdate"":""20141204"",""bidfinance"":""0"",""offerfinance"":""0"",""orderid"":"""",""bidquotedepth"":""1"",""offerquotedepth"":"""",""externalquoteid"":""02ACZL02N501103D"",""qclientid"":"""",""cashorderqty"":""72.510000""
 *             */
    /*
 *         db.hmset("mmProducts:TEST.L", "symbol", "TEST.L", "position", "50", "positioncost", "100", "positionlimitlong", "100", "positionlimitshort", "100", "pricealgor", "0.05", "priceask", "99", "pricebid", "101", "spreadalgor", "0.05", "spreadmax", "10", "spreadmin", "1", "pllimitmin", "-2500", "pllimitmax", "10000")
 *             */

    /*
 *         get previous quote sequence number
 *             */
    db.get("mmquotesequencenumber", function (err, obj) {
        if (err) {
            var f = new Date()
            console.log("Market maker automation: " + f.toISOString() + " makequote: Error. Failed to get mmquotesequencenumber: " + err)
            return
        }
        else {
            /*
 *             inc quote sequence number
 *                         */
            var mmquotesequencenumber = Number(obj) + 1

            /*
 *             calculate new prices
 *                         */

            /* is there a qty or must we make one? */
            var mid = (Number(price.ask) + Number(price.bid)) / 2
            var myqty = 0
            if (Number(rfq.quantity) != 0) {
                myqty = Number(rfq.quantity)
            }
            else if (Number(rfq.quantity) == 0 && Number(rfq.cashorderqty) != 0) {
                myqty = truncate(Number(rfq.cashorderqty) / Number(mid))
            }
            else {
                var f0 = new Date()
                console.log("Market maker automation: " + f0.toISOString() + " makequote: Error. Unable to create quantity.")
                return
            }

            var f1 = new Date()
            console.log("Market maker automation: " + f1.toISOString() + " makequote: " + myqty)

            var spread = price.ask - price.bid
            var mypercentageoflimit = getpercentage(Number(myqty), Number(rfqsizemax))
            spread = Number(spread) + (Number(spread) * mypercentageoflimit * Math.pow(sizealgor, mypercentageoflimit) / Math.pow(sizealgor, 100) / 100)

            /* check spread limits */
            if (spread < Number(spreadmin)) {
                spread = Number(spreadmin)
            }
            else if (spread > Number(spreadmax)) {
                spread = Number(spreadmax)
            }
            /* apply new spread */
            var bid = mid - spread / 2
            var ask = bid + spread

            /*
 *             compile quote
 *                         */
            var quote = {}
            quote.bestbid = bid
            quote.bestoffer = ask
            quote.bidfinance = "0"
            quote.bidpx = bid
            quote.bidquantity = rfq.quantity
            quote.bidquotedepth = "1"
            quote.bidsize = rfq.quantity
            quote.cashorderqty = rfq.cashorderqty
            quote.clientid = rfq.clientid
            quote.currency = rfq.currency
            quote.externalquoteid = mmquotesequencenumber
            quote.futsettdate = rfq.futsettdate
            quote.nosettdays = rfq.nosettdays
            quote.offerfinance = "0"
            quote.offerpx = ask
            quote.offerquantity = rfq.quantity
            quote.offerquotedepth = "1"
            quote.offersize = rfq.quantity
            quote.orderid = ""
            quote.qbroker = "TGRANT"
            quote.qclientid = "999999"
            quote.quoteid = mmquotesequencenumber
            quote.quotereqid = rfq.quoterequest
            quote.settlcurrency = rfq.settlcurrency
            quote.symbol = rfq.symbol

            var d = new Date()
            var MM = addZero(Number(d.getMonth()) + 1) /* add 1 as 0-11 */
            var dd = addZero(d.getDate())
            var hh = addZero(d.getHours())
            var mm = addZero(d.getMinutes())
            var ss = addZero(d.getSeconds())
            /* 20141202-13:50:14 */
            quote.transacttime = d.getFullYear().toString() + MM + dd + "-" + hh + ":" + mm + ":" + ss

            var dv = new Date()
            dv.setSeconds(dv.getSeconds() + 30) /* add 30 seconds as valid quote time */
            var MMv = addZero(Number(dv.getMonth()) + 1) /* add 1 as 0-11 */
            var ddv = addZero(dv.getDate())
            var hhv = addZero(dv.getHours())
            var mmv = addZero(dv.getMinutes())
            var ssv = addZero(dv.getSeconds())
            /* 20141202-13:50:14 */
            quote.validuntiltime = dv.getFullYear().toString() + MMv + ddv + "-" + hhv + ":" + mmv + ":" + ssv

            /* update database*/
            var myreturn = db.set("mmquotesequencenumber", mmquotesequencenumber)
            if (myreturn == true) {
                /* publish quote */
                var myreturn1 = db.publish(common.tradechannel, JSON.stringify(quote))
                if (myreturn1 == true) {
                    var f = new Date()
                    console.log("Market maker automation: " + f.toISOString() + " Published quote #" + mmquotesequencenumber)
                }
                else {
                    var f1 = new Date()
                    console.log("Market maker automation: " + f1.toISOString() + " makequote: Error. Failed to publish quote.")
                }
            }
            else {
                var f2 = new Date()
                console.log("Market maker automation: " + f2.toISOString() + " makequote: Error: No quote published. Update mmquotesequencenumber in database: " + myreturn)
            }

            /*
 *             var d = new Date()
 *                         console.log("Market maker automation: " + d.toISOString() + " Database result: " + myreturn)
 *                                     var dq = new Date()
 *                                                 console.log("Market maker automation: " + dq.toISOString() + " Quotation: " + JSON.stringify(quote))
 *                                                             */
        }
    })
}

/* add leading 0 to turn 9 into 09 etc. */
function addZero(i) {
    if (i < 10) {
        i = "0" + i
    }
    return i
}

/*
 * returns % of p1 in p2 regardless of sign
 * */
function getpercentage(p1, p2) {
    return Math.abs(100 * Number(p1) / Number(p2))
}

function truncate(_value) {
    if (_value < 0) return Math.ceil(_value)
    else return Math.floor(_value)
}

