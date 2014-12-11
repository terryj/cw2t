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
        console.log(err)
        return
    }
    var d = new Date()
    console.log("Market maker automation: " + d.toISOString() + " Connected to Redis on host: " + redishost + ", port: " + redisport)
    initialise()
})

db.on("error", function (err) {
    console.log("Db Error:" + err)
})

function initialise() {
    var d = new Date()
    console.log("Market maker automation: " + d.toISOString() + " Registering common scripts.")
    common.registerCommonScripts()
    starttime()
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
        var d3 = new Date()
        console.log("Market maker automation: " + d3.toISOString() + " Message received from channel: " + channel + ", Message reads: " + message)
        try {
            var obj = JSON.parse(message)
            if ("quoterequest" in obj) {
                rfqreceived(obj)
            }
        } catch (e) {
            var d4 = new Date()
            console.log("Market maker automation: " + d4.toISOString() + " Program error catch in 'mmtapp.dbsub.on', Error reads: " + e)
        }
    });

    /* subscribe to any required channels here */
    dbsub.subscribe(common.quoterequestchannel)
}

/* rfq received */
function rfqreceived(rfq) {
    /*
 *             Dim rfqmessage As String = "{""quoterequest"":""1"",""clientid"":""1"",""symbol"":""LLOY.L.CFD"",""quantity"":""100"",""cashorderqty"":""coq"",""currency"":""GBP"",""settlcurrency"":""GBP"",""nosettdays"":""2"",""futsettdate"":""20141204"",""quotestatus"":""0"",""timestamp"":""20141202-13:50:11"",""quoterejectreason"":""qrr"",""quotereqid"":""857"",""operatortype"":""1"",""operatorid"":""1""}"
 *                 */
    var d = new Date()
    console.log("Market maker automation: " + d.toISOString() + " RFQ received: quoterequest: " + rfq.quoterequest + ", symbol: " + rfq.symbol + ", quantity: " + rfq.quantity)
    var symbol = {}
    db.hgetall("symbol:" + rfq.symbol, function (err, obj) {
        symbol = obj
        makequote(rfq, symbol)
    })
}

/* make quote */
/*
 * ""quotereqid"":""857"",""clientid"":""1"",""quoteid"":""755"",""symbol"":""LLOYL.CFD"",""bestbid"":""75.97"",""bestoffer"":""76"",""bidpx"":""0.7251"",""offerpx"":"""",""bidquantity"":""100"",""offerquantity"":"""",""bidsize"":""15000"",""offersize"":"""",""validuntiltime"":""20141202-13:50:44"",""transacttime"":""20141202-13:50:14"",""currency"":""GBP"",""settlcurrency"":""GBP"",""qbroker"":""WNTSGB2LBIC"",""nosettdays"":""2"",""futsettdate"":""20141204"",""bidfinance"":""0"",""offerfinance"":""0"",""orderid"":"""",""bidquotedepth"":""1"",""offerquotedepth"":"""",""externalquoteid"":""02ACZL02N501103D"",""qclientid"":"""",""cashorderqty"":""72.510000""
 * */
function makequote(rfq, symbol) {
/*
 *     Dim rfqmessage As String = "{""quoterequest"":""1"",""clientid"":""1"",""symbol"":""LLOY.L.CFD"",""quantity"":""100"",""cashorderqty"":""coq"",""currency"":""GBP"",""settlcurrency"":""GBP"",""nosettdays"":""2"",""futsettdate"":""20141204"",""quotestatus"":""0"",""timestamp"":""20141202-13:50:11"",""quoterejectreason"":""qrr"",""quotereqid"":""857"",""operatortype"":""1"",""operatorid"":""1""}"
 *     */
/*
 *     db.hmset("mmProducts:TEST.L", "symbol", "TEST.L", "position", "50", "positioncost", "100", "positionlimitlong", "100", "positionlimitshort", "100", "pricealgor", "0.05", "priceask", "99", "pricebid", "101", "spreadalgor", "0.05", "spreadmax", "10", "spreadmin", "1", "pllimitmin", "-2500", "pllimitmax", "10000")
 *     */

    /*
 *     get previous quote sequence number
 *         inc quote sequence number
 *             */
    db.get("mmquotesequencenumber", function (err, obj) {
        var mmquotesequencenumber = Number(obj) + 1

    /*    console.log("Market maker automation: " + d2.toISOString() + " symbol data: " + JSON.stringify(symbol)) */

        var ask = 123.21
        var bid = 123.12
        var askquantity = 1000
        var bidquantity = 1000

        var quote = {}
        quote.bestbid = bid
        quote.bestoffer = ask
        quote.bidfinance = "0"
        quote.bidpx = bid
        quote.bidquantity = bidquantity
        quote.bidquotedepth = "1"
        quote.bidsize = bidquantity
        quote.cashorderqty = "???"
        quote.clientid = rfq.clientid
        quote.currency = rfq.currency
        quote.externalquoteid = mmquotesequencenumber
        quote.futsettdate = rfq.futsettdate
        quote.nosettdays = rfq.nosettdays
        quote.offerfinance = "0"
        quote.offerpx = ask
        quote.offerquantity = askquantity
        quote.offerquotedepth = "1"
        quote.offersize = askquantity
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
    
        /* send it */
        db.publish(common.tradechannel, JSON.stringify(quote))

        /* update database*/
        db.set("mmquotesequencenumber", mmquotesequencenumber)

        console.log("Market maker automation: " + d.toISOString() + " Quote returned: " + JSON.stringify(quote))

    })
    }

function addZero(i) {
    if (i < 10) {
        i = "0" + i;
    }
    return i;
}


