/*
 * mmtadmin.js
 * market maker admin web page server
 * Cantwaittotrade Limited
 * Paul J. Weighell
 * September 2014
 * */

/* node libraries */
var http = require('http')
var fs = require("fs")

/* external libraries */
var sockjs = require('sockjs')
var node_static = require('node-static')

/* cw2t database */
var redis = require('redis')

/* cw2t library */
var common = require('../common.js')

/* redis */
var redishost = "127.0.0.1"
var redisport = 6379

/* globals */
var connections = {} /* added to when a client logs on */
var static_directory = new node_static.Server(__dirname) /* static files server */
var cw2tport = 8083 /* listen port */
var mmid = 1 /* could have multiple mm's */

/* sockjs server */
var sockjs_opts = { sockjs_url: "http://cdn.jsdelivr.net/sockjs/0.3.4/sockjs.min.js" }
var sockjs_svr = sockjs.createServer(sockjs_opts)

/***** starts here *****/
var d = new Date()
console.log("Market maker automation: " + d.toISOString() + " Starting...")

/* connect to redis */
var dd = new Date()
console.log("Market maker automation: " + dd.toISOString() + " Connecting to database...")
db = redis.createClient(redisport, redishost)
db.on("connect", function (err) {
    if (err) {
        console.log(err)
        return
    }
    console.log("Market maker automation: " + dd.toISOString() + " Connected to Redis at " + redishost + " port " + redisport)
    initialise()
})

/* initialise */
function initialise() {
    var d = new Date()
    console.log("Market maker automation: " + d.toISOString() + " Registering common scripts...")
    common.registerCommonScripts()

    var dd = new Date()
    console.log("Market maker automation: " + dd.toISOString() + " Checking database keys...")
    /*
 *     checkrediskeys()
 *         */
    listen()
}

/* http server */
function listen() {
    var server = http.createServer()

    server.addListener('request', function (req, res) {
        static_directory.serve(req, res)
    })

    server.addListener('upgrade', function (req, res) {
        res.end()
    })

    sockjs_svr.installHandlers(server, { prefix: '/echo' })

    server.listen(cw2tport, function () {
        var d = new Date()
        console.log("Market maker automation: " + d.toISOString() + " Listening on port: " + cw2tport + "...")
    })

    sockjs_svr.on('connection', function (conn) {
        var d = new Date()
        console.log("Market maker automation: " + d.toISOString() + " New connection opened.")
        /* add connection to our list */
        connections[mmid] = conn
        /* web page data arrives here */
        conn.on('data', function (msg) {
            /*console.log(msg);*/
            try {
                var obj = JSON.parse(msg)
                if ("mmparams" in obj) {
                    var dd = new Date()
                    console.log("Market maker automation: " + dd.toISOString() + " Save admin data request...")
                    putmmparams(obj.mmparams)
                } else if ("readadmindata" in obj) {
                    var ddd = new Date()
                    console.log("Market maker automation: " + ddd.toISOString() + " Read admin data request...")
                    readadmindata(obj.mmadmindata)
                } else if ("mmgetnewquote" in obj) {
                    var dddd = new Date()
                    console.log("Market maker automation: " + dddd.toISOString() + " RFQ received: " + msg)

                    /* augment rfq */

                    var rfq = {}
                    rfq.id = obj.mmgetnewquote.rfqid
                    rfq.product = obj.mmgetnewquote.product
                    rfq.size = obj.mmgetnewquote.size
                    rfq.testtype = obj.mmgetnewquote.testtype
                    db.hgetall("mm:1", function (err, obj) {
                        console.dir(obj)
                        rfq.currentask = Number(obj.mmpriceaskcurrent)
                        rfq.currentbid = Number(obj.mmpricebidcurrent)
                        rfq.globalpl = Number(obj.mmglobalpl)
                        rfq.globalplalgor = 1 + Number(obj.mmglobalplalgor)
                        rfq.globalpllimitloss = Number(obj.mmglobalpllimitloss)
                        rfq.globalpllimitprofit = Number(obj.mmglobalpllimitprofit)
                        rfq.globalpositioncost = Number(obj.mmglobalpositioncost)
                        rfq.plalgor = 1 + Number(obj.mmplalgor)
                        rfq.pllimitloss = Number(obj.mmpllimitloss)
                        rfq.pllimitprofit = Number(obj.mmpllimitprofit)
                        rfq.position = Number(obj.mmposition)
                        rfq.positionalgor = 1 + Number(obj.mmpositionalgor)
                        rfq.positioncost = Number(obj.mmpositioncost)
                        rfq.positionlimitlong = Number(obj.mmpositionlimitlong)
                        rfq.positionlimitshort = Number(obj.mmpositionlimitshort)
                        rfq.sizealgor = 1 + Number(obj.mmsizealgor)
                        rfq.sizemax = Number(obj.mmsizemax)
                        rfq.sizemin = Number(obj.mmsizemin)
                        rfq.spread = Number(obj.mmspread)
                        rfq.spreadalgor = 1 + Number(obj.mmspreadalgor)
                        rfq.spreadmax = Number(obj.mmspreadmax)
                        rfq.spreadmin = Number(obj.mmspreadmin)
                        /* what sort of test rfq? do correct test */
                        var result = {}
                        switch (rfq.testtype) {
                            case "position":
                                result = getquotevposition(rfq)
                                break
                            case "size":
                                result = getquotevsize(rfq)
                                break
                            case "pl":
                                result = getquotevpl(rfq)
                                break
                            case "combined":
                                result = getquotevcombined(rfq)
                                break
                            default:
                                var e = new Date()
                                console.log("Market maker automation: " + e.toISOString() + " RFQ data error in testtype: " + rfq.testtype)
                                break
                            }
                        /* do quote return from rfq */
                        var quote = {}
                        quote.rfqid = rfq.id
                        quote.size = rfq.size
                        quote.ask = result.ask
                        quote.bid = result.bid
                        connections[mmid].write("{\"rfqreply\":" + JSON.stringify(quote) + "}")
                        var dddd = new Date()
                        console.log("Market maker automation: " + dddd.toISOString() + " RFQ quoted: " + JSON.stringify(quote))
                    })
                } else {
                    var dddddd = new Date()
                    console.log("Market maker automation: " + dddddd.toISOString() + " Unknown message received: " + msg)
                    }
            } catch (e) {
                console.log(e)
                return
                }
        })
        conn.on('close', function () {
            var dd = new Date()
            console.log("Market maker automation: " + dd.toISOString() + " Connection has closed.")
        })
    })
}

/* form parameters received */
function putmmparams(mmparams) {
    db.hmset("mm:1", "mmglobalpl", mmparams.mmglobalpl, "mmglobalplalgor", mmparams.mmglobalplalgor, "mmglobalpllimitloss", mmparams.mmglobalpllimitloss, "mmglobalpllimitprofit", mmparams.mmglobalpllimitprofit, "mmglobalpositioncost", mmparams.mmglobalpositioncost, "mmplalgor", mmparams.mmplalgor, "mmpllimitloss", mmparams.mmpllimitloss, "mmpllimitprofit", mmparams.mmpllimitprofit, "mmposition", mmparams.mmposition, "mmpositionalgor", mmparams.mmpositionalgor, "mmpositioncost", mmparams.mmpositioncost, "mmpositionlimitlong", mmparams.mmpositionlimitlong, "mmpositionlimitshort", mmparams.mmpositionlimitshort, "mmpricealgor", mmparams.mmpricealgor, "mmpriceask", "0", "mmpriceaskclose",  mmparams.mmpriceaskclose, "mmpriceaskcurrent", mmparams.mmpriceaskcurrent, "mmpricebid", "0", "mmpricebidclose", mmparams.mmpricebidclose, "mmpricebidcurrent", mmparams.mmpricebidcurrent, "mmpricegapovernight", mmparams.mmpricegapovernight, "mmpricegapweekend", mmparams.mmpricegapweekend, "mmquotesequencenumber", "0", "mmrfqsize", mmparams.mmrfqsize, "mmrfqsizealgor", mmparams.mmrfqsizealgor, "mmrfqsizemax", mmparams.mmrfqsizemax, "mmrfqsizemin", mmparams.mmrfqsizemin, "mmspread", mmparams.mmspread, "mmspreadalgor", mmparams.mmspreadalgor, "mmspreadmax", mmparams.mmspreadmax, "mmspreadmin", mmparams.mmspreadmin)
    /* build a reply */
    var mmparamsreply = {}
    mmparamsreply.updated = true
    connections[mmid].write("{\"savedatareply\":" + JSON.stringify(mmparamsreply) + "}")
    var d = new Date()
    console.log("Market maker automation: " + d.toISOString() + " Database updated from manager.")
}

/* form get parameters received */
function readadmindata(getmmadmindata) {
    var mmadmindata = {};
    db.hgetall("mm:1", function (err, obj) {
        mmadmindata.mmglobalpl = obj.mmglobalpl
        mmadmindata.mmglobalplalgor = obj.mmglobalplalgor
        mmadmindata.mmglobalpllimitloss = obj.mmglobalpllimitloss
        mmadmindata.mmglobalpllimitprofit = obj.mmglobalpllimitprofit
        mmadmindata.mmglobalpositioncost = obj.mmglobalpositioncost
        mmadmindata.mmplalgor = obj.mmplalgor
        mmadmindata.mmpllimitloss = obj.mmpllimitloss
        mmadmindata.mmpllimitprofit = obj.mmpllimitprofit
        mmadmindata.mmposition = obj.mmposition
        mmadmindata.mmpositionalgor = obj.mmpositionalgor
        mmadmindata.mmpositioncost = obj.mmpositioncost
        mmadmindata.mmpositionlimitlong = obj.mmpositionlimitlong
        mmadmindata.mmpositionlimitshort = obj.mmpositionlimitshort
        mmadmindata.mmpricealgor = obj.mmpricealgor
        mmadmindata.mmpriceaskclose = obj.mmpriceaskclose
        mmadmindata.mmpriceaskcurrent = obj.mmpriceaskcurrent
        mmadmindata.mmpricebidclose = obj.mmpricebidclose
        mmadmindata.mmpricebidcurrent = obj.mmpricebidcurrent
        mmadmindata.mmpricegapovernight = obj.mmpricegapovernight
        mmadmindata.mmpricegapweekend = obj.mmpricegapweekend
        mmadmindata.mmrfqsize = obj.mmrfqsize
        mmadmindata.mmrfqsizealgor = obj.mmrfqsizealgor
        mmadmindata.mmrfqsizemax = obj.mmrfqsizemax
        mmadmindata.mmrfqsizemin = obj.mmrfqsizemin
        mmadmindata.mmspread = obj.mmspread
        mmadmindata.mmspreadalgor = obj.mmspreadalgor
        mmadmindata.mmspreadmax = obj.mmspreadmax
        mmadmindata.mmspreadmin = obj.mmspreadmin
        connections[mmid].write("{\"mmadmindata\":" + JSON.stringify(mmadmindata) + "}");
        var d = new Date()
        console.log("Market maker automation: " + d.toISOString() + " Manager updated from database.")
    })
}

/*
 * returns % of p1 in p2 regardless of sign
 * p3 is global PL % of limit
 * use largest 
 * */
function getpercentage(p1, p2) {
    return Math.abs(100 * Number(p1) / Number(p2))
}

/* mm math - calc new prices re position limits */
function getquotevposition(rfq) {
    var ask = 0
    var bid = 0
    var newpositionhi = Number(rfq.position) + Number(rfq.size)
    var newpositionlo = Number(rfq.position) - Number(rfq.size)

    if (Math.abs(newpositionhi) > Math.abs(newpositionlo)) {
        /*Calculate new price based on newpositionhi*/
        /* are we long or short at the moment? */
        if (Number(rfq.position) < 0) {
            /*short*/
            var mypercentageoflimit = getpercentage(newpositionhi, rfq.positionlimitshort)
            limitratio = mypercentageoflimit
            bid = Number(rfq.currentbid) + (Number(rfq.currentbid) * mypercentageoflimit * Math.pow(rfq.positionalgor, mypercentageoflimit) / Math.pow(rfq.positionalgor, 100) / 100)
            ask = bid + Number(rfq.spread)
        }
        else if (Number(rfq.position) > 0) {
            /*long*/
            var mypercentageoflimit = getpercentage(newpositionhi, rfq.positionlimitlong)
            limitratio = mypercentageoflimit
            bid = Number(rfq.currentbid) - (Number(rfq.currentbid) * mypercentageoflimit * Math.pow(rfq.positionalgor, mypercentageoflimit) / Math.pow(rfq.positionalgor, 100) / 100)
            ask = bid + Number(rfq.spread)
        }
        else {
            /*square*/
            console.log("Market maker automation: " + d.toISOString() + " Position square.")
        }
    }
    else if (Math.abs(newpositionhi) < Math.abs(newpositionlo)) {
        /*Calculate new price based on newpositionlo*/
        /* are we long or short at the moment? */
        if (Number(rfq.position) < 0) {
            /*short*/
            var mypercentageoflimit = getpercentage(newpositionlo, rfq.positionlimitshort)
            limitratio = mypercentageoflimit
            bid = Number(rfq.currentbid) + (Number(rfq.currentbid) * mypercentageoflimit * Math.pow(rfq.positionalgor, mypercentageoflimit) / Math.pow(rfq.positionalgor, 100) / 100)
            ask = bid + Number(rfq.spread)
        }
        else if (Number(rfq.position) > 0) {
            /*long*/
            var mypercentageoflimit = getpercentage(newpositionlo, rfq.positionlimitlong)
            limitratio = mypercentageoflimit
            bid = Number(rfq.currentbid) - (Number(rfq.currentbid) * mypercentageoflimit * Math.pow(rfq.positionalgor, mypercentageoflimit) / Math.pow(rfq.positionalgor, 100) / 100)
            ask = bid + Number(rfq.spread)
        }
        else {
            /*square*/
            console.log("Market maker automation: " + d.toISOString() + " Position square.")
        }
    }
    else {
        /*balanced, use current prices*/
        console.log("Market maker automation: " + d.toISOString() + " Position balanced.")
        ask = Number(rfq.currentask)
        bid = Number(rfq.currentbid)
    }
    return { ask: ask, bid: bid, limitratio: limitratio }
}

/* mm math - calc new spread re size limits */
function getquotevsize(rfq) {
    var mypercentageoflimit = getpercentage(Number(rfq.size), Number(rfq.sizemax))
    var limitratio = mypercentageoflimit
    var spread = Number(rfq.spread) + (Number(rfq.spread) * mypercentageoflimit * Math.pow(rfq.sizealgor, mypercentageoflimit) / Math.pow(rfq.sizealgor, 100) / 100)
    /* check spread limits */
    if (spread < Number(rfq.spreadmin)) {
        spread = Number(rfq.spreadmin)
    }
    else if (spread > Number(rfq.spreadmax)) {
        spread = Number(rfq.spreadmax)
    }
    /* apply new spread */
    var mid = (Number(rfq.currentask) + Number(rfq.currentbid)) / 2
    var bid = mid - spread / 2
    var ask = bid + spread
    return { ask: ask, bid: bid, limitratio: limitratio }
}

/* mm math - calc new prices re pl limits */
function getquotevpl(rfq) {

    var ask = 0
    var bid = 0
    var d = new Date()

    /* get pl */
    var totalpositioncost = Number(rfq.position) * Number(rfq.positioncost)
    var newcostlong = totalpositioncost + (Number(rfq.size) * Number(rfq.currentbid))
    var newcostshort = totalpositioncost - (Number(rfq.size) * Number(rfq.currentask))
    var newpositionlong = Number(rfq.position) + Number(rfq.size)
    var newpositionshort = Number(rfq.position) - Number(rfq.size)
    var averagepositionmax = (Number(rfq.positionlimitshort) + Number(rfq.positionlimitlong)) / 2
    var newvaluelong = newpositionlong * Number(rfq.currentask)
    var newvalueshort = newpositionshort * Number(rfq.currentbid)
    var newpllong = newvaluelong - newcostlong
    var newplshort = newvalueshort - newcostshort

    if (newpllong == newplshort) {
        /*square*/
        ask = rfq.currentask
        bid = rfq.currentbid
        console.log("Market maker automation: " + d.toISOString() + " square, new pl if long: " + newpllong + ", new pl if short: " + newplshort)
    }
    else if ((newpllong < 0 && newplshort < 0 && Math.abs(newplshort) < Math.abs(newpllong))
        || (newpllong > 0 && newplshort > 0 && Math.abs(newplshort) > Math.abs(newpllong))
        || (newpllong < 0 && newplshort > 0)) {
        /*discourage cwtt long/client short, encourage cwtt short/client long - raise prices*/
        var mypercentageoflimit = getpercentage(Number(rfq.position) + Number(rfq.size), averagepositionmax)
        limitratio = mypercentageoflimit
        bid = Number(rfq.currentbid) + (Number(rfq.currentbid) * mypercentageoflimit * Math.pow(rfq.plalgor, mypercentageoflimit) / Math.pow(rfq.plalgor, 100) / 100)
        ask = bid + Number(rfq.spread)
        console.log("Market maker automation: " + d.toISOString() + ", new pl if long: " + newpllong + ", new pl if short: " + newplshort + " discourage cwtt long/client short, encourage cwtt short/client long - raise prices.")
    }
    else if ((newpllong < 0 && newplshort < 0 && Math.abs(newpllong) < Math.abs(newplshort))
        || (newpllong > 0 && newplshort > 0 && Math.abs(newpllong) > Math.abs(newplshort))
        || (newpllong > 0 && newplshort < 0)) {
        /*discourage cwtt short/client long, encourage cwtt longt/client short - lower prices*/
        var mypercentageoflimit = getpercentage(Number(rfq.position) + Number(rfq.size), averagepositionmax)
        limitratio = mypercentageoflimit
        bid = Number(rfq.currentbid) - (Number(rfq.currentbid) * mypercentageoflimit * Math.pow(rfq.plalgor, mypercentageoflimit) / Math.pow(rfq.plalgor, 100) / 100)
        ask = bid + Number(rfq.spread)
        console.log("Market maker automation: " + d.toISOString() + ", new pl if long: " + newpllong + ", new pl if short: " + newplshort + " discourage cwtt short/client long, encourage cwtt long/client short - lower prices.")
    }
    else {
        console.log("Market maker automation: " + d.toISOString() + " getquotevpl error: " + JSON.stringify(rfq))
    }
    return { ask: ask, bid: bid, limitratio: limitratio }
}

/* mm math - calc new limitratios then use highest */
function getquotevcombined(rfq) {
    var ask = 0
    var bid = 0
    var limitratio = 0

    var resulta = {}
    var resultb = {}
    var resultc = {}

    /* pre-calculate all */
    resulta = getquotevpl(rfq)
    resultb = getquotevposition(rfq)
    resultc = getquotevsize(rfq)

    /* choose prices from largest limit ratio ??? */
    if (Number(resulta.limitratio) >= Number(resultb.limitratio) && Number(resulta.limitratio) >= Number(resultc.limitratio)) {
        ask = resulta.ask
        bid = resulta.bid
        limitratio = resulta.limitratio
    }
    else if (Number(resultb.limitratio) >= Number(resulta.limitratio) && Number(resultb.limitratio) >= Number(resultc.limitratio)) {
        ask = resultb.ask
        bid = resultb.bid
        limitratio = resultb.limitratio
    }
    else if (Number(resultc.limitratio) >= Number(resulta.limitratio) && Number(resultc.limitratio) >= Number(resultb.limitratio)) {
        ask = resultc.ask
        bid = resultc.bid
        limitratio = resultc.limitratio
    }
    return { ask: ask, bid: bid, limitratio: limitratio }
}

/*
 * make sure redis has keys we need
 * */
function checkrediskeys() {
    db.hmset("mm:1", "mmglobalpl", "1", "mmglobalplalgor", "2", "mmglobalpllimitloss", "3", "mmglobalpllimitprofit", "4", "mmglobalpositioncost", "5", "mmplalgor", "6", "mmpllimitloss", "7", "mmpllimitprofit", "8", "mmposition", "9", "mmpositionalgor", "10", "mmpositioncost", "11", "mmpositionlimitlong", "12", "mmpositionlimitshort", "13", "mmpricealgor", "14", "mmpriceask", "15", "mmpriceaskclose", "16", "mmpriceaskcurrent", "17", "mmpricebid", "18", "mmpricebidclose", "19", "mmpricebidcurrent", "20", "mmpricegapovernight", "21", "mmpricegapweekend", "22", "mmquotesequencenumber", "23", "mmrfqsize", "24", "mmrfqsizealgor", "25", "mmrfqsizemax", "26", "mmrfqsizemin", "27", "mmrfqsizemin", "28", "mmspread", "29", "mmspreadalgor", "30", "mmspreadmax", "31", "mmspreadmin", "32")
    db.hgetall("mm:1", function (err, obj) {
        console.dir(obj);
        })
}

