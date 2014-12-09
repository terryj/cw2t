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
    checkrediskeys()

    listen()
}

/* make sure redis has keys we need*/
function checkrediskeys() {

    db.set("mmGlobalPL", "123.45")
    db.set("mmGlobalPLLimitProfit", "1000")
    db.set("mmGlobalPLLimitLoss", "1000")
    db.set("mmGlobalPLAlgoR", "0.05")
    db.set("mmGlobalPositionCost", "100")

    db.set("mmPLLimitProfit", "100")
    db.set("mmPLLimitLoss", "100")
    db.set("mmPLAlgoR", "0.05")

    db.set("mmPosition", "20")
    db.set("mmPositionCost", "100")
    db.set("mmPositionLimitLong", "100")
    db.set("mmPositionLimitShort", "100")
    db.set("mmPositionAlgoR", "0.05")

    db.set("mmPriceAsk", "101")
    db.set("mmPriceBid", "99")
    db.set("mmPriceAskCurrent", "101")
    db.set("mmPriceBidCurrent", "99")
    db.set("mmPriceAskClose", "101")
    db.set("mmPriceBidClose", "99")
    db.set("mmPriceAlgoR", "0.05")

    db.set("mmProduct", "TEST.L")

    db.set("mmSpread", "2")
    db.set("mmSpreadAlgoR", "0.05")
    db.set("mmSpreadMax", "10")
    db.set("mmSpreadMin", "1")

    db.set("mmSize", "20")
    db.set("mmSizeMin", "100")
    db.set("mmSizeMax", "100")
    db.set("mmSizeAlgoR", "0.05")

    /*
 *     db.hmset("mmProducts:TEST.L", "symbol", "TEST.L", "position", "50", "positioncost", "100", "positionlimitlong", "100", "positionlimitshort", "100", "pricealgor", "0.05", "priceask", "99", "pricebid", "101", "spreadalgor", "0.05", "spreadmax", "10", "spreadmin", "1", "pllimitmin", "-2500", "pllimitmax", "10000")
 *         */
    
    /*
 *     db.hgetall("mmProducts:TEST.L", function (err, obj) {
 *             console.dir(obj)
 *                     })
 *                         */
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
                    var rfq = {}
                    rfq.id = obj.mmgetnewquote.rfqid
                    rfq.product = obj.mmgetnewquote.product
                    rfq.size = obj.mmgetnewquote.size
                    rfq.testtype = obj.mmgetnewquote.testtype
                    rfq.limitratio = "error"
                        db.get("mmGlobalPLAlgoR", function (err, obj) {
                            rfq.globalplalgor = 1 + Number(obj)
                            db.get("mmGlobalPLLimitProfit", function (err, obj) {
                                rfq.globalpllimitprofit = Number(obj)
                                db.get("mmGlobalPLLimitLoss", function (err, obj) {
                                    rfq.globalpllimitloss = Number(obj)
                                db.get("mmGlobalPositionCost", function (err, obj) {
                                  rfq.globalpositioncost = Number(obj)
                                  db.get("mmGlobalPL", function (err, obj) {
                                      rfq.mmglobalpl= Number(obj)
                        db.get("mmPriceAskCurrent", function (err, obj) {
                            rfq.currentask = Number(obj)
                            db.get("mmPriceBidCurrent", function (err, obj) {
                                rfq.currentbid = Number(obj)
                                db.get("mmPosition", function (err, obj) {
                                    rfq.position = Number(obj)
                                    db.get("mmPositionAlgoR", function (err, obj) {
                                        rfq.positionalgor = 1 + Number(obj)
                                        db.get("mmPositionLimitLong", function (err, obj) {
                                            rfq.positionlimitlong = Number(obj)
                                            db.get("mmPositionLimitShort", function (err, obj) {
                                                rfq.positionlimitshort = Number(obj)
                                                db.get("mmPositionCost", function (err, obj) {
                                                    rfq.positioncost = Number(obj)
                                                    db.get("mmSizeAlgoR", function (err, obj) {
                                                        rfq.sizealgor = 1 + Number(obj)
                                                        db.get("mmSizeMin", function (err, obj) {
                                                            rfq.sizemin = Number(obj)
                                                            db.get("mmSizeMax", function (err, obj) {
                                                                rfq.sizemax = Number(obj)
                                                                db.get("mmPLAlgoR", function (err, obj) {
                                                                    rfq.plalgor = 1 + Number(obj)
                                                                    db.get("mmPLLimitProfit", function (err, obj) {
                                                                        rfq.pllimitprofit = Number(obj)
                                                                        db.get("mmPLLimitLoss", function (err, obj) {
                                                                            rfq.pllimitloss = Number(obj)
                                                                            db.get("mmSpread", function (err, obj) {
                                                                                rfq.spread = Number(obj)
                                                                                db.get("mmSpreadMin", function (err, obj) {
                                                                                    rfq.spreadmin = Number(obj)
                                                                                    db.get("mmSpreadMax", function (err, obj) {
                                                                                        rfq.spreadmax = Number(obj)
                                                                                        db.get("mmSpreadAlgoR", function (err, obj) {
                                                                                            rfq.spreadalgor = 1 + Number(obj)

                                                                                /* what sort of test rfq? do correct test */
                                                                                var result={}
                                                                                switch (rfq.testtype) {
                                                                                    case "position":
                                                                                        result = getquotevposition(rfq)
                                                                                        break
                                                                                    case "Size":
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
                                                                                /* do return to rfq */
                                                                                var quote = {}
                                                                                quote.rfqid = rfq.id
                                                                                quote.size = rfq.size
                                                                                quote.ask = result.ask
                                                                                quote.bid = result.bid
                                                                                quote.limitratio = result.limitratio
                                                                                connections[mmid].write("{\"rfqreply\":" + JSON.stringify(quote) + "}")
                                                                                var dddd = new Date()
                                                                                console.log("Market maker automation: " + dddd.toISOString() + " RFQ quoted: " + JSON.stringify(quote))
                                                                                        })
                                                                                    })
                                                                                })
                                                                            })
                                                                        })
                                                                    })
                                                                })
                                                            })
                                                                    })
                                                                })
                                                            })
                                                        })
                                                    })
                                                })
                                            })
                                        })
                                    })
                                })
                            })
                        })
                    })
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

    /* global */
    var mmGlobalPL = mmparams.mmGlobalPL
    db.set("mmGlobalPL", mmGlobalPL)
    var mmGlobalPLLimitProfit = mmparams.mmGlobalPLLimitProfit
    db.set("mmGlobalPLLimitProfit", mmGlobalPLLimitProfit)
    var mmGlobalPLLimitLoss = mmparams.mmGlobalPLLimitLoss
    db.set("mmGlobalPLLimitLoss", mmGlobalPLLimitLoss)
    var mmGlobalPLAlgoR = mmparams.mmGlobalPLAlgoR
    db.set("mmGlobalPLAlgoR", mmGlobalPLAlgoR)
    var mmGlobalPositionCost = mmparams.mmGlobalPositionCost
    db.set("mmGlobalPositionCost", mmGlobalPositionCost)

    /* PL */
    var mmPLLimitProfit = mmparams.mmPLLimitProfit
    db.set("mmPLLimitProfit", mmPLLimitProfit)
    var mmPLLimitLoss = mmparams.mmPLLimitLoss
    db.set("mmPLLimitLoss", mmPLLimitLoss)
    var mmPLAlgoR = mmparams.mmPLAlgoR
    db.set("mmPLAlgoR", mmPLAlgoR)

    /* position */
    var mmPosition = mmparams.mmPosition
    db.set("mmPosition", mmPosition)
    /* positionCost */
    var mmPositionCost = mmparams.mmPositionCost
    db.set("mmPositionCost", mmPositionCost)
    /* PositionLimitLong */
    var mmPositionLimitLong = mmparams.mmPositionLimitLong
    db.set("mmPositionLimitLong", mmPositionLimitLong)
    /* PositionLimitShort */
    var mmPositionLimitShort = mmparams.mmPositionLimitShort
    db.set("mmPositionLimitShort", mmPositionLimitShort)
    /* PositionAlgoR */
    var mmPositionAlgoR= mmparams.mmPositionAlgoR
    db.set("mmPositionAlgoR", mmPositionAlgoR)

    /* Last in hours close Ask */
    var mmPriceAskClose = mmparams.mmPriceAskClose
    db.set("mmPriceAskClose", mmPriceAskClose)
    /* Last in hours close bid */
    var mmPriceBidClose = mmparams.mmPriceBidClose
    db.set("mmPriceBidClose", mmPriceBidClose)
    /* Estimated overnight mid-price gap */
    var mmPriceGapOvernight = mmparams.mmPriceGapOvernight
    db.set("mmPriceGapOvernight", mmPriceGapOvernight)
    /* Estimated Weekend mid-price gap */
    var mmPriceGapWeekend = mmparams.mmPriceGapWeekend
    db.set("mmPriceGapWeekend", mmPriceGapWeekend)
    /* Current Ask */
    var mmPriceAskCurrent = mmparams.mmPriceAskCurrent
    db.set("mmPriceAskCurrent", mmPriceAskCurrent)
    /* Current Bid */
    var mmPriceBidCurrent = mmparams.mmPriceBidCurrent
    db.set("mmPriceBidCurrent", mmPriceBidCurrent)
    /* PriceAlgoR */
    var mmPriceAlgoR = mmparams.mmPriceAlgoR
    db.set("mmPriceAlgoR", mmPriceAlgoR)

    /* spread */
    var mmSpread = mmparams.mmSpread
    db.set("mmSpread", mmSpread)
    /* SpreadMin */
    var mmSpreadMin= mmparams.mmSpreadMin
    db.set("mmSpreadMin", mmSpreadMin)
    /* SpreadMax */
    var mmSpreadMax = mmparams.mmSpreadMax
    db.set("mmSpreadMax", mmSpreadMax)
    /* SpreadAlgoR */
    var mmSpreadAlgoR = mmparams.mmSpreadAlgoR
    db.set("mmSpreadAlgoR", mmSpreadAlgoR)

    /* size */
    var mmSize = mmparams.mmSize
    db.set("mmSize", mmSize)
    /* size Min */
    var mmSizeMin = mmparams.mmSizeMin
    db.set("mmSizeMin", mmSizeMin)
    /* size Max */
    var mmSizeMax = mmparams.mmSizeMax
    db.set("mmSizeMax", mmSizeMax)
    /* size AlgoR */
    var mmSizeAlgoR = mmparams.mmSizeAlgoR
    db.set("mmSizeAlgoR", mmSizeAlgoR)

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
    db.get("mmGlobalPL", function (err, obj) {
        mmadmindata.mmGlobalPL = obj;
        db.get("mmGlobalPLLimitLoss", function (err, obj) {
            mmadmindata.mmglobalPLLimitLoss = obj;                              
            db.get("mmGlobalPLLimitProfit", function (err, obj) {
                mmadmindata.mmglobalPLLimitProfit = obj;  
                db.get("mmGlobalPLAlgoR", function (err, obj) {
                    mmadmindata.mmglobalPLAlgoR = obj;
                    db.get("mmGlobalPositionCost", function (err, obj) {
                        mmadmindata.mmglobalpositioncost = obj;
      db.get("mmPosition", function (err, obj) {
        mmadmindata.mmposition = obj;
        db.get("mmPositionCost", function (err, obj) {
            mmadmindata.mmpositioncost = obj;
            db.get("mmPositionLimitLong", function (err, obj) {
                mmadmindata.mmPositionLimitLong = obj;
                db.get("mmPositionLimitShort", function (err, obj) {
                    mmadmindata.mmPositionLimitShort = obj;
                    db.get("mmPositionAlgoR", function (err, obj) {
                        mmadmindata.mmPositionAlgoR = obj;
                        db.get("mmPriceAskClose", function (err, obj) {
                            mmadmindata.mmPriceAskClose = obj;
                            db.get("mmPriceBidClose", function (err, obj) {
                                mmadmindata.mmPriceBidClose = obj;
                                db.get("mmPriceGapOvernight", function (err, obj) {
                                    mmadmindata.mmPriceGapOvernight = obj;
                                    db.get("mmPriceGapWeekend", function (err, obj) {
                                        mmadmindata.mmPriceGapWeekend = obj;                              
                                        db.get("mmPriceAskCurrent", function (err, obj) {
                                            mmadmindata.mmPriceAskCurrent = obj;                              
                                            db.get("mmPriceBidCurrent", function (err, obj) {
                                                mmadmindata.mmPriceBidCurrent = obj;                              
                                                db.get("mmPriceAlgoR", function (err, obj) {
                                                    mmadmindata.mmPriceAlgoR = obj;                              
                                                    db.get("mmSpread", function (err, obj) {
                                                        mmadmindata.mmSpread = obj;                              
                                                        db.get("mmSpreadMin", function (err, obj) {
                                                            mmadmindata.mmSpreadMin = obj;                              
                                                            db.get("mmSpreadMax", function (err, obj) {
                                                                mmadmindata.mmSpreadMax = obj;                              
                                                                db.get("mmSpreadAlgoR", function (err, obj) {
                                                                    mmadmindata.mmSpreadAlgoR = obj;                              
                                                                    db.get("mmSize", function (err, obj) {
                                                                        mmadmindata.mmSize = obj;                              
                                                                        db.get("mmSizeMin", function (err, obj) {
                                                                            mmadmindata.mmSizeMin = obj;                              
                                                                            db.get("mmSizeMax", function (err, obj) {
                                                                                mmadmindata.mmSizeMax = obj;                              
                                                                                db.get("mmSizeAlgoR", function (err, obj) {
                                                                                    mmadmindata.mmSizeAlgoR = obj;
                                                                                    db.get("mmPLLimitLoss", function (err, obj) {
                                                                                        mmadmindata.mmPLLimitLoss = obj;                              
                                                                                        db.get("mmPLLimitProfit", function (err, obj) {
                                                                                            mmadmindata.mmPLLimitProfit = obj;  
                                                                                            db.get("mmPLAlgoR", function (err, obj) {
                                                                                                mmadmindata.mmPLAlgoR = obj;
                                                                                                connections[mmid].write("{\"mmadmindata\":" + JSON.stringify(mmadmindata) + "}");
                                                                                                var d = new Date()
                                                                                                console.log("Market maker automation: " + d.toISOString() + " Manager updated from database.")
                                                                                            })
                                                                                        })
                                                                                    })
                                                                                })
                                                                           })
                                                                                        })
                                                                                    })
                                                                                })
                                                                            })
                                                                        })
                                                                    })
                                                                })
                                                            })
                                                        })
                                                    })
                                                })
                                            })
                                        })
                                    })
                                })
                            })
                        })
                    })
                })
            })
        })
    })
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

    if (Math.abs(newpositionhi) > Math.abs(newpositionlo))
        {
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
    if (spread < Number(rfq.spreadmin)){
        spread = Number(rfq.spreadmin)
    } 
    else if (spread > Number(rfq.spreadmax)){
        spread = Number(rfq.spreadmax)
    } 
    /* apply new spread */
    var mid = (Number(rfq.currentask) + Number(rfq.currentbid))/2
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
    else
        {
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
    if (Number(resulta.limitratio) >= Number(resultb.limitratio) && Number(resulta.limitratio) >= Number(resultc.limitratio))
    {
        ask = resulta.ask
        bid = resulta.bid
        limitratio = resulta.limitratio
    }
    else if (Number(resultb.limitratio) >= Number(resulta.limitratio) && Number(resultb.limitratio) >= Number(resultc.limitratio))
    {
        ask = resultb.ask
        bid = resultb.bid
        limitratio = resultb.limitratio
    }
    else if (Number(resultc.limitratio) >= Number(resulta.limitratio) && Number(resultc.limitratio) >= Number(resultb.limitratio))
    {
        ask = resultc.ask
        bid = resultc.bid
        limitratio = resultc.limitratio
    }
    return { ask: ask, bid: bid, limitratio: limitratio }
}

