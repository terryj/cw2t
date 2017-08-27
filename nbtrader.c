/*********************
 * nbtrader.c
 * Client connection to NBTrader FIX Interface
 * Cantwaittotrade Limited
 * Terry Johnston
 * July 2017
 * Modifications
 * *********************/

#include "fix/fix_common.h"

#include "libtrading/compat.h"
#include "libtrading/array.h"
#include "libtrading/time.h"
#include "libtrading/die.h"

#include <netinet/tcp.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <inttypes.h>
#include <libgen.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <float.h>
#include <netdb.h>
#include <stdio.h>
#include <math.h>
#include <pthread.h>

#include <hiredis/async.h>

#include "redis.h"
#include "nbtrader.h"
#include "test.h"

static const char *program;
static sig_atomic_t stop;

static void signal_handler(int signum)
{
	if (signum == SIGINT)
            stop = 1;
}

char scriptquoteack_sha[64];
char scriptquote_sha[64];

static int fix_client_session(struct fix_session_cfg *cfg)
{
	struct fix_session *session = NULL;
	struct timespec cur, prev;
	struct fix_message *msg;
	int ret = -1;
	int diff;

        printf("fix_client_session\n");

	if (signal(SIGINT, signal_handler) == SIG_ERR) {
		fprintf(stderr, "Unable to register a signal handler\n");
		goto exit;
	}

	session	= fix_session_new(cfg);
	if (!session) {
		fprintf(stderr, "FIX session cannot be created\n");
		goto exit;
	}

	ret = fix_session_logon(session);
	if (ret == -1) {
		fprintf(stderr, "Client Logon FAILED\n");
		goto exit;
	}

	printf("Client Logon OK\n");

	clock_gettime(CLOCK_MONOTONIC, &prev);

	while (!stop && session->active) {
		clock_gettime(CLOCK_MONOTONIC, &cur);
		diff = cur.tv_sec - prev.tv_sec;

		if (diff > 0.1 * session->heartbtint) {
			prev = cur;

			if (!fix_session_keepalive(session, &cur)) {
				stop = 1;
				break;
			}
		}

		if (fix_session_time_update(session)) {
			stop = 1;
			break;
		}

                check_for_data(session);

 		if (fix_session_recv(session, &msg, FIX_RECV_FLAG_MSG_DONTWAIT) > 0) {
			fprintmsg(stdout, msg);

			if (fix_session_admin(session, msg))
				continue;

                        switch (msg->type) {
                        case FIX_MSG_TYPE_EXECUTION_REPORT:
                                fprintf(stdout, "Execution report\n");
                                fix_execution_report(session, msg);
                                break;
                        case FIX_MSG_QUOTE:
                                fprintf(stdout, "Quote\n");
                                fix_quote(session, msg);
                                break;
                        case FIX_MSG_QUOTE_ACKNOWLEDGEMENT:
                                fprintf(stdout, "Quote Acknowledgement\n");
                                fix_quote_ack(session, msg);
                                break;
                        case FIX_MSG_ORDER_CANCEL_REJECT:
                                fprintf(stdout, "Cancel reject\n");
                                break;
                        case FIX_MSG_TYPE_LOGOUT:
                                fprintf(stdout, "Logout\n");
                                stop = 1;
                                break;
                        case FIX_MSG_TYPE_REJECT:
                                fprintf(stdout, "Message reject\n");
                                fix_message_reject(msg);
                                break;
                        default:
                                stop = 1;
                                break;
                        }
		}
	}

	if (session->active) {
		ret = fix_session_logout(session, NULL);
		if (ret) {
			fprintf(stderr, "Client Logout FAILED\n");
			goto exit;
		}
	}

	fprintf(stdout, "Client Logout OK\n");

exit:
	fix_session_free(session);

	return ret;
}

static int fix_message_reject(struct fix_message *msg) {
    struct fix_field *field;
    char refmsgtype[10] = "";
    char text[128] = "";

    field = fix_get_field(msg, RefTagID);
    if (field) {
      printf("RefTagID:%lu\n", field->int_value);
    }
 
    field = fix_get_field(msg, RefMsgType);
    if (field) {
      fix_get_string(field, refmsgtype, sizeof(refmsgtype));
      printf("RefMsgType:%s\n", refmsgtype);
    }
   
    field = fix_get_field(msg, SessionRejectReason);
    if (field) {
      printf("SessionRejectReason:%lu\n", field->int_value);
    }
 
    field = fix_get_field(msg, RefSeqNum);
    if (field) {
      printf("RefSeqNum:%lu\n", field->int_value);
    }
 
    field = fix_get_field(msg, Text);
    if (field) {
      fix_get_string(field, text, sizeof(text));
      printf("Text:%s\n", text);
    }

    return 0;
}

static int fix_quote_ack(struct fix_session *session, struct fix_message *msg) {
    struct fix_field *field;
    struct fix_quoteack quoteack;

    quoteack.quotestatusid = fix_get_int(msg, QuoteAckStatus, 5);
    quoteack.quoterejectreasonid = fix_get_int(msg, QuoteRejectReason, 0);

    field = fix_get_field(msg, QuoteReqID);
    if (!field) {
      fprintf(stdout, "QuoteReqID not found\n");
      goto fail;
    }
    fix_get_string(field, quoteack.quotereqid, sizeof(quoteack.quotereqid));

    field = fix_get_field(msg, Text);
    if (field) {
      fix_get_string(field, quoteack.text, sizeof(quoteack.text));
      printf("Text:%s\n", quoteack.text);
    } else
      strcpy(quoteack.text, "");

    field = fix_get_field(msg, SendingTime);
    if (field) {
      fix_get_string(field, quoteack.markettimestamp, sizeof(quoteack.markettimestamp));
      printf("SendingTime:%s\n", quoteack.markettimestamp);
    } else
      strcpy(quoteack.markettimestamp, ""); 

    // publish it
    send_quote_ack(session, &quoteack);

    return 0;

fail:
    return -1;
}

static int send_quote_ack(struct fix_session *session, struct fix_quoteack *quoteack) {
  char jsonquoteack[256];

  sprintf(jsonquoteack, "%s%s%s%s%s%d%s%d%s%s%s%s%lu%s%s%s%s%s%s%s", "{\"quoteack\":{"
    , ",\"quoterequestid\":\"", quoteack->quotereqid, "\""
    , ",\"quotestatusid\":", quoteack->quotestatusid
    , ",\"quoterejectreasonid\":", quoteack->quoterejectreasonid
    , ",\"text\":\"", quoteack->text, "\""
    , ",\"fixseqnumid\":", session->in_msg_seq_num
    , ",\"timestamp\":\"", session->str_now, "\""
    , ",\"markettimestamp\":\"", quoteack->markettimestamp, "\""
    , "}}");

  printf("%s\n", jsonquoteack);

  return 0;
}

/*static int store_quote_ack(struct fix_session *session, struct fix_quoteack *quoteack) {
  printf("store_quote_ack\n");
  redisReply *reply;
  // TODO...
  char fixseqnumid[8] = "1";

  // we are using all string parameters as per hiredis api
  reply = redisCommand(c, "evalsha %s 0 %s %s %s %s %s %s %s %s", scriptquoteack_sha, quoteack->brokerid, quoteack->quoterequestid, quoteack->quotestatusid, quoteack->quoterejectreasonid, quoteack->text, fixseqnumid, session->str_now, quoteack->markettimestamp);
  if (reply->type == REDIS_REPLY_ERROR) {
    printf("%s\n", reply->str);
    return 1;
  }

  freeReplyObject(reply);

  return 0;
}*/

static int fix_quote(struct fix_session *session, struct fix_message *msg) {
    struct fix_field *field;
    struct fix_quote quote;
    struct tm tm_validuntil;
    struct tm tm_now;

    memset(&tm_validuntil, 0, sizeof(tm_validuntil));
    memset(&tm_now, 0, sizeof(tm_now));

    field = fix_get_field(msg, Qbroker);
    if (field) {
      fix_get_string(field, quote.quoterid, sizeof(quote.quoterid));
      printf("Qbroker:%s\n", quote.quoterid);
    } else {
      printf("Qbroker not present\n");
      goto fail;
    }

    field = fix_get_field(msg, QuoteReqID);
    if (field) {
      fix_get_string(field, quote.quotereqid, sizeof(quote.quotereqid));
    } else {
      printf("QuoteReqID not present\n");
      goto fail;
    }

    field = fix_get_field(msg, QuoteID);
    if (field) {
      fix_get_string(field, quote.externalquoteid, sizeof(quote.externalquoteid));
      printf("QuoteID:%s\n", quote.externalquoteid);
    } else
      strcpy(quote.externalquoteid, "");

    field = fix_get_field(msg, SendingTime);
    if (field) {
      fix_get_string(field, quote.markettimestamp, sizeof(quote.markettimestamp));
      if (strptime(quote.markettimestamp, "%Y%m%d-%H:%M:%S", &tm_now) == NULL) {
        printf("Unable to parse SendingTime:%s\n", quote.markettimestamp);
        strcpy(quote.markettimestamp, "");
      }
      printf("SendingTime:%s\n", quote.markettimestamp);
    } else
      strcpy(quote.markettimestamp, "");

/* 
    field = fix_get_field(msg, Symbol);
    if (field) {
      fix_get_string(field, quote.symbolid, sizeof(quote.symbolid));
      printf("Symbol:%s\n", quote.symbolid);
    } else
      strcpy(quote.symbolid, "");
*/
    quote.offerpx = fix_get_float(msg, OfferPx, 0);
    quote.offersize = fix_get_float(msg, OfferSize, 0);
    quote.offerspotrate = fix_get_float(msg, OfferSpotRate, 0);
    quote.bidpx = fix_get_float(msg, BidPx, 0);
    quote.bidsize = fix_get_float(msg, BidSize, 0);
    quote.bidspotrate = fix_get_float(msg, BidSpotRate, 0);
    quote.bidquotedepth = fix_get_float(msg, BidQuoteDepth, 0);
    quote.offerquotedepth = fix_get_float(msg, OfferQuoteDepth, 0);
    quote.cashorderqty= fix_get_float(msg, CashOrderQty, 0);
    quote.settledays = fix_get_int(msg, SettleDays, 0);

    field = fix_get_field(msg, FutSettDate);
    if (field) {
      fix_get_string(field, quote.futsettdate, sizeof(quote.futsettdate));
      printf("FutSettDate:%s\n", quote.futsettdate);
    } else
      strcpy(quote.futsettdate, "");

    field = fix_get_field(msg, Currency);
    if (field) {
      fix_get_string(field, quote.currencyid, sizeof(quote.currencyid));
      printf("Currency:%s\n", quote.currencyid);
    } else
      strcpy(quote.currencyid, "");

    field = fix_get_field(msg, SettlCurrency);
    if (field) {
      fix_get_string(field, quote.settlcurrencyid, sizeof(quote.settlcurrencyid));
      printf("SettlCurrency:%s\n", quote.settlcurrencyid);
    } else
      strcpy(quote.settlcurrencyid, "");

    field = fix_get_field(msg, ValidUntilTime);
    if (field) {
      fix_get_string(field, quote.validuntiltime, sizeof(quote.validuntiltime));
      printf("ValidUntilTime:%s\n", quote.validuntiltime);
      if (strptime(quote.validuntiltime, "%Y%m%d-%H:%M:%S", &tm_validuntil) == NULL) {
        printf("Unable to parse ValidUntilTime:%s\n", quote.validuntiltime);
        goto fail;
      }

      // calculate how many seconds the quote is valid for
      time_t validuntil = mktime(&tm_validuntil);
      time_t now = mktime(&tm_now);
      quote.noseconds = (int) difftime(validuntil, now);
    } else {
      printf("ValidUntilTime not present\n");
      goto fail;
    }
 
    field = fix_get_field(msg, SecurityID);
    if (field) {
      fix_get_string(field, quote.isin, sizeof(quote.isin));
      printf("isin:%s\n", quote.isin);
    } else {
      printf("Isin not present\n");
      goto fail; 
    }

    quote.quotertype = 1; // counterparty

    send_quote(session, &quote);

    return 0;

fail:
    return 1;
}

/*static int store_quote(struct fix_session *session, struct fix_quote *quote) {
  printf("store_quote\n");
  redisReply *reply;
  // TODO...
  char fixseqnumid[8] = "1";
  char timestampms[16] = "123456";

  // we are using all string parameters as per hiredis api
  reply = redisCommand(c, "evalsha %s 0 %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s", scriptquote_sha, quote->quoterequestid, quote->symbolid, quote->bidpx, quote->offerpx, quote->bidsize, quote->offersize, quote->validuntiltime, session->str_now, quote->currencyid, quote->settlcurrencyid, quote->quoterid, quote->quotertype, quote->futsettdate, quote->bidquotedepth, quote->offerquotedepth, quote->externalquoteid, quote->cashorderqty, quote->settledays, quote->noseconds, quote->brokerid, quote->settlmnttypid, quote->bidspotrate, quote->offerspotrate, timestampms, fixseqnumid, quote->markettimestamp);
  if (reply->type == REDIS_REPLY_ERROR) {
    printf("%s\n", reply->str);
    return 1;
  }

  freeReplyObject(reply);

  return 0;
}*/

static int send_quote(struct fix_session *session, struct fix_quote *quote) {
  char jsonquote[512];

  if (quote->bidpx != 0.0) {
    sprintf(jsonquote, "%s%s%s%s%s%f%s%f%s%s%s%s%s%s%s%lu%s%s%s%s%s%s%s%d%s%s%s%s%d%s%f%s%d%s%s%s%s%f%s%d%s%s%s%s%s%s%s%s%s%s", "{\"quote\":{"
      , ",\"quoterequestid\":\"", quote->quotereqid, "\""
      , ",\"bidpx\":", quote->bidpx
      , ",\"bidsize\":", quote->bidsize
      , ",\"validuntiltime\":\"", quote->validuntiltime, "\""
      , ",\"currencyid\":\"", quote->currencyid, "\""
      , ",\"fixseqnumid\":", session->in_msg_seq_num
      , ",\"timestamp\":\"", session->str_now, "\""
      , ",\"markettimestamp\":\"", quote->markettimestamp, "\""
      , ",\"quotertype\":", quote->quotertype
      , ",\"isin\":\"", quote->isin, "\""
      , ",\"bidquotedepth\":", quote->bidquotedepth
      , ",\"bidspotrate\":", quote->bidspotrate
      , ",\"noseconds\":", quote->noseconds
      , ",\"quoterid\":\"", quote->quoterid, "\""
      , ",\"cashorderqty\":", quote->cashorderqty
      , ",\"settledays\":", quote->settledays  
      , ",\"futsettdate\":\"", quote->futsettdate, "\""
      , ",\"settlcurrencyid\":\"", quote->settlcurrencyid, "\""
      , ",\"externalquoteid\":\"", quote->externalquoteid, "\""
      , "}}");
  } else {
    sprintf(jsonquote, "%s%s%s%s%s%f%s%f%s%s%s%s%s%s%s%lu%s%s%s%s%s%s%s%d%s%s%s%s%d%s%f%s%d%s%s%s%s%f%s%d%s%s%s%s%s%s%s%s%s%s", "{\"quote\":{"
      , ",\"quoterequestid\":\"", quote->quotereqid, "\""
      , ",\"offerpx\":", quote->offerpx
      , ",\"offersize\":", quote->offersize
      , ",\"validuntiltime\":\"", quote->validuntiltime, "\""
      , ",\"currencyid\":\"", quote->currencyid, "\""
      , ",\"fixseqnumid\":", session->in_msg_seq_num
      , ",\"timestamp\":\"", session->str_now, "\""
      , ",\"markettimestamp\":\"", quote->markettimestamp, "\""
      , ",\"quotertype\":", quote->quotertype
      , ",\"isin\":\"", quote->isin, "\""
      , ",\"offerquotedepth\":", quote->offerquotedepth
      , ",\"offerspotrate\":", quote->offerspotrate
      , ",\"noseconds\":", quote->noseconds
      , ",\"quoterid\":\"", quote->quoterid, "\""
      , ",\"cashorderqty\":", quote->cashorderqty
      , ",\"settledays\":", quote->settledays  
      , ",\"futsettdate\":\"", quote->futsettdate, "\""
      , ",\"settlcurrencyid\":\"", quote->settlcurrencyid, "\""
      , ",\"externalquoteid\":\"", quote->externalquoteid, "\""
      , "}}");
  }

  printf("%s\n", jsonquote);

  return 0;
}

static void check_for_data(struct fix_session *session)
{
  int i;
  struct fix_field *fields = NULL;
  unsigned long nr;

  /* anything to do with the shared memory needs to sit inside the lock */
  pthread_mutex_lock(&lock);

  /* handle any quoterequests  */
  for (i = 0; i < numquoterequests; i++) {
      fields = calloc(FIX_MAX_FIELD_NUMBER, sizeof(struct fix_field));
      if (!fields) {
        fprintf(stderr, "Cannot allocate memory\n");
        goto exit;
      }

      nr = fix_quoterequest_fields(session, fields, &quoterequests[i]);

      fix_quote_request(session, fields, nr);

      free(fields);
  }
  
  /* handle any orders */
  for (i = 0; i < numorders; i++) {
      fields = calloc(FIX_MAX_FIELD_NUMBER, sizeof(struct fix_field));
      if (!fields) {
        fprintf(stderr, "Cannot allocate memory\n");
        goto exit;
      }
      
      nr = fix_new_order_single_fields(session, fields, &orders[i]);
      
      fix_session_new_order_single(session, fields, nr);
      
      free(fields);
  }

exit:
  numquoterequests = 0;
  numorders = 0;
  pthread_mutex_unlock(&lock);
}

static unsigned long fix_quoterequest_fields(struct fix_session *session, struct fix_field *fields, struct fix_quoterequest *quoterequest)
{
    unsigned long nr = 0;

    fields[nr++] = FIX_STRING_FIELD(OnBehalfOfCompID, "TGRANT");
    fields[nr++] = FIX_STRING_FIELD(TransactTime, session->str_now);
    fields[nr++] = FIX_STRING_FIELD(QuoteReqID, quoterequest->quoterequestid);
    fields[nr++] = FIX_STRING_FIELD(SecurityID, quoterequest->isin);
    fields[nr++] = FIX_FLOAT_FIELD(OrderQty, quoterequest->quantity);

    if (quoterequest->symbolid[0] != '\0') 
      fields[nr++] = FIX_STRING_FIELD(Symbol, quoterequest->symbolid);

    if (quoterequest->side != '\0')
      fields[nr++] = FIX_CHAR_FIELD(Side, quoterequest->side);

    if (quoterequest->currencyid[0] != '\0')
      fields[nr++] = FIX_STRING_FIELD(Currency, quoterequest->currencyid);
 
    fields[nr++] = FIX_STRING_FIELD(IDSource, "4");
    fields[nr++] = FIX_INT_FIELD(NoRelatedSym, 1);

    return nr;
}

static int fix_quote_request(struct fix_session *session, struct fix_field *fields, long nr_fields)
{
        struct fix_message quote_request_msg;

        quote_request_msg       = (struct fix_message) {
                .type           = FIX_MSG_QUOTE_REQUEST,
                .nr_fields      = nr_fields,
                .fields         = fields,
        };

        return fix_session_send(session, &quote_request_msg, 0);
}

static unsigned long fix_new_order_single_fields(struct fix_session *session, struct fix_field *fields, struct fix_order *order)
{
	unsigned long nr = 0;

        fields[nr++] = FIX_STRING_FIELD(OnBehalfOfCompID, "TGRANT");
	fields[nr++] = FIX_STRING_FIELD(TransactTime, session->str_now);
	fields[nr++] = FIX_STRING_FIELD(ClOrdID, order->orderid);
        fields[nr++] = FIX_STRING_FIELD(SecurityID, order->isin);
	fields[nr++] = FIX_STRING_FIELD(OrdType, order->ordertypeid);
	fields[nr++] = FIX_CHAR_FIELD(Side, order->side);

        if (order->quantity != 0)
	  fields[nr++] = FIX_FLOAT_FIELD(OrderQty, order->quantity);

        if (order->cashorderquantity != 0)
          fields[nr++] = FIX_FLOAT_FIELD(CashOrderQty, order->cashorderquantity);

        if (order->price != 0)
          fields[nr++] = FIX_FLOAT_FIELD(Price, order->price);

        if (order->symbolid[0] != '\0')
	  fields[nr++] = FIX_STRING_FIELD(Symbol, order->symbolid);

        if (order->settlcurrencyid[0] != '\0')
	  fields[nr++] = FIX_STRING_FIELD(SettlCurrency, order->settlcurrencyid);

        if (strcmp(order->ordertypeid, "D") == 0) {
          fields[nr++] = FIX_STRING_FIELD(DeliverToCompID, order->quoterid);
          fields[nr++] = FIX_STRING_FIELD(QuoteID, order->externalquoteid);
        } else if (strcmp(order->ordertypeid, "1") == 0)
          fields[nr++] = FIX_STRING_FIELD(DeliverToCompID, "BEST");
        else if (order->delivertocompid[0] != '\0')
          fields[nr++] = FIX_STRING_FIELD(DeliverToCompID, order->delivertocompid);

        fields[nr++] = FIX_STRING_FIELD(IDSource, "4");
	fields[nr++] = FIX_CHAR_FIELD(TimeInForce, '4');

	return nr;
}

static int fix_execution_report(struct fix_session *session, struct fix_message *msg)
{
        struct fix_field *field;
        struct fix_executionreport executionreport;

        executionreport.orderstatusid = fix_get_char(msg, OrdStatus, '\0');
        executionreport.exectype = fix_get_char(msg, ExecType, '\0');

        field = fix_get_field(msg, OrderID);
        if (field) {
          fix_get_string(field, executionreport.orderid, sizeof(executionreport.orderid));
        } else {
          fprintf(stdout, "OrderID not present in execution report\n");
          goto fail;
        }

        field = fix_get_field(msg, ClOrdID);
        if (field) {
          fix_get_string(field, executionreport.clordid, sizeof(executionreport.clordid));
        } else {
          fprintf(stdout, "ClOrdID not present in execution report\n");
          goto fail;
        }

       executionreport.orderqty = fix_get_float(msg, OrderQty, 0);
       executionreport.cumqty = fix_get_float(msg, CumQty, 0);
       executionreport.side = fix_get_char(msg, Side, '0');
       executionreport.lastshares = fix_get_float(msg, LastShares, 0);
       executionreport.lastpx = fix_get_float(msg, LastPx, 0);

       field = fix_get_field(msg, FutSettDate);
        if (field) {
          fix_get_string(field, executionreport.futsettdate, sizeof(executionreport.futsettdate));
        } else
          strcpy(executionreport.futsettdate, "");
 
        field = fix_get_field(msg, SecurityID);
        if (field) {
          fix_get_string(field, executionreport.securityid, sizeof(executionreport.securityid));
        } else
          strcpy(executionreport.securityid, "");

        field = fix_get_field(msg, TransactTime);
        if (field) {
          fix_get_string(field, executionreport.markettimestamp, sizeof(executionreport.markettimestamp));
          printf("TransactTime:%s\n", executionreport.markettimestamp);
        } else {
          field = fix_get_field(msg, SendingTime);
          if (field) {
            fix_get_string(field, executionreport.markettimestamp, sizeof(executionreport.markettimestamp));
            printf("SendingTime:%s\n", executionreport.markettimestamp);
          } else {
            strcpy(executionreport.markettimestamp, ""); 
          }
        }

        field = fix_get_field(msg, SettlCurrency);
        if (field) {
          fix_get_string(field, executionreport.settlcurrencyid, sizeof(executionreport.settlcurrencyid));
        } else
          strcpy(executionreport.settlcurrencyid, "");

        field = fix_get_field(msg, ExecID);
        if (field) {
          fix_get_string(field, executionreport.execid, sizeof(executionreport.execid));
        } else
          strcpy(executionreport.execid, "");

        send_execution_report(session, &executionreport);

        return 0;

fail:
        return -1;
}

static int send_execution_report(struct fix_session *session, struct fix_executionreport *executionreport) {
  char jsonexecutionreport[512];

  sprintf(jsonexecutionreport, "%s%s%c%s%s%c%s%s%s%s%s%s%s%s%f%s%f%s%s%s%s%s%s%s%f%s%f%s%s%s%s%s%s%s%c%s%s%s%s%s%lu%s", "{\"executionreport\":{"
    , "\"orderstatusid\":\"", executionreport->orderstatusid, "\""
    , ",\"exectype\":\"", executionreport->exectype, "\""
    , ",\"orderid\":\"", executionreport->orderid, "\""
    , ",\"clordid\":\"", executionreport->clordid, "\""
    , ",\"orderqty\":", executionreport->orderqty
    , ",\"cumqty\":", executionreport->cumqty
    , ",\"futsettdate\":\"", executionreport->futsettdate, "\""
    , ",\"securityid\":\"", executionreport->securityid, "\""
    , ",\"lastshares\":", executionreport->lastshares
    , ",\"lastpx\":", executionreport->lastpx
    , ",\"markettimestamp\":\"", executionreport->markettimestamp, "\""
    , ",\"settlcurrencyid\":\"", executionreport->settlcurrencyid, "\""
    , ",\"side\":\"", executionreport->side, "\""
    , ",\"execid\":\"", executionreport->execid, "\""
    , ",\"fixseqnumid\":", session->in_msg_seq_num
    , "}}");
  printf("%s\n", jsonexecutionreport);
  return 0;
}

static void usage(void)
{
	printf("\n usage: %s [-m mode] [-d dialect] [-f filename] [-n orders] [-s sender-comp-id] [-t target-comp-id] [-r password] [-w warmup orders] -h hostname -p port\n\n", program);

	exit(EXIT_FAILURE);
}

static int socket_setopt(int sockfd, int level, int optname, int optval)
{
	return setsockopt(sockfd, level, optname, (void *) &optval, sizeof(optval));
}

static enum fix_version strversion(const char *dialect)
{
	if (!strcmp(dialect, "fix42"))
		return FIX_4_2;
	else if (!strcmp(dialect, "fix43"))
		return FIX_4_3;
	else if (!strcmp(dialect, "fix44"))
		return FIX_4_4;

	return FIX_4_4;
}

/*void signal_callback_handler(int signum)
{
   // cleanup and close up stuff here
   redis_disconnect();

   fix_session_free(session);

   shutdown(cfg.sockfd, SHUT_RDWR);

   if (close(cfg.sockfd) < 0)
     die("close");
 
   // terminate
   exit(signum);
}*/

/*static int test_store_quote_ack() {
  printf("test_store_quote_ack\n");
  redisReply *reply;
  
  reply = redisCommand(c, "evalsha %s 0 %s %s %s %s %s %s %s", scriptquoteack_sha, "1", "1", "1", "1", "text", "1", "timestamp");
  if (reply->type == REDIS_REPLY_ERROR) {
    printf("%s\n", reply->str);
    return 1;
  }

  freeReplyObject(reply);

  return 0;
}*/

/*static int test_store_quote() {
  printf("test_store_quote\n");
  redisReply *reply;

  reply = redisCommand(c, "evalsha %s 0 %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s", scriptquote_sha, "1", "BARC.L", "1.23", "0.0", "1000", "0", "vuntiltime", "ttime", "GBP", "GBP", "quoterid", "1", "fdate", "1", "0", "equoteid", "0.0", "2", "20", "1", "0", "1.23", "1.25", "123456", "1", "mtimestamp");
  if (reply->type == REDIS_REPLY_ERROR) {
    printf("%s\n", reply->str);
    return 1;
  }

  freeReplyObject(reply);

  return 0;
}*/

/* load lua scripts & store SHA1 digest of each script */
/*int load_scripts() {
  FILE *ptr_file;
  redisReply *reply;
  char buf[512];
  char scriptquoteack[4096] = "";
  char scriptquote[4096] = "";

  // quote ack
  ptr_file = fopen("scriptquoteack.lua", "r");
  if (!ptr_file)
    return 1;

  while (fgets(buf, sizeof(buf), ptr_file) != NULL)
    strcat(scriptquoteack, buf);

  fclose(ptr_file);

  reply = redisCommand(c, "script load %s", scriptquoteack);
  if (reply->type == REDIS_REPLY_ERROR) {
    printf("%s\n", reply->str);
    return 1;
  } else if (reply->type == REDIS_REPLY_STRING) {
    strcpy(scriptquoteack_sha, reply->str);
  }

  freeReplyObject(reply);

  // quote
  ptr_file = fopen("scriptquote.lua", "r");
  if (!ptr_file)
    return 1;

  while (fgets(buf, sizeof(buf), ptr_file) != NULL)
    strcat(scriptquote, buf);

  fclose(ptr_file);

  reply = redisCommand(c, "script load %s", scriptquote);
  if (reply->type == REDIS_REPLY_ERROR) {
    printf("%s\n", reply->str);
    return 1;
  } else if (reply->type == REDIS_REPLY_STRING) {
    strcpy(scriptquote_sha, reply->str);
  }

  freeReplyObject(reply);

  return 0;
}*/

int main(int argc, char *argv[])
{
	enum fix_version version = FIX_4_2;
	const char *target_comp_id = NULL;
	const char *sender_comp_id = NULL;
	//struct fix_client_arg arg = {0};
	const char *password = NULL;
	struct fix_session_cfg cfg;
	const char *host = NULL;
	struct sockaddr_in sa;
	int saved_errno = 0;
	struct hostent *he;
	int port = 0;
	int ret = 0;
	char **ap;
	int opt;

       /* handle ctrl-c signal */
  //      signal(SIGINT, signal_callback_handler);

	program = basename(argv[0]);

	while ((opt = getopt(argc, argv, "f:h:p:d:s:t:m:n:o:r:w:")) != -1) {
		switch (opt) {
		case 'd':
			version = strversion(optarg);
			break;
		/*case 'n':
			arg.orders = atoi(optarg);
			break;*/
		case 's':
			sender_comp_id = optarg;
			break;
		case 't':
			target_comp_id = optarg;
			break;
		case 'r':
			password = optarg;
			break;
		/*case 'm':
			mode = strclientmode(optarg);
			break;*/
		case 'p':
			port = atoi(optarg);
			break;
		/*case 'f':
			arg.script = optarg;
			break;
		case 'o':
			arg.output = optarg;
			break;*/
		case 'h':
			host = optarg;
			break;
		/*case 'w':
			arg.warmup_orders = atoi(optarg);
			break;*/
		default: /* '?' */
			usage();
		}
	}

	if (!port || !host)
		usage();

	fix_session_cfg_init(&cfg);

	cfg.dialect	= &fix_dialects[version];

	if (!password) {
		memset(cfg.password, 0, ARRAY_SIZE(cfg.password));
	} else {
		strncpy(cfg.password, password, ARRAY_SIZE(cfg.password));
	}

	if (!sender_comp_id) {
		strncpy(cfg.sender_comp_id, "BUYSIDE", ARRAY_SIZE(cfg.sender_comp_id));
	} else {
		strncpy(cfg.sender_comp_id, sender_comp_id, ARRAY_SIZE(cfg.sender_comp_id));
	}

	if (!target_comp_id) {
		strncpy(cfg.target_comp_id, "SELLSIDE", ARRAY_SIZE(cfg.target_comp_id));
	} else {
		strncpy(cfg.target_comp_id, target_comp_id, ARRAY_SIZE(cfg.target_comp_id));
	}

        he = gethostbyname(host);
	if (!he)
          error("Unable to look up %s (%s)", host, hstrerror(h_errno));

        printf("trying to connect to redis\n");
        if (!redis_connect("127.0.0.1", 6379)) {
          error("Unable to connect to Redis");
          return 0;
        }
        printf("Redis started\n");

        //load_scripts();

        //redis_test();

        if (!redis_async_connect("127.0.0.1", 6379)) {
          error("Unable to start async connection to Redis");
          return 0;;
        }

        printf("Redis pubsub started\n");

        // get a socket connection
	for (ap = he->h_addr_list; *ap; ap++) {
          cfg.sockfd = socket(he->h_addrtype, SOCK_STREAM, IPPROTO_TCP);
	  if (cfg.sockfd < 0) {
	    saved_errno = errno;
	    continue;
	  }

	  sa = (struct sockaddr_in) {
	    .sin_family	= he->h_addrtype,
	    .sin_port = htons(port),
	  };
	  memcpy(&sa.sin_addr, *ap, he->h_length);

          printf("trying to connect to %s\n", host);

	  if (connect(cfg.sockfd, (const struct sockaddr *)&sa, sizeof(struct sockaddr_in)) < 0) {
	    saved_errno = errno;
	    close(cfg.sockfd);
	    cfg.sockfd = -1;
	    continue;
	  }
	  break;
	}

	if (cfg.sockfd < 0)
	  error("Unable to connect to a socket (%s)", strerror(saved_errno));

	if (socket_setopt(cfg.sockfd, IPPROTO_TCP, TCP_NODELAY, 1) < 0)
	  die("cannot set socket option TCP_NODELAY");

        printf("Connected to %s\n", host);

	cfg.heartbtint = 30;

        printf("about to try a session\n");
        printf("sender comp id: %s\n", cfg.sender_comp_id);
         printf("target comp id: %s\n", cfg.target_comp_id);
         printf("heartbtint: %i\n", cfg.heartbtint);
           printf("in msg seq: %lu\n", cfg.in_msg_seq_num);
         printf("out msg seq: %lu\n", cfg.out_msg_seq_num);

        /* start a FIX session, will run until fails or stopped */
        ret = fix_client_session(&cfg);
printf("out of client session\n");
        //redis_disconnect(c);
        //redis_async_disconnect();
        //thread_tidy();

	shutdown(cfg.sockfd, SHUT_RDWR);

	if (close(cfg.sockfd) < 0)
	  die("close");

        redis_disconnect(c);
        redis_async_disconnect();

        thread_tidy();

	return ret;
}
