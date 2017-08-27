/*********************
 * redis.c
 * Redis connections
 * Cantwaittotrade Limited
 * Terry Johnston
 * July 2017
 * Modifications
 * *********************/

#include "fix/fix_common.h"

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <pthread.h>
#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libevent.h>
#include <jsmn.h>
#include "redis.h"

redisContext *c;              // data connection
static redisAsyncContext *ac; // pub/sub connection
jsmn_parser p;                // JSON parser
jsmntok_t t[100];             // tokens used by parser
pthread_t thread_id;          // id of async redis thread
struct event_base* base;      // libevent event

/* data shared between threads */
struct fix_quoterequest quoterequests[20];
int numquoterequests = 0;
struct fix_order orders[20];
int numorders = 0;
pthread_mutex_t lock;

/* compare a parsed token with a string literal */
static int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
  if (tok->type == JSMN_STRING && (int) strlen(s) == tok->end - tok->start &&
    strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
    return 0;
  }
  return -1;
}

/*
 * called when any pubsub messages received
 * parses message and builds a shared array of message structures
 * that is used by the FIX connection thread to send messages
 * */ 
void onMessage(redisAsyncContext *c, void *reply, void *privdata) {
  int r;
  int i;
  char brokerid[8];
  char quoterequestid[8];
  char orderid[8];
  char quantity[16];
  char cashorderquantity[16];
  char price[16];
  char fixmsgtype = ' ';

  printf("onMessage\n");
  redisReply *rr = reply;
  if (reply == NULL) return;
 printf("type:%d\n",rr->type);

  /* we are only interested in the message strings */
  if (rr->type == REDIS_REPLY_ARRAY) {
    if (rr->element[2]->type == REDIS_REPLY_STRING) {
      jsmn_init(&p);

      r = jsmn_parse(&p, rr->element[2]->str, strlen(rr->element[2]->str), t, sizeof(t)/sizeof(t[0]));
      if (r < 0) {
        printf("Failed to parse JSON: %d\n", r);
        if (r == JSMN_ERROR_INVAL) {
          printf("JSON string corrupted\n");
        } else if (r == JSMN_ERROR_NOMEM) {
          printf("not enough tokens\n");
        } else if (r == JSMN_ERROR_PART) {
          printf("JSON string too short\n");
        }
        return;
      }

      printf("Parsed JSON ok\n");

      /* assume top-level element is an object */
      if (r < 1 || t[0].type != JSMN_OBJECT) {
        printf("Object expected, type found: %d\n" , t[0].type);
        return;
      }

      /* updating shared memory, so must lock */
      pthread_mutex_lock(&lock);

      /* loop over all keys of the root object */
      for (i = 1; i < r; i++) {
        if (jsoneq(rr->element[2]->str, &t[i], "quoterequest") == 0) {
          printf("- quoterequest: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);

          /* initialise a quote request */
          memset(quoterequests[numquoterequests].currencyid, '\0', sizeof(quoterequests[numquoterequests].currencyid));
          memset(quoterequests[numquoterequests].symbolid, '\0', sizeof(quoterequests[numquoterequests].symbolid));
          memset(quoterequests[numquoterequests].isin, '\0', sizeof(quoterequests[numquoterequests].isin));
          quoterequests[numquoterequests].quantity = 0;
          quoterequests[numquoterequests].cashorderquantity = 0;
          quoterequests[numquoterequests].side = '\0';
          strcpy(quoterequests[numquoterequests].idsource, "4");
          quoterequests[numquoterequests].norelatedsym = 1;
          fixmsgtype = 'R';
        } else if (jsoneq(rr->element[2]->str, &t[i], "order") == 0) {
          printf("- order: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);

          /* initialise an order */
          memset(orders[numorders].currencyid, '\0', sizeof(orders[numorders].currencyid));
          memset(orders[numorders].symbolid, '\0', sizeof(orders[numorders].symbolid));
          memset(orders[numorders].isin, '\0', sizeof(orders[numorders].isin));
          memset(orders[numorders].quoterid, '\0', sizeof(orders[numorders].quoterid));
          memset(orders[numorders].externalquoteid, '\0', sizeof(orders[numorders].externalquoteid));
          memset(orders[numorders].ordertypeid, '\0', sizeof(orders[numorders].ordertypeid));
          memset(orders[numorders].delivertocompid, '\0', sizeof(orders[numorders].delivertocompid));
          orders[numorders].quantity = 0;
          orders[numorders].cashorderquantity = 0;
          orders[numorders].side = '\0';
          orders[numorders].price = 0;
          strcpy(orders[numorders].idsource, "4");
          orders[numorders].norelatedsym = 1;
          fixmsgtype = 'D';
        } else if (jsoneq(rr->element[2]->str, &t[i], "brokerid") == 0) {
          printf("- brokerid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(brokerid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(brokerid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "quoterequestid") == 0) {
          printf("- quoterequestid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(quoterequestid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(quoterequestid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "orderid") == 0) {
          printf("- orderid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orderid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orderid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "symbolid") == 0) {
          printf("- symbolid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].symbolid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].symbolid + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].symbolid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].symbolid + t[i+1].end - t[i+1].start) = '\0';
          } 
         i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "isin") == 0) {
          printf("- isin: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].isin, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].isin + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].isin, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].isin + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "settlmnttypid") == 0) {
          printf("- settlmnttypid: %c\n", *(rr->element[2]->str + t[i+1].start));
          if (fixmsgtype == 'R')
            quoterequests[numquoterequests].settlmnttypid = *(rr->element[2]->str + t[i+1].start);
          else if (fixmsgtype == 'D')
            orders[numorders].settlmnttypid = *(rr->element[2]->str + t[i+1].start);
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "quantity") == 0) {
          printf("- quantity: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(quantity, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(quantity + t[i+1].end - t[i+1].start) = '\0';
          if (fixmsgtype == 'R')
            quoterequests[numquoterequests].quantity = atof(quantity);
          else if (fixmsgtype == 'D')
            orders[numorders].quantity = atof(quantity);
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "cashorderquantity") == 0) {
          printf("- cashorderquantity: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(cashorderquantity, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(cashorderquantity + t[i+1].end - t[i+1].start) = '\0';
          if (fixmsgtype == 'R')
            quoterequests[numquoterequests].cashorderquantity = atof(cashorderquantity);
          else if (fixmsgtype == 'D')
            orders[numorders].cashorderquantity = atof(cashorderquantity);
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "side") == 0) {
          printf("- side: %c\n", *(rr->element[2]->str + t[i+1].start));
          if (fixmsgtype == 'R')
            quoterequests[numquoterequests].side = *(rr->element[2]->str + t[i+1].start);
          else if (fixmsgtype == 'D')
            orders[numorders].side = *(rr->element[2]->str + t[i+1].start);
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "exchangeid") == 0) {
          printf("- exchangeid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].exchangeid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].exchangeid + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].exchangeid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].exchangeid + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "currencyid") == 0) {
          printf("- currencyid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].currencyid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].currencyid + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].currencyid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].currencyid + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "settlcurrencyid") == 0) {
          printf("- settlcurrencyid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].settlcurrencyid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].settlcurrencyid + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].settlcurrencyid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].settlcurrencyid + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "timestamp") == 0) {
          printf("- timestamp: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].timestamp, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].timestamp + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].timestamp, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].timestamp + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "futsettdate") == 0) {
          printf("- futsettdate: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].futsettdate, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].futsettdate + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].futsettdate, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].futsettdate + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "quoterid") == 0) {
          printf("- quoterid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orders[numorders].quoterid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orders[numorders].quoterid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "externalquoteid") == 0) {
          printf("- externalquoteid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orders[numorders].externalquoteid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orders[numorders].externalquoteid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "ordertypeid") == 0) {
          printf("- ordertypeid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orders[numorders].ordertypeid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orders[numorders].ordertypeid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "delivertocompid") == 0) {
          printf("- delivertocompid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orders[numorders].delivertocompid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orders[numorders].delivertocompid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "price") == 0) {
          printf("- price: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(price, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(price + t[i+1].end - t[i+1].start) = '\0';
          orders[numorders].price = atof(price);
          i++;
        } else {
          printf("Unexpected key: %.*s\n", t[i].end-t[i].start, rr->element[2]->str + t[i].start);
        }
      }

      /* combine brokerid & quote request id */
      if (fixmsgtype == 'R') {
        strcpy(quoterequests[numquoterequests].quoterequestid, brokerid);
        strcat(quoterequests[numquoterequests].quoterequestid, ":");
        strcat(quoterequests[numquoterequests].quoterequestid, quoterequestid);

        /* increment the number of quote requests, will be reset by the processing thread */
        numquoterequests++;
      } else if (fixmsgtype == 'D') {
        strcpy(orders[numorders].orderid, brokerid);
        strcat(orders[numorders].orderid, ":");
        strcat(orders[numorders].orderid, orderid);

        /* increment the number of orders, will be reset by the processing thread */
        numorders++;
      }

      pthread_mutex_unlock(&lock);
    }
  }
}

/* connect to Redis */
int redis_connect(char *hostname, int port) {
  printf("redis_connect\n");

  /* data connection */
  c = redisConnect(hostname, port);

  /*redisReply *reply;
  reply = redisCommand(c, "AUTH password");
  freeReplyObject(reply);*/

  if (c != NULL && c->err) {
    printf("Error: %s\n", c->errstr);
    return 0;
  }

  printf("Connected to Redis\n");

  return 1;
}

void *thread_run(void *args) {
  printf("thread starting\n");

  /* start the event loop */
  struct event_base *base = (struct event_base*) args;
  event_base_dispatch(base);

  return 0;
}

/* async connection to Redis for pubsub */
int redis_async_connect(char *hostname, int port) {
  //signal(SIGPIPE, SIG_IGN);

  printf("redis_async_connect\n");

  // create an event loop
  //struct event_base *base = event_base_new();
  base = event_base_new();

  // create an async connection to Redis
  ac = redisAsyncConnect(hostname, port);

  if (ac != NULL && ac->err) {
    printf("Error: %s\n", ac->errstr);
    return 0;
  }

  /* attach the connection to the event loop */
  redisLibeventAttach(ac, base);
  redisAsyncCommand(ac, onMessage, NULL, "SUBSCRIBE testtopic");

  pthread_create(&thread_id, NULL, thread_run, base);

  return 1;
}

void thread_tidy() {
printf("thread_tidy\n");
  pthread_join(thread_id, NULL);
}

/* tidy */
void redis_disconnect(redisContext *c) {
  redisFree(c);
}

void redis_async_disconnect() {
  redisAsyncDisconnect(ac);
  event_base_loopexit(base, NULL);
  event_base_free(base);
}

/* test data connection */
void redis_test(redisContext *c) {
  redisReply *reply;

  reply = redisCommand(c,"SET %s %s","foo","bar");
  freeReplyObject(reply);

  reply = redisCommand(c,"GET %s","foo");
  printf("%s\n",reply->str);

  freeReplyObject(reply);
}
