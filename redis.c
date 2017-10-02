/*********************
 * redis.c
 * Redis connections for Comms Server
 * Cantwaittotrade Limited
 * Terry Johnston
 * July 2017
 * Sets up the Redis connections and handles
 * the async pubsub message handling in
 * it's own thread - see nbtrader.c
 *
 * Modifications
 * 1 Oct 2017 - changes to support authorisation of a Redis connection
 * *********************/

#include "fix/fix_common.h"

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
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

/* ata shared between threads */
struct fix_quoterequest quoterequests[20];
int numquoterequests = 0;
struct fix_order orders[20];
int numorders = 0;
pthread_mutex_t lock;

/*
 * Compare a parsed token with a string literal
 */
static int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
  if (tok->type == JSMN_STRING && (int) strlen(s) == tok->end - tok->start &&
    strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
    return 0;
  }
  return -1;
}

/*
 * Called when attempting connection authorisation
 */
void onAuth(redisAsyncContext *c, void *reply, void *privdata) {
  redisReply *rr = reply;
  if (reply == NULL) {
    fprintf(stdout, "Error authorising pubsub connection\n");
    return;
  }

  if (rr->type != REDIS_REPLY_STATUS || strcmp(rr->str, "OK") != 0) {
    fprintf(stdout, "Unable to authorise pubsub connection\n");
    return;
  }

  fprintf(stdout, "Connection authorised ok\n");
}

/*
 * Called when any pubsub messages received
 * Parses message and builds a shared array of message structures
 * that is used by the FIX connection thread to send messages
 */ 
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

  redisReply *rr = reply;
  if (reply == NULL) return;

  /* we are only interested in the message strings */
  if (rr->type == REDIS_REPLY_ARRAY) {
    if (rr->element[2]->type == REDIS_REPLY_STRING) {
      jsmn_init(&p);
      
      fprintf(stdout, "--- pubsub message received\n");
      fprintf(stdout, "%s\n", rr->element[2]->str);

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

      /* assume top-level element is an object */
      if (r < 1 || t[0].type != JSMN_OBJECT) {
        /* this may be the end */
        if (t[0].type == 4) {
          if (strcmp(rr->element[2]->str, "end") == 0) {
            redisAsyncDisconnect(ac);
          }
        }
        return;
      }

      /* updating shared memory, so must lock */
      pthread_mutex_lock(&lock);

      /* loop over all keys of the root object */
      for (i = 1; i < r; i++) {
        if (jsoneq(rr->element[2]->str, &t[i], "quoterequest") == 0) {
          //printf("- quoterequest: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);

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
          //printf("- order: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);

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
          //printf("- brokerid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(brokerid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(brokerid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "quoterequestid") == 0) {
          //printf("- quoterequestid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(quoterequestid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(quoterequestid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "orderid") == 0) {
          //printf("- orderid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orderid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orderid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "symbolid") == 0) {
          //printf("- symbolid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].symbolid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].symbolid + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].symbolid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].symbolid + t[i+1].end - t[i+1].start) = '\0';
          } 
         i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "isin") == 0) {
          //printf("- isin: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].isin, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].isin + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].isin, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].isin + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "settlmnttypid") == 0) {
          //printf("- settlmnttypid: %c\n", *(rr->element[2]->str + t[i+1].start));
          if (fixmsgtype == 'R')
            quoterequests[numquoterequests].settlmnttypid = *(rr->element[2]->str + t[i+1].start);
          else if (fixmsgtype == 'D')
            orders[numorders].settlmnttypid = *(rr->element[2]->str + t[i+1].start);
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "quantity") == 0) {
          //printf("- quantity: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(quantity, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(quantity + t[i+1].end - t[i+1].start) = '\0';
          if (fixmsgtype == 'R')
            quoterequests[numquoterequests].quantity = atof(quantity);
          else if (fixmsgtype == 'D')
            orders[numorders].quantity = atof(quantity);
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "cashorderquantity") == 0) {
          //printf("- cashorderquantity: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(cashorderquantity, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(cashorderquantity + t[i+1].end - t[i+1].start) = '\0';
          if (fixmsgtype == 'R')
            quoterequests[numquoterequests].cashorderquantity = atof(cashorderquantity);
          else if (fixmsgtype == 'D')
            orders[numorders].cashorderquantity = atof(cashorderquantity);
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "side") == 0) {
          //printf("- side: %c\n", *(rr->element[2]->str + t[i+1].start));
          if (fixmsgtype == 'R')
            quoterequests[numquoterequests].side = *(rr->element[2]->str + t[i+1].start);
          else if (fixmsgtype == 'D')
            orders[numorders].side = *(rr->element[2]->str + t[i+1].start);
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "exchangeid") == 0) {
          //printf("- exchangeid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].exchangeid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].exchangeid + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].exchangeid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].exchangeid + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "currencyid") == 0) {
          //printf("- currencyid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].currencyid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].currencyid + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].currencyid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].currencyid + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "settlcurrencyid") == 0) {
          //printf("- settlcurrencyid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].settlcurrencyid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].settlcurrencyid + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].settlcurrencyid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].settlcurrencyid + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "timestamp") == 0) {
          //printf("- timestamp: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].timestamp, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].timestamp + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].timestamp, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].timestamp + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "futsettdate") == 0) {
          //printf("- futsettdate: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          if (fixmsgtype == 'R') {
            strncpy(quoterequests[numquoterequests].futsettdate, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(quoterequests[numquoterequests].futsettdate + t[i+1].end - t[i+1].start) = '\0';
          } else if (fixmsgtype == 'D') {
            strncpy(orders[numorders].futsettdate, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
            *(orders[numorders].futsettdate + t[i+1].end - t[i+1].start) = '\0';
          }
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "quoterid") == 0) {
          //printf("- quoterid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orders[numorders].quoterid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orders[numorders].quoterid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "externalquoteid") == 0) {
          //printf("- externalquoteid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orders[numorders].externalquoteid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orders[numorders].externalquoteid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "ordertypeid") == 0) {
          //printf("- ordertypeid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orders[numorders].ordertypeid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orders[numorders].ordertypeid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "delivertocompid") == 0) {
          //printf("- delivertocompid: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(orders[numorders].delivertocompid, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(orders[numorders].delivertocompid + t[i+1].end - t[i+1].start) = '\0';
          i++;
        } else if (jsoneq(rr->element[2]->str, &t[i], "price") == 0) {
          //printf("- price: %.*s\n", t[i+1].end-t[i+1].start, rr->element[2]->str + t[i+1].start);
          strncpy(price, rr->element[2]->str + t[i+1].start, t[i+1].end - t[i+1].start);
          *(price + t[i+1].end - t[i+1].start) = '\0';
          orders[numorders].price = atof(price);
          i++;
        } else {
          //printf("Unexpected key: %.*s\n", t[i].end-t[i].start, rr->element[2]->str + t[i].start);
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

/*
 * Start a connection to Redis
 */
int redis_connect(const char *hostname, int port) {
  char cmd[30];

  fprintf(stdout, "Starting Redis connection to %s, port:%d\n", hostname, port);

  /* data connection */
  c = redisConnect(hostname, port);
  if (c != NULL && c->err) {
    fprintf(stdout, "Error: %s\n", c->errstr);
    return 0;
  }

  // may need authorisation
  if (auth != NULL) {
    strcpy(cmd, "auth ");
    strcat(cmd, auth);

    redisReply *rr;
    rr = redisCommand(c, cmd);
    if (rr->type != REDIS_REPLY_STATUS || strcmp(rr->str, "OK") != 0) {
      fprintf(stdout, "Unable to authorise Redis connection\n");
      freeReplyObject(rr);
      return 0;
    }

    freeReplyObject(rr);
  }

  fprintf(stdout, "Connected to Redis\n");

  return 1;
}

/*
 * Called when async thread starts
 */
void *thread_run(void *args) {
  fprintf(stdout, "Async thread started\n");

  /* start the event loop */
  struct event_base *base = (struct event_base*) args;
  event_base_dispatch(base);

  return 0;
}

/*
 * Called when async connection connects
 */
void connectCallback(const redisAsyncContext *c, int status) {
  if (status != REDIS_OK) {
    printf("Error: %s\n", c->errstr);
    return;
  }

  fprintf(stdout, "Redis pubsub connected\n");
}

/*
 * Called when async connection disconnects
 */
void disconnectCallback(const redisAsyncContext *c, int status) {
  char *ret = NULL;

  if (status != REDIS_OK) {
    fprintf(stdout, "Error: %s\n", c->errstr);
    return;
  }

  fprintf(stdout, "Redis pubsub disconnected\n");

  /* stop the event loop */
  event_base_loopexit(base, NULL);
  event_base_free(base);

  fprintf(stdout, "Exiting...\n");

  pthread_exit(ret);
}

/*
 * Async connection to Redis for pubsub
 */
int redis_async_connect(const char *hostname, int port) {
  char cmd[30];

  printf("Starting Redis pubsub connection\n");

  // create an event loop
  base = event_base_new();

  // create an async connection to Redis
  ac = redisAsyncConnect(hostname, port);
  if (ac != NULL && ac->err) {
    printf("Error: %s\n", ac->errstr);
    return 0;
  }

  /* attach the connection to the event loop */
  redisLibeventAttach(ac, base);

  /* set-up connect/disconnect callbacks */
  redisAsyncSetConnectCallback(ac, connectCallback);
  redisAsyncSetDisconnectCallback(ac, disconnectCallback);

  /* may need to authorise connection */
  if (auth != NULL) {
    strcpy(cmd, "auth ");
    strcat(cmd, auth);
    redisAsyncCommand(ac, onAuth, NULL, cmd);
  }

  /* listen for messages on comms server channel */
  redisAsyncCommand(ac, onMessage, NULL, "SUBSCRIBE 16");

  /* attach event loop to a separate thread */
  pthread_create(&thread_id, NULL, thread_run, base);

  return 1;
}

/*
 * Stop the redis pubsub connection
 */
void redis_async_disconnect() {
  redisAsyncDisconnect(ac);

  /* wait for disconnect callback */
}

/*
 * Wait for async thread to terminate
 */
void thread_tidy() {
  pthread_join(thread_id, NULL);
}

/*
 * Tidy the redis connection
 */
void redis_disconnect(redisContext *c) {
  redisFree(c);
}
