/* thread shared data */
struct fix_quoterequest {
  char quoterequestid[8];
  char symbolid[16];
  char isin[16];
  float quantity;
  float cashorderquantity;
  char settlmnttypid;
  char side;
  char exchangeid[8];
  char currencyid[4];
  char settlcurrencyid[4];
  char timestamp[20];
  char futsettdate[10];
  char idsource[8];
  int norelatedsym;
};

struct fix_order {
  char orderid[8];
  char ordertypeid[3];
  char symbolid[16];
  char isin[16];
  float quantity;
  float cashorderquantity;
  char settlmnttypid;
  char side;
  char exchangeid[8];
  char currencyid[4];
  char settlcurrencyid[4];
  char timestamp[20];
  char futsettdate[10];
  char idsource[8];
  int norelatedsym;
  float price;
  char delivertocompid[16];
  char quoterid[16];
  char externalquoteid[32];
};

struct fix_quote {
  char quotereqid[16];
  char quoterid[16];
  char externalquoteid[32];
  char markettimestamp[20];
  char symbolid[16];
  float offerpx;
  float offersize;
  float offerspotrate;
  float bidpx;
  float bidsize;
  float bidspotrate;
  char validuntiltime[20];
  char currencyid[4];
  char settlcurrencyid[4];
  int quotertype;
  char futsettdate[10];
  int bidquotedepth;
  int offerquotedepth;
  float cashorderqty;
  int settledays;
  int noseconds;
  char isin[16];
};

struct fix_quoteack {
  char quotereqid[16];
  int  quotestatusid;
  int  quoterejectreasonid;
  char text[128];
  char markettimestamp[20];
  char timestamp[20];
};

struct fix_executionreport {
  char orderstatusid;
  char exectype;
  char orderid[30];
  char clordid[10];
  float orderqty;
  float cumqty;
  char futsettdate[10];
  char securityid[16];
  char side;
  float lastshares;
  float lastpx;
  char markettimestamp[20];
  char settlcurrencyid[4];
  char execid[30];
  char onbehalfofcompid[30];
  char exchangeid[6];
  char lastmkt[6];
};

struct fix_message_list {
  struct fix_quoterequest* head;
  struct fix_quoterequest* tail;
};

extern struct fix_quoterequest quoterequests[];
extern int numquoterequests;
extern struct fix_order orders[];
extern int numorders;
extern pthread_mutex_t lock;
extern redisContext *c;       // data connection
extern const char *auth;

void onMessage(redisAsyncContext *c, void *reply, void *privdata);
int redis_connect(const char *hostname, int port);
int redis_async_connect(const char *hostname, int port);
void thread_tidy();
void redis_disconnect(redisContext *c);
void disconnectCallback(const redisAsyncContext *c, int status);
void redis_async_disconnect();
