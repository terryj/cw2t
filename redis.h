/* thread shared data */
struct fix_quoterequest {
  char quoterequestid[8];
  char symbolid[16];
  char isin[16];
  double quantity;
  double cashorderquantity;
  char settlmnttypid;
  char side;
  char exchangeid[8];
  char currencyid[4];
  char settlcurrencyid[4];
  char timestamp[24];
  char futsettdate[10];
  char idsource[8];
  int norelatedsym;
};

struct fix_order {
  char orderid[16];
  char ordertypeid[3];
  char symbolid[16];
  char isin[16];
  double quantity;
  double cashorderquantity;
  char settlmnttypid;
  char side;
  char exchangeid[8];
  char currencyid[4];
  char settlcurrencyid[4];
  char timestamp[24];
  char futsettdate[10];
  char idsource[8];
  int norelatedsym;
  double price;
  char delivertocompid[16];
  char quoterid[16];
  char externalquoteid[32];
};

struct fix_ordercancelrequest {
  char orderid[16];
  char origorderid[16];
  char symbolid[16];
  char isin[16];
  char timestamp[24];
  char idsource[8];
  char delivertocompid[16];
};

struct fix_quote {
  char quotereqid[16];
  char quoterid[16];
  char externalquoteid[32];
  char markettimestamp[24];
  char symbolid[16];
  double  offerpx;
  double offersize;
  double offerspotrate;
  double bidpx;
  double bidsize;
  double bidspotrate;
  char validuntiltime[24];
  char currencyid[4];
  char settlcurrencyid[4];
  int quotertype;
  char futsettdate[10];
  int bidquotedepth;
  int offerquotedepth;
  double cashorderqty;
  int settledays;
  int noseconds;
  char isin[16];
};

struct fix_quoteack {
  char quotereqid[16];
  int  quotestatusid;
  int  quoterejectreasonid;
  char text[128];
  char markettimestamp[24];
  char timestamp[24];
};

struct fix_executionreport {
  char orderstatusid;
  char exectype;
  char orderid[30];
  char clordid[16];
  double orderqty;
  double cumqty;
  double settlcurramt;
  char futsettdate[10];
  char securityid[16];
  char side;
  double lastshares;
  double lastpx;
  char markettimestamp[24];
  char settlcurrencyid[4];
  char execid[30];
  char onbehalfofcompid[30];
  char exchangeid[6];
  char lastmkt[6];
  char text[128];
  int orderrejectreasonid;
};

struct fix_ordercancelreject {
  char ordercancelrequestid[16];
  char orderid[16];
  char externalorderid[16];
  char symbolid[16];
  char isin[16];
  int  orderstatusid;
  int  ordercancelrejectreasonid;
  char text[128];
  char markettimestamp[24];
};

struct fix_message_list {
  struct fix_quoterequest* head;
  struct fix_quoterequest* tail;
};

extern struct fix_quoterequest quoterequests[];
extern int numquoterequests;
extern struct fix_order orders[];
extern int numorders;
extern struct fix_ordercancelrequest ordercancelrequests[];
extern int numordercancelrequests;
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
