struct ping {
  int status;
  char text[32];
};

static int fix_client_session(struct fix_session_cfg *cfg);
static unsigned long fix_quote_request_fields(struct fix_session *session, struct fix_field *fields, struct fix_quoterequest *quoterequest);
static int fix_quote_request(struct fix_session *session, struct fix_field *fields, long nr_fields);
static int fix_message_reject(struct fix_message *msg);
static int fix_quote_ack(struct fix_session *session, struct fix_message *msg);
static int fix_quote(struct fix_session *session, struct fix_message *msg);
static int fix_execution_report(struct fix_session *session, struct fix_message *msg);
static int fix_order_cancel_reject(struct fix_session *session, struct fix_message *msg);
static int send_quote_ack(struct fix_session *session, struct fix_quoteack *quoteack);
static int send_quote(struct fix_session *session, struct fix_quote *quote);
static int send_execution_report(struct fix_session *session, struct fix_executionreport *executionreport);
static int send_order_cancel_reject(struct fix_session *session, struct fix_ordercancelreject *ordercancelreject);
int load_scripts();
static unsigned long fix_new_order_single_fields(struct fix_session *session, struct fix_field *fields, struct fix_order *order);
static unsigned long fix_order_cancel_request_fields(struct fix_session *session, struct fix_field *fields, struct fix_ordercancelrequest *ordercancelrequest);
static int send_ping(struct fix_session *session, struct ping *png);
static void check_for_data();
