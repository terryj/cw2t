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

