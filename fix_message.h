#ifndef LIBTRADING_FIX_MESSAGE_H
#define LIBTRADING_FIX_MESSAGE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/uio.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

struct buffer;

struct fix_dialect;

/*
 * Message types:
 */
enum fix_msg_type {
	FIX_MSG_TYPE_HEARTBEAT			= 0,
	FIX_MSG_TYPE_TEST_REQUEST		= 1,
	FIX_MSG_TYPE_RESEND_REQUEST		= 2,
	FIX_MSG_TYPE_REJECT			= 3,
	FIX_MSG_TYPE_SEQUENCE_RESET		= 4,
	FIX_MSG_TYPE_LOGOUT			= 5,
	FIX_MSG_TYPE_EXECUTION_REPORT		= 6,
	FIX_MSG_TYPE_LOGON			= 7,
	FIX_MSG_TYPE_NEW_ORDER_SINGLE		= 8,
	FIX_MSG_TYPE_SNAPSHOT_REFRESH		= 9,
	FIX_MSG_TYPE_INCREMENT_REFRESH		= 10,
	FIX_MSG_TYPE_SESSION_STATUS		= 11,
	FIX_MSG_TYPE_SECURITY_STATUS		= 12,
	FIX_MSG_ORDER_CANCEL_REPLACE		= 13,
	FIX_MSG_ORDER_CANCEL_REJECT		= 14,
	FIX_MSG_ORDER_CANCEL_REQUEST		= 15,
	FIX_MSG_ORDER_MASS_CANCEL_REQUEST	= 16,
	FIX_MSG_ORDER_MASS_CANCEL_REPORT	= 17,
	FIX_MSG_QUOTE_REQUEST			= 18,
	FIX_MSG_SECURITY_DEFINITION_REQUEST	= 19,
	FIX_MSG_NEW_ORDER_CROSS			= 20,
	FIX_MSG_MASS_QUOTE			= 21,
	FIX_MSG_QUOTE_CANCEL			= 22,
	FIX_MSG_SECURITY_DEFINITION		= 23,
	FIX_MSG_QUOTE_ACKNOWLEDGEMENT		= 24,
	FIX_MSG_ORDER_MASS_STATUS_REQUEST	= 25,
	FIX_MSG_ORDER_MASS_ACTION_REQUEST	= 26,
	FIX_MSG_ORDER_MASS_ACTION_REPORT	= 27,
        FIX_MSG_QUOTE                           = 28,

	FIX_MSG_TYPE_MAX,		/* non-API */

	FIX_MSG_TYPE_UNKNOWN		= ~0UL,
};

/*
 * Maximum FIX message size
 */
#define FIX_MAX_HEAD_LEN	256UL
#define FIX_MAX_BODY_LEN	1024UL
#define FIX_MAX_MESSAGE_SIZE	(FIX_MAX_HEAD_LEN + FIX_MAX_BODY_LEN)

/* Total number of elements of fix_tag type*/
#define FIX_MAX_FIELD_NUMBER	48

#define	FIX_MSG_STATE_PARTIAL	1
#define	FIX_MSG_STATE_GARBLED	2

extern const char *fix_msg_types[FIX_MSG_TYPE_MAX];

enum fix_type {
	FIX_TYPE_INT,
	FIX_TYPE_FLOAT,
	FIX_TYPE_CHAR,
	FIX_TYPE_STRING,
	FIX_TYPE_CHECKSUM,
	FIX_TYPE_MSGSEQNUM,
	FIX_TYPE_STRING_8,
};

enum fix_tag {
	Account			= 1,
	AvgPx			= 6,
	BeginSeqNo		= 7,
	BeginString		= 8,
	BodyLength		= 9,
	CheckSum		= 10,
	ClOrdID			= 11,
	CumQty			= 14,
        Currency                = 15,
	EndSeqNo		= 16,
	ExecID			= 17,
	ExecTransType		= 20,
        IDSource                = 22,
        LastMkt                 = 30,
	LastPx			= 31,
	LastShares		= 32,
	MsgSeqNum		= 34,
	MsgType			= 35,
	NewSeqNo		= 36,
	OrderID			= 37,
	OrderQty		= 38,
	OrdStatus		= 39,
	OrdType			= 40,
	OrigClOrdID		= 41,
	PossDupFlag		= 43,
	Price			= 44,
	RefSeqNum		= 45,
	SecurityID		= 48,
	SenderCompID		= 49,
	SendingTime		= 52,
	Side			= 54,
	Symbol			= 55,
	TargetCompID		= 56,
	Text			= 58,
        TimeInForce             = 59,
	TransactTime		= 60,
        ValidUntilTime          = 62,
        SettlmntTyp             = 63,
        FutSettDate             = 64,
	RptSeq			= 83,
	EncryptMethod		= 98,
	CXlRejReason		= 102,
	OrdRejReason		= 103,
	HeartBtInt		= 108,
	TestReqID		= 112,
        OnBehalfOfCompID        = 115,
        QuoteID                 = 117,
        SettlCurrency           = 120,
	GapFillFlag		= 123,
        DeliverToCompID         = 128,
        QuoteReqID              = 131,
        BidPx                   = 132,
        OfferPx                 = 133,
	BidSize                 = 134,
        OfferSize               = 135,
	ResetSeqNumFlag		= 141,
        NoRelatedSym            = 146,
	ExecType		= 150,
	LeavesQty		= 151,
        CashOrderQty            = 152,
        BidSpotRate             = 188,
        OfferSpotRate           = 190,
        SecurityExchange        = 207,
	MDEntryType		= 269,
	MDEntryPx		= 270,
	MDEntrySize		= 271,
	MDUpdateAction		= 279,
        QuoteAckStatus          = 297,
        QuoteRejectReason       = 300,
	TradingSessionID	= 336,
	LastMsgSeqNumProcessed	= 369,
        RefTagID                = 371,
        RefMsgType              = 372,
        SessionRejectReason     = 373,
	MultiLegReportingType	= 442,
	Password		= 554,
	MDPriceLevel		= 1023,
        BidQuoteDepth           = 5816,
        OfferQuoteDepth         = 5817,
        Qbroker                 = 6158,
        SettleDays              = 9007,
};

struct fix_field {
	int				tag;
	enum fix_type			type;

	union {
		int64_t			int_value;
		double			float_value;
		char			char_value;
		const char		*string_value;
		char			string_8_value[8];
	};
};

#define FIX_INT_FIELD(t, v)				\
	(struct fix_field) {				\
		.tag		= t,			\
		.type		= FIX_TYPE_INT,		\
		{ .int_value	= v },			\
	}

#define FIX_STRING_FIELD(t, s)				\
	(struct fix_field) {				\
		.tag		= t,			\
		.type		= FIX_TYPE_STRING,	\
		{ .string_value	= s },			\
	}

#define FIX_FLOAT_FIELD(t, v)				\
	(struct fix_field) {				\
		.tag		= t,			\
		.type		= FIX_TYPE_FLOAT,	\
		{ .float_value  = v },			\
	}

#define FIX_CHECKSUM_FIELD(t, v)			\
	(struct fix_field) {				\
		.tag		= t,			\
		.type		= FIX_TYPE_CHECKSUM,	\
		{ .int_value	= v },			\
	}

#define FIX_CHAR_FIELD(t, v)				\
	(struct fix_field) {				\
		.tag		= t,			\
		.type		= FIX_TYPE_CHAR,	\
		{ .char_value	= v },			\
	}

#define FIX_STRING_8_FIELD(t)				\
	(struct fix_field) {				\
		.tag		= t,			\
		.type		= FIX_TYPE_STRING_8,	\
	}

struct fix_message {
	enum fix_msg_type		type;

	/*
	 * These are required fields.
	 */
	const char			*begin_string;
	unsigned long			body_length;
	const char			*msg_type;
	const char			*sender_comp_id;
	const char			*target_comp_id;
	unsigned long			msg_seq_num;
	/* XXX: SendingTime */
	const char			*check_sum;
	char				*str_now;

	/*
	 * These buffers are used for wire protocol parsing and unparsing.
	 */
	struct buffer			*head_buf;	/* first three fields */
	struct buffer			*body_buf;	/* rest of the fields including checksum */

	unsigned long			nr_fields;
	struct fix_field		*fields;

	struct iovec			iov[2];
};

static inline size_t fix_message_size(struct fix_message *self)
{
	return (self->iov[0].iov_len + self->iov[1].iov_len);
}

enum fix_parse_flag {
	FIX_PARSE_FLAG_NO_CSUM = 1UL << 0,
	FIX_PARSE_FLAG_NO_TYPE = 1UL << 1
};

int64_t fix_atoi64(const char *p, const char **end);
int fix_uatoi(const char *p, const char **end);

bool fix_field_unparse(struct fix_field *self, struct buffer *buffer);

struct fix_message *fix_message_new(void);
void fix_message_free(struct fix_message *self);

void fix_message_add_field(struct fix_message *msg, struct fix_field *field);

void fix_message_unparse(struct fix_message *self);
int fix_message_parse(struct fix_message *self, struct fix_dialect *dialect, struct buffer *buffer, unsigned long flags);

int fix_get_field_count(struct fix_message *self);
struct fix_field *fix_get_field_at(struct fix_message *self, int index);
struct fix_field *fix_get_field(struct fix_message *self, int tag);

const char *fix_get_string(struct fix_field *field, char *buffer, unsigned long len);

double fix_get_float(struct fix_message *self, int tag, double _default_);
int64_t fix_get_int(struct fix_message *self, int tag, int64_t _default_);
char fix_get_char(struct fix_message *self, int tag, char _default_);

void fix_message_validate(struct fix_message *self);
int fix_message_send(struct fix_message *self, int sockfd, int flags);

enum fix_msg_type fix_msg_type_parse(const char *s, const char delim);
bool fix_message_type_is(struct fix_message *self, enum fix_msg_type type);

char *fix_timestamp_now(char *buf, size_t len);

#ifdef __cplusplus
}
#endif

#endif
