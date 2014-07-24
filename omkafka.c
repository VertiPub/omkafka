
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>

#include "librdkafka/rdkafka.h"

#include "config.h"
#include "rsyslog.h"
#include "syslogd-types.h"
#include "srUtils.h"
#include "template.h"
#include "module-template.h"
#include "cfsysline.h"
#include "errmsg.h"

MODULE_TYPE_OUTPUT
MODULE_TYPE_NOKEEP

/* internal structures */
DEF_OMOD_STATIC_DATA
DEFobjCurrIf(errmsg)

/* global data */
char *brokers = NULL;
char *topic = NULL;
char *key = NULL;
int partition = RD_KAFKA_PARTITION_UA;
char *acks = NULL;
char *maxBuffMsgs = NULL;
char *maxRetries = NULL;
char *retryBackoffMS = NULL;
static uchar *dfltTplName = NULL;

char errstr[512];

typedef struct _instanceData {
    char *brokers;
    char *topic;
    char *key;
    int partition;
    char *acks;
    char *maxBuffMsgs;
    char *maxRetries;
    char *retryBackoffMS;

    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
} instanceData;

/* is compatible with feature */
BEGINisCompatibleWithFeature
    DBGPRINTF("==> omkafka: isCompatibleWithFeature() begin...\n");
CODESTARTisCompatibleWithFeature
    if(eFeat == sFEATURERepeatedMsgReduction)
        iRet = RS_RET_OK;
    DBGPRINTF("==> omkafka: isCompatibleWithFeature() end...\n");
ENDisCompatibleWithFeature

/* debug print instance info */
BEGINdbgPrintInstInfo
CODESTARTdbgPrintInstInfo
    DBGPRINTF("brokers: %s\n", pData->brokers);
    DBGPRINTF("topic: %s\n", pData->topic);
    DBGPRINTF("key: %s\n", pData->key);
    DBGPRINTF("parition: %d\n", pData->partition);
    DBGPRINTF("acks: %s\n", pData->acks);
ENDdbgPrintInstInfo

/* create instance */
BEGINcreateInstance
    DBGPRINTF("==> omkafka: kafka_createInstance() begin...\n");
CODESTARTcreateInstance
    // my code
    DBGPRINTF("==> omkafka: kafka_createInstance() end...\n");
ENDcreateInstance

static void err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    DBGPRINTF("%% ERROR CALLBACK: %s: %s: %s\n",
           rd_kafka_name(rk), rd_kafka_err2str(err), reason);
}


static void logger (const rd_kafka_t *rk, int level,
            const char *fac, const char *buf) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    printf("%u.%03u RDKAFKA-%i-%s: %s: %s\n",
        (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
        level, fac, rd_kafka_name(rk), buf);
}

static rsRetVal kafka_init(instanceData *pData, int bSilent) { 
    DBGPRINTF("==> omkafka: kafka_init() begin...\n");
    DEFiRet;

    ASSERT(pData != NULL);
    ASSERT(pData->rk == NULL);

    pData->conf = rd_kafka_conf_new();
    rd_kafka_conf_set_error_cb(pData->conf, err_cb);

    if(!(pData->rk = rd_kafka_new(RD_KAFKA_PRODUCER, pData->conf, errstr, sizeof(errstr)))) {
        DBGPRINTF(0, RS_RET_SUSPENDED, "omkafka_init: rd_kafka_new failed\n");
        iRet = RS_RET_SUSPENDED;
    }

    rd_kafka_set_logger(pData->rk, logger);
    rd_kafka_set_log_level(pData->rk, LOG_DEBUG);

    if(rd_kafka_brokers_add(pData->rk, pData->brokers) < 1) {
        DBGPRINTF(0, RS_RET_SUSPENDED, "omkafka_init: rd_kafka_brokers_add of '%s' failed\n", pData->brokers);
        iRet = RS_RET_SUSPENDED;
    }

    pData->rkt = rd_kafka_topic_new(pData->rk, pData->topic, pData->topic_conf);

    /* Producer config */
    rd_kafka_conf_set(pData->conf, "queue.buffering.max.messages", pData->maxBuffMsgs, NULL, 0);
    rd_kafka_conf_set(pData->conf, "message.send.max.retries", pData->maxRetries, NULL, 0);
    rd_kafka_conf_set(pData->conf, "retry.backoff.ms", pData->retryBackoffMS, NULL, 0);

    /* Kafka topic configuration */
    pData->topic_conf = rd_kafka_topic_conf_new();
    rd_kafka_topic_conf_set(pData->topic_conf, "request.required.acks", pData->acks, errstr, sizeof(errstr));

    DBGPRINTF("==> omkafka: kafka_init() end...\n");
    RETiRet; 
}

static void kafka_close(instanceData *pData) { 
    DBGPRINTF("==> omkafka: kafka_close() begin...\n");
    ASSERT(pData != NULL);

    if(pData->rkt != NULL)
        rd_kafka_topic_destroy(pData->rkt);

    if(pData->rk != NULL)
        rd_kafka_destroy(pData->rk);
    DBGPRINTF("==> omkafka: kafka_close() end...\n");
}

/* Send Message to Kafka
 * BabakBehzad, 2014-07-09
 */
static rsRetVal kafka_sendmsg(instanceData *pData, uchar *msg)
{
    DEFiRet;
    DBGPRINTF("==> omkafka: kafka_sendmsg() begin...\n");

    /* String should no be NULL */
    ASSERT(msg != NULL);

    if(pData->rk == NULL)
        CHKiRet(kafka_init(pData, 1));

    int keylen = pData->key ? strlen(key) : 0;
    int sendflags = RD_KAFKA_MSG_F_COPY; 

    int msgsize = strlen(msg);
    if (msg[msgsize-1] == '\n')
                msg[--msgsize] = '\0';

    DBGPRINTF("omkafka_sendmsg: ENTER - Syslogmessage = '%s' to Broker = '%s-%s'\n", (char*) msg, pData->brokers, pData->topic);

    if(rd_kafka_produce(pData->rkt, pData->partition, sendflags, msg, msgsize, pData->key, keylen, NULL) == -1) {
        fprintf(stderr, "%% Failed to produce to topic %s partition %i: %s\n", rd_kafka_topic_name(pData->rkt), pData->partition, rd_kafka_err2str(rd_kafka_errno2err(errno)));
    }
    rd_kafka_poll(pData->rk, 0);
    /*while (rd_kafka_outq_len(pData->rk) > 0)
        rd_kafka_poll(pData->rk, 100);
    */

finalize_it:
    DBGPRINTF("==> Sent %zd bytes to topic %s partition %i\n", msgsize, rd_kafka_topic_name(pData->rkt), pData->partition);

    DBGPRINTF("==> omkafka: kafka_sendmsg() end...\n");

    RETiRet;
}


/* try resume */
BEGINtryResume
CODESTARTtryResume
    DBGPRINTF("==> omkafka: tryResume() start...\n");
    if(pData->rk == NULL) {
        iRet = kafka_init(pData, 1);
    }
    DBGPRINTF("==> omkafka: tryResume() end...\n");
ENDtryResume

/* DO ACTION */
BEGINdoAction
CODESTARTdoAction
    DBGPRINTF("==> omkafka: doAction() start...\n");

    /* This will generate and send the SNMP Trap */
    iRet = kafka_sendmsg(pData, ppString[0]);

    DBGPRINTF("==> omkafka: doAction() done...\n");
finalize_it:
ENDdoAction

BEGINbeginTransaction
CODESTARTbeginTransaction
DBGPRINTF("==> omkafka: beginTransaction\n");
ENDbeginTransaction

BEGINendTransaction
CODESTARTendTransaction
DBGPRINTF("==> omkafka: endTransaction\n");
ENDendTransaction

/* free instance */
BEGINfreeInstance
CODESTARTfreeInstance
    kafka_close(pData);
ENDfreeInstance

/* parse selector act */
BEGINparseSelectorAct
    // mystuff
CODESTARTparseSelectorAct
    DBGPRINTF("==> omkafka: parseSelectorAct() begin...\n");

	/* first check if this config line is actually for us */
    if(strncmp((char*) p, ":omkafka:", sizeof(":omkafka:") - 1)) {
        ABORT_FINALIZE(RS_RET_CONFLINE_UNPROCESSED);
    }

    /* ok, if we reach this point, we have something for us */
    p += sizeof(":omkafka:") - 1; /* eat indicator sequence (-1 because of '\0'!) */
    CHKiRet(createInstance(&pData));
    CODE_STD_STRING_REQUESTparseSelectorAct(1)
	CHKiRet(cflineParseTemplateName(&p, *ppOMSR, 0, 0,
				       (dfltTplName == NULL) ? (uchar*)"RSYSLOG_FileFormat" : dfltTplName));

	/* Check Brokers */
	if (brokers == NULL) {
        errmsg.LogError(0, RS_RET_ERR_HDFS_OPEN, "omkafka: no brokers specified, can not continue");
		ABORT_FINALIZE( RS_RET_PARAM_ERROR );
	} 
    else {
		/* Copy Brokers */
		CHKmalloc(pData->brokers = (uchar*) strdup((char*)brokers));
	}

	/* Check Topic */
	if (topic == NULL) {
        errmsg.LogError(0, RS_RET_ERR_HDFS_OPEN, "omkafka: no topic specified, can not continue");
		ABORT_FINALIZE( RS_RET_PARAM_ERROR );
	} 
    else {
		/* Copy Brokers */
		CHKmalloc(pData->topic = (uchar*) strdup((char*)topic));
	}

    /* Copy Partition */
    if (partition != RD_KAFKA_PARTITION_UA)
        pData->partition = partition;

    /* Copy Acks */
    if (acks == NULL)
		CHKmalloc(pData->acks = (uchar*) strdup("0"));
    else
		CHKmalloc(pData->acks = (uchar*) strdup((char*)acks));

    /* Copy MaxBuffMsgs */
    if (maxBuffMsgs == NULL)
		CHKmalloc(pData->maxBuffMsgs = (uchar*) strdup("500000"));
    else
		CHKmalloc(pData->maxBuffMsgs = (uchar*) strdup((char*)maxBuffMsgs));

    /* Copy MaxRetries */
    if (maxRetries == NULL)
		CHKmalloc(pData->maxRetries = (uchar*) strdup("3"));
    else
		CHKmalloc(pData->maxRetries = (uchar*) strdup((char*)maxRetries));

    /* Copy MaxRetries */
    if (retryBackoffMS == NULL)
		CHKmalloc(pData->retryBackoffMS = (uchar*) strdup("500"));
    else
		CHKmalloc(pData->retryBackoffMS = (uchar*) strdup((char*)retryBackoffMS));

CODE_STD_FINALIZERparseSelectorAct
    DBGPRINTF("==> omkafka: parseSelectorAct() done...\n");
ENDparseSelectorAct

/* mode exit */
BEGINmodExit
CODESTARTmodExit
    DBGPRINTF("==> omkafka: modExit() done...\n");
ENDmodExit

/* query etry point */
BEGINqueryEtryPt
CODESTARTqueryEtryPt
    DBGPRINTF("==> omkafka: modqueryEtryPt() begin...\n");
CODEqueryEtryPt_STD_OMOD_QUERIES
    DBGPRINTF("==> omkafka: modqueryEtryPt() done...\n");
ENDqueryEtryPt


BEGINmodInit()
CODESTARTmodInit
	*ipIFVersProvided = CURR_MOD_IF_VERSION; /* we only support the current interface specification */
CODEmodInit_QueryRegCFSLineHdlr
	CHKiRet(objUse(errmsg, CORE_COMPONENT));
    DBGPRINTF("==> omkafka: modInit() begin...\n");

    CHKiRet(regCfSysLineHdlr((uchar *)"omkafkabrokers", 0, eCmdHdlrGetWord, NULL, &brokers, NULL));
    CHKiRet(regCfSysLineHdlr((uchar *)"omkafkatopic", 0, eCmdHdlrGetWord, NULL, &topic, NULL));
    CHKiRet(regCfSysLineHdlr((uchar *)"omkafkakey", 0, eCmdHdlrGetWord, NULL, &key, NULL));
    CHKiRet(regCfSysLineHdlr((uchar *)"omkafkapartition", 0, eCmdHdlrInt, NULL, &partition, NULL));
    CHKiRet(regCfSysLineHdlr((uchar *)"omkafkaacks", 0, eCmdHdlrGetWord, NULL, &acks, NULL));
    CHKiRet(regCfSysLineHdlr((uchar *)"omkafkamaxbuffmsgs", 0, eCmdHdlrGetWord, NULL, &maxBuffMsgs, NULL));
    CHKiRet(regCfSysLineHdlr((uchar *)"omkafkamaxretries", 0, eCmdHdlrGetWord, NULL, &maxRetries, NULL));
    CHKiRet(regCfSysLineHdlr((uchar *)"omkafkaretrybackoffms", 0, eCmdHdlrGetWord, NULL, &retryBackoffMS, NULL));
	CHKiRet(regCfSysLineHdlr((uchar *)"omkafkadefaulttemplate", 0, eCmdHdlrGetWord, NULL, &dfltTplName, NULL));

    DBGPRINTF("==> omkafka: modInit() done...\n");
ENDmodInit
/*
 * vi:set ai:
 */
