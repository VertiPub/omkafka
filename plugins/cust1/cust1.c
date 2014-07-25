/* cust1.c
 */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>
#include <ctype.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/types.h>
#include	<sys/socket.h>
#include "rsyslog.h"
#include "dirty.h"
#include "cfsysline.h"
#include "module-template.h"
#include "unicode-helper.h"
#include "net.h"
#include "netstrm.h"
#include "errmsg.h"
#include "strmsrv.h"
#include "srUtils.h"
#include "msg.h"
#include "datetime.h"
#include "diis-gw.h"

MODULE_TYPE_INPUT

/* static data */
DEF_IMOD_STATIC_DATA
DEFobjCurrIf(strmsrv)
DEFobjCurrIf(strms_sess)
DEFobjCurrIf(net)
DEFobjCurrIf(netstrm)
DEFobjCurrIf(errmsg)
DEFobjCurrIf(datetime)


/* Module specific definitions */
typedef enum {
	DIIS_IN_HDR = 0,		/* reading header */
	DIIS_IN_BODY = 1		/* reading message body */
} diis_state_t;	/* states for our state machine */

typedef struct {
	diis_state_t state;
	bmf_resp_data_t resp_data;	/* response data structure */
	bmf_req_data_t req_data;	/* request data structure */
	unsigned int iBuf;			/* buffer index during reception */
	char *body_buf;			/* buffer for the message body */
} diis_sess_t;


/* Module static data */
static strmsrv_t *pOurStrmsrv = NULL;  /* our STRM server(listener) */
static permittedPeers_t *pPermPeersRoot = NULL;

static int bKeepAlive = 1;		/* support keep-alive packets */

/* config settings */
static int iSTRMSessMax = 200; /* max number of sessions */
static int iStrmDrvrMode = 0; /* mode for stream driver, driver-dependent (0 mostly means plain strm) */
static uchar *pszStrmDrvrAuthMode = NULL; /* authentication mode to use */
static uchar *pszInputName = NULL; /* value for inputname property, NULL is OK and handled by core engine */


/* Called by strmsrv on acceptance of a new session. This allocates our
 * own session data.
 * rgerhards, 2009-06-02
 */
static rsRetVal
OnSessConstructFinalize(void *pUsr)
{
	diis_sess_t *pData;
	DEFiRet;

	assert(pUsr != NULL);
	CHKmalloc(pData = calloc(1, sizeof(diis_sess_t)));
	pData->state = DIIS_IN_HDR;
	*((diis_sess_t**) pUsr) = pData;

finalize_it:
	RETiRet;
}


/* Called by strmsrv when a session is destructed. Must do cleanup.
 * rgerhards, 2009-06-02
 */
static rsRetVal
OnSessDestruct(void *pUsr)
{
	diis_sess_t *pData;
	DEFiRet;

	assert(pUsr != NULL);
	pData = *((diis_sess_t**) pUsr);
	free(pData);
	*((diis_sess_t**) pUsr) = NULL;

	RETiRet;
}


/* process a received request header. This is based on the contributed code,
 * which has only lightly be adopted to rsyslog design.
 */
static rsRetVal
processHdr(diis_sess_t *pData)
{
	uint32_t net_long;
	unsigned char *resp_buf_ptr;
	uchar *buf_ptr;
	DEFiRet;
	assert(pData != NULL);

        /* Determine whether response header should have hostname field */
        if((pData->req_data.raw_hdr[REQ_TYPE_FLAG_OFFSET] & REQ_HNAME_FLAG_MASK)
	     == REQ_HNAME_FLAG_MASK)
        {
		buf_ptr = pData->resp_data.raw_hdr + RESP_HNAME_OFFSET;
		sprintf((char*)buf_ptr, "test.hostname.domainname"); // TODO: What does this do???
		pData->resp_data.host_used = TRUE;
		pData->resp_data.raw_hdr[RESP_TYPE_FLAG_OFFSET] = RESP_HNAME_FLAG_MASK;
	}

	/* Extract the IID from the BMF header - some values used for testing */
	buf_ptr = pData->req_data.raw_hdr + REQ_IID_OFFSET;
	memcpy(&net_long, buf_ptr, sizeof(net_long));
	pData->resp_data.resp_iid = ntohl(net_long);

	buf_ptr = pData->req_data.raw_hdr + REQ_LEN_OFFSET;
	memcpy(&net_long, buf_ptr, sizeof(net_long));
	pData->req_data.length = ntohl(net_long);

	/* Populate the response header/
	 * Type flag is already set 
	 * Use the request type code, IID, and length for normal response
	 * OK to use length because we are returning the request message body */

	buf_ptr = pData->req_data.raw_hdr + REQ_TYPE_CODE_OFFSET;
	resp_buf_ptr = pData->resp_data.raw_hdr + RESP_TYPE_CODE_OFFSET;
	memcpy(resp_buf_ptr, buf_ptr, RESP_TYPE_CODE_SIZE + RESP_IID_SIZE + RESP_LEN_SIZE);

	/* Allocate a buffer to hold the message body */
	CHKmalloc(pData->body_buf = malloc(pData->req_data.length));

finalize_it:
	RETiRet;
}


/* read in the DIIS header
 * When we enter this function, there is at least one more character
 * to be processed read for the header. Initiates header processing and
 * state switch when everything was read.
 */
static rsRetVal
doHdr(diis_sess_t *pData, uchar c)
{
	DEFiRet;
	assert(pData != NULL);

	((char*) (pData->req_data.raw_hdr))[pData->iBuf] = c;

	if(++pData->iBuf == REQ_HDR_LEN) {
		/* request header read, do state transition */
		CHKiRet(processHdr(pData));
		pData->iBuf = 0;
		pData->state = DIIS_IN_BODY;
		dbgprintf("diis message body length: %d\n", pData->req_data.length);
	}

finalize_it:
	RETiRet;
}


/* process the body part of a message
 * This again mainly is the original code.
 */
static rsRetVal
processBody(strms_sess_t *pSess, diis_sess_t *pData)
{
	unsigned char *resp_buf_ptr;
	uint32_t net_long;
	int length;
	int dummy;
	ssize_t sendLen;
	DEFiRet;
	assert(pData != NULL);

	dbgprintf("diis message body : '%s'\n", pData->body_buf);

	/* Test client uses some specific iid values to request failure tests */
	switch (pData->resp_data.resp_iid)
	{
	case 999999999:
		/*****  Test Bad Response Type Code  *****/
		dbgprintf("Test Bad Response Type Code");
		resp_buf_ptr = pData->resp_data.raw_hdr + RESP_TYPE_CODE_OFFSET;
		*resp_buf_ptr =  100;
		break;
	case 999999998:
		/*****  Test too large response length  *****/
		dbgprintf( "Test too large response length");
		resp_buf_ptr = pData->resp_data.raw_hdr + RESP_LEN_OFFSET;
		length = b2c_rsp_LIMIT + 1;
		net_long = htonl(length);
		memcpy(resp_buf_ptr, &net_long, RESP_LEN_SIZE);
		break;
	case 999999997:
		/*****  Test response DIIS Message Header - length mismatch  *****/
		dbgprintf( "Test response DIIS Message Header - length mismatch");
		resp_buf_ptr = pData->resp_data.raw_hdr + RESP_LEN_OFFSET;
		length = pData->req_data.length - 1;
		net_long = htonl(length);
		memcpy(resp_buf_ptr, &net_long, RESP_LEN_SIZE);
		break;
	case 999999996:
		/*****  Test response DIIS Message Header - IID mismatch  *****/
		dbgprintf( "Test response DIIS Message Header - IID mismatch");
		resp_buf_ptr = (unsigned char*) pData->body_buf + DIIS_MSG_HDR_TAG1_LEN;
		strncpy((char*) resp_buf_ptr, "00000000", 8);
		break;
	case 999999995:
		/*****  Test response BMF Header - IID mismatch  *****/
		dbgprintf( "Test response BMF Header - IID mismatch");
		resp_buf_ptr = pData->resp_data.raw_hdr + RESP_IID_OFFSET;
		dummy = 1;
		net_long = htonl(dummy);
		memcpy(resp_buf_ptr, &net_long, RESP_IID_SIZE);
		break;
	default:
		break;
	}

	// ????????????????????????????????????????????????????????????????????????????????
	/* TODO
	 * I think something needs to be changed here. This looks much like echo server
	 * code, not the actual code. So what do we need to reply?
	 * Also, I assume that we can now submit the message to the rsyslog core at this
	 * point.
	 */
	// ????????????????????????????????????????????????????????????????????????????????


	if (pData->resp_data.host_used) {
		sendLen = RESP_HDR_LEN;
	} else {
		sendLen = RESP_HDR_1ST_READ;
	}

	CHKiRet(netstrm.Send(pSess->pStrm, pData->resp_data.raw_hdr, &sendLen));
	sendLen = pData->req_data.length;
	CHKiRet(netstrm.Send(pSess->pStrm, (uchar*)pData->body_buf, &sendLen));
	//bytes_written = net_write(0, body_buf, bytes_read, 0);

finalize_it:
	RETiRet;
}


/* read in the DIIS body
 * When we enter this function, there is at least one more character
 * to be processed read for the header. Initiates body processing and
 * state switch when everything was read.
 */
static rsRetVal
doBody(strms_sess_t *pSess, diis_sess_t *pData, uchar c)
{
	DEFiRet;
	assert(pData != NULL);

	pData->body_buf[pData->iBuf] = c;

	if(++pData->iBuf == pData->req_data.length) {
		/* request header read, do state transition */
		/* we do not use CHKiRet() below, because we MUST do the cleanup
		 * in any case!
		 */
		iRet = processBody(pSess, pData);
		/* cleanup */
		pData->iBuf = 0;
		pData->state = DIIS_IN_HDR;
		free(pData->body_buf);
		pData->body_buf = NULL;
	}

	RETiRet;
}


/* Function to handle received characters.
 * rgerhards, 2009-06-02
 */
static rsRetVal
OnCharRcvd(strms_sess_t *pSess, uchar c)
{
	diis_sess_t *pData;
	DEFiRet;

	assert(pSess != NULL);
	pData = strms_sess.GetUsrP(pSess);
	assert(pData != NULL);

	switch(pData->state) {
		case DIIS_IN_HDR:
			CHKiRet(doHdr(pData, c));
			break;
		case DIIS_IN_BODY:
			CHKiRet(doBody(pSess, pData, c));
			break;
		default:dbgprintf("program error: invalid diis state %d\n",
				  (int) pData->state);
			assert(0);
			break;
	}

finalize_it:
	RETiRet;
}


/* set permitted peer -- rgerhards, 2008-05-19
 */
static rsRetVal
setPermittedPeer(void __attribute__((unused)) *pVal, uchar *pszID)
{
	DEFiRet;
	CHKiRet(net.AddPermittedPeer(&pPermPeersRoot, pszID));
	free(pszID); /* no longer needed, but we need to free as of interface def */
finalize_it:
	RETiRet;
}


static rsRetVal addSTRMListener(void __attribute__((unused)) *pVal, uchar *pNewVal)
{
	DEFiRet;

	if(pOurStrmsrv == NULL) {
		CHKiRet(strmsrv.Construct(&pOurStrmsrv));
		CHKiRet(strmsrv.SetSessMax(pOurStrmsrv, iSTRMSessMax));
		CHKiRet(strmsrv.SetDrvrMode(pOurStrmsrv, iStrmDrvrMode));
		CHKiRet(strmsrv.SetOnCharRcvd(pOurStrmsrv, OnCharRcvd));
		CHKiRet(strmsrv.SetKeepAlive(pOurStrmsrv, bKeepAlive));
		CHKiRet(strmsrv.SetCBOnSessConstructFinalize(pOurStrmsrv, OnSessConstructFinalize));
		CHKiRet(strmsrv.SetCBOnSessDestruct(pOurStrmsrv, OnSessDestruct));
		/* now set optional params, but only if they were actually configured */
		if(pszStrmDrvrAuthMode != NULL) {
			CHKiRet(strmsrv.SetDrvrAuthMode(pOurStrmsrv, pszStrmDrvrAuthMode));
		}
		if(pPermPeersRoot != NULL) {
			CHKiRet(strmsrv.SetDrvrPermPeers(pOurStrmsrv, pPermPeersRoot));
		}
	}

	/* initialized, now add socket */
	CHKiRet(strmsrv.SetInputName(pOurStrmsrv, pszInputName == NULL ?
						UCHAR_CONSTANT("diis") : pszInputName));
	strmsrv.configureSTRMListen(pOurStrmsrv, pNewVal);

finalize_it:
	if(iRet != RS_RET_OK) {
		errmsg.LogError(0, NO_ERRCODE, "error %d trying to add diis listener", iRet);
		if(pOurStrmsrv != NULL)
			strmsrv.Destruct(&pOurStrmsrv);
	}
	RETiRet;
}

/* This function is called to gather input.
 */
BEGINrunInput
CODESTARTrunInput
	CHKiRet(strmsrv.ConstructFinalize(pOurStrmsrv));
	iRet = strmsrv.Run(pOurStrmsrv);
finalize_it:
ENDrunInput


/* initialize and return if will run or not */
BEGINwillRun
CODESTARTwillRun
	/* first apply some config settings */
	if(pOurStrmsrv == NULL)
		ABORT_FINALIZE(RS_RET_NO_RUN);
finalize_it:
ENDwillRun


BEGINafterRun
CODESTARTafterRun
	/* do cleanup here */
ENDafterRun


BEGINmodExit
CODESTARTmodExit
	if(pOurStrmsrv != NULL)
		iRet = strmsrv.Destruct(&pOurStrmsrv);

	if(pPermPeersRoot != NULL) {
		net.DestructPermittedPeers(&pPermPeersRoot);
	}

	/* release objects we used */
	objRelease(net, LM_NET_FILENAME);
	objRelease(netstrm, LM_NETSTRMS_FILENAME);
	objRelease(strms_sess, LM_STRMSRV_FILENAME);
	objRelease(strmsrv, LM_STRMSRV_FILENAME);
	objRelease(errmsg, CORE_COMPONENT);
	objRelease(datetime, CORE_COMPONENT);
ENDmodExit


static rsRetVal
resetConfigVariables(uchar __attribute__((unused)) *pp, void __attribute__((unused)) *pVal)
{
	iSTRMSessMax = 200;
	iStrmDrvrMode = 0;
	bKeepAlive = 1;
	free(pszInputName);
	pszInputName = NULL;
	if(pszStrmDrvrAuthMode != NULL) {
		free(pszStrmDrvrAuthMode);
		pszStrmDrvrAuthMode = NULL;
	}
	return RS_RET_OK;
}



BEGINqueryEtryPt
CODESTARTqueryEtryPt
CODEqueryEtryPt_STD_IMOD_QUERIES
ENDqueryEtryPt


BEGINmodInit()
CODESTARTmodInit
	*ipIFVersProvided = CURR_MOD_IF_VERSION; /* we only support the current interface specification */
CODEmodInit_QueryRegCFSLineHdlr
	pOurStrmsrv = NULL;
	/* request objects we use */
	CHKiRet(objUse(net, LM_NET_FILENAME));
	CHKiRet(objUse(netstrm, LM_NETSTRMS_FILENAME));
	CHKiRet(objUse(strms_sess, LM_STRMSRV_FILENAME));
	CHKiRet(objUse(strmsrv, LM_STRMSRV_FILENAME));
	CHKiRet(objUse(errmsg, CORE_COMPONENT));
	CHKiRet(objUse(datetime, CORE_COMPONENT));

	/* register config file handlers */
	CHKiRet(omsdRegCFSLineHdlr(UCHAR_CONSTANT("diisserverkeepalive"), 0, eCmdHdlrBinary,
				   NULL, &bKeepAlive, STD_LOADABLE_MODULE_ID, eConfObjGlobal));
	CHKiRet(omsdRegCFSLineHdlr(UCHAR_CONSTANT("diisserverrun"), 0, eCmdHdlrGetWord,
				   addSTRMListener, NULL, STD_LOADABLE_MODULE_ID, eConfObjGlobal));
	CHKiRet(omsdRegCFSLineHdlr(UCHAR_CONSTANT("diismaxsessions"), 0, eCmdHdlrInt,
				   NULL, &iSTRMSessMax, STD_LOADABLE_MODULE_ID, eConfObjGlobal));
	CHKiRet(omsdRegCFSLineHdlr(UCHAR_CONSTANT("diisserverstreamdrivermode"), 0,
				   eCmdHdlrInt, NULL, &iStrmDrvrMode, STD_LOADABLE_MODULE_ID, eConfObjGlobal));
	CHKiRet(omsdRegCFSLineHdlr(UCHAR_CONSTANT("diisserverstreamdriverauthmode"), 0,
				   eCmdHdlrGetWord, NULL, &pszStrmDrvrAuthMode, STD_LOADABLE_MODULE_ID, eConfObjGlobal));
	CHKiRet(omsdRegCFSLineHdlr(UCHAR_CONSTANT("diisserverstreamdriverpermittedpeer"), 0,
				   eCmdHdlrGetWord, setPermittedPeer, NULL, STD_LOADABLE_MODULE_ID, eConfObjGlobal));
	CHKiRet(omsdRegCFSLineHdlr(UCHAR_CONSTANT("diisserverinputname"), 0,
				   eCmdHdlrGetWord, NULL, &pszInputName, STD_LOADABLE_MODULE_ID, eConfObjGlobal));
	CHKiRet(omsdRegCFSLineHdlr(UCHAR_CONSTANT("resetconfigvariables"), 1, eCmdHdlrCustomHandler,
		resetConfigVariables, NULL, STD_LOADABLE_MODULE_ID, eConfObjGlobal));
ENDmodInit


/* vim:set ai:
 */
