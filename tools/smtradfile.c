/* smtradfile.c
 * This is a strgen module for the traditional file format.
 *
 * Format generated:
 * "%TIMESTAMP% %HOSTNAME% %syslogtag%%msg:::sp-if-no-1st-sp%%msg:::drop-last-lf%\n"
 *
 * NOTE: read comments in module-template.h to understand how this file
 *       works!
 *
 * File begun on 2010-06-01 by RGerhards
 *
 * Copyright 2010 Rainer Gerhards and Adiscon GmbH.
 *
 * This file is part of rsyslog.
 *
 * Rsyslog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Rsyslog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Rsyslog.  If not, see <http://www.gnu.org/licenses/>.
 *
 * A copy of the GPL can be found in the file "COPYING" in this distribution.
 */
#include "config.h"
#include "rsyslog.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include "syslogd.h"
#include "conf.h"
#include "syslogd-types.h"
#include "template.h"
#include "msg.h"
#include "module-template.h"
#include "unicode-helper.h"

MODULE_TYPE_STRGEN
MODULE_TYPE_NOKEEP
STRGEN_NAME("RSYSLOG_TraditionalFileFormat")

/* internal structures
 */
DEF_SMOD_STATIC_DATA


/* config data */


/* This strgen tries to minimize the amount of reallocs be first obtaining pointers to all strings
 * needed (including their length) and then calculating the actual space required. So when we 
 * finally copy, we know exactly what we need. So we do at most one alloc.
 */
BEGINstrgen
	register int iBuf;
	uchar *pTimeStamp;
	uchar *pHOSTNAME;
	size_t lenHOSTNAME;
	uchar *pTAG;
	int lenTAG;
	uchar *pMSG;
	size_t lenMSG;
	size_t lenTotal;
CODESTARTstrgen
	/* first obtain all strings and their length (if not fixed) */
	pTimeStamp = (uchar*) getTimeReported(pMsg, tplFmtRFC3164Date);
	pHOSTNAME = (uchar*) getHOSTNAME(pMsg);
	lenHOSTNAME = getHOSTNAMELen(pMsg);
	getTAG(pMsg, &pTAG, &lenTAG);
	pMSG = getMSG(pMsg);
	lenMSG = getMSGLen(pMsg);

	/* calculate len, constants for spaces and similar fixed strings */
	lenTotal = CONST_LEN_TIMESTAMP_3164 + 1 + lenHOSTNAME + 1 + lenTAG + lenMSG + 2;
	if(pMSG[0] != ' ')
		++lenTotal; /* then we need to introduce one additional space */

	/* now make sure buffer is large enough */
	if(lenTotal  >= *pLenBuf)
		CHKiRet(ExtendBuf(ppBuf, pLenBuf, lenTotal));

	/* and concatenate the resulting string */
	memcpy(*ppBuf, pTimeStamp, CONST_LEN_TIMESTAMP_3164);
	*(*ppBuf + CONST_LEN_TIMESTAMP_3164) = ' ';

	memcpy(*ppBuf + CONST_LEN_TIMESTAMP_3164 + 1, pHOSTNAME, lenHOSTNAME);
	iBuf = CONST_LEN_TIMESTAMP_3164 + 1 + lenHOSTNAME;
	*(*ppBuf + iBuf++) = ' ';

	memcpy(*ppBuf + iBuf, pTAG, lenTAG);
	iBuf += lenTAG;

	if(pMSG[0] != ' ')
		*(*ppBuf + iBuf++) = ' ';
	memcpy(*ppBuf + iBuf, pMSG, lenMSG);
	iBuf += lenMSG;

	/* trailer */
	*(*ppBuf + iBuf++) = '\n';
	*(*ppBuf + iBuf) = '\0';

finalize_it:
ENDstrgen


BEGINmodExit
CODESTARTmodExit
ENDmodExit


BEGINqueryEtryPt
CODESTARTqueryEtryPt
CODEqueryEtryPt_STD_SMOD_QUERIES
ENDqueryEtryPt


BEGINmodInit(smtradfile)
CODESTARTmodInit
	*ipIFVersProvided = CURR_MOD_IF_VERSION; /* we only support the current interface specification */
CODEmodInit_QueryRegCFSLineHdlr
	dbgprintf("traditional file format strgen init called, compiled with version %s\n", VERSION);
ENDmodInit
