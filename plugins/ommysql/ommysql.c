/* ommysql.c
 * This is the implementation of the build-in output module for MySQL.
 *
 * NOTE: read comments in module-template.h to understand how this file
 *       works!
 *
 * File begun on 2007-07-20 by RGerhards (extracted from syslogd.c)
 *
 * Copyright 2007-2012 Adiscon GmbH.
 *
 * This file is part of rsyslog.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 *       -or-
 *       see COPYING.ASL20 in the source distribution
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "config.h"
#include "rsyslog.h"
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <mysql.h>
#include "conf.h"
#include "syslogd-types.h"
#include "srUtils.h"
#include "template.h"
#include "ommysql.h"
#include "module-template.h"
#include "errmsg.h"
#include "cfsysline.h"

MODULE_TYPE_OUTPUT
MODULE_TYPE_NOKEEP

/* internal structures
 */
DEF_OMOD_STATIC_DATA
DEFobjCurrIf(errmsg)

typedef struct _instanceData {
	MYSQL	*f_hmysql;		/* handle to MySQL */
	char	f_dbsrv[MAXHOSTNAMELEN+1];	/* IP or hostname of DB server*/ 
	unsigned int f_dbsrvPort;		/* port of MySQL server */
	char	f_dbname[_DB_MAXDBLEN+1];	/* DB name */
	char	f_dbuid[_DB_MAXUNAMELEN+1];	/* DB user */
	char	f_dbpwd[_DB_MAXPWDLEN+1];	/* DB user's password */
	unsigned uLastMySQLErrno;	/* last errno returned by MySQL or 0 if all is well */
	uchar * f_configfile; /* MySQL Client Configuration File */
	uchar * f_configsection; /* MySQL Client Configuration Section */
} instanceData;

/* config variables */
static uchar * pszMySQLConfigFile = NULL;	/* MySQL Client Configuration File */
static uchar * pszMySQLConfigSection = NULL;	/* MySQL Client Configuration Section */ 
static int iSrvPort = 0;	/* database server port */


BEGINcreateInstance
CODESTARTcreateInstance
ENDcreateInstance


BEGINisCompatibleWithFeature
CODESTARTisCompatibleWithFeature
	if(eFeat == sFEATURERepeatedMsgReduction)
		iRet = RS_RET_OK;
ENDisCompatibleWithFeature


/* The following function is responsible for closing a
 * MySQL connection.
 * Initially added 2004-10-28
 */
static void closeMySQL(instanceData *pData)
{
	ASSERT(pData != NULL);

	if(pData->f_hmysql != NULL) {	/* just to be on the safe side... */
		mysql_server_end();
		mysql_close(pData->f_hmysql);	
		pData->f_hmysql = NULL;
	}
	if(pData->f_configfile!=NULL){
		free(pData->f_configfile);
		pData->f_configfile=NULL;
	}
	if(pData->f_configsection!=NULL){
		free(pData->f_configsection);
		pData->f_configsection=NULL;
	}
}

BEGINfreeInstance
CODESTARTfreeInstance
	closeMySQL(pData);
ENDfreeInstance


BEGINdbgPrintInstInfo
CODESTARTdbgPrintInstInfo
	/* nothing special here */
ENDdbgPrintInstInfo


/* log a database error with descriptive message.
 * We check if we have a valid MySQL handle. If not, we simply
 * report an error, but can not be specific. RGerhards, 2007-01-30
 */
static void reportDBError(instanceData *pData, int bSilent)
{
	char errMsg[512];
	unsigned uMySQLErrno;

	ASSERT(pData != NULL);

	/* output log message */
	errno = 0;
	if(pData->f_hmysql == NULL) {
		errmsg.LogError(0, NO_ERRCODE, "unknown DB error occured - could not obtain MySQL handle");
	} else { /* we can ask mysql for the error description... */
		uMySQLErrno = mysql_errno(pData->f_hmysql);
		snprintf(errMsg, sizeof(errMsg)/sizeof(char), "db error (%d): %s\n", uMySQLErrno,
			mysql_error(pData->f_hmysql));
		if(bSilent || uMySQLErrno == pData->uLastMySQLErrno)
			dbgprintf("mysql, DBError(silent): %s\n", errMsg);
		else {
			pData->uLastMySQLErrno = uMySQLErrno;
			errmsg.LogError(0, NO_ERRCODE, "%s", errMsg);
		}
	}
		
	return;
}


/* The following function is responsible for initializing a
 * MySQL connection.
 * Initially added 2004-10-28 mmeckelein
 */
static rsRetVal initMySQL(instanceData *pData, int bSilent)
{
	DEFiRet;

	ASSERT(pData != NULL);
	ASSERT(pData->f_hmysql == NULL);

	pData->f_hmysql = mysql_init(NULL);
	if(pData->f_hmysql == NULL) {
		errmsg.LogError(0, RS_RET_SUSPENDED, "can not initialize MySQL handle");
		iRet = RS_RET_SUSPENDED;
	} else { /* we could get the handle, now on with work... */
		mysql_options(pData->f_hmysql,MYSQL_READ_DEFAULT_GROUP,((pData->f_configsection!=NULL)?(char*)pData->f_configsection:"client"));
		if(pData->f_configfile!=NULL){
			FILE * fp;
			fp=fopen((char*)pData->f_configfile,"r");
			int err=errno;
			if(fp==NULL){
				char msg[512];
				snprintf(msg,sizeof(msg)/sizeof(char),"Could not open '%s' for reading",pData->f_configfile);
				if(bSilent) {
					char errStr[512];
					rs_strerror_r(err, errStr, sizeof(errStr));
					dbgprintf("mysql configuration error(%d): %s - %s\n",err,msg,errStr);
				} else
					errmsg.LogError(err,NO_ERRCODE,"mysql configuration error: %s\n",msg);
			} else {
				fclose(fp);
				mysql_options(pData->f_hmysql,MYSQL_READ_DEFAULT_FILE,pData->f_configfile);
			}
		}
		/* Connect to database */
		if(mysql_real_connect(pData->f_hmysql, pData->f_dbsrv, pData->f_dbuid,
				      pData->f_dbpwd, pData->f_dbname, pData->f_dbsrvPort, NULL, 0) == NULL) {
			reportDBError(pData, bSilent);
			closeMySQL(pData); /* ignore any error we may get */
			iRet = RS_RET_SUSPENDED;
		}
	}

	RETiRet;
}


/* The following function writes the current log entry
 * to an established MySQL session.
 * Initially added 2004-10-28 mmeckelein
 */
rsRetVal writeMySQL(uchar *psz, instanceData *pData)
{
	DEFiRet;

	ASSERT(psz != NULL);
	ASSERT(pData != NULL);

	/* see if we are ready to proceed */
	if(pData->f_hmysql == NULL) {
		CHKiRet(initMySQL(pData, 0));
	}

	/* try insert */
	if(mysql_query(pData->f_hmysql, (char*)psz)) {
		/* error occured, try to re-init connection and retry */
		closeMySQL(pData); /* close the current handle */
		CHKiRet(initMySQL(pData, 0)); /* try to re-open */
		if(mysql_query(pData->f_hmysql, (char*)psz)) { /* re-try insert */
			/* we failed, giving up for now */
			reportDBError(pData, 0);
			closeMySQL(pData); /* free ressources */
			ABORT_FINALIZE(RS_RET_SUSPENDED);
		}
	}

finalize_it:
	if(iRet == RS_RET_OK) {
		pData->uLastMySQLErrno = 0; /* reset error for error supression */
	}

	RETiRet;
}


BEGINtryResume
CODESTARTtryResume
	if(pData->f_hmysql == NULL) {
		iRet = initMySQL(pData, 1);
	}
ENDtryResume

BEGINdoAction
CODESTARTdoAction
	dbgprintf("\n");
	iRet = writeMySQL(ppString[0], pData);
ENDdoAction


BEGINparseSelectorAct
	int iMySQLPropErr = 0;
CODESTARTparseSelectorAct
CODE_STD_STRING_REQUESTparseSelectorAct(1)
	/* first check if this config line is actually for us
	 * The first test [*p == '>'] can be skipped if a module shall only
	 * support the newer slection syntax [:modname:]. This is in fact
	 * recommended for new modules. Please note that over time this part
	 * will be handled by rsyslogd itself, but for the time being it is
	 * a good compromise to do it at the module level.
	 * rgerhards, 2007-10-15
	 */
	if(*p == '>') {
		p++; /* eat '>' '*/
	} else if(!strncmp((char*) p, ":ommysql:", sizeof(":ommysql:") - 1)) {
		p += sizeof(":ommysql:") - 1; /* eat indicator sequence  (-1 because of '\0'!) */
	} else {
		ABORT_FINALIZE(RS_RET_CONFLINE_UNPROCESSED);
	}

	/* ok, if we reach this point, we have something for us */
	CHKiRet(createInstance(&pData));

	/* rger 2004-10-28: added support for MySQL
	 * >server,dbname,userid,password
	 * Now we read the MySQL connection properties 
	 * and verify that the properties are valid.
	 */
	if(getSubString(&p, pData->f_dbsrv, MAXHOSTNAMELEN+1, ','))
		iMySQLPropErr++;
	if(*pData->f_dbsrv == '\0')
		iMySQLPropErr++;
	if(getSubString(&p, pData->f_dbname, _DB_MAXDBLEN+1, ','))
		iMySQLPropErr++;
	if(*pData->f_dbname == '\0')
		iMySQLPropErr++;
	if(getSubString(&p, pData->f_dbuid, _DB_MAXUNAMELEN+1, ','))
		iMySQLPropErr++;
	if(*pData->f_dbuid == '\0')
		iMySQLPropErr++;
	if(getSubString(&p, pData->f_dbpwd, _DB_MAXPWDLEN+1, ';'))
		iMySQLPropErr++;
	/* now check for template
	 * We specify that the SQL option must be present in the template.
	 * This is for your own protection (prevent sql injection).
	 */
	if(*(p-1) == ';')
		--p;	/* TODO: the whole parsing of the MySQL module needs to be re-thought - but this here
			 *       is clean enough for the time being -- rgerhards, 2007-07-30
			 */
	CHKiRet(cflineParseTemplateName(&p, *ppOMSR, 0, OMSR_RQD_TPL_OPT_SQL, (uchar*) " StdDBFmt"));
	
	/* If we detect invalid properties, we disable logging, 
	 * because right properties are vital at this place.  
	 * Retries make no sense. 
	 */
	if (iMySQLPropErr) { 
		errmsg.LogError(0, RS_RET_INVALID_PARAMS, "Trouble with MySQL connection properties. -MySQL logging disabled");
		ABORT_FINALIZE(RS_RET_INVALID_PARAMS);
	} else {
		pData->f_dbsrvPort = (unsigned) iSrvPort;	/* set configured port */
		pData->f_configfile = pszMySQLConfigFile;
		pData->f_configsection = pszMySQLConfigSection;
		pData->f_hmysql = NULL; /* initialize, but connect only on first message (important for queued mode!) */
	}

CODE_STD_FINALIZERparseSelectorAct
ENDparseSelectorAct


BEGINmodExit
CODESTARTmodExit
ENDmodExit


BEGINqueryEtryPt
CODESTARTqueryEtryPt
CODEqueryEtryPt_STD_OMOD_QUERIES
ENDqueryEtryPt


/* Reset config variables for this module to default values.
 */
static rsRetVal resetConfigVariables(uchar __attribute__((unused)) *pp, void __attribute__((unused)) *pVal)
{
	DEFiRet;
	iSrvPort = 0; /* zero is the default port */
	free(pszMySQLConfigFile);
	pszMySQLConfigFile = NULL;
	free(pszMySQLConfigSection);
	pszMySQLConfigSection = NULL;
	RETiRet;
}

BEGINmodInit()
CODESTARTmodInit
	*ipIFVersProvided = CURR_MOD_IF_VERSION; /* we only support the current interface specification */
CODEmodInit_QueryRegCFSLineHdlr
	CHKiRet(objUse(errmsg, CORE_COMPONENT));
	/* register our config handlers */
	CHKiRet(omsdRegCFSLineHdlr((uchar *)"actionommysqlserverport", 0, eCmdHdlrInt, NULL, &iSrvPort, STD_LOADABLE_MODULE_ID));
	CHKiRet(omsdRegCFSLineHdlr((uchar *)"resetconfigvariables", 1, eCmdHdlrCustomHandler, resetConfigVariables, NULL, STD_LOADABLE_MODULE_ID));
	CHKiRet(omsdRegCFSLineHdlr((uchar *)"ommysqlconfigfile",0,eCmdHdlrGetWord,NULL,&pszMySQLConfigFile,STD_LOADABLE_MODULE_ID));
	CHKiRet(omsdRegCFSLineHdlr((uchar *)"ommysqlconfigsection",0,eCmdHdlrGetWord,NULL,&pszMySQLConfigSection,STD_LOADABLE_MODULE_ID));
ENDmodInit

/* vi:set ai:
 */
