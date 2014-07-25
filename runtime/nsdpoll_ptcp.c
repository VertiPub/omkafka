/* nsdpoll_ptcp.c
 *
 * An implementation of the nsd epoll() interface for plain tcp sockets.
 * 
 * Copyright 2009 Rainer Gerhards and Adiscon GmbH.
 *
 * This file is part of the rsyslog runtime library.
 *
 * The rsyslog runtime library is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The rsyslog runtime library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with the rsyslog runtime library.  If not, see <http://www.gnu.org/licenses/>.
 *
 * A copy of the GPL can be found in the file "COPYING" in this distribution.
 * A copy of the LGPL can be found in the file "COPYING.LESSER" in this distribution.
 */
#include "config.h"

#ifdef HAVE_EPOLL_CREATE /* this module requires epoll! */

#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#if HAVE_SYS_EPOLL_H
#	include <sys/epoll.h>
#endif

#include "rsyslog.h"
#include "module-template.h"
#include "obj.h"
#include "errmsg.h"
#include "srUtils.h"
#include "nspoll.h"
#include "nsd_ptcp.h"
#include "nsdpoll_ptcp.h"

/* static data */
DEFobjStaticHelpers
DEFobjCurrIf(errmsg)
DEFobjCurrIf(glbl)


/* -START------------------------- helpers for event list ------------------------------------ */

/* add new entry to list. We assume that the fd is not already present and DO NOT check this!
 * Returns newly created entry in pEvtLst.
 * Note that we currently need to use level-triggered mode, because the upper layers do not work
 * in parallel. As such, in edge-triggered mode we may not get notified, because new data comes
 * in after we have read everything that was present. To use ET mode, we need to change the upper
 * peers so that they immediately start a new wait before processing the data read. That obviously
 * requires more elaborate redesign and we postpone this until the current more simplictic mode has
 * been proven OK in practice.
 * rgerhards, 2009-11-18
 */
static inline rsRetVal
addEvent(nsdpoll_ptcp_t *pThis, int id, void *pUsr, int mode, nsd_ptcp_t *pSock, nsdpoll_epollevt_lst_t **pEvtLst) {
	nsdpoll_epollevt_lst_t *pNew;
	DEFiRet;

	CHKmalloc(pNew = (nsdpoll_epollevt_lst_t*) malloc(sizeof(nsdpoll_epollevt_lst_t)));
	pNew->id = id;
	pNew->pUsr = pUsr;
	pNew->pSock = pSock;
	pNew->event.events = 0; /* TODO: at some time we should be able to use EPOLLET */
	if(mode & NSDPOLL_IN)
		pNew->event.events |= EPOLLIN;
	if(mode & NSDPOLL_OUT)
		pNew->event.events |= EPOLLOUT;
	pNew->event.data.u64 = (uint64) pNew;
	pNew->pNext = pThis->pRoot;
	pThis->pRoot = pNew;
	*pEvtLst = pNew;

finalize_it:
	RETiRet;
}


/* find and unlink the entry identified by id/pUsr from the list.
 * rgerhards, 2009-11-23
 */
static inline rsRetVal
unlinkEvent(nsdpoll_ptcp_t *pThis, int id, void *pUsr, nsdpoll_epollevt_lst_t **ppEvtLst) {
	nsdpoll_epollevt_lst_t *pEvtLst;
	nsdpoll_epollevt_lst_t *pPrev = NULL;
	DEFiRet;

	pEvtLst = pThis->pRoot;
	while(pEvtLst != NULL && !(pEvtLst->id == id && pEvtLst->pUsr == pUsr)) {
		pPrev = pEvtLst;
		pEvtLst = pEvtLst->pNext;
	}
	if(pEvtLst == NULL)
		ABORT_FINALIZE(RS_RET_NOT_FOUND);

	*ppEvtLst = pEvtLst;

	/* unlink */
	if(pPrev == NULL)
		pThis->pRoot = pEvtLst->pNext;
	else
		pPrev->pNext = pEvtLst->pNext;

finalize_it:
	RETiRet;
}


/* destruct the provided element. It must already be unlinked from the list.
 * rgerhards, 2009-11-23
 */
static inline rsRetVal
delEvent(nsdpoll_epollevt_lst_t **ppEvtLst) {
	DEFiRet;
	free(*ppEvtLst);
	*ppEvtLst = NULL;
	RETiRet;
}


/* -END--------------------------- helpers for event list ------------------------------------ */


/* Standard-Constructor
 */
BEGINobjConstruct(nsdpoll_ptcp) /* be sure to specify the object type also in END macro! */
#if defined(EPOLL_CLOEXEC) && defined(HAVE_EPOLL_CREATE1)
	DBGPRINTF("nsdpoll_ptcp uses epoll_create1()\n");
	pThis->efd = epoll_create1(EPOLL_CLOEXEC);
	if(pThis->efd < 0 && errno == ENOSYS)
#endif
	{
		DBGPRINTF("nsdpoll_ptcp uses epoll_create()\n");
		pThis->efd = epoll_create(100); /* size is ignored in newer kernels, but 100 is not bad... */
	}

	if(pThis->efd < 0) {
		DBGPRINTF("epoll_create1() could not create fd\n");
		ABORT_FINALIZE(RS_RET_IO_ERROR);
	}
finalize_it:
ENDobjConstruct(nsdpoll_ptcp)


/* destructor for the nsdpoll_ptcp object */
BEGINobjDestruct(nsdpoll_ptcp) /* be sure to specify the object type also in END and CODESTART macros! */
CODESTARTobjDestruct(nsdpoll_ptcp)
ENDobjDestruct(nsdpoll_ptcp)


/* Modify socket set */
static rsRetVal
Ctl(nsdpoll_t *pNsdpoll, nsd_t *pNsd, int id, void *pUsr, int mode, int op) {
	nsdpoll_ptcp_t *pThis = (nsdpoll_ptcp_t*) pNsdpoll;
	nsd_ptcp_t *pSock = (nsd_ptcp_t*) pNsd;
	nsdpoll_epollevt_lst_t *pEventLst;
	int errSave;
	char errStr[512];
	DEFiRet;

	if(op == NSDPOLL_ADD) {
		dbgprintf("adding nsdpoll entry %d/%p, sock %d\n", id, pUsr, pSock->sock);
		CHKiRet(addEvent(pThis, id, pUsr, mode, pSock, &pEventLst));
		if(epoll_ctl(pThis->efd, EPOLL_CTL_ADD,  pSock->sock, &pEventLst->event) < 0) {
			errSave = errno;
			rs_strerror_r(errSave, errStr, sizeof(errStr));
			errmsg.LogError(errSave, RS_RET_ERR_EPOLL_CTL,
				"epoll_ctl failed on fd %d, id %d/%p, op %d with %s\n",
				pSock->sock, id, pUsr, mode, errStr);
		}
	} else if(op == NSDPOLL_DEL) {
		dbgprintf("removing nsdpoll entry %d/%p, sock %d\n", id, pUsr, pSock->sock);
		CHKiRet(unlinkEvent(pThis, id, pUsr, &pEventLst));
		if(epoll_ctl(pThis->efd, EPOLL_CTL_DEL, pSock->sock, &pEventLst->event) < 0) {
			errSave = errno;
			rs_strerror_r(errSave, errStr, sizeof(errStr));
			errmsg.LogError(errSave, RS_RET_ERR_EPOLL_CTL,
				"epoll_ctl failed on fd %d, id %d/%p, op %d with %s\n",
				pSock->sock, id, pUsr, mode, errStr);
			ABORT_FINALIZE(RS_RET_ERR_EPOLL_CTL);
		}
		CHKiRet(delEvent(&pEventLst));
	} else {
		dbgprintf("program error: invalid NSDPOLL_mode %d - ignoring request\n", op);
		ABORT_FINALIZE(RS_RET_ERR);
	}

finalize_it:
	RETiRet;
}


/* Wait for io to become ready. After the successful call, idRdy contains the
 * id set by the caller for that i/o event, ppUsr is a pointer to a location
 * where the user pointer shall be stored.
 * TODO: this is a trivial implementation that only polls one event at a time. We
 *       may later extend it to poll for multiple events, what would cause less 
 *       overhead.
 * rgerhards, 2009-11-18
 */
static rsRetVal
Wait(nsdpoll_t *pNsdpoll, int timeout, int *idRdy, void **ppUsr) {
	nsdpoll_ptcp_t *pThis = (nsdpoll_ptcp_t*) pNsdpoll;
	nsdpoll_epollevt_lst_t *pOurEvt;
	struct epoll_event event;
	int nfds;
	DEFiRet;

	assert(idRdy != NULL);
	assert(ppUsr != NULL);

	nfds = epoll_wait(pThis->efd, &event, 1, timeout);
	if(nfds == -1) {
		if(errno == EINTR) {
			ABORT_FINALIZE(RS_RET_EINTR);
		} else {
			DBGPRINTF("epoll() returned with error code %d\n", errno);
			ABORT_FINALIZE(RS_RET_ERR_EPOLL);
		}
	} else if(nfds == 0) {
		ABORT_FINALIZE(RS_RET_TIMEOUT);
	}

	/* we got a valid event, so tell the caller... */
	pOurEvt = (nsdpoll_epollevt_lst_t*) event.data.u64;
	*idRdy = pOurEvt->id;
	*ppUsr = pOurEvt->pUsr;

finalize_it:
	RETiRet;
}


/* ------------------------------ end support for the epoll() interface ------------------------------ */


/* queryInterface function */
BEGINobjQueryInterface(nsdpoll_ptcp)
CODESTARTobjQueryInterface(nsdpoll_ptcp)
	if(pIf->ifVersion != nsdCURR_IF_VERSION) {/* check for current version, increment on each change */
		ABORT_FINALIZE(RS_RET_INTERFACE_NOT_SUPPORTED);
	}

	/* ok, we have the right interface, so let's fill it
	 * Please note that we may also do some backwards-compatibility
	 * work here (if we can support an older interface version - that,
	 * of course, also affects the "if" above).
	 */
	pIf->Construct = (rsRetVal(*)(nsdpoll_t**)) nsdpoll_ptcpConstruct;
	pIf->Destruct = (rsRetVal(*)(nsdpoll_t**)) nsdpoll_ptcpDestruct;
	pIf->Ctl = Ctl;
	pIf->Wait = Wait;
finalize_it:
ENDobjQueryInterface(nsdpoll_ptcp)


/* exit our class
 */
BEGINObjClassExit(nsdpoll_ptcp, OBJ_IS_CORE_MODULE) /* CHANGE class also in END MACRO! */
CODESTARTObjClassExit(nsdpoll_ptcp)
	/* release objects we no longer need */
	objRelease(glbl, CORE_COMPONENT);
	objRelease(errmsg, CORE_COMPONENT);
ENDObjClassExit(nsdpoll_ptcp)


/* Initialize the nsdpoll_ptcp class. Must be called as the very first method
 * before anything else is called inside this class.
 * rgerhards, 2008-02-19
 */
BEGINObjClassInit(nsdpoll_ptcp, 1, OBJ_IS_CORE_MODULE) /* class, version */
	/* request objects we use */
	CHKiRet(objUse(errmsg, CORE_COMPONENT));
	CHKiRet(objUse(glbl, CORE_COMPONENT));

	/* set our own handlers */
ENDObjClassInit(nsdpoll_ptcp)
#endif /* #ifdef HAVE_EPOLL_CREATE this module requires epoll! */

/* vi:set ai:
 */
