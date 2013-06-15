/*
 * Copyright (c) 2013 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.Inteos for details.
 *
 * Common definitions and utility functions for SYS-V IPC communications.
 */

#ifndef _IPCLIB_H_
#define _IPCLIB_H_

#ifdef __cplusplus
extern "C" {
#endif

/* definitions */
#define MSGBUFLEN 1024

/*
 * IPC MSG Buffer
 */
typedef struct _ipc_msg_buf_t ipc_msg_buf_t;
struct _ipc_msg_buf_t {
   long mtype;
   char mtext [ MSGBUFLEN ];
};

/* Message Queues */
int ipcl_msg_init ( char * path, int prg );
int ipcl_msg_send ( int msqid, uint type, const char * message );
int ipcl_msg_recv ( int msqid, int type, char * message );
void ipcl_msg_shutdown ( int msqid );
/* Shared Memory */
int ipcl_shm_init ( char * path, int prg, int size );
void * ipcl_shm_get ( int shmid );
int ipcl_shm_rel ( void * shmaddr );
void ipcl_shm_shutdown ( int shmid );

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* _IPCLIB_H_ */
