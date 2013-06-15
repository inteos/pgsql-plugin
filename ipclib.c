/*
 * Copyright (c) 2012 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.Inteos for details.
 *
 * Common definitions and utility functions for SYS-V IPC communications.
 */
/*
 * TODO:
 * - change error logging
 */

#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include "ipclib.h"
/* assertions only */
#include "utils.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * message queue (IPC) initialization
 * 
 * input:
 *    path - unique identifier for a queue
 *    prg - unique identifier for a key
 * output:
 *    msqid - on success, message queue id
 *    -1 - on error
 */
int ipc_msg_init ( char * path, int prg ){

   key_t key;     /* key to be passed to msgget() */ 
   int msgflg;    /* msgflg to be passed to msgget() */
   int msqid;     /* return value from msgget() */ 

   ASSERT_NVAL_RET_NONE ( path );

   key = ftok ( path, prg );
   if ( key == -1 ){
      /* TODO: change error logging */
      perror ( " msgget failed" );
      return -1;
   }

   msgflg = IPC_CREAT | 0666 ;

   if ( ( msqid = msgget ( key, msgflg ) ) == -1 ){
      /* TODO: change error logging */
      perror ( "msgget failed" );
      return -1;
   }

   return msqid;
}

/*
 * message queue send
 * 
 * input:
 *    msqid - message queue id
 *    type - message type
 *    message - message string
 * output:
 *    0 - on success
 *    -1 - on error
 */
int ipc_msg_send ( int msqid, uint type, const char * message ){

   ipc_msg_buf_t msg;
   int err;

   ASSERT_NVAL_RET_NONE ( type );
   ASSERT_NVAL_RET_NONE ( message );

   msg.mtype = type;
   strncpy ( msg.mtext, message, MSGBUFLEN );

   err = msgsnd ( msqid, &msg, sizeof ( ipc_msg_buf_t ), 0 );
//   printf ( "message sent(%i): %s\n", type, message );

   return err;
}

/*
 * message queue receive
 * 
 * in:
 *    msqid - message queue id
 *    type - message type
 *    message - message buffer at least MSGBUFLEN bytes
 * out:
 *    -1 - on error
 *    number of received 
 */
int ipc_msg_recv ( int msqid, int type, char * message ){

   ipc_msg_buf_t msg;
   int err;

   ASSERT_NVAL_RET_NONE ( type );

   err = msgrcv ( msqid, &msg, sizeof ( ipc_msg_buf_t ), type, 0 );
   if ( err != -1 ){
      strncpy ( message, msg.mtext, MSGBUFLEN );
   } else {
      message[0] = '\0';
   }
//   printf ( "message received(%i): %s\n", type, message );

   return err;
}

/*
 * message queue shutdown
 *
 * input:
 *    msgid - message queue id
 * output:
 *    none
 */
void ipc_msg_shutdown ( int msqid ){

   msgctl ( msqid, IPC_RMID, NULL );
}

/*
 * shared memory buffer (IPC) initialization
 * 
 * input:
 *    path - unique identifier for a key
 *    prg - unique identifier for a key
 *    size - size of a shared memory buffer to initialize
 * output:
 *    shmid - on success, shared memory id
 *    -1 - on error
 */
int ipc_shm_init ( char * path, int prg, int size ){

   key_t key;     /* key to be passed to shmget() */ 
   int shmflg;    /* shmflg to be passed to shmget() */
   int shmid;     /* return value from shmget() */ 

   ASSERT_NVAL_RET_NONE ( path );
   ASSERT_NVAL_RET_NONE ( size );

   key = ftok ( path, prg );
   if ( key == -1 ){
      /* TODO: change error logging */
      perror ( " shmget failed" );
      return -1;
   }

   shmflg = IPC_CREAT | 0666 ;

   if ( ( shmid = shmget ( key, size, shmflg ) ) == -1 ){
      /* TODO: change error logging */
      perror ( "shmget failed" );
      return -1;
   }

   return shmid;
}

/*
 * shared memory buffer (IPC) segment attach and get
 *
 * input:
 *    shmid - shared memory id
 * output:
 *    shmaddr - address to shared memory segment
 */
void * ipcl_shm_get ( int shmid ){

   void * shmaddr;   /* return value from shmat */
   int shmflg;       /* shmflg to be passed to shmget() */

   ASSERT_VAL_RET_NULL ( shmid < 0 );

   shmflg = SHM_RND;
   if ( ( shmaddr = shmat ( shmid, NULL, shmflg ) ) == -1 ){
      /* TODO: change error logging */
      perror ( "shmat failed" );
      return NULL;
   }

   return shmaddr;
}

/*
 * shared memory (IPC) release
 *
 * input:
 *    shmaddr - shared memory segment address
 *             (grabbed from ipcl_shm_get)
 * output:
 *    '0'  - on success
 *    '-1' - on error
 */
int ipcl_shm_rel ( void * shmaddr ){

   ASSERT_NVAL_RET_NONE ( shmaddr );

   return shmdt ( shmaddr );
}

/*
 * shared memory shutdown and removal
 *
 * input:
 *    shmid - shared memory id
 * output:
 *    none
 */
void ipc_shm_shutdown ( int shmid ){

   shmctl ( shmid, IPC_RMID, NULL );
}

#ifdef __cplusplus
}
#endif
