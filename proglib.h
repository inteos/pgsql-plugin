/*
 * Copyright (c) 2011 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.pgsql for details.
 *
 * Common definitions and utility functions for pgsql plugin.
 * Functions defines a common framework used in all utilities and plugins
 */

#ifndef _PROGLIB_H_
#define _PROGLIB_H_

#include "keylist.h"
#include "parseconfig.h"
#include "utils.hs"

/* definitions */
#define LOGMSGLEN    (6 + 32 + 7 + 24 + 1)

/* memory allocation/deallocation */
#define MALLOC(size) \
   (char *) malloc ( size );

#define FREE(ptr) \
   if ( ptr != NULL ){ \
      free ( ptr ); \
      ptr = NULL; \
   }

/* log levels (utils only) */
typedef enum {
   LOGERROR,
   LOGWARNING,
   LOGINFO,
} LOG_LEVEL_T;

/* uid/gid struct */
typedef struct _pgugid pgugid;
struct _pgugid {
   uid_t uid;
   gid_t gid;
};

/* progdata for util apps */
typedef struct _progdata progdata;

/* utilities functions */
void proginit ( int argc, char * argv[] );
char * get_program_name ( void );
char * get_program_directory ( void );
progdata * allocpdata ( void );
void freepdata ( progdata * pdata );
char * logstr ( char * msg, LOG_LEVEL_T level );
void logprg ( LOG_LEVEL_T level, const char * msg );
void abortprg ( pgsqldata * pdata, int err, const char * msg );

#endif /* _PROGLIB_H_ */
