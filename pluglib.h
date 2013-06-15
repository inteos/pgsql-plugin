/*
 * Copyright (c) 2013 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.Inteos for details.
 *
 * Common definitions and utility functions for Inteos plugins.
 * Functions defines a common framework used in our utilities and plugins
 */

#ifndef _PLUGLIB_H_
#define _PLUGLIB_H_

#include <sys/stat.h>
#include <sys/types.h>
#include <ctype.h>

#include "bacula.h"
#include "utils.h"
#include "ipclib.h"

/* definitions */

/* size of different string or sql buffers */
#define CONNSTRLEN   256
#define BUFLEN       1024
#define BIGBUFLEN    65536
#define LOGMSGLEN    (6 + 32 + 7 + 24 + 1)

/* Assertions definitions */
#define ASSERT_bfuncs_RET_BRCERROR \
   if ( ! bfuncs ) \
   { \
      return bRC_Error; \
   }

#define ASSERT_ctx_RET_BRCERROR \
   if ( ! ctx ) \
   { \
      return bRC_Error; \
   }

#define ASSERT_ctxp_RET \
   if ( ! ctx || ! ctx->pContext ) \
   { \
      return; \
   }

#define ASSERT_ctxp_RET_BRCERROR \
   if ( ! ctx || ! ctx->pContext ) \
   { \
      return bRC_Error; \
   }

#define ASSERT_ctxp_RET_NULL \
   if ( ! ctx || ! ctx->pContext ) \
   { \
      return NULL; \
   }

/* check valid pointer with bRC return */
#define ASSERT_NVAL_RET_BRCERROR(value)   ASSERT_bp(value)
#define ASSERT_NVAL_RET_BRCERR(value)     ASSERT_bp(value)
#define ASSERT_bp(value) \
   if ( ! value ) \
   { \
      return bRC_Error; \
   }

/* check a valid pointer with bRC return */ 
#define ASSERT_NVAL_RET_BRCOK(value) \
   if ( ! value ) \
   { \
      return bRC_OK; \
   }

/* check valid pointer if not exit with error */
#define ASSERT_NVAL_EXIT_BRCERR(value) ASSERT_bpex(value)
#define ASSERT_bpex(value) \
   if ( ! value ){ \
      exit ( bRC_Error ); \
   }

/* checks error value then bRC return */
#define ASSERT_VAL_RET_BRCERR(value)   ASSERT_bn(value)
#define ASSERT_bn(value) \
   if ( value ) \
   { \
      return bRC_Error; \
   }

/* memory allocation/deallocation */
/* use bacula->malloc function */
#define MALLOC(size) \
   (char *) malloc ( size );

#define MALLOCT(size,type) \
   (typeof(type)*) malloc ( size );
   
/* use bacula->free function */
#define FREE(ptr) \
   if ( ptr != NULL ){ \
      free ( ptr ); \
      ptr = NULL; \
   }

/* debug and messages functions */
#define JMSG0(ctx,type,msg) \
      bfuncs->JobMessage ( ctx, __FILE__, __LINE__, type, 0, PLUGIN_INFO msg );

#define JMSG(ctx,type,msg,var) \
      bfuncs->JobMessage ( ctx, __FILE__, __LINE__, type, 0, PLUGIN_INFO msg, var );

#define JMSG2(ctx,type,msg,var1,var2) \
      bfuncs->JobMessage ( ctx, __FILE__, __LINE__, type, 0, PLUGIN_INFO msg, var1, var2 );

#define DMSG0(ctx,level,msg) \
      bfuncs->DebugMessage ( ctx, __FILE__, __LINE__, level, PLUGIN_INFO msg );

#define DMSG1(ctx,level,msg,var) \
      bfuncs->DebugMessage ( ctx, __FILE__, __LINE__, level, PLUGIN_INFO msg, var );

#define DMSG2(ctx,level,msg,var1,var2) \
      bfuncs->DebugMessage ( ctx, __FILE__, __LINE__, level, PLUGIN_INFO msg, var1, var2 );

#define DMSG3(ctx,level,msg,var1,var2,var3) \
      bfuncs->DebugMessage ( ctx, __FILE__, __LINE__, level, PLUGIN_INFO msg, var1, var2, var3 );

#define DMSG4(ctx,level,msg,var1,var2,var3,var4) \
      bfuncs->DebugMessage ( ctx, __FILE__, __LINE__, level, PLUGIN_INFO msg, var1, var2, var3, var4 );

/* fixed debug level definitions */
#define D1  1     /* debug for every error */
#define DERROR D1
#define D2  10    /* debug only important stuff */
#define DINFO  D2
#define D3  100   /* debug for information only */
#define DDEBUG D3

/* functions */
//char * check_ofname ( struct restore_pkt *rp );
//int strisprintable ( char * str, int len );
//char * bstrndup ( char * str, unsigned int n );

#endif /* _INTEOSLIB_H_ */
