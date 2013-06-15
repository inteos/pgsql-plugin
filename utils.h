/*
 * Copyright (c) 2012 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.Inteos for details.
 *
 * Common definitions and utility functions for Inteos utils.
 * Functions defines a common framework used in our utilities and plugins
 */

#ifndef _UTIL_H_
#define _UTIL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#ifdef __WIN32__
 #include <windef.h>
#endif

/* Assertions definitions */
/* check valid pointer if not simply return */
#ifndef ASSERT_NVAL_RET
#define ASSERT_NVAL_RET(value) \
   if ( ! value ){ \
      return; \
   }
#endif

/* check an error if so return */
#ifndef ASSERT_VAL_RET
#define ASSERT_VAL_RET(value) \
   if ( value ){ \
      return; \
   }
#endif

/* check valid pointer with Null return */
#ifndef ASSERT_NVAL_RET_NULL
#define ASSERT_NVAL_RET_NULL(value) \
   if ( ! value ) \
   { \
      return NULL; \
   }
#endif

/* if value then Null return */
#ifndef ASSERT_VAL_RET_NULL
#define ASSERT_VAL_RET_NULL(value) \
   if ( value ) \
   { \
      return NULL; \
   }
#endif

/* check valid pointer with int/err return */
#ifndef ASSERT_NVAL_RET_ONE
#define ASSERT_NVAL_RET_ONE(value) \
   if ( ! value ) \
   { \
      return 1; \
   }
#endif

/* check valid pointer with int/err return */
#ifndef ASSERT_NVAL_RET_NONE
#define ASSERT_NVAL_RET_NONE(value) \
   if ( ! value ) \
   { \
      return -1; \
   }
#endif

/* check error if not exit with error */
#ifndef ASSERT_NVAL_EXIT_ONE
#define ASSERT_NVAL_EXIT_ONE(value) \
   if ( ! value ){ \
      exit ( 1 ); \
   }
#endif

/* check error if not return zero */
#ifndef ASSERT_NVAL_RET_ZERO
#define ASSERT_NVAL_RET_ZERO(value) \
   if ( ! value ){ \
      return 0; \
   }
#endif

/* check error if not return value */
#ifndef ASSERT_NVAL_RET_V
#define ASSERT_NVAL_RET_V(value,rv) \
   if ( ! value ){ \
      return rv; \
   }
#endif

/* checks error value then int/err return */
#ifndef ASSERT_VAL_RET_ONE
#define ASSERT_VAL_RET_ONE(value) \
   if ( value ) \
   { \
      return 1; \
   }
#endif

/* checks error value then int/err return */
#ifndef ASSERT_VAL_RET_NONE
#define ASSERT_VAL_RET_NONE(value) \
   if ( value ) \
   { \
      return -1; \
   }
#endif

/* checks error value then exit one */
#ifndef ASSERT_VAL_EXIT_ONE
#define ASSERT_VAL_EXIT_ONE(value) \
   if ( value ) \
   { \
      exit (1); \
   }
#endif

/* check error if not return zero */
#ifndef ASSERT_VAL_RET_ZERO
#define ASSERT_VAL_RET_ZERO(value) \
   if ( value ){ \
      return 0; \
   }
#endif

/* check error if not return value */
#ifndef ASSERT_VAL_RET_V
#define ASSERT_VAL_RET_V(value,rv) \
   if ( value ){ \
      return rv; \
   }
#endif

#ifndef __WIN32__
/* uid/gid struct */
typedef struct _uugid_t uugid_t;
struct _uugid_t {
   uid_t uid;
   gid_t gid;
};
#endif

/*
 * date/time verification function structs
 */
typedef struct _udtime_t udtime_t;
struct _udtime_t {
   int y;
   int m;
   int d;
   int h;
   int mi;
   int s;
};

/* utilities functions */
#ifndef __WIN32__
int check_program_is_running ( char * pidfile );
#endif
int readline ( int fd, char * buf, int size );
int freadline ( FILE * stream, char * buf, int size );
//char * format_btime ( const char * str );
int strisprintable ( char * str, int len );
#ifdef __sun__
int getgrouplist (const char *uname, gid_t agroup, gid_t *groups, int *grpcnt);
#endif
#ifdef __WIN32__
#define RTLD_LAZY 0
#ifndef PATH_MAX
 #define PATH_MAX MAX_PATH
#endif
void *dlopen ( const char *file, int mode );
void *dlsym ( void *handle, const char *name );
int dlclose ( void *handle );
char *dlerror ( void );
char *realpath(const char *path, char resolved_path[PATH_MAX]);
#endif

#ifdef __cplusplus
}
#endif

#endif /* _UTIL_H_ */
