/*
 * Copyright (c) 2012 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.Inteos for details.
 *
 * Common definitions and utility functions for Inteos utils.
 * Functions defines a common framework used in all utilities and plugins.
 */

#ifdef __cplusplus
extern "C" {
#endif
 
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>
#ifndef __WIN32__
 #include <grp.h>
#else
 #include <windef.h>
 #include <winbase.h>
#endif
#include "utils.h"

#if 0
/*
 * perform a file copy from src into dst
 * copy is performed by reading a file into memory buffer and wrining it into newly
 * created file
 *
 * in:
 *    src - source file
 *    dst - destination file
 * out:
 *    0 - success
 *    1 - error
 */
int _copy_file ( char * src, char * dst ){

   int fdsrc, fddst;
   int err;
   struct stat file_stat;
   void * filebuf;

   fddst = open ( dst, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR );
   if ( fddst < 0 ){
      logprg ( LOGERROR, "Destination WAL access problem:" );
      logprg ( LOGERROR, strerror ( errno ) );
      return 1;
   }

   err = stat ( src, &file_stat );
   if ( err ) {
      close ( fddst );
      return 1;
   }

   fdsrc = open ( src, O_RDONLY );
   if ( fdsrc < 0 ){
      close ( fddst );
      return 1;
   }

   /* TODO: check if mmaping a file instead of read/write will be more efective */
   filebuf = malloc ( file_stat.st_size );
   if ( ! filebuf ){
      close ( fddst );
      close ( fdsrc );
      return 1;
   }

   err = read ( fdsrc, filebuf, file_stat.st_size );
   if ( err != file_stat.st_size ){
      close ( fddst );
      close ( fdsrc );
      FREE ( filebuf );
      return 1;
   }

   err = write ( fddst, filebuf, file_stat.st_size );
   if ( err != file_stat.st_size ){
      FREE ( filebuf );
      close ( fddst );
      close ( fdsrc );
      return 1;
   }
   
   FREE ( filebuf );
   close ( fddst );
   close ( fdsrc );
   err = chown ( dst, file_stat.st_uid, file_stat.st_gid );

   return 0;
}
#endif

#ifndef __WIN32__
/*
 * checks if program instance is running
 * 
 * in:
 *    pidfile - program pid file
 * out:
 *    1 - is running
 *    0 - is not running
 *    -1 - error
 */
int check_program_is_running ( char * pidfile ){

   int pidfd;
   int err;
   int pid;
   int out;
   char buf[32];
   int rc = 0;
   struct stat st;

   ASSERT_NVAL_RET_NONE ( pidfile );
   
   err = stat ( pidfile, &st );
   ASSERT_VAL_RET_NONE ( err );

   pidfd = open ( pidfile, O_RDONLY );

   if ( pidfd > 0 ){
      /* file exist, check if program is running on that pid, if so
       * read contents (pid number encoded as ascii number) */
      err = read ( pidfd, buf, sizeof ( buf ) );
      if ( err > 2 ){
         /* minimal valid file found, fd no longer needed */
         close ( pidfd );
         /* check for pid number in the first line of the file */
         out = sscanf ( buf, "%i\n", &pid );
         if ( out == 1 ){
            /* valid pid number found, check process existence */
            //printf ( "PID %i\n", pid );
            snprintf ( buf, sizeof ( buf ), "/proc/%i/status", pid );
            pidfd = open ( buf, O_RDONLY );
            if ( pidfd > 0 ){
               close ( pidfd );
               rc = 1;
            }
         }
      } else {
         close ( pidfd );
         rc = 0;
      }
   }

   return rc;
}
#endif

/*
 * standard readline
 */
int readline ( int fd, char * buf, int size ){

   char c;
   int n;

   for ( n = 0; n < size; n++ ){
      if ( read ( fd, &c, 1 ) == 1 ){
         /* read a char */
         if ( c == '\n' ){
            buf [ n ] = 0;
            break;
         } else {
            buf [ n ] = c;
         }
      } else {
         buf [ n ] = 0;
         break;
      }
   }
   return n;
}

/*
 * standard readline for file stream
 */
int freadline ( FILE * stream, char * buf, int size ){

   char c;
   int n;

   for ( n = 0; n < size; n++ ){
      if ( fread ( &c, 1, 1, stream ) == 1 ){
         /* read a char */
         if ( c == '\n' ){
            buf [ n ] = 0;
            break;
         } else {
            buf [ n ] = c;
         }
      } else {
         buf [ n ] = 0;
         break;
      }
   }
   return n;
}

#if 0
/*
 * time format correction
 */
void _correct_time ( udtime_t * t ){

   if ( t->y < 70 ){
      t->y += 2000;
   } else
   if ( t->y < 100 ){
      t->y += 1900;
   }
   if ( t->m < 1 )
      t->m = 1;
   if ( t->m > 12 )
      t->m = 12;
   if ( t->d < 1 )
      t->d = 1;
   if ( t->d > 31 )
      t->d = 31;
   if ( t->h < 0 )
      t->h = 0;
   if ( t->h > 23 )
      t->h = 23;
   if ( t->mi < 0 )
      t->mi = 0;
   if ( t->mi > 59 )
      t->mi = 59;
   if ( t->s < 0 )
      t->s = 0;
   if ( t->s > 59 )
      t->s = 59;
}

/*
 * scans a supplied string for date&time definition, corrects it and prepare
 * a string for bacula restore
 * 
 * input:
 *    str - user supplied date/time string
 * output:
 *    computer interpreted date/time
 */
char * format_btime ( const char * str ){

   char * out;
   char buf[64];
   udtime_t t;

   memset ( &t, 0, sizeof ( t ) );
   sscanf(str, "%d-%d-%d %d:%d:%d", &t.y, &t.m, &t.d, &t.h, &t.mi, &t.s );
   _correct_time ( &t );
   snprintf ( buf, sizeof ( buf ), "%d-%02d-%02d %02d:%02d:%02d", t.y, t.m, t.d, t.h, t.mi, t.s );
   out = strndup ( buf, sizeof ( buf ) );

   return out;
}
#endif

#ifdef __sun__
/*
 * missing routine in Solaris
 */
int getgrouplist (const char *uname, gid_t agroup, gid_t *groups, int *grpcnt)
{
   const struct group *grp;
   int i, maxgroups, ngroups, ret;

   ret = 0;
   ngroups = 0;
   maxgroups = *grpcnt;
   /*
    * When installing primary group, duplicate it;
    * the first element of groups is the effective gid
    * and will be overwritten when a setgid file is executed.
    */
   groups ? groups[ngroups++] = agroup : ngroups++;
   if (maxgroups > 1)
      groups ? groups[ngroups++] = agroup : ngroups++;
 
   /*
    * Scan the group file to find additional groups.
    */
   setgrent();
   while ((grp = getgrent()) != NULL) {
      if (groups) {
         for (i = 0; i < ngroups; i++) {
            if (grp->gr_gid == groups[i])
               goto skip;
         }
      }

      for (i = 0; grp->gr_mem[i]; i++) {
         if (!strcmp(grp->gr_mem[i], uname)) {
            if (ngroups >= maxgroups) {
               ret = -1;
               break;
            }
            groups ? groups[ngroups++] = grp->gr_gid : ngroups++;
            break;
         }
      }
   
   skip:
    ;
   }

   endgrent();
   *grpcnt = ngroups;

   return (ret);
}
#endif

#ifdef __WIN32__
/* Win32 missing functions */

static const char *errorString ( void ){

   LPVOID lpMsgBuf;

   FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                 FORMAT_MESSAGE_FROM_SYSTEM |
                 FORMAT_MESSAGE_IGNORE_INSERTS,
                 NULL,
                 GetLastError(),
                 MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                 (LPTSTR) &lpMsgBuf,
                 0,
                 NULL);

   /* Strip any \r or \n */
   char *rval = (char *) lpMsgBuf;
   char *cp = strchr(rval, '\r');
   if (cp != NULL) {
      *cp = 0;
   } else {
      cp = strchr(rval, '\n');
      if (cp != NULL)
         *cp = 0;
   }

   return rval;
}

void *dlopen ( const char *file, int mode ){

   void *handle;

   handle = LoadLibrary ( file );
   return handle;
}

void *dlsym ( void *handle, const char *name ){

   void *symaddr;

   symaddr = (void*) GetProcAddress ((HMODULE)handle, name);
   return symaddr;
}

int dlclose ( void *handle ){

   if (handle && !FreeLibrary ((HMODULE) handle )){
      return 1;
   }
   return 0;
}

char *dlerror ( void ){

   static char buf[200];
   const char *err = errorString();

   strncpy ( buf, (char *) err, sizeof(buf) );
   LocalFree ( (void*) err);

   return buf;
}

/* realpath implementation grabbed from TC as public domain */
char *realpath ( const char *path, char resolved_path[PATH_MAX] ){

  char *return_path = NULL;

   if ( path ){
      if ( resolved_path ){
         return_path = resolved_path;
      } else {
         //Non standard extension that glibc uses
         return_path = (char*) malloc (PATH_MAX);
      }

      if ( return_path ){
         //This is a Win32 API function similar to what realpath() is supposed to do
         size_t size = GetFullPathNameA(path, PATH_MAX, return_path, 0);

         //GetFullPathNameA() returns a size larger than buffer if buffer is too small
         if ( size > PATH_MAX ){
            if ( return_path != resolved_path ){
               //Malloc'd buffer - Unstandard extension retry
               size_t new_size;
               free(return_path);
               return_path = (char*) malloc(size);

               if ( return_path ){
                  new_size = GetFullPathNameA(path, size, return_path, 0); //Try again
   
                  if ( new_size > size ){ //If it's still too large, we have a problem, don't try again
                     free(return_path);
                     return_path = 0;
                     errno = ENAMETOOLONG;
                  } else {
                     size = new_size;
                  }
               } else {
                  //I wasn't sure what to return here, but the standard does say to return EINVAL
                  //if resolved_path is null, and in this case we couldn't malloc large enough buffer
                  errno = EINVAL;
               }
            } else { //resolved_path buffer isn't big enough
               return_path = 0;
               errno = ENAMETOOLONG;
            }
         }

      //GetFullPathNameA() returns 0 if some path resolve problem occured
      if ( !size ){
         if ( return_path != resolved_path ){
            //Malloc'd buffer
            free ( return_path );
         }

         return_path = 0;

         //Convert MS errors into standard errors
         switch ( GetLastError() ){
            case ERROR_FILE_NOT_FOUND:
               errno = ENOENT;
               break;

            case ERROR_PATH_NOT_FOUND: case ERROR_INVALID_DRIVE:
               errno = ENOTDIR;
               break;

            case ERROR_ACCESS_DENIED:
               errno = EACCES;
               break;
          
            default: //Unknown Error
               errno = EIO;
               break;
         }
      }

      //If we get to here with a valid return_path, we're still doing good
      if ( return_path ){

         struct stat stat_buffer;

         //Make sure path exists, stat() returns 0 on success
         if ( stat ( return_path, &stat_buffer ) ){
            if ( return_path != resolved_path ){
               free(return_path);
            }
            return_path = 0;
            //stat() will set the correct errno for us
         }
         //else we succeeded!
      }
      } else {
         errno = EINVAL;
      }
   } else {
      errno = EINVAL;
   }
   return return_path;
}
#endif

/* 
 * checks if supplied string could be printed
 */
int strisprintable ( char * str, int len ){

   int a;

   for ( a = 0; a < len; a++ ){
      if ( ! isprint( str[a] ) ){
         return 0;
      }
   }
   return 1;
}

#ifdef __cplusplus
}
#endif
