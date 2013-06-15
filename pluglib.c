/*
 * Copyright (c) 2012 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.Inteos for details.
 *
 * Common utility functions for Inteos plugins.
 * Functions defines a common framework used in all utilities and plugins.
 */

#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "bacula.h"
#include "fd_plugins.h"
#include "pluglib.h"

#if 0
/*
 * XXX: from unknown reason struct restore_pkt (esp. struct stat) differ in size and
 * alligment which produce SEGV
 */
/*
 * Check if 'Where' is set then we have to remove it from restored filename.
 * This is a very stupid Bacula BUG, but Kern doesn't see it, what a shame !!!
 * It works as designed and Bacula Team doesn't want to change it because 
 * they has to fix all existing plugins and perform a regression tests, but
 * no one want to do it. We have to workaround a plugin API design mismatch!
 */
char * check_ofname ( struct restore_pkt *rp ){

   char * ofname;
   int len;

   ofname = (char *)rp->ofname;
   if ( rp->where ){
      len = strlen ( rp->where );
      if ( len ){
         ofname += len + 1;
      }
   }
   return ofname;
}

/*
 * providing missing bstrndup function
 */
char * bstrndup ( char * str, unsigned int n ){

   char * buf = NULL;

   if ( !str ){
      return NULL;
   }
   buf = (char *) malloc ( n + 1 );
   if ( !buf ){
      return NULL;
   }

   if ( n ){
      strncpy ( buf, str, n + 1 );
   }

   buf [ n ] = '\0';

   return buf;
}
#endif
