/*
 * Copyright (c) 2013 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.Inteos for details.
 * 
 * Parseconfig utility functions are used for parsing simple configfile and prepare a key/value
 * parameters list (using keylist linked list implementation). Config file consist of a one
 * parameter per line with template of key = value. Any line started by '#' sign indicate a remark.
 * Any spaces available in key string will be removed, same as with a value string unless a value
 * string will be surrounded by '"' signs (quotation marks).
 */

#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include "parseconfig.h"
#include "pluglib.h"

/*
 * providing missing bstrndup function
 */
char * bstrndup ( char * str, uint n ){

   char * buf = NULL;

   if ( !str ){
      return NULL;
   }
   buf = (char *) malloc ( n + 1 );
   if ( !buf ){
      return NULL;
   }

   if ( n ){
      bstrncpy ( buf, str, n + 1 );
   }
   
   buf [ n ] = '\0';

   return buf;
}

/*
 *
 */
char * open_config ( const char * config ){

   char * buf;
   int err;
   int len;
   int nread;
   int fd;
   struct stat config_stat;

   err = stat ( config ? config : CONFIG_FILE, &config_stat);

   if (err){
//    printf ("WARNING: config file not found!\n");
      return NULL;
   }

   len = config_stat.st_size;

// printf ("parse: config length=%i\n", config_len);

   buf = (char *) malloc ( len + 1 );
   if ( ! buf ){
      /* change to perror */
      printf ( "out of memory\n" );
      return NULL;
   }

   fd = open ( config ? config : CONFIG_FILE, O_RDONLY );

   if (fd < 0){
      /* change to perror */
      printf ("ERROR on open!\n");
      free ( buf );
      return NULL;
   }

// printf ("parse: fd=%i\n", fd);

   nread = read (fd, buf, len);

   if ( nread != len ){
      /* change to perror */
      printf ("ERROR reading config file!\n");
      free ( buf );
      return NULL;
   }

   /* Terminate file with a nul sign (\0) */
   buf [ len ] = 0;

// printf ("parse: buf_read=%i\n", buf_read);

// printf("BUF:\n%s\nBUFEND\n", config_buf);

   err = close ( fd );

   if (err){
      /* change to perror */
      printf ("ERROR on close!\n");
      free ( buf );
      return NULL;
   }

   return buf;
}

void close_config ( char * config_buf ){

   free ( config_buf );
}

/*
 * Function deletes all remarks which begin with 'REMCHAR' to the end of the line by
 * replacing chars with a :space:' ' char. Spaces will be deleted in remove_whitespaces function.
 * TODO: add a procedure for delete an empty lines
 */
void remove_remarks (char * buf){

   char *s1, *s2;

   s1 = buf;
   while ( s1 ){
      s1 = strstr ( s1, REMCHAR );
      if ( s1 ) {
//       printf ("parse: Remark '%s' found at:\n%s\n", REMCHAR, s1);
         s2 = strstr ( s1, "\n" );
//       printf ("parse: Remark end at:\n%s\n", s2);
         for (;s1 < s2;s1++)
            *s1 = ' ';
      }
   }
}

/*
 * Function delete :space:' ' chars from config file (located in buf) excluding parameters
 * with parenthesis '"'. Functions should be called _after_ remove_remarks to clean up the mess.
 * TODO: add deleting of empty lines
 */
void remove_whitespaces (char * buf){

   char *s1, *s2;
   int len;
   int instring = 0;

   s1 = buf;

   while ( *s1 ){
      if ( !instring && (isblank (*s1) || *s1 == '\r')){
         s2 = s1 + 1;
         len = strlen ( s2 );

         memcpy ( s1, s2, len );

//       printf ("-----------\n%s\n--------------\n", s1);
      } else
      if ( *s1 == '"' ){
         if (!instring){
            instring = 1;
         } else {
            instring = 0;
         }
         s1++;
      }
/*    else
      if ( *s1 == '\n' ){

      }
*/    else {
         s1++;
      }
   }
}

/*
 * Funkcja parsująca plik konfiguracyjny w pamieci (* buf) w celu rozdzielenia
 * nazw parametrów ( key ) od wartości im przypisanych. Pary nazw parametrów i
 * ich wartości są następnie przypisywane do listy parametrów 'keylist' i
 * zwracane jako wynik działania. Wyłuskane nazwy parametrów jak i ich wartości
 * są duplikowane, dzięki czemu bufor (* buf) można swobodnie kasować i zwolnić.
 */
keylist * parse_config_buf ( char * buf ){

   char * a = buf;
   char * line;
   char * nl, * delim;
   char * k, * v;
   keylist * list = NULL;
   int cstart = 0;
   int cend = 0;
   int len;

   while ( *a ){
      nl = strchr ( a, '\n' );
      if ( ! nl )
         break;

      if ( nl - a ){
         line = bstrndup ( a, nl - a );

         delim = strchr ( line, '=' );

         if ( delim ){
            k = bstrndup ( line, delim - line );
            delim++;
            len = strlen ( delim );
            if ( *delim == '"' )
               cstart = 1;
            if ( delim[len - 1] == '"' )
               cend = 1;
            v = bstrndup ( delim + cstart, len - cstart - cend);
   
            list = add_keylist ( list, k, v );
   
            cstart = cend = 0;
            free ( k );
            free ( v );
         }

         free ( line );
      }
      a = nl + 1;
   }
   return list;
}

keylist * parse_config ( char * buf ){

   remove_remarks ( buf );
   remove_whitespaces ( buf );
   return parse_config_buf ( buf );
}

keylist * parse_config_file ( const char * config ){

   char * buf;
   keylist * param;

   buf = open_config ( config );
   if ( ! buf )
      return NULL;

   param = parse_config ( buf );
   free ( buf );

   return param;
}
