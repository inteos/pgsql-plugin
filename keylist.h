/*
 * Copyright (c) 2012 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.Inteos for details.
 * 
 * KeyList is a linked list implementation (using dlist from libbac) with searcheable
 * key attribute. It is used in utility functions and plugins. List item consist of three 
 * atributes: key - string, value - string and attrs - number
 */

#ifndef _KEYLIST_H_
#define _KEYLIST_H_

#ifndef __WIN32__
 #include "bacula.h"
#endif

#define KEY_NO_ATTR     (-1)
#define KEY_NOT_FOUND   (-2)
//#define KEY_ATTR_MASK   (0xFFFFFF)

/* keylist is a dlist object, prepare an alias for it */
typedef dlist keylist;
typedef struct _keyitem keyitem;

/* main keyitem structure */
struct _keyitem {
   char * key;
   char * value;
   int   attrs;
   dlink link;
};

keyitem * new_keyitem ( const char * key, const char * value, const int attrs );
keylist * new_keylist ( const char * key, const char * value, const int attrs );
void print_keylist ( keylist * list );
keylist * add_keylist ( keylist * list, const char * key, const char * value );
keylist * add_keylist_attr ( keylist * list, const char * key, const char * value, const int attrs );
keylist * add_keylist_item ( keylist * list, keyitem * add );
void keylist_free ( keylist * list );
char * search_key ( keylist * list, const char * key );
int search_key_attr ( keylist * list, const char * key );

#endif /* _KEYLIST_H_ */
