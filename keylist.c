/*
 * Copyright (c) 2013 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.Inteos for details.
 * 
 * KeyList is a linked list implementation (using dlist from libbac) with searcheable
 * key attribute. It is used in utility functions and plugins. List item consist of three 
 * atributes: key - string, value - string and attrs - number
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "keylist.h"

/*
 * prints keylist - for debuging
 */
void print_keylist ( keylist * list ){

   keyitem * ki;

   foreach_dlist ( ki, list ){
      printf ( "%p: K(%p)[%s] V(%p)[%s] A[%i]\n", ki, ki->key, ki->key, ki->value, ki->value, ki->attrs);
   }
}

/*
 * frees memory allocated for keyitem
 */
void keyitem_free ( keyitem * item ){

   if ( item ){
      if ( item->key )
         free ( item->key );
      if ( item->value )
         free ( item->value );
//      free ( item );
   }
}

/*
 * create new keyitem element with required values: key, value, attrs,
 * without connections to other nodes.
 * in:
 *    key, value and attrs
 * out:
 *    keyitem on success
 *    NULL on error
 */
keyitem * new_keyitem ( const char * key, const char * value, const int attrs ){

   keyitem * item;

   /* allocate item */
   item = (keyitem *) malloc ( sizeof ( keyitem ) );
   if ( ! item ){
      /* no memory error? */
      return NULL;
   }
   /* reset all variables */
   memset ( item, 0, sizeof ( keyitem ) );
   /* produce an item from supplied data */
   item->key = key ? bstrdup(key) : NULL;
   item->value = value ? bstrdup(value) : NULL;
   item->attrs = attrs;

   return item;
}

/*
 * create a new keylist with supplied keyitem
 * in:
 *    key, value and attrs
 * out:
 *    keylist on success
 *    NULL on error
 */ 
keylist * new_keylist ( keyitem * item ){

   keylist * list;

   /* create a list */
   list = New ( keylist ( item, &item->link ) );
   if ( ! list ){
      /* no memory error? */
      keyitem_free(item);
      return NULL;
   }

   return list;
}

/*
 * create a new keylist with new keyitem allocated from supplied data
 * in:
 *    key, value and attrs
 * out:
 *    keylist on success
 *    NULL on error
 */ 
keylist * new_keylist ( const char * key, const char * value, const int attrs){

   keylist * list;
   keyitem * item;

   /* allocate item */
   item = new_keyitem ( key, value, attrs );
   if ( ! item ){
      /* no memory error? */
      return NULL;
   }

   /* create a list */
   list = New ( keylist ( item, &item->link ) );
   if ( ! list ){
      /* no memory error? */
      keyitem_free(item);
      return NULL;
   }

   return list;
}

/*
 * appends an item to the list
 * if list is null then create a new list
 * in:
 *    keyitem do add
 * out:
 *    old list or newly created
 */
keylist * add_keylist_item ( keylist * list, keyitem * add ){

   keylist * nl = list;

   if ( ! list ){
      nl = New ( keylist ( add, &add->link ) );
   }

   if ( add ){
      nl->append ( add );
   }

   /* return list or add as a new list */
   return nl;
}

/* 
 * add new keyitem structure do the linked list pointed by list
 * in:
 *    key, value, attrs
 * out:
 *    keylist
 */
keylist * add_keylist_attr ( keylist * list, const char * key, const char * value, const int attrs){

   keyitem * item;
   keylist * l;

   /* allocate item */
   item = new_keyitem ( key, value, attrs );
   if ( ! item ){
      /* no memory error? */
      return NULL;
   }

   l = add_keylist_item ( list, item );

   return l;
}

/* 
 * add new keylist structure do the linked list pointed by list
 * in:
 *    key, value
 * out:
 *    keylist
 */
keylist * add_keylist ( keylist * list, const char * key, const char * value){

   keylist * nl;

   nl = add_keylist_attr ( list, key, value, KEY_NO_ATTR );

   return nl;
}


/* 
 * frees all nodes of a keylist including every key and value. suplied keylist pointer cannot
 * be used.
 */
void keylist_free ( keylist * list ){

   keyitem * ki;

   if ( list ){
      foreach_dlist ( ki, list ){
         keyitem_free ( ki );
      }
      list->destroy();
   }
}

/*
 * searches for a value of supplied key. it return only first occurance of a key. search is
 * not case sensitive, so values of "Key" or "KEY" are returned
 */
char * search_key ( keylist * list, const char * key ){

   keyitem * item;

   if ( list ){
      foreach_dlist ( item, list ){
         if ( !strcasecmp ( key, item->key ) ){
            return item->value;
         }
      }
   }
   return NULL;
}

/*
 * searches for a attrs of supplied key. it return only first occurance of a key. search is
 * not case sensitive, so attrs of "Key" or "KEY" are returned
 */
int search_key_attr ( keylist * list, const char * key ){

   keyitem * item;

   if ( list ){
      foreach_dlist ( item, list ){
         if ( !strcasecmp ( key, item->key ) ){
            return item->attrs;
         }
      }
   }

   return KEY_NOT_FOUND;
}
