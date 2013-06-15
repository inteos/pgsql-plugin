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

#ifndef _PARSECONFIG_H_
#define _PARSECONFIG_H_

#include "keylist.h"

#define CONFIG_FILE	"config.conf"
#define REMCHAR		"#"

char * open_config ( const char * config );
void close_config ( char * config_buf );
//void remove_remarks (char * buf);
//void remove_whitespaces (char * buf);
keylist * parse_config ( char * buf );
keylist * parse_config_file ( const char * config );

#endif
