/*
 * Copyright (c) 2011 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.pgsql for details.
 *
 * Common utility functions for pgsql plugin.
 * Functions defines a common framework used in all utilities and plugins
 */
/*
TODO:
   * more framework integration with pgsql-fd plugin
   * change framework name and some functions into pluglib
   * add functions for mysql and oracle databases
   * prepare a docummentation about plugin
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
#include "config.h"
#include "pgsqllib.h"

#ifdef __cplusplus
extern "C" {
#endif

/* variables required for application named messages */
static char * program_name = NULL;
static char * program_directory = NULL;

/* 
 * library initialization function
 * in:
 *    argc, argv - application envinronment variables
 */
/* 
 * curently it assigns argv[0] into program_name variable only, future enchancement possible
 */
void pgsqllibinit ( int argc, char * argv[] ){

   char * prg;
   char * nprg;
   char * dirtmp;
   
   prg = bstrdup ( argv[0] );
   nprg = basename ( prg );
   program_name = bstrdup ( nprg );
   FREE ( prg );
   
   dirtmp = MALLOC ( PATH_MAX );
   if ( realpath ( argv[0], dirtmp ) == NULL ){
      /* error in resolving path */
      FREE ( dirtmp );
      dirtmp = argv[0];
   }
   program_directory = bstrdup ( dirtmp );
   
   FREE ( dirtmp );
}

/* returns a pointer into program name variable */
char * get_program_name ( void ){

   return program_name;   
}

/* returns a pointer into a stored full pathname of program name */
char * get_program_directory ( void ){

   return program_directory;   
}

/* allocates and resets a main program data variable */
pgsqldata * allocpdata ( void ){

   pgsqldata * pdata;

   pdata = (pgsqldata *) malloc ( sizeof ( pgsqldata ) );
   memset ( pdata, 0, sizeof ( pgsqldata ) );
   
   return pdata;
}

/* releases all allocated program data resources */
void freepdata ( pgsqldata * pdata ){

   keylist_free ( pdata->paramlist );
   if ( pdata->configfile )
      FREE ( pdata->configfile );
   if ( pdata->walfilename )
      FREE ( pdata->walfilename );
   if ( pdata->pathtowalfilename )
      FREE ( pdata->pathtowalfilename );

   free ( pdata );
}

/* allocated and resets a main plugin instance variable */
pgsqlpinst * allocpinst ( void ){

   pgsqlpinst * pinst;

   pinst = (pgsqlpinst *) malloc ( sizeof( pgsqlpinst ) );
   if ( pinst == NULL ){
      return NULL;
   }

   /* initialize pinst contents */
   memset ( pinst, 0, sizeof ( pgsqlpinst ) );
   pinst->JobId = -1;
   pinst->mode = PGSQL_NONE;

   return pinst;
}

/* releases all allocated plugin instance resources */
void freepinst ( pgsqlpinst * pinst ){

   if ( pinst->configfile )
      FREE ( pinst->configfile );
   if ( pinst->linkval )
      FREE ( pinst->linkval );
   keylist_free ( pinst->paramlist );
   keylist_free ( pinst->filelist );

   free ( pinst );
}

/*
 * logstr - render head of log string into msg. return msg. msg have to be at least LOGMSGLEN
 * long.
 * level: [LOGERROR, LOGWARNING, LOGINFO]
 */
char * logstr ( char * msg, LOG_LEVEL_T level ){

   time_t t;
   char * lvl;
   char mtime[ 24 + 1 ];
   /*
    * msg in format: '%date %level:  %program_name: ' = 6 chars
    * %program_name no more then 32 chars
    * %level as follows: '[ERROR|WARNING|INFO]' = max 7 chars
    * %date as follows: 'YYYY-MM-DD HH:MM:SS TTZZ' = 24 chars
    * + '\0'
    */
   /* if msg == NULL, there is no place to render log message */
   if ( !msg )
      return NULL;

   /* current time */
   time ( &t );

   /* render current time in required format into msg */
   strftime ( mtime, LOGMSGLEN, "%F %T %Z", localtime ( &t ) );

   /* choose log level string */
   switch (level){
   case LOGERROR:
      lvl = (char *) "ERROR";
      break;
   case LOGWARNING:
      lvl = (char *) "WARNING";
      break;
   case LOGINFO:
      lvl = (char *) "INFO";
      break;
   default:
      lvl = (char *) "Unknown";
   }

   /* render all info into msg: '%date %level:  %program_name: ' */
   snprintf ( msg, LOGMSGLEN, "%s %s:  %s: ", mtime, lvl, program_name );

   return msg;
}

/*
 * logprg prints msg at log level of level
 * 
 * in:
 *    level - a level number consisted with LOG_LEVEL_T
 *    msg - a log message
 * out:
 *    log displayed on stdout
 */
void logprg ( LOG_LEVEL_T level, const char * msg ){

   char s[LOGMSGLEN];
   char * copy;
   char * start = (char *)msg;
   char * end;
   
   logstr ( s, level );
   /* first we search for end line characters */
   end = strchr ( start, '\n' );
   if ( end ){
      /* if found we need to erase them */
      copy = MALLOC ( strlen ( msg ) + 1 );
      while ( ( end = strchr ( start, '\n' )) ){
         /* end line chars erase */
         if ( start != end ){
            strncpy ( copy, start, end - start );
            copy [ end - start ] = '\0';
            printf ( "%s %s\n", s, copy );         
         }
         start = end + 1;
      }
      FREE ( copy );
   } else {
      /* if not found simply display it */
      printf ( "%s %s\n", s, msg );
   }
}

/*
 * abortprg prints msg at a log level of LOGERROR and exits program with error code of err.
 *
 * in:
 *    pdata - main program data
 *    err - error code
 *    msg - error message
 * out:
 *    program abort and exit
 */
void abortprg ( pgsqldata * pdata, int err, const char * msg ){

   char h[LOGMSGLEN];

   /* Program aborting always generate ERROR message */
   logstr ( h, LOGERROR );
   printf ( "%s %s\n", h, msg );
   printf ( "%s Aborting!\n", h );

   if ( pdata->catdb )
      PQfinish ( pdata->catdb );
   freepdata ( pdata );
   exit (err);
}

/*
 * checking for plugin schema version
 * in:
 *    db - database connection handle
 *    version - required schema version
 * out
 *    1 - schema version match
 *    0 - schema version invalid or error
 */
int check_schema_version ( PGconn * db, int version ){

   ConnStatusType status;
   PGresult * result;
   int pver;

   result = PQexec ( db, "select versionid from pgsql_version" );

   if ( PQresultStatus ( result ) != PGRES_TUPLES_OK ){
      logprg ( LOGERROR, "CATDB: SQL Exec error!" );
      return 0;
   }

   if ( PQntuples ( result ) ){
      pver = atoi ((char *) PQgetvalue ( result, 0, PQfnumber ( result, "versionid") ));
   } else {
      logprg ( LOGERROR, "CATDB: No schema version!" );
      return 0;
   }
   if ( pver == version ){
      return 1;
   } else {
      return 0;
   }
}

/*
 * connect to pgsql catalog database
 *
 * in:
 *    paramlist - list of parameters from config file
 *       required params: CATDBHOST, CATDBPORT, CATDB, CATUSER, CATPASSWD
 * out:
 *    on success: PGconn * - working catalog connection
 *    on error:   NULL
 */
PGconn * catdbconnect ( keylist * paramlist ){

   char * catdbconnstring = NULL;
   PGconn * conndb = NULL;
   ConnStatusType status;

   catdbconnstring = MALLOC ( CONNSTRLEN );
   ASSERT_p ( catdbconnstring );

   snprintf ( catdbconnstring, CONNSTRLEN, "host=%s port=%s dbname=%s user=%s password=%s",
         search_key ( paramlist, "CATDBHOST" ),
         search_key ( paramlist, "CATDBPORT" ),
         search_key ( paramlist, "CATDB" ),
         search_key ( paramlist, "CATUSER" ),
         search_key ( paramlist, "CATPASSWD" ));

   conndb = PQconnectdb ( catdbconnstring );
   FREE ( catdbconnstring );

   status = PQstatus ( conndb );
   if ( status == CONNECTION_BAD ){
      return NULL;
   }
   /* check for schema version number */
   if ( !check_schema_version ( conndb, 1 )){
      printf ( "Schema version error\n" );
   }

   return conndb;
}

/* 
 * parse configfile, if invalid config found, setup defaults
 * 
 * in:
 *    configfile - path and name of the pgsql.conf file
 * out:
 *    keylist of parameters
 */
keylist * parse_pgsql_conf ( char * configfile ){

   keylist * paramlist;

   paramlist = parse_config_file ( configfile );

   if ( ! paramlist ){
      logprg ( LOGINFO, "Config file not found, using defaults." );
   }

   if ( ! search_key ( paramlist, "CATDB" ) )
      paramlist = add_keylist ( paramlist, "CATDB", "catdb" );
   if ( ! search_key ( paramlist, "CATDBHOST" ) )
      paramlist = add_keylist ( paramlist, "CATDBHOST", "localhost" );
   if ( ! search_key ( paramlist, "CATDBPORT" ) )
      paramlist = add_keylist ( paramlist, "CATDBPORT", "5432" );
   if ( ! search_key ( paramlist, "CATUSER" ) )
      paramlist = add_keylist ( paramlist, "CATUSER", "catdbuser" );
   if ( ! search_key ( paramlist, "CATPASSWD" ) )
      paramlist = add_keylist ( paramlist, "CATPASSWD", "catdpasswd" );
   if ( ! search_key ( paramlist, "ARCHDEST" ) )
      paramlist = add_keylist ( paramlist, "ARCHDEST", "/tmp" );
   if ( ! search_key ( paramlist, "ARCHCLIENT" ) )
      paramlist = add_keylist ( paramlist, "ARCHCLIENT", "catdb" );
   if ( ! search_key ( paramlist, "DIRNAME" ) )
      paramlist = add_keylist ( paramlist, "DIRNAME", "director" );
   if ( ! search_key ( paramlist, "DIRHOST" ) )
      paramlist = add_keylist ( paramlist, "DIRHOST", "localhost" );
   if ( ! search_key ( paramlist, "DIRPORT" ) )
      paramlist = add_keylist ( paramlist, "DIRPORT", "9101" );
   if ( ! search_key ( paramlist, "DIRPASSWD" ) )
      paramlist = add_keylist ( paramlist, "DIRPASSWD", "dirpasswd" );

   return paramlist;
}

#if 0
/* XXX: function integration from pgsql-fd.c */
/* perform an internal connection to the database in separate process and execute sql
 * statement */
bRC pg_internal_conn ( bpContext *ctx, const char * sql ){

   PGconn * db;
   ConnStatusType status;
   PGresult * result;
   int pid = 0;
   struct stat st;
   uid_t pguid;
   int err;
   char connstring[CONNSTRLEN];
   pgsqlpinst * pinst;
   bRC exitstatus = bRC_OK;

   /* check input data */
   ASSERT_ctx_p;
   pinst = (pgsqlpinst *)ctx->pContext;

   /* dynamic production database owner verification, we use it do connect to production
    * database. Required PGDATA from config file */
   ASSERT_p ( pinst->paramlist );
   err = stat ( search_key ( pinst->paramlist, "PGDATA" ) , &st );
   if ( err ){
      /* error, invalid PGDATA in config file */
//FIXME      printf ( PLUGIN_INFO "invalid 'PGDATA' variable in config file.\n" );
      return bRC_Error;
   }
   /* we will fork to the owner of PGDATA database cluster */
   pguid = st.st_uid;

   /* switch pg xlog in different process, so make a fork */
   pid = fork ();
   if ( pid == 0 ){
//      printf ( PLUGIN_INFO "forked process\n" );
      /* sleep used for debuging forked process in gdb */
      // sleep ( 60 );

      /* we are in forked process, do a real job */
      /* switch to pgcluster owner (postgres) */
      err = seteuid ( pguid );
      if ( err ){
         /* error switching uid, report a problem */
         /* TODO: add an errorlog display */
//FIXME         printf ( PLUGIN_INFO "seteuid to uid=%i failed!\n", pguid );
         exit (bRC_Error);
      }
      /* host is a socket directory, port is a socket 'port', we perform an 'internal'
       * connection through a postgresql socket, which is required by plugin */
      snprintf ( connstring, CONNSTRLEN, "host=%s port=%s",
            search_key ( pinst->paramlist, "PGHOST" ),
            search_key ( pinst->paramlist, "PGPORT" ) );

      db = PQconnectdb ( connstring );
      status = PQstatus ( db );
      if ( status == CONNECTION_BAD ){
         /* TODO: add an errorlog display */
//FIXME         printf ( PLUGIN_INFO "conndb failed!\n");
         exit (bRC_Error);
      }

      /* we have a successful production database connection, so execute sql */
      result = PQexec ( db, sql );
      if ( PQresultStatus ( result ) != PGRES_TUPLES_OK ){
         /* TODO: add an errorlog display */
//FIXME         printf ( PLUGIN_INFO "pg_internal_conn failed! (%s)\n", sql);
         exit (bRC_Error);
      }

      /* finish database connection */
      PQfinish ( db );

      /* finish forked process */
      exit (bRC_OK);
   } else {
      /* we are waiting for background process to finish */
      waitpid ( pid, (int*)&exitstatus, 0 );
   }

   /* XXX: we should check for forked process exit status */

   return exitstatus;
}
#endif

/* 
 * recurence function for building file list
 * 
 * in:
 *    list - current filename list
 *    base - indicate if filename path is relative or absolute
 *           '$ROOT$' - path is absolute, no need to concatenate both
 *           other => absolute filename path = $base/$path
 *    path - filename path (file or directory)
 * out:
 *    list->key - full pathname to backup file
 *    list->value - relative filename
 */
keylist * get_file_list ( keylist * list, const char * base, const char * path ){

   int err;
   struct stat st;
   DIR * dirp;
   DIR * dirp_tab;
   char * npath = NULL;
   char * bpath = NULL;
   char * link = NULL;
   keylist * nlist = list;
   struct dirent * filedir;
   struct dirent * filedir_tab;
   int dl;
   int plen = strlen ( path );


   bpath = MALLOC ( PATH_MAX );
   if ( ! bpath )
      return list;

   if ( strncmp ( base, "$ROOT$", PATH_MAX ) == 0 ){
      /* indicate a path variable include absolute filename path */
      strncpy ( bpath, path, PATH_MAX );
   } else {
      /* bpath has a real and absolute path into a filename as $base/$path,
       * path has a relative filename path
       * first concatenate a path for normalise */
      snprintf ( bpath, PATH_MAX, "%s/%s",
            base, path );
   }
   /* bpath has a full path for analized filename/directory */
   err = lstat ( bpath, &st );

   if ( err ){
      printf ( "stat error on %s.\n", path );
      FREE ( bpath );
      return list;
   }

   if ( S_ISDIR (st.st_mode) ){
//      printf ( "found: %s\n", bpath );
//      printf ( "add dir to the list\n" );

      if ( strncmp ( base, "$ROOT$", PATH_MAX ) == 0 ){
         nlist = add_keylist_attr ( nlist, base, bpath, PG_DIR );
      } else {
         //nlist = add_keylist ( nlist, bpath, plen ? path : "$BASE$" );
         nlist = add_keylist_attr ( nlist, bpath, path, PG_DIR );
      }

      dirp = opendir ( bpath );
      if ( !dirp ){
         FREE ( bpath );
         return nlist;
      }

      while ( ( filedir = readdir ( dirp ) ) ){
         /* check for tablespaces when path is empty and base has a absolute path */
         if ( strcmp ( filedir->d_name, "pg_tblspc" ) == 0 && plen == 0 ){

            /* add all tablespaces into a list */
            nlist = get_file_list ( nlist, base, "pg_tblspc" );

            /* building an absolute path for base variable */
            snprintf ( bpath, PATH_MAX, "%s/pg_tblspc", base );

            dirp_tab = opendir ( bpath );

            if ( !dirp_tab ){
               FREE ( bpath );
               return nlist;
            }

            /* all tablespaces are symbolic links into a destination location */
            while ( (filedir_tab = readdir ( dirp_tab )) ){
               if ( strcmp ( filedir_tab->d_name, "." ) != 0 &&
                    strcmp ( filedir_tab->d_name, ".." ) != 0 ){

//                  printf ("tablespace found: %s\n", filedir_tab->d_name );

                  link = MALLOC ( PATH_MAX );
                  if ( ! link ){
                     FREE ( bpath );
                     return nlist;
                  }

                  /* building an absolute path for a link */
                  snprintf ( bpath, PATH_MAX, "%s/pg_tblspc/%s",
                        base, filedir_tab->d_name );

                  dl = readlink ( bpath, link, PATH_MAX - 1 );
                  if ( dl < 0 ){
                     printf ("ERR: %s\n", strerror ( errno ) );
                     FREE ( bpath );
                     FREE ( link );
                     closedir ( dirp_tab );
                     return nlist;
                  }
                  link [ dl ] = 0;
//                  printf ( "dl %i, %s\n", dl, link );

                  /* check if link is absolute or not */
                  if ( link [ 0 ] == '/' ){
                     strncpy ( bpath, link, PATH_MAX );
                  } else {
                     /* according to PostgreSQL a tablespace location has to be an absolute,
                      * if not we have an error, raise it. */
                     printf ( "Relative link Error!\n" );
                     FREE ( bpath );
                     FREE ( link );
                     closedir ( dirp_tab );
                     return nlist;
                     //snprintf ( bpath, PATH_MAX, "%s/pg_tblspc/%s",
                     //      base, link );
                  }

                  /* all tablespaces are backuped with absolute path */
                  link = realpath ( bpath, link );

//                  printf ( "filelink %s\n", link );

                  nlist = get_file_list ( nlist, "$ROOT$", link );

                  FREE ( link );
               }
            }
            closedir ( dirp_tab );
         } else {
//            printf ("found: %s\n", filedir->d_name );
            if ( strcmp ( filedir->d_name, "." ) != 0 &&
                 strcmp ( filedir->d_name, ".." ) != 0 &&
                 strcmp ( filedir->d_name, "pg_xlog" ) != 0
                 ){

               npath = MALLOC ( PATH_MAX );
               snprintf ( npath, PATH_MAX, plen ? "%s/%s" : "%s%s",
                     path, filedir->d_name );
               nlist = get_file_list ( nlist, base, npath );
               FREE ( npath );
            } else {
//               printf ("ignored\n");
            }
         }
      }
      closedir ( dirp );
   } else
   if ( S_ISLNK ( st.st_mode ) ){
      /* indicate filetype of link */
      if ( strncmp ( base, "$ROOT$", PATH_MAX ) == 0 ){
         nlist = add_keylist_attr ( nlist, base, path, PG_LINK );
      } else {
         nlist = add_keylist_attr ( nlist, bpath, path, PG_LINK );
      }
   } else
   if ( S_ISREG ( st.st_mode ) ){
      /* indicate filetype of regular file */
      if ( strncmp ( base, "$ROOT$", PATH_MAX ) == 0 ){
         nlist = add_keylist_attr ( nlist, base, path, PG_FILE );
      } else {
         nlist = add_keylist_attr ( nlist, bpath, path, PG_FILE );
      }
   }
   FREE ( bpath );
   return nlist;
}

/*
 * searches at catdb for rowid of input data
 * 
 * in:
 *    pdata->paramlist,"ARCHCLIENT"
 *    pdata->walfilename
 * out:
 *    >= 0 - vaule of id for selected wal and client
 *    < 0 - value not found
 */
int _get_walid_from_catalog ( pgsqldata * pdata ){

   int pgid = -1;
   char * sql;
   PGresult * result;

   sql = MALLOC ( SQLLEN );
   snprintf ( sql, SQLLEN, "select id from pgsql_archivelogs where client='%s' and filename='%s'",
         search_key ( pdata->paramlist, "ARCHCLIENT" ),
         pdata->walfilename );

   result = PQexec ( pdata->catdb, sql );
   FREE ( sql );

   if ( PQresultStatus ( result ) != PGRES_TUPLES_OK ){
      abortprg ( pdata, 6, "CATDB: SQL Exec error!" );
   }

   if ( PQntuples ( result ) ){
      /* we found a row and pgid will be a primary key of the row */
      pgid = atoi ((char *) PQgetvalue ( result, 0, PQfnumber ( result, "id") ));
   }

   return pgid;
}

/*
 * searches at catdb for rowid of input data
 * 
 * in:
 *    pdata->paramlist,"ARCHCLIENT"
 *    pdata->walfilename
 * out:
 *    >= 0 - on success, status vaule for selected wal and client
 *    < 0 - value not found or error
 */
int _get_walstatus_from_catalog ( pgsqldata * pdata ){

   int pgstatus = -1;
   char * sql;
   PGresult * result;

   sql = MALLOC ( SQLLEN );
   snprintf ( sql, SQLLEN, "select status from pgsql_archivelogs where client='%s' and filename='%s'",
         search_key ( pdata->paramlist, "ARCHCLIENT" ),
         pdata->walfilename );

   result = PQexec ( pdata->catdb, sql );
   FREE ( sql );

   if ( PQresultStatus ( result ) != PGRES_TUPLES_OK ){
      return -1;
   }

   if ( PQntuples ( result ) ){
      /* we found a row and pgid will be a primary key of the row */
      pgstatus = atoi ((char *) PQgetvalue ( result, 0, PQfnumber ( result, "status") ));
   }

   return pgstatus;
}

/*
 * inserts a status value into a catalog
 * 
 * in:
 *    pdata->paramlist,"ARCHCLIENT"
 *    pdata->walfilename
 *    status - status number to insert into catdb
 * out:
 *    pgid - id of inserted row
 *    >= 0 - on success, vaule of id for selected wal and client
 *    < 0 - on error
 */
int _insert_status_in_catalog ( pgsqldata * pdata, int status ){

   char * sql;
   PGresult * result;
   int pgid;

   sql = MALLOC ( SQLLEN );
   /* insert status in catalog */
   snprintf ( sql, SQLLEN, "insert into pgsql_archivelogs (client, filename, status) values ('%s', '%s', '%i')",
         search_key ( pdata->paramlist, "ARCHCLIENT" ),
         pdata->walfilename, status );

   result = PQexec ( pdata->catdb, sql );
   FREE ( sql );
   
   if ( PQresultStatus ( result ) != PGRES_COMMAND_OK ){
      return -1;
   }

   pgid = _get_walid_from_catalog ( pdata );

   return pgid;
}

/*
 * updates a status column onto a catalog
 * 
 * input:
 *    pdata - primary data
 *    pgid - pgsql_archivelog ID for updating
 *    status - status number to update on catdb
 * output:
 *    0 - on success
 *    1 - on error
 */
int _update_status_in_catalog ( pgsqldata * pdata, int pgid, int status ){

   char * sql;
   PGresult * result;
   
   sql = MALLOC ( SQLLEN );
   /* update status in catalog */
   snprintf ( sql, SQLLEN, "update pgsql_archivelogs set status = '%i', mod_date = now() where id='%i'",
         status, pgid );

   result = PQexec ( pdata->catdb, sql );
   FREE ( sql );

   if ( PQresultStatus ( result ) != PGRES_COMMAND_OK ){
      return 1;
   }

   return 0;
}

/*
 * perform wal file copy from pdata->pathtowalfilename into
 * $ARCHDEST/pdata->walfilename
 * copy is performed by reading wal file into memory buffer and wrining it into newly
 * created file
 *
 * in:
 *    pdata - primary data
 * out:
 *    0 - success
 *    1 - error
 */
int _copy_wal_file ( pgsqldata * pdata, char * src, char * dst ){

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

/*
 * checks if PostgreSQL instance is running
 * 
 * in:
 *    pdata - program data
 *    pgdataloc - PGDATA location
 * out:
 *    1 - is running
 *    0 - is not running
 *    -1 - error
 */
int _check_postgres_is_running ( pgsqldata * pdata, char * pgdataloc ){

   int pidfd;
   int err;
   int pid;
   int out;
   char * pidcont;
   char * buf;
   int rc = 0;
   struct stat st;

   buf = MALLOC ( BUFLEN );
   ASSERT_NVAL_RET_NONE ( buf );
   
   pidcont = MALLOC ( 64 );
   ASSERT_NVAL_RET_NONE ( pidcont );

   snprintf ( buf, BUFLEN, "%s/postmaster.pid", pgdataloc );

   err = stat ( buf, &st );
   ASSERT_VAL_RET_NONE ( err );

   pidfd = open ( buf, O_RDONLY );

   if ( pidfd > 0 ){
      /* file exist, check if postmaster is running on that pid, if so
       * read contents (pid number encoded as ascii number) */
      err = read ( pidfd, pidcont, 64 );
      if ( err > 2 ){
         /* minimal valid file found, fd no longer needed */
         close ( pidfd );
         /* check for pid number in the first line of the file */
         out = sscanf ( pidcont, "%i\n", &pid );
         if ( out == 1 ){
            /* valid pid number found, check process existence */
            //printf ( "PID %i\n", pid );
            snprintf ( buf, BUFLEN, "/proc/%i/status", pid );
            pidfd = open ( buf, O_RDONLY );
            if ( pidfd > 0 ){
               close ( pidfd );
               rc = 1;
            }
         }
      }
   }

   FREE ( pidcont );
   FREE ( buf );

   return rc;
}


/* 
 * a pg_ctl location list
 *    for later supplement
 */
const char * pgctlpaths[] =
{  "/usr/bin/pg_ctl",
   "/usr/sbin/pg_ctl",
   "/usr/local/bin/pg_ctl",
   "/usr/local/sbin/pg_ctl",
   "/usr/local/postgresql/bin/pg_ctl",
   "/usr/local/postgresql/8.0/bin/pg_ctl",
   "/usr/local/postgresql/8.1/bin/pg_ctl",
   "/usr/local/postgresql/8.2/bin/pg_ctl",
   "/usr/local/postgresql/8.3/bin/pg_ctl",
   "/usr/local/postgresql/8.4/bin/pg_ctl",
   "/usr/local/postgresql/9.0/bin/pg_ctl",
   "/usr/lib/postgresql/bin/pg_ctl",
   "/usr/lib/postgresql/8.0/bin/pg_ctl",
   "/usr/lib/postgresql/8.1/bin/pg_ctl",
   "/usr/lib/postgresql/8.2/bin/pg_ctl",
   "/usr/lib/postgresql/8.3/bin/pg_ctl",
   "/usr/lib/postgresql/8.4/bin/pg_ctl",
   "/usr/lib/postgresql/9.0/bin/pg_ctl",
   /* Solaris locations */
   "/usr/lib/postgres/8.0/bin/pg_ctl",
   "/usr/lib/postgres/8.1/bin/pg_ctl",
   "/usr/lib/postgres/8.2/bin/pg_ctl",
   "/usr/lib/postgres/8.3/bin/pg_ctl",
   "/usr/lib/postgres/8.4/bin/pg_ctl",
   "/usr/lib/postgres/9.0/bin/pg_ctl",
   NULL
};

/*
 * searches for pg_ctl location
 * 
 * in:
 *    pdata - program data
 * out:
 *    absolute path location
 */
const char * find_pgctl ( pgsqldata * pdata ){

   int i;
   int err;
   const char * pgctl = NULL;
   struct stat st;

   /* searching for pg_ctl location */
   for ( i = 0; pgctlpaths[i]; i++ ){
      //printf ( "%s\n", pgctlpaths[i] );
      err = stat ( pgctlpaths[i] , &st );
      if ( ! err ){
         pgctl = pgctlpaths[i];
         break;
      }
   }

   return pgctl;
}

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

/*
 * message queue (IPC) initialization
 * 
 * input:
 *    pdata->paramlist[PGDATA]
 * output:
 *    msqid - on success, message queue id
 *    -1 - on error
 */
int pgsql_msg_init ( pgsqldata * pdata, int prg ){

   key_t key;     /* key to be passed to msgget() */ 
   int msgflg;    /* msgflg to be passed to msgget() */
   int msqid;     /* return value from msgget() */ 

   key = ftok ( search_key ( pdata->paramlist, "PGDATA" ), prg );
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
 *    pdata - primary data (why???)
 *    msqid - message queue id
 *    type - message type
 *    message - message string
 * output:
 *    0 - on success
 *    -1 - on error
 */
int pgsql_msg_send ( pgsqldata * pdata, int msqid, uint type, const char * message ){

   PGSQL_MSG_BUF_T msg;
   int err;

   ASSERT_NVAL_RET_NONE ( type );
   msg.mtype = type;
   strncpy ( msg.mtext, message, MSGBUFLEN );

   err = msgsnd ( msqid, &msg, MSGBUFLEN + sizeof ( long ), 0 );
//   printf ( "message sent(%i): %s\n", type, message );

   return err;
}

/*
 * message queue receive
 * 
 * in:
 *    pdata - program data (why do we need???)
 *    msqid - message queue id
 *    type - message type
 *    message - message buffer at least MSGBUFLEN bytes
 * out:
 *    -1 - on error
 *    number of received 
 */
int pgsql_msg_recv ( pgsqldata * pdata, int msqid, int type, char * message ){

   PGSQL_MSG_BUF_T msg;
   int err;

   ASSERT_NVAL_RET_NONE ( type );

   err = msgrcv ( msqid, &msg, MSGBUFLEN + sizeof ( long ), type, 0 );
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
 */
void pgsql_msg_shutdown ( pgsqldata * pdata, int msqid ){

   msgctl ( msqid, IPC_RMID, NULL );
}

/*
 * time format correction
 */
void correct_time ( pg_time * t ){

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

#if 0
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
   char * buf;
   int nr;
   pg_time t;

   memset ( &t, 0, sizeof ( t ) );
   nr = sscanf(str, "%d-%d-%d %d:%d:%d", &t.y, &t.m, &t.d, &t.h, &t.mi, &t.s );
   correct_time ( &t );
   buf = MALLOC ( BUFLEN );
   snprintf ( buf, BUFLEN, "%d-%02d-%02d %02d:%02d:%02d", t.y, t.m, t.d, t.h, t.mi, t.s );
   out = bstrdup ( buf );
   FREE ( buf );

   return out;
}
#endif

#ifdef __cplusplus
}
#endif
