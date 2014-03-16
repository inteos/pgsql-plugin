/*
 * Copyright (c) 2013 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.pgsql for details.
 * 
 * Utility tool is used for performing PostgreSQL WAL files archiving according to supplied
 * config file. It is directly called by PGSQL instance and will by run with database owner
 * permissions.
 */
/*
   To enable WAL Archiving you should do the following:
   in postgresql.conf
   - enable archive_mode = on
   - set up an archive_command = '/usr/local/bacula/sbin/pgsql-archlog -c <config.file> %f %p'
   in <config.file>

   #
   # Config file for pgsql plugin
   #
   PGDATA = <pg.data.cluster.path>
   PGHOST = <path.to.sockets.directory>
   PGPORT = <socket.'port'>
   CATDB = <catalog.db.name>
   CATDBHOST = <catalog.db.host>
   CATDBPORT = <catalog.db.port>
   CATUSER = <catalog.db.user>
   CATPASSWD = <catalog.db.password>
   ARCHDEST = <destination.of.archived.wal's.path>
   ARCHCLIENT = <name.of.archived.client>

   Next, you have to restart database instance, and check database log if everything is ok.
*/
/*
TODO:
   * add exitstatus and detect of archive destination dir does not exist !!! - done
   * add permission denied at archive destination dir !!! - done
   * 
   * if (catdb is unavailable) 
   * then
   *  copy_wal_file
   *  prepare_queue_file [ walfilename.queued ]
   * else
   *  perform_queue_batch
   *  copy_wal_file_as_usual
   * fi 
   * 
   * add timeout watchdog thread to check if everything is ok.
   * add remote ARCHDEST using SSH/SCP
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
#include <string.h>
#include "pgsqllib.h"
 
/*
 * pgsql-archlog exit status:
 *  0 - OK
 *  1 - "Not enough parameters!"
 *  2 - "WAL filename and pathname required!"
 *  3 - "Problem connecting to catalog database!"
 *  
 *  5 - "another wal backup is in progress"
 *  6 - "another wal archiving is in progress"
 *  7 - "CATDB: SQL Exec error!"
 *  8 - "CATDB: SQL insert error!"
 *  9 - "CATDB: SQL update error!"
 *  10 - "Archived wal file exits!"
 *  11 - "Multiply WAL archiving problem!"    
 *  12 - "ARCHDEST access problem: %errno"
 *  13 - "Unknown catalog status!"
 *  14 - "ARCHDEST is not a directory"
 *  15 - "ARCHDEST access problem: %errno"
 *  16 - "CATDB: transaction problem!"
 *  17 - "WAL archiving problem!"
 */
enum ARCHEXITS {
   EXITOK         = 0,
   EXITNOTENOUGH  = 1,
   EXITWALREQ     = 2,
   EXITECATDB     = 3,
   EXITBACKINPRGS = 5,
   EXITARCHINPRGS = 6,
   EXITCATDBERROR = 7,
   EXITCATDBEINS  = 8,
   EXITCATDBEUPD  = 9,
   EXITARCHEXIST  = 10,
   EXITMULARCH    = 11,
   EXITARCHDPROB  = 12,
   EXITUNKCATSTAT = 13,
   EXITARCHDSTDIR = 14,
   EXITARCHDSTACC = 15,
   EXITCATDBTRANS = 16,
   EXITARCHERROR  = 17,
};

/*
 * connects into catalog database
 * uppon succesful fills required pgdata structure fields
 * 
 * input:
 *    pdata->paramlist - connection and configuration parameters
 * output:
 *    pdata->catdb - connection handle
 */
void dbconnect ( pgsqldata * pdata, int abort ){

   pdata->catdb = catdbconnect ( pdata->paramlist );
   if ( !pdata->catdb && abort ){
      abortprg ( pdata, EXITECATDB, "Problem connecting to catalog database!" );
   }
}

/*
 * displays a short help
 */
void print_help ( pgsqldata * pdata ){
   printf ("\nUsage: pgsql-archlog -c <config> <walfile> <pathtowalfile>\n\n"
           ">> you can check your setup with: pgsql-archlog -c <config> check <<\n" );
}

/*
 * check environment setup
 *
 * checks if pgsql-archlog can perform transaction logs archiving with supplied
 *  configuration parameters in pgsql.conf file.
 */
void check_env_setup ( pgsqldata * pdata ){

   char * archdest;
   int err = 0;
   char * sql;
   PGresult * result;
  
   printf (">> PGSQL-ARCHLOG check <<\n"); 
   /* check avaliability of ARCHDEST */
   printf ("Checking config parameters ... ");
   archdest = search_key ( pdata->paramlist, "ARCHDEST" );
   if ( !archdest ){
      printf ("\n> ARCHDEST parameter not found!");
      err = 1;
   }
   /* check availiability of CATDB* parameters */
   if ( !search_key ( pdata->paramlist, "CATDBHOST" ) ){
      printf ("\n> CATDBHOST parameter not found!");
      err = 1;
   }
   if ( !search_key ( pdata->paramlist, "CATDBPORT" ) ){
      printf ("\n> CATDBPORT parameter not found!");
      err = 1;
   }
   if ( !search_key ( pdata->paramlist, "CATDB" ) ){
      printf ("\n> CATDB parameter not found!");
      err = 1;
   }
   if ( !search_key ( pdata->paramlist, "CATUSER" ) ){
      printf ("\n> CATDBUSER parameter not found!");
      err = 1;
   }
   if ( !search_key ( pdata->paramlist, "CATPASSWD" ) ){
      printf ("\n> CATDBPASSWD parameter not found!");
      err = 1;
   }
   if (err){
      printf("\nRequired parameters not found!\n");
      exit(1);
   } else {
      printf ("all ok\n");
   }
   /* check ARCHDEST avaliability */
   printf ("Checking ARCHDEST avaliability ... ");
   err = access ( archdest, F_OK );
   if (err){
      printf ("%s is not available!\n", archdest);
      exit(1);
   }
   err = access ( archdest, W_OK );
   if (err){
      printf ("%s is not writable!\n", archdest);
      exit(1);
   }
   printf ("ok\n");

   /* check catalogdb connection */
   printf ("Checking catalogdb connection ... ");
   dbconnect ( pdata, 0 );
   if (pdata->catdb){
      printf ("success\n");
      /* checking sql execution and pgsql_archivelogs avaliability */
      printf ("Checking pgsql_archivelogs on catdb ... ");
   
      sql = MALLOC ( SQLLEN );
      snprintf ( sql, SQLLEN, "select 1 from pgsql_archivelogs limit 1;");

      result = PQexec ( pdata->catdb, sql );
      FREE ( sql );
   
      if ( PQresultStatus ( result ) != PGRES_TUPLES_OK ){
         printf ("failed!");
         exit(1);
      } else {
         printf ("success\n");
      }

      PQfinish ( pdata->catdb );
   } else {
      printf ("failed\n");
      exit(1);
   }
   freepdata ( pdata );
   exit(0);
}

/*
 * parse execution arguments and fills required pdata structure fields
 * 
 * input:
 *    pdata - pointer to primary data structure
 *    argc, argv - execution envinroment variables
 * output:
 *    pdata - required structure fields
 */
void parse_args ( pgsqldata * pdata, int argc, char* argv[] ){

   char * configfile = NULL;
   int i;

   if ( argc < 3 ){
      /* TODO - add help screen */
      print_help ( pdata );
      abortprg ( pdata, EXITNOTENOUGH, "Not enough parameters!" );
   }

   for (i = 1; i < argc; i++) {
      if ( !strcmp ( argv[i], "check" ) && configfile ){
         /* check if env is setup corectly */
         pdata->paramlist = parse_pgsql_conf ( configfile );
         check_env_setup (pdata);
         exit(0);
      }
      if ( ! strcmp ( argv[i], "-c" )){
         /* we have got custom config file */
         configfile = argv[ i + 1 ];
         i++;
         continue;
      }
      if ( ! pdata->walfilename ){
         pdata->walfilename = bstrdup ( argv[i] );
         continue;
      }

      if ( ! pdata->pathtowalfilename ){
         pdata->pathtowalfilename = bstrdup ( argv[i] );
         break;
      }
   }

   if ( ! pdata->walfilename || ! pdata->pathtowalfilename ){
      abortprg ( pdata,  EXITWALREQ, "WAL filename and pathname required!" );
   }

/* for DEBUG only */
#ifdef DEBUG_PGSQL
   printf ( "config file: %s\n", configfile);
   printf ( "wal file: %s\n", pdata->walfilename );
   printf ( "path to wal file: %s\n", pdata->pathtowalfilename );
#endif

   pdata->paramlist = parse_pgsql_conf ( configfile );
}

/*
 * perform wal file copy from pdata->pathtowalfilename into
 * $ARCHDEST/pdata->walfilename
 * copy is performed by reading wal file into memory buffer and wrining it into newly
 * created file
 *
 * input:
 *  pdata - primary data
 * output return codes:
 *  0 - success;
 *  1 - error;
 */
int perform_copy_wal_file ( pgsqldata * pdata ){

   int err;
   char * arch;
   
   arch = MALLOC ( BUFLEN );

   ASSERT_NVAL_RET_ONE ( arch );

   snprintf ( arch, BUFLEN, "%s/%s",
         search_key ( pdata->paramlist, "ARCHDEST" ),
         pdata->walfilename );

   err = _copy_wal_file ( pdata, pdata->pathtowalfilename, arch);

   FREE ( arch );
   
   return err;
}

/*
 * gets information of walid from catalog database
 * 
 * input:
 *    pdata - context data
 * output:
 *    >= 0 - vaule of id for selected wal and client
 *    < 0 - value not found
 */
int get_walid_from_catalog ( pgsqldata * pdata ){
   
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
 * perform an insert of archival status into catalog
 * 
 * input:
 *  pdata - primary data
 *  status - status number to insert into catdb
 * output:
 *  pgid - id of inserted row
 */
int insert_status_in_catalog ( pgsqldata * pdata, int status ){
   
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
      abortprg ( pdata, EXITCATDBEINS, "CATDB: SQL insert error!" );
   }
   
   pgid = get_walid_from_catalog ( pdata );
   
   return pgid;
}

/*
 * input:
 *  pdata - primary data
 *  pgid - pgsql_archivelog ID for updating
 *  status - status number to update on catdb
 * output:
 *  pgid - id of inserted row
 */
int update_status_in_catalog ( pgsqldata * pdata, int pgid, int status ){

   char * sql;
   PGresult * result;
   
   sql = MALLOC ( SQLLEN );
   /* update status in catalog */
   snprintf ( sql, SQLLEN, "update pgsql_archivelogs set status = %i, mod_date = now() where id='%i'",
         status, pgid );

   result = PQexec ( pdata->catdb, sql );
   FREE ( sql );
   
   if ( PQresultStatus ( result ) != PGRES_COMMAND_OK ){
      abortprg ( pdata, EXITCATDBEUPD, "CATDB: SQL update error!" );
   }
   
   return 0;
}

/*
 * checks if archive destination is available for archiving
 * input:
 *    pdata - primary data
 * output:
 *    program exits on every error with apropirate exit code and error message
 */
void check_wal_archdest ( pgsqldata * pdata ){
   
   char * buf;
   struct stat statp;
   int err;
   int fd;
   
   buf = MALLOC ( BUFLEN );
   ASSERT_NVAL_RET ( buf );
   
   err = stat ( search_key ( pdata->paramlist, "ARCHDEST" ), &statp );
   if ( err != 0 ){
      /* archive destination does not exist or other problem */ 
      snprintf ( buf, BUFLEN, "ARCHDEST access problem: %s", strerror ( errno ) );
      abortprg ( pdata, EXITARCHDSTACC, buf );
   }
   
   if ( ! S_ISDIR ( statp.st_mode ) ){
      /* archive destination is not a directory problem, rise en error */
      abortprg ( pdata, EXITARCHDSTDIR, "ARCHDEST is not a directory" );
   }

   /* check if destination arch wal file exist */
   snprintf ( buf, BUFLEN, "%s/%s",
         search_key ( pdata->paramlist, "ARCHDEST" ),
         pdata->walfilename );
   err = stat ( buf, &statp );
   if ( err == 0 ){
      /* yes, destination arch wal file exist, not good, we should abort to avoid data coruption. */
      abortprg ( pdata, 7, "Archived wal file exits!" );
   }

   fd = open ( buf, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR );
   if ( fd < 0 ){
      /* write permission denied, or other error */
      snprintf ( buf, BUFLEN, "ARCHDEST access problem: %s", strerror ( errno ) );
      abortprg ( pdata, EXITARCHDPROB, buf );
   } else {
      close ( fd );
      unlink ( buf );
   }
}

/*
 * 
 */
int perform_wal_archive ( pgsqldata * pdata ){

   int err;
   int pgid;
   
   /* first - check if archdest is available for archiving */
   check_wal_archdest ( pdata );

   /* archive wal destination is available, start a process
    * insert a valid status into catdb */
   pgid = insert_status_in_catalog ( pdata, PGSQL_STATUS_WAL_ARCH_START );
   if ( pgid < 0 ){
      /* strange, insert wasn't ok? */
      abortprg ( pdata, EXITCATDBTRANS, "CATDB: transaction problem!" );
   }

   /* perform a wal copy */
   err = perform_copy_wal_file ( pdata );

   update_status_in_catalog ( pdata, pgid,
         /* if err != 0 then copy was unsuccesfull */
            err ? PGSQL_STATUS_WAL_ARCH_FAILED : PGSQL_STATUS_WAL_ARCH_FINISH );

   return err;
}

/*
 * 
 */
int perform_another_wal_archive ( pgsqldata * pdata, int pgid ){

   int pgstatus;
   char * sql;
   PGresult * result;
   int err = 0;

   sql = MALLOC ( SQLLEN );
   ASSERT_NVAL_RET_ONE ( sql );
   
   snprintf ( sql, SQLLEN, "select status from pgsql_archivelogs where id='%i'", pgid);
   result = PQexec ( pdata->catdb, sql );
   FREE ( sql );
   if ( PQresultStatus ( result ) != PGRES_TUPLES_OK ){
      abortprg ( pdata, EXITCATDBERROR, "SQL Exec error!" );
   }
   /* pgstatus is a status of another (previous) archive process */
   pgstatus = atoi ((char *) PQgetvalue ( result, 0, PQfnumber ( result, "status") ));

   switch (pgstatus){
      case PGSQL_STATUS_WAL_ARCH_START:
      case PGSQL_STATUS_WAL_ARCH_INPROG:
      case PGSQL_STATUS_WAL_ARCH_FAILED:         
         /* previous archiving process (copy wal) was unsuccessfull, copy one more time */
         update_status_in_catalog ( pdata, pgid, PGSQL_STATUS_WAL_ARCH_START );
      
         /* perform a wal copy */
         err = perform_copy_wal_file ( pdata );
      
         /* if err != 0 this is an error, one more time */
         update_status_in_catalog ( pdata, pgid,
                  err ? PGSQL_STATUS_WAL_ARCH_FAILED : PGSQL_STATUS_WAL_ARCH_FINISH );
                  
         break;
      case PGSQL_STATUS_WAL_ARCH_FINISH: /* archiving finish without error */
      case PGSQL_STATUS_WAL_ARCH_MULTI: /* multiply archiving when previous was errorless */
      case PGSQL_STATUS_WAL_BACK_DONE:
      case PGSQL_STATUS_WAL_BACK_FAILED:
         /* previous archive or backup was succesfull, inform about strange behavioral */
         update_status_in_catalog ( pdata, pgid, PGSQL_STATUS_WAL_ARCH_START );

         /* perform a wal copy */
         err = perform_copy_wal_file ( pdata );
      
         /* if err != 0 this is an error, one more time */
         update_status_in_catalog ( pdata, pgid,
                  err ? PGSQL_STATUS_WAL_ARCH_FAILED : PGSQL_STATUS_WAL_ARCH_MULTI );

         break;
      case PGSQL_STATUS_WAL_BACK_START:
      case PGSQL_STATUS_WAL_BACK_INPROG:
         /* do nothing to avoid coruption */
         break;
      default:
         abortprg ( pdata, EXITUNKCATSTAT, "Unknown catalog status!" );
   }

   return err;
}

/*
 * input parameters:
 *    argv[0] -c config_file <wal_filename> <full_path_to_wal_filename>
 */
int main(int argc, char* argv[]){

   pgsqldata * pdata;
   int pgid;
   int err;

   pgsqllibinit ( argc, argv );

   pdata = allocpdata ();
   parse_args ( pdata, argc, argv );
   dbconnect ( pdata, 1 );

   /* check if another operation with the same wal is or was in progress */
   pgid = get_walid_from_catalog ( pdata );
   if ( pgid < 0 ){
      /* we found no wal/client in cat db so we should have a clean situation */
      err = perform_wal_archive ( pdata );
   } else {
      /* there was a prievious or is an archiving process, check what was or is going on */
      err = perform_another_wal_archive ( pdata, pgid );
   }
   /* check status of operation */
   if ( err ){
      /* inform about copy wal file error */
      abortprg ( pdata, EXITARCHDPROB, "WAL archiving problem!" );
   } else {
      /* no error, operation successful! */
      logprg ( LOGINFO, "WAL archiving successful!" );
   }

   PQfinish ( pdata->catdb );
   freepdata ( pdata );
   return EXITOK;
}
