/*
 * Copyright (c) 2013 by Inteos sp. z o.o.
 * All rights reserved. See LICENSE.pgsql for details.
 * 
 * This is a bacula recovery tool for PostgreSQL database. It is designed to work both with
 * root (administrator) and database owner (postgres) permissions. Utility will switch its
 * permissions to get required functionality.
 */
/*
   To perform Point-In-Time-Recovery you need a valid <config.file>

   in <config.file>:

   #
   # Config file for pgsql plugin
   #
   PGDATA = <pg.data.cluster.path>
   PGHOST = <path.to.sockets.directory>
   PGPORT = <socket.'port'>
   PGSTART = <pg_ctl.command.location.and.options>
   PGSTOP = <pg_ctl.command.location.and.options>
   CATDB = <catalog.db.name>
   CATDBHOST = <catalog.db.host>
   CATDBPORT = <catalog.db.port>
   CATUSER = <catalog.db.user>
   CATPASSWD = <catalog.db.password>
   ARCHDEST = <destination.of.archived.wal's.path>
   ARCHCLIENT = <name.of.archived.client>
   DIRNAME = <director.name>
   DIRHOST = <director.address>
   DIRPORT = <director.service.port>
   DIRPASSWD = <director.connection.password>

   and valid bacula console resource in bacula-dir.conf:

   #
   # Restricted console used for client initiated restore
   #
   Console {
      Name = <name.of.bacula.client>
      Password = <console.password>
      CommandACL = restore,.filesets,wait
      ClientACL = <name.of.bacula.client>
      CatalogACL = <bacula.catalog>       # it is not pgsql catalog database
      WhereACL = "*all*"
      JobACL = <job.names.for.pgsql.backup.and.pgsql.archbackup>
      PoolACL = <pool.names.of.pgsql.backup.and.pgsql.archbackup>
      StorageACL = <bacula.sd.name>
      FileSetACL = <fileset.name.of.pgsql.db.backup>,<and.pgsql.archbackup>
   }

   Next, you have to execute pgsql-restore command:
   $ pgsql-restore -c <config.file> [-v][-t <recovery.time> | -x <recovery.xid> ] [-w <where>] restore

   Arch restore:
   $ pgsql-restore -c <config.file> [-v] wal <name.of.wal> <path.to.restore>

   * -c = config file
   * -v = verbose
   * -t = recovery time point
   * -x = recovery transaction point
   * -w = where database cluster restore to
   * 
*/
/* Recomended PostgreSQL recovery procedure we'd like to implement:

   1.  Stop the postmaster, if it's running.
   2.  If you have the space to do so, copy the whole cluster data directory and any
   tablespaces to a temporary location in case you need them later. Note that this precaution
   will require that you have enough free space on your system to hold two copies of your
   existing database. If you do not have enough space, you need at the least to copy the
   contents of the pg_xlog subdirectory of the cluster data directory, as it may contain logs
   which were not archived before the system went down.
   3.  Clean out all existing files and subdirectories under the cluster data directory and
   under the root directories of any tablespaces you are using.
   4.  Restore the database files from your backup dump. Be careful that they are restored
   with the right ownership (the database system user, not root!) and with the right
   permissions. If you are using tablespaces, you may want to verify that the symbolic links
   in pg_tblspc/ were correctly restored.
   5.  Remove any files present in pg_xlog/; these came from the backup dump and are therefore
   probably obsolete rather than current. If you didn't archive pg_xlog/ at all, then
   re-create it, and be sure to re-create the subdirectory pg_xlog/archive_status/ as well.
   6.  If you had unarchived WAL segment files that you saved in step 2, copy them into
   pg_xlog/. (It is best to copy them, not move them, so that you still have the unmodified
   files if a problem occurs and you have to start over.)
   7.  Create a recovery command file recovery.conf in the cluster data directory (see
   Recovery Settings). You may also want to temporarily modify pg_hba.conf to prevent ordinary
   users from connecting until you are sure the recovery has worked.
   8.  Start the postmaster. The postmaster will go into recovery mode and proceed to read
   through the archived WAL files it needs. Upon completion of the recovery process, the
   postmaster will rename recovery.conf to recovery.done (to prevent accidentally re-entering
   recovery mode in case of a crash later) and then commence normal database operations.
   9.  Inspect the contents of the database to ensure you have recovered to where you want to
   be. If not, return to step 1. If all is well, let in your users by restoring pg_hba.conf to
   normal.

   $PGDATA/recovery.conf:
      restore_command (string)
      recovery_target_time (timestamp) || recovery_target_xid (string)
      recovery_target_inclusive (boolean)

   bconsole command for archive logs restoration:
   * restore file=pgsqlarch:<client-name>/<walfilename> where=<where> done yes

   bconsole command for database files restoration:
   * .filesets
   for every filesets execute
   * restore fileset=<$fileset> select
   * ls
   now we should get which fileset is available, we will verify it, if it will be ok, proceed:
   * restore where=<where> fileset=<$fileset> [restoreclient=<client>] [current,before="YYYY-MM-DD HH:MM:SS"] select
   now, use "5" => "Select a current backup of the client"
   * 5
   *
 */
/*
TODO:
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
#include <libpq-fe.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <libgen.h>
#include <pwd.h>
#include <grp.h>
#include "pgsqllib.h"
#include "utils.h"

/* 
 * libbac uses its own sscanf implementation which is not compatible with
 * libc implementation, unfortunately.
 * Usage of bsscanf require format string rewriting.
 */
#ifdef sscanf
#undef sscanf
#endif

#ifdef __cplusplus
extern "C" {
#endif

void inteos_info ( pgsqldata * pdata ){

   if ( pdata->verbose || pdata->mode == PGSQL_DB_RESTORE ){
      logprg ( LOGINFO, " =============================" );
      logprg ( LOGINFO, "| PostgreSQL restore utility. |" );
      logprg ( LOGINFO, "|     (c) 2013 by Inteos      |" );
      logprg ( LOGINFO, " =============================" );
   }
}

void print_help ( pgsqldata * pdata ){
   printf ("Usage: pgsql-restore -c <config.file> [-v] [-t <recovery.time> | -x <recovery.xid> ] [-w <where>] [-r restoreclient] restore\n" );
}

void dbconnect ( pgsqldata * pdata ){

   pdata->catdb = catdbconnect ( pdata->paramlist );

   if ( ! pdata->catdb ){
      abortprg ( pdata, 4, "Problem connecting to catalog database!" );
   }
}

void parse_args ( pgsqldata * pdata, int argc, char* argv[] ){

   int i;
   char * fullconfpath;

   if ( argc < 3 ){
      /* TODO - add help screen */
      print_help ( pdata );
      abortprg ( pdata, 1, "Not enough parameters!" );
   }

   for (i = 1; i < argc; i++) {
//      printf ( "%s\n", argv[i] );
      if ( !strcmp ( argv[i], "-c" ) ){
         /* we have got a custom config file */
         fullconfpath = MALLOC ( BUFLEN );
         ASSERT_NVAL_RET ( fullconfpath );
         realpath ( argv [ i + 1 ], fullconfpath );
         pdata->configfile = bstrdup ( fullconfpath );
         FREE ( fullconfpath );
         i++;
         continue;
      }
      if ( !strcmp ( argv[i], "-t" ) && pdata->pitr == PITR_CURRENT ){
         pdata->restorepit = format_btime ( argv [ i + 1 ] );
         pdata->pitr = PITR_TIME;
         i++;
         continue;
      }
      if ( !strcmp ( argv[i], "-x" ) && pdata->pitr == PITR_CURRENT ){
         pdata->restorepit = bstrdup ( argv [ i + 1 ] );
         pdata->pitr = PITR_XID;
         i++;
         continue;
      }
      if ( !strcmp ( argv[i], "-w" ) ){
         pdata->where = bstrdup ( argv [ i + 1 ] );
         i++;
         continue;
      }
      if ( !strcmp ( argv[i], "-r" ) ){
         pdata->restoreclient = bstrdup ( argv [ i + 1 ] );
         i++;
         continue;
      }
      if ( !strcasecmp ( argv[i], "-v" ) ){
         pdata->verbose = 1;
         continue;
      }
      if ( !strcasecmp ( argv[i], "restore" ) ){
         pdata->mode = PGSQL_DB_RESTORE;
         continue;
      }
      if ( !strcasecmp ( argv[i], "wal" ) ){
         pdata->mode = PGSQL_ARCH_RESTORE;
         continue;
      }
      if ( pdata->mode == PGSQL_ARCH_RESTORE && ! pdata->walfilename ){
         pdata->walfilename = bstrdup ( argv[i] );
         continue;
      }
      if ( pdata->mode == PGSQL_ARCH_RESTORE && ! pdata->pathtowalfilename ){
         pdata->pathtowalfilename = bstrdup ( argv[i] );
         break;
      }
   }

   if ( pdata->mode == PGSQL_NONE ){
      abortprg ( pdata,  2, "Operation mode [restore,wal] required!" );
   }

   if ( pdata->mode == PGSQL_ARCH_RESTORE ){
      if ( ! pdata->walfilename || ! pdata->pathtowalfilename ){
         abortprg ( pdata,  2, "WAL filename and pathname required!" );
      }
   }

   pdata->paramlist = parse_pgsql_conf ( pdata->configfile );

//   if ( pdata->mode == PGSQL_DB_RESTORE && ( pdata->pitr != PITR_CURRENT ) ){
//
//   }
}

/*
 * prints restore configuration data
 */
void print_restore_info ( pgsqldata * pdata )
{
   char * buf;

   if ( pdata->verbose ){
      buf = MALLOC ( BUFLEN );
      if ( ! buf ){
         abortprg ( pdata, 6, "memory allocation error" );
      }
   
      snprintf ( buf, BUFLEN, "CLIENT = %s", search_key ( pdata->paramlist, "ARCHCLIENT" ) );
      logprg ( LOGINFO, buf );
      snprintf ( buf, BUFLEN, "PGDATA = %s", search_key ( pdata->paramlist, "PGDATA" ) );
      logprg ( LOGINFO, buf );
      snprintf ( buf, BUFLEN, "PGHOST = %s", search_key ( pdata->paramlist, "PGHOST" ) );
      logprg ( LOGINFO, buf );
      snprintf ( buf, BUFLEN, "PGPORT = %s", search_key ( pdata->paramlist, "PGPORT" ) );
      logprg ( LOGINFO, buf );
   
      switch ( pdata->pitr ){
         case PITR_TIME:
         case PITR_XID:
            snprintf ( buf, BUFLEN, "PITR until %s", pdata->restorepit );
            break;
         case PITR_CURRENT:
            snprintf ( buf, BUFLEN, "PITR until end of WAL data" );
            break;
      }
      logprg ( LOGINFO, buf );

      if ( pdata->where ){
         snprintf ( buf, BUFLEN, "WHERE = %s", pdata->where );
      } else {
         snprintf ( buf, BUFLEN, "WHERE = < original location >" );
      }
      logprg ( LOGINFO, buf );

      if ( pdata->restoreclient ){
         snprintf ( buf, BUFLEN, "restoreclient = %s", pdata->restoreclient );
         logprg ( LOGINFO, buf );
      }

      FREE ( buf );
   }
}

/*
 * input:
 *    pdata->where : search_key ( pdata->paramlist, "PGDATA"
 * output:
 *    1 - is running
 *    0 - is not running
 */
int check_postgres_is_running ( pgsqldata * pdata ){

   int pidfd;
   int err;
   int pid;
   int out;
   char * pidcont;
   char * buf;
   int rc = 0;
   struct stat st;

   buf = MALLOC ( BUFLEN );
   if ( ! buf ){
      abortprg ( pdata, 6, "memory allocation error" );
   }

   pidcont = MALLOC ( 64 );
   if ( ! pidcont ){
      abortprg ( pdata, 6, "memory allocation error" );
   }

   snprintf ( buf, BUFLEN, "%s/postmaster.pid",
         pdata->where ? pdata->where : search_key ( pdata->paramlist, "PGDATA" ) );

   err = stat ( buf, &st );
   if ( err ){
      err = errno;
      if ( err == EACCES ){
         abortprg ( pdata, 7, "access denied at checking database destination" );
      }
   }

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
            /* TODO: add checking for other OS'es then Linux */
            snprintf ( buf, BUFLEN, "/proc/%i/status", pid );
            pidfd = open ( buf, O_RDONLY );
            if ( pidfd > 0 ){
               close ( pidfd );
               rc = 1;
            }
         }
      }
   } else {
      if ( pdata->verbose ){
         logprg ( LOGINFO, "postmaster should be already down on that cluster, good." );
      }
   }

   FREE ( pidcont );
   FREE ( buf );

   return rc;
}

/*
 * executes a command process with popen call
 *
 * input:
 *    pdata - primary data
 *    command - command to execute
 * output:
 *    stdout/stderr stream into logprg (LOGINFO)
 *    exitstatus - command exit code
 */
int exec_popen_process ( pgsqldata * pdata, char * command ){

   char * buf;
   FILE * file;
   int out;
   char * execommand;
   int scnr;
   int exitstatus;
   const char * EXITSTATUS = " 2>&1;echo \"POPENEXITSTATUS: $?\"";


   buf = MALLOC ( BUFLEN );
   ASSERT_NVAL_RET_ONE ( buf );

   execommand = MALLOC ( strlen ( command ) + strlen ( EXITSTATUS ) + 1 );
   execommand [ 0 ] = '\0';
   strcat ( execommand, command );
   strcat ( execommand, EXITSTATUS );

   file = popen ( execommand, "r" );
   ASSERT_NVAL_RET_ONE ( file );

   while ( ( out = freadline ( file, buf, BUFLEN ) ) > 0 ){
      scnr = sscanf ( buf, "POPENEXITSTATUS: %d", &exitstatus );
      if ( scnr ){
         // printf ( "SSCANF: %i, %d\n", scnr, exitstatus );
         break;
      } else
      if ( pdata->verbose ){
         logprg ( LOGINFO, buf );
      }
   }

   pclose ( file );
   FREE ( buf );
   FREE ( execommand );

   return exitstatus;
}

/*
 * check an owner of PGDATA directory
 * input:
 *    pdata->paramlist[PGDATA]
 * output:
 *    0 - on success, uid/gid of PGDATA directory at pgid
 *    1 - on error
 */
int get_pgdata_pgugid ( pgsqldata * pdata, pgugid * pgid ){

   int err;
   struct stat st;
   
   /* Required PGDATA from config file */
   err = stat ( pdata->where ? pdata->where : search_key ( pdata->paramlist, "PGDATA" ) , &st );
   if ( err ){
      return 1;
   }

   pgid->uid = st.st_uid;
   pgid->gid = st.st_gid;

   return 0;
}

/*
 * shutes down a running postmaster
 * input:
 *    pdata->paramlist
 *    mode - stoping mode
 * output
 *    0 - on success
 *    other - error number from exec_popen
 */
int perform_postmaster_shutdown ( pgsqldata * pdata, char mode ){

   int err;
   char * pgctlrun;
   const char * pgctl;

   pgctlrun = MALLOC ( BUFLEN );
   ASSERT_NVAL_RET_ONE ( pgctlrun );

   /* we re looking for pg_ctl location */
   pgctl = search_key ( pdata->paramlist, "PGSTOP" );
   if ( !pgctl ){
      /* autodetect a pg_ctl and stop options */
      pgctl = find_pgctl ( pdata );
      ASSERT_NVAL_RET_ONE ( pgctl );
   
      /* biulding a command for instance shutdown in abort mode */
      snprintf ( pgctlrun, BUFLEN, "%s stop -s -D \"%s\" -m %c",
                  pgctl,
                  pdata->where ? pdata->where : search_key ( pdata->paramlist, "PGDATA" ),
                  mode );
   } else {
      /* use a user supplied stop command and options
       * building a command for instance shutdown */
      snprintf ( pgctlrun, BUFLEN, "%s -D \"%s\" -m %c",
                  pgctl,
                  pdata->where ? pdata->where : search_key ( pdata->paramlist, "PGDATA" ),
                  mode );
   }

   logprg ( LOGINFO, pgctlrun );

   /* exec a required process */
   err = exec_popen_process ( pdata, pgctlrun );

   return err;
}

/*
 * shutdown a running PostgreSQL instance pointed by PGDATA
 */
int shutdown_postmaster ( pgsqldata * pdata ){

   pgugid pgid;
   uid_t curuid = 0;
//   uid_t testuid;
   int err;

   ASSERT_NVAL_RET_ONE ( pdata );

   if ( pdata->verbose ){
      int a;
      char msg[8] = "x ...";
      logprg ( LOGINFO, "shutting down a runing PostgreSQL instance at PGDATA" );
      logprg ( LOGINFO, "it is your last chance to interrupt recovery process ..." );
      for ( a = 3; a > 0; a-- ){
         msg [ 0 ] = '0' + a;
         logprg ( LOGINFO, msg );
         sleep ( 1 ); 
      }
      logprg ( LOGINFO, "OK, as you wish, its' your database ..." );
   }
   /* save a current permissions */
   curuid = geteuid ();

   /* dynamic production database owner verification, we use it to shutdown database instance */
   err = get_pgdata_pgugid ( pdata, &pgid );

   /* switch to pgcluster owner (postgres) */
   err = seteuid ( pgid.uid );
   ASSERT_VAL_RET_ONE ( err );
   //testuid = geteuid ();
   
   /* exec a required process with 'immediate' mode */
   err = perform_postmaster_shutdown ( pdata, 'i' );
   ASSERT_VAL_RET_ONE ( err );

   /* switch to curent user */
   err = seteuid ( curuid );
   ASSERT_VAL_RET_ONE ( err );
   //testuid = geteuid ();
   if ( pdata->verbose ){
      logprg ( LOGINFO, "shutdown complete" );
   }

   return 0;
}

/*
 * 
 */
void read_log_handler ( pgsqldata * pdata, const int msqid, const char * pgctlfifo ){

   int fdfifo;
   char * buffifo;
   char * str;
   int outfifo;

   /* read log handler */
   fdfifo = open ( pgctlfifo, O_RDONLY ); 
//   printf ( "open fifo\n" );

   /* buffifo as a postmaster log output buffer */
   buffifo = MALLOC ( BUFLEN );

   while ( ( outfifo = readline ( fdfifo, buffifo, BUFLEN ) ) > 0 ){
      if ( pdata->verbose ) {
         printf ( "%s\n", buffifo );
      }
      /* search for archive recovery completed */
      str = strstr ( buffifo, "LOG:  archive recovery complete" );
      if ( str ){
         logprg ( LOGINFO, "RECOVERY COMPLETED!!!" );
         pgsql_msg_send ( pdata, msqid, 2, "R. Recovery Completed" );
      }
      /* search for database shutdown completed */
      str = strstr ( buffifo, "LOG:  database system is shut down" );
      if ( str ){
         logprg ( LOGINFO, "Shutdown COMPLETED!!!" );
         pgsql_msg_send ( pdata, msqid, 2, "S. Shutdown" );
      }
      /* search for any error */
      str = strstr ( buffifo, "FATAL:  " );
      //str = strstr ( buffifo, "postgres cannot access" );
      if ( str ){
         logprg ( LOGINFO, "ERROR!!!" );
         pgsql_msg_send ( pdata, msqid, 2, "E. ERROR" );
      }
   }
   close ( fdfifo );
   FREE ( buffifo );
}

/*
 * input:
 *    pdata - primary data
 *    msqid - message queue id
 *    pgctlfifo - name of log handler fifo
 * output:
 *    pid - forked process pid
 */
int start_read_log_handler ( pgsqldata * pdata, const int msqid, const char * pgctlfifo ){

   int pid;

   pid = fork ();
   if ( pid == 0 ){
//      printf ( "Hello from forked process: start_read_log_handler\n" );
      /* sleep used for debuging forked process in gdb */
      //sleep ( 60 );

      /* sync with other process */
      pgsql_msg_send ( pdata, msqid, 1, "A. log handler ready" );

      /* read log handler */
      read_log_handler ( pdata, msqid, pgctlfifo );
      
      /* finish forked process */
      exit ( 0 );
   } else {
//      printf ("PID (start_read_log_handler): %i\n", pid );
   }

   return pid;
}

/*
 * input:
 *    pgid - uid/gid for switch to
 * output:
 *    0 - on success
 *    1 - on error
 */
int set_user_groups ( const pgugid * pgid ){

   passwd * pw;
   int err;
   gid_t * groups = NULL;
   int ngroups = 0;

   /* check required uid/gid */
   if ( getuid() != pgid->uid || getgid() != pgid->gid ){
      /* user switching required */
      pw = getpwuid ( pgid->uid );
      /* what is a number of suplementary groups of required user */
#ifdef __APPLE__
      err = getgrouplist ( pw->pw_name, pgid->gid, (int*)groups, &ngroups );
#else
      err = getgrouplist ( pw->pw_name, pgid->gid, groups, &ngroups );
#endif
      if ( err == -1 ){
         groups = (gid_t *) malloc ( ( ngroups + 1 ) * sizeof ( gid_t ) );
         ASSERT_NVAL_RET_ONE ( groups );
      }
      /* get a suplementary group list */
#ifdef __APPLE__
      ngroups = getgrouplist ( pw->pw_name, pgid->gid, (int*)groups, &ngroups);
#else
      ngroups = getgrouplist ( pw->pw_name, pgid->gid, groups, &ngroups);
#endif
      /* extend possible suplementary groups for user */
      err = setgroups ( ngroups, groups );
      /* set primary group for process */
      err = setgid ( pgid->gid );
      /* switch to required user */
      err = setuid ( pgid->uid );
//      printf ( "switched uid:gid = %i:%i\n", getuid(), getgid() );
      FREE ( groups );
   }

   return 0;
}

/*
 * input:
 *    pdata - primary data
 *    msqid - message queue id
 *    pgctlfifo - name of log handler fifo
 * output:
 *    0 - on success
 *    1 - on error 
 */
int perform_postmaster_startup_recovery ( pgsqldata * pdata, pgugid * pgid, const int msqid, const char * pgctlfifo ){

   char * pgctlrun;        // allocated
   const char * pgctl;
   int pid = 0;
   int err;
   char * buf;             // allocated
   int exitstatus;

   pgctlrun = MALLOC ( BUFLEN );
   ASSERT_NVAL_RET_ONE ( pgctlrun );

   /* find a pg_ctl location */
   pgctl = search_key ( pdata->paramlist, "PGSTART" );
   if ( !pgctl ){
      /* autodetect a pg_ctl and start options */
      pgctl = find_pgctl ( pdata );
      ASSERT_NVAL_RET_ONE ( pgctl );

      /* building a command for instance startup */
      snprintf ( pgctlrun, BUFLEN, "%s start -l %s -w -s -o \"-c config_file=%s/postgresql.conf -c logging_collector=off\" -D \"%s\"",
               pgctl, pgctlfifo,
               pdata->where ? pdata->where : search_key ( pdata->paramlist, "PGDATA" ),
               pdata->where ? pdata->where : search_key ( pdata->paramlist, "PGDATA" ) );
   
      buf = MALLOC ( BUFLEN );
   } else {
      /* use a user supplied start command and options
       * building a command for instance startup */
      snprintf ( pgctlrun, BUFLEN, "%s -w -s -l %s -D \"%s\"",
               pgctl,
               pgctlfifo,
               pdata->where ? pdata->where : search_key ( pdata->paramlist, "PGDATA" ) );
   }

   logprg ( LOGINFO, pgctlrun );

   pid = fork ();
   if ( pid == 0 ){
      // printf ( "Hello from forked process: perform_postmaster_startup_recovery\n" );
      /* sleep used for debuging forked process in gdb */
      // sleep ( 60 );

      /* sync with read log process */
      pgsql_msg_recv ( pdata, msqid, 1, buf );
      // printf ( "RECV: %s\n" , buf );

      /* switch to pgcluster owner (postgres) and all its groups */
      err = set_user_groups ( pgid );
      ASSERT_VAL_EXIT_ONE ( err );

      /* exec a required process */
      err = exec_popen_process ( pdata, pgctlrun );

      /* finish forked process */
      exit ( err );
   } else {
      // printf ("PID (perform_postmaster_startup_recovery): %i\n", pid );
      waitpid ( pid, (int*)&exitstatus, 0 );
      // printf ( "EXITSTATUS: %i\n", exitstatus );
   }

   /* check if pg_ctl run sucessfully */
   if ( ! exitstatus ){
      pid = fork ();
      if ( pid == 0 ){
         // printf ( "Hello from forked process: perform_postmaster_startup_recovery\n" );
         /* sleep used for debuging forked process in gdb */
         // sleep ( 60 );

         /* sync with read log process */
         pgsql_msg_recv ( pdata, msqid, 2, buf );
         // printf ( "RECV: %s\n" , buf );
         if ( buf[0] == 'E' ){
            /* error occurred */
            return 1;
         }

         /* switch to pgcluster owner (postgres) */
         err = set_user_groups ( pgid );
         ASSERT_VAL_RET_ONE ( err );

         /* shutdown postgresql */
         err = perform_postmaster_shutdown ( pdata, 's' );

         /* wait for shutdown to complite */
         pgsql_msg_recv ( pdata, msqid, 2, buf );
      //   printf ( "RECV: %s\n" , buf );
         if ( buf[0] == 'E' ){
            /* error occurred */
            return 1;
         }
         exit ( err );
      } else {
         // printf ("PID (perform_postmaster_startup_recovery): %i\n", pid );
         waitpid ( pid, (int*)&exitstatus, 0 );
         // printf ( "EXITSTATUS: %i\n", exitstatus );
      }
   }

   FREE ( pgctlrun );
   FREE ( buf );

   return exitstatus;
}

/*
 * startup a PostgreSQL instance pointed by PGDATA
 */
int startup_postmaster ( pgsqldata * pdata ){

   int msqid;
   char * pgctlfifo;
   char * unlinkfile;
   int err;
   int pidfifo;
   int exitstatus;
   pgugid pgid;

   ASSERT_NVAL_RET_ONE ( pdata );
   logprg ( LOGINFO, "RECOVERY START!!!" );
   if ( pdata->verbose ){
      logprg ( LOGINFO, "startup a PostgreSQL instance in Recovery Mode" );
   }

   /* cleanup some files */
   unlinkfile = MALLOC ( PATH_MAX );
   ASSERT_NVAL_RET_ONE ( unlinkfile );
   snprintf ( unlinkfile, PATH_MAX, "%s/postmaster.pid",
               pdata->where ? pdata->where : search_key ( pdata->paramlist, "PGDATA" ) );
   unlink ( unlinkfile );

   /* dynamic production database owner verification, we use it to startup a database instance */
   err = get_pgdata_pgugid ( pdata, &pgid );

   /* fifo used for postgres instance log handler located at /tmp/<ARCHCLIENT>.ctl */
   pgctlfifo = MALLOC ( BUFLEN );
   ASSERT_NVAL_RET_ONE ( pgctlfifo );
   snprintf ( pgctlfifo, BUFLEN, "/tmp/%s.ctl", search_key ( pdata->paramlist, "ARCHCLIENT" ) );
   /* create a fifo */
   unlink ( pgctlfifo );
   err = mkfifo ( pgctlfifo, S_IRUSR | S_IWUSR );
   ASSERT_VAL_RET_ONE ( err );
   chown ( pgctlfifo, pgid.uid, pgid.gid );

   /* message queue id */
   msqid = pgsql_msg_init ( pdata, 'R' );

   /* run a read log handler */
   pidfifo = start_read_log_handler ( pdata, msqid, pgctlfifo );

   /* execute pg_ctl */
   err = perform_postmaster_startup_recovery ( pdata, &pgid, msqid, pgctlfifo );

   waitpid ( pidfifo, (int*)&exitstatus, 0 );
//   printf ( "EXITSTATUS: %i\n", exitstatus );

   pgsql_msg_shutdown ( pdata, msqid );

   if ( err ) {
      logprg ( LOGERROR, "Recovery failed" );
   } else
   if ( pdata->verbose ){
      logprg ( LOGINFO, "Recovery complete" );
   }

   return err;
}

/*
 *
 */
int copy_unarchived_wals ( pgsqldata * pdata ){

   DIR * dirp;
   struct dirent * filedir;
   char * path;
   char * file;
   char * dst;
   struct stat st;
   PGresult * result;
   char * sql;
   int err;

   dbconnect ( pdata );

   path = MALLOC ( PATH_MAX );
   if ( ! path ){
      logprg ( LOGERROR, "out of memeory!" );
      return 1;
   }

   snprintf ( path, PATH_MAX, "%s/pg_xlog",
            pdata->where ? pdata->where :
            search_key ( pdata->paramlist, "PGDATA" ) );

   dirp = opendir ( path );
   if ( dirp ){
      /* katalog pg_xlog istnieje, sprawdzmy czy są tam jakieś pliki do
       * archiwizacji */
      if ( pdata->verbose ){
         logprg ( LOGINFO, "copying unarchived wal logs" );
      }
      
      file = MALLOC ( PATH_MAX );
      if ( ! file ){
         FREE ( path );
         logprg ( LOGERROR, "out of memeory!" );
         return 1;
      }

      dst = MALLOC ( PATH_MAX );
      if ( ! dst ){
         FREE ( path );
         FREE ( file );
         logprg ( LOGERROR, "out of memeory!" );
         return 1;
      }

      sql = MALLOC ( SQLLEN );
      if ( ! sql ){
         FREE ( path );
         FREE ( file );
         FREE ( dst );
         logprg ( LOGERROR, "out of memeory!" );
         return 1;
      }

      while ( (filedir = readdir ( dirp )) ){
         if ( strcmp ( filedir->d_name, "."  ) != 0 &&
              strcmp ( filedir->d_name, ".." ) != 0 ){
            /* building a name to check */
            snprintf ( file, PATH_MAX, "%s/%s", path, filedir->d_name );

            if ( stat ( file, &st ) == 0 && S_ISREG ( st.st_mode ) ){
               /* in pg_xlog directory has at least one file, we assume that it is a
                * wal file nd we would like to copy it, but we have to check if it was
                * previously archived
                * TODO: archived wal means status in (1,3,6),
                * PGSQL_STATUS_WAL_OK
                * others means error or unfinished archiving */
               snprintf ( sql, SQLLEN,
                     "select status from pgsql_archivelogs where client='%s' and \
                      filename='%s' and status in (%s)",
                     search_key ( pdata->paramlist, "ARCHCLIENT" ),
                     filedir->d_name,
                     PGSQL_STATUS_WAL_OK );

               result = PQexec ( pdata->catdb, sql );
               if ( PQresultStatus ( result ) != PGRES_TUPLES_OK ){
                  abortprg ( pdata, 6, "SQL Exec error!" );
               }

               if ( PQntuples ( result ) ){
                  /* file was previous archived -> ignoring */
                  continue;
               }

               /* insert status in catalog */
               snprintf ( sql, SQLLEN,
                     "insert into pgsql_archivelogs (client, filename, status) \
                     values ('%s', '%s', '%i')",
                     search_key ( pdata->paramlist, "ARCHCLIENT" ),
                     filedir->d_name, PGSQL_STATUS_WAL_ARCH_START );

               result = PQexec ( pdata->catdb, sql );
               if ( PQresultStatus ( result ) != PGRES_COMMAND_OK ){
                  abortprg ( pdata, 6, "SQL Exec error!" );
               }

               /* budujemy nazwę miejsca docelowego kopiowanego pliku */
               snprintf ( dst, PATH_MAX, "%s/%s", search_key ( pdata->paramlist, "ARCHDEST" ), filedir->d_name );

               /* perform a wal copy */
               err = _copy_wal_file ( pdata, file, dst );

               /* update status in catalog */
               snprintf ( sql, SQLLEN,
                     "update pgsql_archivelogs set status='%i' where client='%s' and filename='%s'",
                     /* if err != 0 then copy was unsuccesfull */
                     err ? PGSQL_STATUS_WAL_ARCH_FAILED : PGSQL_STATUS_WAL_ARCH_FINISH, 
                     search_key ( pdata->paramlist, "ARCHCLIENT" ),
                     filedir->d_name );

               result = PQexec ( pdata->catdb, sql );
               if ( PQresultStatus ( result ) != PGRES_COMMAND_OK ){
                  abortprg ( pdata, 6, "SQL Exec error!" );
               }

            }
         }
      }
      FREE ( sql );
      FREE ( path );
      FREE ( file );
      closedir ( dirp );
   }

   /* XXX: czy napewno musimy zamykać połączenie do bazy danych? */
   PQfinish ( pdata->catdb );
   pdata->catdb = NULL;

   return 0;
}

/* funkcja rekursywnie kasująca katalog wraz z zawartością */
int remove_dir ( pgsqldata * pdata, char * dir ){

   DIR * dirp;
   struct dirent * filedir;
   char * file;
   struct stat st;
//   int err;
   int ret = 0;

   /* jeśli nazw katalogu jest pusta (NULL) to nie zajmujemy się nią */
   if ( dir ){
      dirp = opendir ( dir );
      if ( dirp ){
         file = MALLOC ( PATH_MAX );
         if ( ! file ){
            logprg ( LOGERROR, "out of memeory!" );
            return 1;
         }
         while ( ( filedir = readdir ( dirp ) ) ){
            if ( strcmp ( filedir->d_name, "."  ) != 0 &&
                 strcmp ( filedir->d_name, ".." ) != 0 ){
   
               /* budujemy nazwę do zbadania */
               snprintf ( file, PATH_MAX, "%s/%s", dir, filedir->d_name );
   
               stat ( file, &st );
               if ( S_ISDIR ( st.st_mode ) ){
                  ret = remove_dir ( pdata, file );
               } else {
                  unlink ( file );
               }
            }
         }
         closedir ( dirp );
         FREE ( file );
         rmdir ( dir );
      }
   }
   return ret;
}

int remove_pgdata_cluster ( pgsqldata * pdata ){

   if ( pdata->verbose ){
      logprg ( LOGINFO, "remove old pgdata cluster" );
   }
   return remove_dir ( pdata, pdata->where ? 
               pdata->where : search_key ( pdata->paramlist, "PGDATA" ) );
}

int remove_pgdata_tablespaces ( pgsqldata * pdata ){

   DIR * dirp;
   struct dirent * filedir;
   char * file;
   char * path;
   char * link;
   int dl;
   int ret = 0;
   
   path = MALLOC ( PATH_MAX );
   if ( ! path ){
      logprg ( LOGERROR, "out of memeory!" );
      return 1;
   }

   snprintf ( path, PATH_MAX, "%s/%s",
            pdata->where ? pdata->where :
            search_key ( pdata->paramlist, "PGDATA" ),
            "pg_tblspc" );

   dirp = opendir ( path );
   if ( dirp ){
      if ( pdata->verbose ){
         logprg ( LOGINFO, "remove old tablespaces" );
      }
      file = MALLOC ( PATH_MAX );
      if ( ! file ){
         logprg ( LOGERROR, "out of memeory!" );
         return 1;
      }
      link = MALLOC ( PATH_MAX );
      if ( ! link ){
         logprg ( LOGERROR, "out of memeory!" );
         return 1;
      }
      while ( ( filedir = readdir ( dirp ) ) ){
         if ( strcmp ( filedir->d_name, "."  ) != 0 &&
              strcmp ( filedir->d_name, ".." ) != 0 ){

            snprintf ( file, PATH_MAX, "%s/%s", path, filedir->d_name );
            dl = readlink ( file, link, PATH_MAX - 1 );
            if ( dl < 0 ){
               continue;
            }
            /* sprawdzamy, czy link jest względny czy bezwzględny */
            if ( link [ 0 ] == '/' ){
               /* bezwzględny link to dobrze */
               ret = remove_dir ( pdata, link );
               if ( ret ){
                  break;
               }
            } else {
               continue;
            }
         }
      }
      closedir ( dirp );
      FREE ( file );
      FREE ( link );
   }

   FREE ( path );
      
/* XXX: nie wiem po co to zrobiłem !!! Do sprawdzenia !!! */
//   return remove_dir ( pdata, pdata->where );
   return ret;
}

/*
 * wykonuje funkcję skrótu na haśle do directora
 * zwracany string jest alokowany przez MALLOC
 * i powinien być zwolniony przez FREE
 */
char * pasword_md5digest ( pgsqldata * pdata ){

   struct MD5Context md5c;
   unsigned char digest[CRYPTO_DIGEST_MD5_SIZE];
   char * password;
   char * p;
   unsigned int i,j;

   password = MALLOC ( CRYPTO_DIGEST_MD5_SIZE * 2 + 1);
   if ( ! password ){
      abortprg ( pdata, 14, "out of memory!" );
   }

   MD5Init(&md5c);
   p = search_key ( pdata->paramlist, "DIRPASSWD" );
   MD5Update(&md5c, (unsigned char *) p, strlen ( p ) );
   MD5Final(digest, &md5c);
   for (i = j = 0; i < sizeof(digest); i++) {
      sprintf ( &password[j], "%02x", digest[i] );
      j += 2;
   }

   return password;
}

/*
 * funkcja tworzy gniazdo do komunikacji z directorem, a następnie
 * wykonuje do niego podłączenie
 * funkcja zwraca 0 jeśli wszystko jest ok lub różne od zera jeśli błąd
 */
int connect_director_socket ( pgsqldata * pdata ){

   char * host;
   char * port;
   int err;
   struct addrinfo * ad;

   host = search_key ( pdata->paramlist, "DIRHOST" );
   port = search_key ( pdata->paramlist, "DIRPORT" );

   err = getaddrinfo ( host, port, NULL, &ad );
   if ( err ){
      logprg ( LOGERROR, "error getting host address:" );
      logprg ( LOGERROR, gai_strerror ( err ) );
      return 1;
   }

   pdata->bsock = socket ( ad->ai_family, ad->ai_socktype, ad->ai_protocol );
   if ( pdata->bsock < 0 ){
      logprg ( LOGERROR, "error creating socket" );
      logprg ( LOGERROR, strerror ( errno ) );
      return 1;
   }

   err = connect ( pdata->bsock, ad->ai_addr, ad->ai_addrlen );
   if ( err ){
      logprg ( LOGERROR, "error connecting host");
      logprg ( LOGERROR, strerror ( errno ) );
      return 1;
   }

   freeaddrinfo ( ad );

   return 0;
}

/*
 *
 */
int send_director_msg ( pgsqldata * pdata, char * msg ){

   consmsgbuf msgbuf;
   int len;
   int err;

   len = strlen ( msg );
   if ( len < MSGBUFLEN ){
      /* komunikat zmieści się w naszym buforze */
      msgbuf.blen = (int) htonl ( len );
      strncpy ( msgbuf.mbuf, msg, MSGBUFLEN );
      len += sizeof ( int );

      // printf ("S: %s\n", msgbuf.buf );
      err = write ( pdata->bsock, &msgbuf, len );
      if ( err !=  len ){
         logprg ( LOGERROR, "error writting to socket" );
         logprg ( LOGERROR, strerror ( errno ) );
         return 1;
      }
   } else {
      logprg ( LOGERROR, "send msg error -> msg to long" );
      return 1;
   }

   return 0;
}

/*
 * funkcja odczytuje komunikat z directora i umieszcza go w zaalokowanym buforze
 * niepotrzebny bufor zwalnia się za pomocą free
 */
char * recv_director_msg ( pgsqldata * pdata ){

   int err;
   int len;
   char * msg;

   err = read ( pdata->bsock, &len, sizeof ( int ) );
   if ( err < 0 || err < (int) sizeof ( int ) ){
      logprg ( LOGERROR, "error reading socket" );
      logprg ( LOGERROR, strerror ( errno ) );
      return NULL;
   }
   /* w zmiennej len będziemy mieli wielkość komunikatu */
   len = ntohl ( len );

   /* sprawdzamy czy przesyłany komunikat zmieści się w naszym buforze */
   if ( len > MSGBUFLEN ){
      logprg ( LOGERROR, "recv msg error -> msg to long" );
      return NULL;
   }

   /* wartosc ujemna oznacza koniec transmisji */
   if ( len < 0 || len == 0 ){
      return NULL;
   }
   /* alokujemy bufor na komunikat + EOS */
   msg = MALLOC ( len + 1 );
   err = read ( pdata->bsock, msg, len );
   /* jeśli mamy błąd (err < 0) lub odczytaliśmy nie tyle co chcieliśmy
    * to sygnalizujemy problem */
   if ( err < 0 || err < len ){
      logprg ( LOGERROR, "error reading socket" );
      logprg ( LOGERROR, strerror ( errno ) );
      return NULL;
   }
   /* dodajemy EOS */
   msg[len] = 0;

   return msg;
}

int auth_director ( pgsqldata * pdata, char * password ){

   char * buf;
   char * msg;
   int err;
   char chal[256];
   int tls;
   uint8_t hmac[20];

   buf = MALLOC ( MSGBUFLEN );
   if ( ! buf ){
      logprg ( LOGERROR, "out of memory" );
      return 1;
   }

   snprintf ( buf, MSGBUFLEN, "Hello %s calling\n",
            search_key ( pdata->paramlist, "ARCHCLIENT" ) );

   /* S: Hello client calling\n */
   err = send_director_msg ( pdata, buf );
   if ( err ){
      /* error */
      return 1;
   }

   /* R: auth cram-md5 <aaa.bbb@director> ssl=c */
   msg = recv_director_msg ( pdata );
   if ( ! msg ){
      /* error */
      return 1;
   }

   /* sprawdzamy co otrzymaliśmy */
   err = sscanf( msg, "auth cram-md5 %s ssl=%d", chal, &tls);
   if ( err != 2 ){
      logprg ( LOGERROR, "error in director challenge!" );
      return 1;
   }
   FREE ( msg );

   /* szyfrujemy token naszym hasłem */
   hmac_md5 ( (uint8_t *) chal,     strlen ( chal ),
              (uint8_t *) password, strlen ( password ),
               hmac );

   /* konwertujemy to wszystko do base64 */
   bin_to_base64 ( buf, 50, (char *) hmac, 16, 1 );

   /* S: <hmd5 chellenge response> */
   err = send_director_msg ( pdata, buf );
   if ( err ){
      /* error */
      return 1;
   }

   /* R: 1000 OK auth */
   msg = recv_director_msg ( pdata );
   if ( ! msg ){
      /* error */
      return 1;
   }
   
   /* 
    * XXX: zakładamy, że odpowiedź była prawidłowa :) 
    * TODO: dopisać sscanf weryfikujący odpowiedź
    */
   FREE ( msg );

   snprintf ( buf, MSGBUFLEN,
            "auth cram-md5 <656267917.1285157106@%s> ssl=%i\n",
            search_key ( pdata->paramlist, "ARCHCLIENT" ),
            tls );

   /* S: auth cram-md5 <aaa.bbb@client> ssl=c */
   err = send_director_msg ( pdata, buf );
   if ( err ){
      /* error */
      return 1;
   }

   /* R: <hmd5 chellenge response> */
   msg = recv_director_msg ( pdata );
   if ( ! msg ){
      /* error */
      return 1;
   }
   FREE ( msg );

   /* zakładamy, że bacula nam dobrze odpowiedziała i wysyłamy OK :) */
   snprintf ( buf, MSGBUFLEN, "1000 OK auth\n" );

   /* S: 1000 OK auth\n */
   err = send_director_msg ( pdata, buf );

   /* R: 1000 OK: <director-dir> Version: <version> */
   msg = recv_director_msg ( pdata );

   /* musimy sprawdzić czy wszystko się udało */
   err = sscanf( msg, "1000 OK: %s", buf);
   if ( err != 1 ){
      logprg ( LOGERROR, "error in authentication!" );
      return 1;
   }
   
   /* XXX: w msg mamy cały string => źle, w buf mamy tylko nazwę, też nie najlepiej...
    * w buf mamy aktualną wersję directora, pochwalmy się tym :) */
   if ( pdata->verbose ){
      logprg ( LOGINFO, msg );
   }
   
   FREE ( msg );
   FREE ( buf );

   return 0;
}

int shutdown_director_socket ( pgsqldata * pdata ){

   shutdown ( pdata->bsock, SHUT_RDWR );

   return 0;
}

int connect_director ( pgsqldata * pdata ){

   char * password;
   int err;

   err = connect_director_socket ( pdata );
   if ( err ){
      /* error */
      return 1;
   }

   password = pasword_md5digest ( pdata );
   if ( ! password ){
      /* error */
      return 1;
   }

   err = auth_director ( pdata, password );
   if ( err ){
      /* error */
      return 1;
   }

   return 0;
}

/* funkcja czyści (odczytuje w kosmos) aktualny bufor danych jakie wysłał
 * do nas director */
void clear_receive_buffer ( pgsqldata * pdata ){

   char * junk;
   /* odczytujemy bufor aż do końca treści */
   while (( junk = recv_director_msg ( pdata ) )){
      FREE ( junk );
   }
}

enum FILESETTYPE {
   DBFILESET      = 0,
   ARCHFILESET    = 1,   
};

static const char * fstemplate[2][2] =
{ 
   { "pgsqldb:", "pgsqltbs:" },
   { "pgsqlarch:", "pgsqlarch:" }
};

/*
 * funkcja weryfikuje po stronie baculi który z dostępnych filesetów
 * odpowiada za backup plików bazodanowych
 * in:
 *    pdata
 * out:
 *    fileset - zaalokowana nazwa filesetu do wykorzystania w komendzie restore
 */
char * find_fileset ( pgsqldata * pdata, int type ){

   int err;
   int len;
   char * msg;
   char * recv;
   char * fileset1;
   char * fileset2;
   char * fileset = NULL;

   msg = MALLOC ( BUFLEN );
   if ( ! msg ){
      logprg ( LOGERROR, "out of memeory!" );
      return NULL;
   }

   /* ".filesets */
   err = send_director_msg ( pdata, (char *) ".filesets" );
   if ( err ){
      logprg ( LOGERROR, "Error sending data (.filesets)" );
      return NULL;
   }

   /* we expect two filesets name only */
   fileset1 = recv_director_msg ( pdata );
   if ( !fileset1 ){
      logprg ( LOGERROR, "Error: No filesets found" );
      return NULL;
   }

   fileset2 = recv_director_msg ( pdata );
   if ( fileset2 ){
      /* read two filesets, clear other data */
      clear_receive_buffer ( pdata );
   } else {
      logprg ( LOGERROR, "Error: No valid filesets found" );
      return NULL;
   }

   /* if fileset names has an end line char we should erase this char */
   len = strlen ( fileset1 ) - 1;
   if ( fileset1 != NULL && len > 0 && fileset1 [ len ] == '\n' ){
      fileset1 [ len ] = 0;
   }
   len = strlen ( fileset2 ) - 1;
   if ( fileset2 != NULL && len > 0 && fileset2 [ len ] == '\n' ){
      fileset2 [ len ] = 0;
   }
   /* check which fileset will be valid, startinf from fileset1 */
   snprintf ( msg, BUFLEN, "restore fileset=\"%s\" select",
               fileset1 );

   err = send_director_msg ( pdata, msg );
   if ( err ){
      FREE ( fileset1 );
      FREE ( fileset2 );
      logprg ( LOGERROR, "Error sending data (restore fileset)" );
      return NULL;
   }
   /* XXX TODO: handle "No Full backup before ..." error message */
   clear_receive_buffer ( pdata );

   /* ls */
   err = send_director_msg ( pdata, (char *) "ls" );
   if ( err ){
      FREE ( fileset1 );
      FREE ( fileset2 );
      logprg ( LOGERROR, "Error sending data (ls)" );
      return NULL;
   }

   /* important */
   recv = recv_director_msg ( pdata );
   if ( recv ){
      clear_receive_buffer ( pdata );
   }
   
   if ( strncmp ( recv, fstemplate[type][0], strlen ( fstemplate[type][0] ) ) == 0 ||
        strncmp ( recv, fstemplate[type][1], strlen ( fstemplate[type][1] ) ) == 0 ){

      /* shoot !!! */
      FREE ( recv );
      FREE ( fileset2 );
      fileset = fileset1;
   } else {
      FREE ( recv );
      FREE ( fileset1 );
      /* check second option */
      err = send_director_msg ( pdata, (char *) "." );
      if ( err ){
         FREE ( fileset2 );
         logprg ( LOGERROR, "Error sending data (.)" );
         return NULL;
      }

      clear_receive_buffer ( pdata );

      snprintf ( msg, BUFLEN, "restore fileset=\"%s\" select",
                  fileset2 );

      err = send_director_msg ( pdata, msg );
      if ( err ){
         FREE ( fileset2 );
         logprg ( LOGERROR, "Error sending data (restore fileset)" );
         return NULL;
      }
      /* XXX TODO: handle "No Full backup before ..." error message */
      clear_receive_buffer ( pdata );

      /* ls */
      err = send_director_msg ( pdata, (char *) "ls" );
      if ( err ){
         FREE ( fileset2 );
         logprg ( LOGERROR, "Error sending data (ls)" );
         return NULL;
      }

      /* important */
      recv = recv_director_msg ( pdata );
      clear_receive_buffer ( pdata );

      if ( strncmp ( recv, fstemplate[type][0], strlen ( fstemplate[type][0] ) ) == 0 ||
           strncmp ( recv, fstemplate[type][1], strlen ( fstemplate[type][1] ) ) == 0 ){
         /* shoot! */
         fileset = fileset2;
         FREE ( recv );
      } else {
         /* no valid fileset or backup found */
         FREE ( recv );
         FREE ( fileset2 );
         return NULL;
      }
   }

   err = send_director_msg ( pdata, (char *) "." );
   if ( err ){
      FREE ( fileset );
      logprg ( LOGERROR, "Error sending data (.)" );
      return NULL;
   }
   clear_receive_buffer ( pdata );

   return fileset;
}

/*
 * performs restore command with any available information about fileset
 * additional it verifies jobid number and waits for its finish
 *
 * in:
 *    pdata
 *    fileset - fileset name which will be used for restore
 * out:
 *    err - 0 if everything OK, 1 on error
 */
int restore_db_files ( pgsqldata * pdata, const char * fileset ){

   char * msg;
   char * recv;
   int err;
   int rjobid,fjobid;
   char jobstatus;

   /* now we can send commands */
   msg = MALLOC ( BUFLEN );
   if ( ! msg ){
      logprg ( LOGERROR, "out of memeory!" );
      return 1;
   }

   /*
    * restore command for recovery:
    * > restore where=<where> fileset=<$fileset> [restoreclient=<client>] [current,before="YYYY-MM-DD HH:MM:SS"] select
    * - where is optional, if we have a restore date then we have to supply before
    */
   snprintf ( msg, BUFLEN, "restore where=\"%s\" fileset=\"%s\" %s%s %s%s%s%s select yes",
               // pdata->where ? pdata->where : search_key ( pdata->paramlist, "PGDATA" ),
               pdata->where ? pdata->where : "\"\"",
               fileset,
               pdata->restoreclient ? "restoreclient=" : "",
               pdata->restoreclient ? pdata->restoreclient : "",
               pdata->pitr == PITR_CURRENT || pdata->pitr == PITR_XID ?
                  "current" : "before=",
               pdata->pitr == PITR_TIME ? "\"" : "",
               pdata->pitr == PITR_TIME ? pdata->restorepit : "",
               pdata->pitr == PITR_TIME ? "\"" : "" );

   err = send_director_msg ( pdata, msg );

   if ( err ){
      logprg ( LOGERROR, "Error sending data (restore fileset select)" );
      return 1;
   }

   clear_receive_buffer ( pdata );

   /* add pgsqldb:<archclient>/ */
   snprintf ( msg, BUFLEN, "add pgsqldb:%s/",
               search_key ( pdata->paramlist, "ARCHCLIENT" ) );

   err = send_director_msg ( pdata, msg );
   if ( err ){
      logprg ( LOGERROR, "Error sending data (add fileset)" );
      return 1;
   }
   clear_receive_buffer ( pdata );

   /* add pgsqltbs:<archclient>/ */
   /* TODO: chech how Bacula handle no tablespaces files
    * done -> OK, prints: No files marked */
   snprintf ( msg, BUFLEN, "add pgsqltbs:%s/",
               search_key ( pdata->paramlist, "ARCHCLIENT" ) );

   err = send_director_msg ( pdata, msg );
   if ( err ){
      logprg ( LOGERROR, "Error sending data (add fileset)" );
      return 1;
   }
   clear_receive_buffer ( pdata );

   /* done */
   err = send_director_msg ( pdata, (char *) "done" );
   if ( err ){
      logprg ( LOGERROR, "Error sending data (done)" );
      return 1;
   }

   /* searching for JobId number in running job */
   while (( recv = recv_director_msg ( pdata ) )){
      if ( pdata->verbose ){
         logprg ( LOGINFO, recv );
      }
      err = sscanf ( recv, "%*[^.]. JobId=%i", &rjobid );
      if ( err == 1 ){
         break;
      }
   }
   clear_receive_buffer ( pdata );

   if ( pdata->verbose ){
      snprintf ( msg, BUFLEN, "starting restore job: JobId=%i", rjobid );
      logprg ( LOGINFO, msg );
      logprg ( LOGINFO, "waiting for job to finish" );
   }
   
   snprintf ( msg, BUFLEN, "wait jobid=%i", rjobid );

   err = send_director_msg ( pdata, msg );
   if ( err ){
      logprg ( LOGERROR, "Error sending data (wait jobid)" );
      return 1;
   }

   /*
    * JobId=NN
    * JobStatus=OK (T)
    */
   recv = recv_director_msg ( pdata );
   err = sscanf ( recv, "JobId=%i", &fjobid );
   FREE ( recv );
   if ( err != 1 ){
      logprg ( LOGERROR, "Error waiting for restore job" );
      return 1;
   }
   recv = recv_director_msg ( pdata );
   //err = sscanf ( recv, "JobStatus=%*[^ ] (%c)", &jobstatus );
   err = sscanf ( recv, "JobStatus=%s (%c)", msg, &jobstatus );
   FREE ( recv );
   if ( err != 2 ){
      logprg ( LOGERROR, "Error restore jobstatus" );
      return 1;
   }
   if ( jobstatus != 'T' ){
      /* job finished with error */
      logprg ( LOGERROR, "Error restore job:" );
      logprg ( LOGERROR, msg );
      return 1;
   }

   clear_receive_buffer ( pdata );

   return 0;
}

/*
 * 
 */
int tune_tablespaces_links ( pgsqldata * pdata ){

   DIR * dirp;
   struct dirent * filedir;
   char * file;
   char * path;
   char * link;
   char * reltab;
   int dl;
   int err;
   struct stat statp;
   pgugid pgid;

   file = MALLOC ( PATH_MAX );
   if ( ! file ){
      logprg ( LOGERROR, "out of memeory!" );
      return 1;
   }
   path = MALLOC ( PATH_MAX );
   if ( ! path ){
      logprg ( LOGERROR, "out of memeory!" );
      FREE ( file );
      return 1;
   }
   link = MALLOC ( PATH_MAX );
   if ( ! link ){
      logprg ( LOGERROR, "out of memeory!" );
      FREE ( path );
      FREE ( file );
      return 1;
   }
   reltab = MALLOC ( PATH_MAX );
   if ( ! reltab ){
      logprg ( LOGERROR, "out of memeory!" );
      FREE ( link );
      FREE ( path );
      FREE ( file );
      return 1;
   }

   get_pgdata_pgugid ( pdata, &pgid );

   snprintf ( path, PATH_MAX, "%s/pg_tblspc",
            pdata->where ? pdata->where :
            search_key ( pdata->paramlist, "PGDATA" ) );

   dirp = opendir ( path );
   if ( dirp ){
      while ( ( filedir = readdir ( dirp ) ) ){
         if ( strcmp ( filedir->d_name, "."  ) != 0 &&
              strcmp ( filedir->d_name, ".." ) != 0 ){

            snprintf ( file, PATH_MAX, "%s/%s", path, filedir->d_name );
            dl = readlink ( file, link, PATH_MAX - 1 );
            if ( dl < 0 ){
               continue;
            }

            /* terminate link string */
            link [ dl ] = '\0';

            /* checking for link owner */
            err = lstat ( file, &statp );
            if ( err ){
               logprg ( LOGERROR, "error stat on link" );
               FREE ( reltab );
               FREE ( link );
               FREE ( path );
               FREE ( file );
               return 1;
            }

            /* symbolic link removal */
            unlink ( file );

            /*  creating new link */
            snprintf ( reltab, PATH_MAX, "%s%s",
                        pdata->where ? pdata->where :
                        search_key ( pdata->paramlist, "PGDATA" ),
                        link );

            if ( pdata->verbose ){
               logprg ( LOGINFO, reltab);
            }

            err = symlink ( reltab, file );
            if ( err ){
               logprg ( LOGERROR, "error creating tablespace reference" );
               FREE ( reltab );
               FREE ( link );
               FREE ( path );
               FREE ( file );
               return 1;
            }
            /* setting a correct owner */
            err = lchown ( file, statp.st_uid, statp.st_gid );
            if ( err ){
               logprg ( LOGERROR, "error creating tablespace reference" );
               FREE ( reltab );
               FREE ( link );
               FREE ( path );
               FREE ( file );
               return 1;
            }

            /* set recursive pgid owner of restored tablespace if we did a relocation */
            if ( pdata->where ){
               while ( strcmp (reltab,pdata->where) != 0 ){
                  /* we finish chown up to where directory */
                  chown ( reltab, pgid.uid, pgid.gid );
                  for (dl=strlen(reltab);reltab[dl]!='/';dl--);
                  reltab[dl] = '\0';
               }
            }
         }
      }
   }

   FREE ( reltab );
   FREE ( link );
   FREE ( path );
   FREE ( file );

   return 0;
}

/*
 * bconsole commands for database restore:
   > * .filesets
 * for any supplied filesets we execute
   > * restore fileset=<$fileset> select
   > * ls
 * now we should get information about which fileset it is, verifing and if everything is ok, execute:
   > * restore where=<where> fileset=<$fileset> [restoreclient=<client>][current,before="YYYY-MM-DD HH:MM:SS"] select
 */
int restore_database ( pgsqldata * pdata ){

   int err;
   char * fileset;   // allocated

   if ( pdata->verbose ){
      logprg ( LOGINFO, "connecting to director" );
   }
   
   err = connect_director ( pdata );
   if ( err ){
      logprg ( LOGERROR, "Error connecting to director" );
      return 1;
   }

   if ( pdata->verbose ){
      logprg ( LOGINFO, "looking for pgsql backups..." );
   }
   
   fileset = find_fileset ( pdata, DBFILESET );
   if ( !fileset ){
      logprg ( LOGERROR, "no valid fileset found" );
      shutdown_director_socket ( pdata );
      return 1;
   }

   if ( pdata->verbose ){
      logprg ( LOGINFO, "pgsql backups found" );
      logprg ( LOGINFO, "preparing for pgsql restore" );
   }
   
   err = restore_db_files ( pdata, fileset );
   FREE ( fileset );
   if ( err ) {
      logprg ( LOGERROR, "error restoring database files" );
      shutdown_director_socket ( pdata );
      return 1;
   }

   if ( pdata->where ){
      if ( pdata->verbose ){
         logprg ( LOGINFO, "tablespace references tuning" );
      }
      err = tune_tablespaces_links ( pdata );
      if ( err ) {
         logprg ( LOGERROR, "error restoring database files" );
         shutdown_director_socket ( pdata );
         return 1;
      }
   }

   if ( pdata->verbose ){
      logprg ( LOGINFO, "datafiles restore done" );
      logprg ( LOGINFO, "disconnecting from director" );
   }
   
   shutdown_director_socket ( pdata );

   return 0;
}

/*
 * performs restore command with any available information about fileset
 * additional it verifies jobid number and waits for its finish, it performs
 * restore of the wal file
 *
 * in:
 *    pdata
 *    fileset - fileset name which will be used for restore
 * out:
 *    err - 0 if everything OK, 1 on error
 */
int restore_wal_file ( pgsqldata * pdata, const char * fileset ){

   char * msg;
   char * recv;
   char * fullpath;
   char * wheredir;
   int err;
   int rjobid,fjobid;
   char jobstatus;

   msg = MALLOC ( BUFLEN );
   if ( ! msg ){
      logprg ( LOGERROR, "out of memeory!" );
      return 1;
   }
   fullpath = MALLOC ( BUFLEN );
   if ( ! fullpath ){
      FREE ( msg );
      logprg ( LOGERROR, "out of memeory!" );
      return 1;
   }
   
   /* building a restore path for where */
   realpath ( pdata->pathtowalfilename, fullpath );
   wheredir = dirname ( fullpath );
   
   /*
    * restore command execution:
    * > restore where=<real_pathtowalfilename> fileset=<$fileset> file=<pgsqlarch:$client/walfilename> [restoreclient=<client>] done yes
    * - where is optional, if we have a restore date then we have to supply before
    * restore where=/tmp/pg_xlog fileset=pgsql-archset file=pgsqlarch:ubuntu-client-test/000000010000000000000024 done yes
    */
   /* XXX: is before really needed for pgsqlarch? */
#if 0
   snprintf ( msg, BUFLEN, 
            "restore where=\"%s\" fileset=\"%s\" file=\"pgsqlarch:%s/%s\" %s%s done yes",
            wheredir, fileset,
            search_key ( pdata->paramlist, "ARCHCLIENT" ),
            pdata->walfilename,
            pdata->pitr == PITR_CURRENT || pdata->pitr == PITR_XID ?
               "current" : "before=",
            pdata->pitr == PITR_TIME ? pdata->restorepit : "" );
#else
   snprintf ( msg, BUFLEN, 
            "restore where=\"%s\" fileset=\"%s\" file=\"pgsqlarch:%s/%s\" %s%s done yes",
            wheredir, fileset,
            search_key ( pdata->paramlist, "ARCHCLIENT" ),
            pdata->walfilename,
            pdata->restoreclient ? "restoreclient=" : "",
            pdata->restoreclient ? pdata->restoreclient : "" );
#endif

   err = send_director_msg ( pdata, msg );

   if ( err ){
      logprg ( LOGERROR, "Error sending data (restore file)" );
      return 1;
   }

   /* searching for JobId number in running job message */
   while (( recv = recv_director_msg ( pdata ) )){
      if ( pdata->verbose ){
         logprg ( LOGINFO, recv );
      }
      err = sscanf ( recv, "%*[^.]. JobId=%i", &rjobid );
      if ( err == 1 ){
         break;
      }
   }
   
   clear_receive_buffer ( pdata );

   if ( pdata->verbose ){
      snprintf ( msg, BUFLEN, "starting restore job: JobId=%i", rjobid );
      logprg ( LOGINFO, msg );
      logprg ( LOGINFO, "waiting for job to finish" );
   }
   
   snprintf ( msg, BUFLEN, "wait jobid=%i", rjobid );

   err = send_director_msg ( pdata, msg );
   if ( err ){
      logprg ( LOGERROR, "Error sending data (wait jobid)" );
      return 1;
   }

   /*
    * JobId=NN
    * JobStatus=OK (T)
    */
   recv = recv_director_msg ( pdata );
   err = sscanf ( recv, "JobId=%i", &fjobid );
   FREE ( recv );
   if ( err != 1 ){
      logprg ( LOGERROR, "Error waiting for restore job" );
      return 1;
   }
   recv = recv_director_msg ( pdata );
   //err = sscanf ( recv, "JobStatus=%*[^ ] (%c)", &jobstatus );
   err = sscanf ( recv, "JobStatus=%s (%c)", msg, &jobstatus );
   FREE ( recv );
   
   if ( err != 2 ){
      logprg ( LOGERROR, "Error restore jobstatus" );
      return 1;
   }
   
   if ( jobstatus != 'T' ){
      /* job finished with an error */
      logprg ( LOGERROR, "Error restore job:" );
      logprg ( LOGERROR, msg );
      return 1;
   }

   clear_receive_buffer ( pdata );

   return 0;
}

/*
 * bconsole commands for wal files restore:
 > * .filesets
 * execute for every supplied fileset
 > * restore fileset=<$fileset> select
 > * ls
 * now we should get information about fileset, if it is a valid one, execute:
 > * restore where=<where> fileset=<$fileset> file=<walfilename> ???
 */
int restore_arch ( pgsqldata * pdata ){

   int err;
   char * fileset;   // allocated

   if ( pdata->verbose ){
      logprg ( LOGINFO, "connecting to director" );
   }
   
   err = connect_director ( pdata );
   if ( err ){
      logprg ( LOGERROR, "Error connecting to director" );
      return 1;
   }
   
   if ( pdata->verbose ){
      logprg ( LOGINFO, "looking for pgsqlarch backups..." );
   }
   
   fileset = find_fileset ( pdata, ARCHFILESET );
   if ( !fileset ){
      logprg ( LOGERROR, "no valid fileset found" );
      shutdown_director_socket ( pdata );
      return 1;
   }

   if ( pdata->verbose ){
      logprg ( LOGINFO, "pgsqlarch backups found" );
      char * msg;
      msg = MALLOC ( BUFLEN );
      ASSERT_NVAL_RET_NONE ( msg );
      snprintf ( msg, BUFLEN, "preparing for wal restore: %s", pdata->walfilename );
      logprg ( LOGINFO, msg );
      FREE ( msg );
   }
   
   err = restore_wal_file ( pdata, fileset );
   FREE ( fileset );
   if ( err ) {
      logprg ( LOGERROR, "error restoring database files" );
      shutdown_director_socket ( pdata );
      return 1;
   }

   if ( pdata->verbose ){
      logprg ( LOGINFO, "wal file restore done" );
      logprg ( LOGINFO, "disconnecting from director" );
   }
   
   shutdown_director_socket ( pdata );

   return 0;
}

/*
 * it creates a recovery.conf file in cluster directory (PGDATA) and fills it with
 * (recovery_command and recovery_target_* if user has supplied a point in time)
 */
int create_recovery_conf ( pgsqldata * pdata ){

   char * recovery_path;         // allocated
   char * recovery_command;      // allocated
   char * recovery_target_point; // allocated
   int recovery_file;
   int err;

   recovery_path = MALLOC ( PATH_MAX );
   if ( ! recovery_path ){
      logprg ( LOGERROR, "out of memeory!" );
      return 1;
   }

   /* recovery.conf file in main cluster directory PGDATA */
   snprintf ( recovery_path, PATH_MAX, "%s/recovery.conf",
            pdata->where ? pdata->where :
            search_key ( pdata->paramlist, "PGDATA" ) );

   recovery_file = open ( recovery_path, O_CREAT | O_WRONLY, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH );
   FREE ( recovery_path );
   if ( recovery_file < 0 ){
      err = errno;
      logprg ( LOGERROR, "error opening recovery.conf file!" );
      logprg ( LOGERROR, strerror ( err ) );
      return 1;
   }

   recovery_command = MALLOC ( PATH_MAX );
   if ( ! recovery_command ){
      logprg ( LOGERROR, "out of memeory!" );
      return 1;
   }

   snprintf ( recovery_command, PATH_MAX, "restore_command = '%s -v -c %s %s%s wal %%f %%p'\n",
               get_program_directory (),
               pdata->configfile,
               pdata->restoreclient ? "-r " : "",
               pdata->restoreclient ? pdata->restoreclient : "" );
   err = write ( recovery_file, recovery_command, strlen (recovery_command) );
   if ( err < (int)strlen ( recovery_command ) ){
      err = errno;
      logprg ( LOGERROR, "error writting to recovery.conf file!" );
      logprg ( LOGERROR, strerror ( err ) );
      FREE ( recovery_command );
      return 1;
   }
   FREE ( recovery_command );
   
   if ( pdata->pitr != PITR_CURRENT ){
      recovery_target_point = MALLOC ( PATH_MAX );
      if ( ! recovery_target_point ){
         logprg ( LOGERROR, "out of memeory!" );
         return 1;
      }

      snprintf ( recovery_target_point, PATH_MAX, 
                  pdata->pitr == PITR_TIME ?
                     "recovery_target_time = '%s'\n" :
                     "recovery_target_xid = '%s'\n",
                     pdata->restorepit );

      err = write ( recovery_file, recovery_target_point, strlen ( recovery_target_point ) );
      if ( err < (int) strlen ( recovery_target_point ) ){
         err = errno;
         logprg ( LOGERROR, "error writting to recovery.conf file!" );
         logprg ( LOGERROR, strerror ( err ) );
         FREE ( recovery_target_point );
         return 1;
      }
      FREE ( recovery_target_point );
   }
   
   close ( recovery_file );
   
   return 0;
}
/*
 * erases a recovery.conf file in cluster directory (PGDATA)
 */
int delete_recovery_conf ( pgsqldata * pdata ){

   char * recovery_path;         // allocated
   int err = 0;

   recovery_path = MALLOC ( PATH_MAX );
   if ( ! recovery_path ){
      logprg ( LOGERROR, "out of memeory!" );
      return 1;
   }

   /* recovery.conf file in main cluster directory PGDATA */
   snprintf ( recovery_path, PATH_MAX, "%s/recovery.done",
            pdata->where ? pdata->where :
            search_key ( pdata->paramlist, "PGDATA" ) );

   if ( !access ( recovery_path, F_OK )){
      /* file exist */
      err = unlink ( recovery_path );
   } /* if not it is not a problem */

   FREE ( recovery_path );

   return err;
}

enum WALLOC {
   WAL_LOC_NOTFOUND = 1,
   WAL_LOC_BACULA,
   WAL_LOC_ARCHDEST,
};

int find_wal_location ( pgsqldata * pdata ){
   
   int status = WAL_LOC_NOTFOUND;
   int pgstatus;

   dbconnect ( pdata );

   pgstatus = _get_walstatus_from_catalog ( pdata );
   switch ( pgstatus ){
      /* archiwizacja */
      case PGSQL_STATUS_WAL_ARCH_FINISH:
      case PGSQL_STATUS_WAL_ARCH_MULTI:
         status = WAL_LOC_ARCHDEST;
         break;
      /* backup */
      case PGSQL_STATUS_WAL_BACK_DONE:
         status = WAL_LOC_BACULA;
         break;
   }

   return status;
}

/*
 * 
 */
int copy_wal_loc_archdest ( pgsqldata * pdata ){
   
   int err;
   char * buf;

   buf = MALLOC ( BUFLEN );
   snprintf ( buf, BUFLEN, "%s/%s", search_key ( pdata->paramlist, "ARCHDEST" ), pdata->walfilename );
   err = _copy_wal_file ( pdata, buf, pdata->pathtowalfilename );      
   FREE ( buf );

   return err;
}

/*
 * input parameters:
 * argv[0] [-c config_file ] [-t <recovery.time> | -x <recovery.xid> ] { -w <where> \
 *    restore | wal <wal.filename> <where> }
 *
 */
int main(int argc, char* argv[]){

   pgsqldata * pdata;
   int err = 0;
   int loc = 0;

   pgsqllibinit ( argc, argv );

   pdata = allocpdata ();
   parse_args ( pdata, argc, argv );

   switch ( pdata->mode ){
      case PGSQL_DB_RESTORE:
         inteos_info ( pdata );
         if ( pdata->verbose ){
            logprg ( LOGINFO, "Database RESTORE mode." );
         }
         print_restore_info ( pdata );

         /* 1.  Stop the postmaster, if it's running. */
         if ( check_postgres_is_running ( pdata ) ){
            /* shutdown postmaster */
            err = shutdown_postmaster ( pdata );
            if ( err ){
               logprg ( LOGERROR, "unable to shutdown PostgreSQL instance" );
               abortprg ( pdata, 8, "you have to shutdown instance manually" );
            }
         }

         /* 2. you need at the least to copy the contents of the pg_xlog subdirectory of the cluster
          * data directory, as it may contain logs which were not archived before the system went down. */
         /* in our case the best will be when we will execute a standard archival procedure as
          * pgsql-archlog does */
         err = copy_unarchived_wals ( pdata );
         if ( err ){
            logprg ( LOGWARNING, "cant copy unarchived wal files. some transactions will be lost" );
         }

         /* 3.  Clean out all existing files and subdirectories under the cluster data directory and
          * under the root directories of any tablespaces you are using. */
         err = remove_pgdata_tablespaces ( pdata );
         if ( err ){
            logprg ( LOGERROR, "cant clean postgres tablespaces" );
            abortprg ( pdata, 11, "you have to clean postgres tablespaces manually" );
         }

         err = remove_pgdata_cluster ( pdata );
         if ( err ){
            logprg ( LOGERROR, "cant clean postgres data cluster" );
            abortprg ( pdata, 10, "you have to clean postgres data cluster manually" );
         }

         /* 4.  Restore the database files from your backup dump. Be careful that they are restored
          * with the right ownership (the database system user, not root!) and with the right
          * permissions. If you are using tablespaces, you may want to verify that the symbolic links
          * in pg_tblspc/ were correctly restored. */
         /* tablespace restore and rellocation require additional steps:
          * 1. update a links in pg_tblspc for new location
          * 2. after database start, execute statement:
          * #  update pg_tablespace set spclocation='<new.location>' where spclocation='<old.location>' */
         err = restore_database ( pdata );
         if ( err ){
            abortprg ( pdata, 12, "cant restore postgres database" );
         }

         /* 5.  Remove any files present in pg_xlog/; these came from the backup dump and are therefore
          * probably obsolete rather than current. If you didn't archive pg_xlog/ at all, then
          * re-create it, and be sure to re-create the subdirectory pg_xlog/archive_status/ as
          * well. */
         /*
          * XXX: do we cover it in restore database?
          *    it suppose yes...
          */

         /* 6.  If you had unarchived WAL segment files that you saved in step 2, copy them into
          * pg_xlog/. (It is best to copy them, not move them, so that you still have the unmodified
          * files if a problem occurs and you have to start over.) */
         /* TODO: we should check if we really required ... 
          *    checked - it is not required, if pgsql needs a wal file it will ask about it */

         /* 7.  Create a recovery command file recovery.conf in the cluster data directory (see
          * Recovery Settings). You may also want to temporarily modify pg_hba.conf to prevent ordinary
          * users from connecting until you are sure the recovery has worked. */
         err = create_recovery_conf ( pdata );
         if ( err ){
            abortprg ( pdata, 13, "cant create restore.conf file" );
         }

         /* 8.  Start the postmaster. The postmaster will go into recovery mode and proceed to read
          * through the archived WAL files it needs. Upon completion of the recovery process, the
          * postmaster will rename recovery.conf to recovery.done (to prevent accidentally re-entering
          * recovery mode in case of a crash later) and then commence normal database
          * operations. */
         /* we will not modify a pg_hba.conf */
         err = startup_postmaster ( pdata );
         if ( err ){
            logprg ( LOGERROR, "unable to startup PostgreSQL instance" );
            abortprg ( pdata, 8, "you have to do it manually" );
         }

         /* 9.  Inspect the contents of the database to ensure you have recovered to where you want to
          * be. If not, return to step 1. If all is well, let in your users by restoring pg_hba.conf to
          * normal. */

         /* TODO: we have to check if recovered instance is working fine */
         err = delete_recovery_conf ( pdata );
         if ( err ){
            logprg ( LOGERROR, "unable to cleanup PostgreSQL cluster" );
            abortprg ( pdata, 8, "you have to do it manually before starting instance" );
         }
         
         break;
      case PGSQL_ARCH_RESTORE:
         if ( pdata->verbose ){
            logprg ( LOGINFO, "WAL RESTORE mode." );
         }
         /* 
          * running parameters should lay in:
          * pdata->walfilename
          * pdata->pathtowalfilename
          * 
          * in most cases path to wal file is relative, to PGDATA */
         loc = find_wal_location ( pdata );
         switch ( loc ){
            case WAL_LOC_BACULA:
               err = restore_arch ( pdata );
               break;
            case WAL_LOC_ARCHDEST:
               err = copy_wal_loc_archdest ( pdata );
               break;
            default:
               logprg ( LOGERROR, "Unable to find required WAL file" );
               abortprg ( pdata, 15, pdata->walfilename );
         }

         if ( err ){
            logprg ( LOGERROR, "unable to restore wal file" );
            logprg ( LOGERROR, pdata->walfilename );
            abortprg ( pdata, 13, "you have to do it manually" );
         }
         if ( pdata->verbose ){
            char * buf;
            buf = MALLOC ( BUFLEN );
            snprintf ( buf, BUFLEN, "WAL %s restore done.", pdata->walfilename );
            logprg ( LOGINFO, buf );
            FREE ( buf );
         }
         break;
   }

   PQfinish ( pdata->catdb );
   freepdata ( pdata );
   return 0;
}

#ifdef __cplusplus
}
#endif
