/*
 *  Copyright (c) 2013 Inteos Sp. z o.o.
 *  All right reserved. See LICENSE.pgsql for details.
 *
 * This is a Bacula plugin for making backup and restore of PostgreSQL database.
 *
 * config file: pgsql.conf

   #
   # Config file for pgsql plugin
   #
   PGDATA = <pg.data.cluster.path>
   PGHOST = <path.to.sockets.directory>
   PGPORT = <socket.'port'>
   PGVERSION = "8.x" | "9.x"
   CATDB = <catalog.db.name>
   CATDBHOST = <catalog.db.host>
   CATDBPORT = <catalog.db.port>
   CATUSER = <catalog.db.user>
   CATPASSWD = <catalog.db.password>
   ARCHDEST = <destination.of.archived.wal's.path>
   ARCHCLIENT = <name.of.archived.client>

 */
/*
TODO:
   * add a timeout watchdog thread to check if everything is ok.
   * add a remote ARCHDEST using SSH/SCP
   * add an offline backup mode of operation
   *   scenario:
   *     if database instance is down then perform a standard file backup (including pg_xlog)
   *     if database is running and plugin command having 'dboff' parameter then shutdown a database
   *        instance and perform a standard file backup (including pg_xlog), at the end startup an
   *        instance
   * add an integration with pgsqllib utility framework
   * prepare a docummentation about plugin
 */

#include "bacula.h"
#include "fd_plugins.h"
#include "fdapi.h"

/* it is PostgreSQL backup plugin, so we need a libpq library */
#include <libpq-fe.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <libgen.h>
#include <utime.h>

#include "keylist.h"
#include "parseconfig.h"

/* 
 * libbac uses its own sscanf implementation which is not compatible with
 * libc implementation, unfortunately. usage of bsscanf require format string rewriting.
 */
#ifdef sscanf
#undef sscanf
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define PLUGIN_LICENSE      "AGPLv3"
#define PLUGIN_AUTHOR       "Inteos Sp. z o.o."
#define PLUGIN_DATE         "April 2013"
#define PLUGIN_VERSION      "2.2"
#define PLUGIN_DESCRIPTION  "PostgreSQL online backup and recovery plugin (c) Inteos Sp. z o.o."

#define PLUGIN_INFO         "pgsql plugin: "

/* Forward referenced functions */
static bRC newPlugin(bpContext *ctx);
static bRC freePlugin(bpContext *ctx);
static bRC getPluginValue(bpContext *ctx, pVariable var, void *value);
static bRC setPluginValue(bpContext *ctx, pVariable var, void *value);
static bRC handlePluginEvent(bpContext *ctx, bEvent *event, void *value);
static bRC startBackupFile(bpContext *ctx, struct save_pkt *sp);
static bRC endBackupFile(bpContext *ctx);
static bRC pluginIO(bpContext *ctx, struct io_pkt *io);
static bRC startRestoreFile(bpContext *ctx, const char *cmd);
static bRC endRestoreFile(bpContext *ctx);
static bRC createFile(bpContext *ctx, struct restore_pkt *rp);
static bRC setFileAttributes(bpContext *ctx, struct restore_pkt *rp);
static bRC checkFile(bpContext *ctx, char *fname);
keylist * get_file_list ( bpContext *ctx, keylist * list, const char * base, const char * path );


/* Pointers to Bacula functions */
static bFuncs *bfuncs = NULL;
static bInfo  *binfo = NULL;

static pInfo pluginInfo = {
   sizeof(pluginInfo),
#ifdef FDAPI
   FDAPI,
#else
   FD_PLUGIN_INTERFACE_VERSION,
#endif
   FD_PLUGIN_MAGIC,
   PLUGIN_LICENSE,
   PLUGIN_AUTHOR,
   PLUGIN_DATE,
   PLUGIN_VERSION,
   PLUGIN_DESCRIPTION,
};

static pFuncs pluginFuncs = {
   sizeof(pluginFuncs),
   FD_PLUGIN_INTERFACE_VERSION,

   /* Entry points into plugin */
   newPlugin,
   freePlugin,
   getPluginValue,
   setPluginValue,
   handlePluginEvent,
   startBackupFile,
   endBackupFile,
   startRestoreFile,
   endRestoreFile,
   pluginIO,
   createFile,
   setFileAttributes,
   checkFile
};

/*
 * pgsql plugin requires working PostgreSQL catalog database
 * it could be Bacula catalog database if it is postgresql
 * it can't be database which we backup, because it has to be available
 * during instance shutdown, when WAls are generated.
 */

enum PGSQLMode {
   PGSQL_NONE = 0,
   PGSQL_ARCH_BACKUP,
   PGSQL_DB_BACKUP,
   PGSQL_ARCH_RESTORE,
   PGSQL_DB_RESTORE,
};

enum PGFileType {
   PG_NONE  = 0,
   PG_FILE,
   PG_LINK,
   PG_DIR,
};

typedef enum {
   PARSE_BACKUP,
   PARSE_RESTORE,
} ParseMode;

typedef struct _pg_plug_inst pg_plug_inst;
struct _pg_plug_inst {
   int      JobId;
   PGconn   * catdb;
   char     * restore_command_value;
   char     * configfile;
   keylist  * paramlist;
   int      mode;
   keylist  * filelist;
   keyitem  * curfile;
   int      curfd;
   int      diropen;
   char     * linkval;
   int      linkread;
};

/* 
 * TODO:
 * TODO: integrate pgsql-fd with pgsqllib
 * TODO:
 */

/* size of different string or sql buffer */
#define SQLLEN     256
#define CONNSTRLEN 128

/* Assertions defines */
#define ASSERT_bfuncs \
   if ( ! bfuncs ){ \
      return bRC_Error; \
   }
#define ASSERT_ctx \
   if ( ! ctx ) { \
      return bRC_Error; \
   }

#define ASSERT_ctxp_RET_BRCERROR ASSERT_ctx_p
#define ASSERT_ctx_p \
   if ( ! ctx || ! ctx->pContext ) { \
      return bRC_Error; \
   }

#define ASSERT_p(value) \
   if ( ! value ){ \
      return bRC_Error; \
   }

#define ASSERT_n(value) \
   if ( value ){ \
      return bRC_Error; \
   }

#define ASSERT_pex(value) \
   if ( ! value ){ \
      exit ( bRC_Error ); \
   }

/* memory allocation/deallocation */
#define MALLOC(size) \
   (char *) malloc ( size );

#define FREE(ptr) \
   free ( ptr ); \
   ptr = NULL;

/*
 * TODO: extend with additional calls for Bacula-FD: baculaAddOptions
 */

#define JMSG0(ctx,type,msg) \
      bfuncs->JobMessage ( ctx, __FILE__, __LINE__, type, 0, PLUGIN_INFO msg );

#define JMSG(ctx,type,msg,var) \
      bfuncs->JobMessage ( ctx, __FILE__, __LINE__, type, 0, PLUGIN_INFO msg, var );

#define JMSG2(ctx,type,msg,var1,var2) \
      bfuncs->JobMessage ( ctx, __FILE__, __LINE__, type, 0, PLUGIN_INFO msg, var1, var2 );

#define DMSG0(ctx,level,msg) \
      bfuncs->DebugMessage ( ctx, __FILE__, __LINE__, level, PLUGIN_INFO msg );

#define DMSG1(ctx,level,msg,var) \
      bfuncs->DebugMessage ( ctx, __FILE__, __LINE__, level, PLUGIN_INFO msg, var );

#define DMSG2(ctx,level,msg,var1,var2) \
      bfuncs->DebugMessage ( ctx, __FILE__, __LINE__, level, PLUGIN_INFO msg, var1, var2 );

#define DMSG3(ctx,level,msg,var1,var2,var3) \
      bfuncs->DebugMessage ( ctx, __FILE__, __LINE__, level, PLUGIN_INFO msg, var1, var2, var3 );

/* fixed debug level definitions */
#define D1  1
#define D2  10
#define D3  100

/* error in sql exec */
#define PGERROR(msg,sql,result,status) \
         DMSG2 ( ctx, D1, "%s (%s)\n", msg, sql); \
         JMSG2 ( ctx, M_ERROR, "%s (%s)\n", msg, sql); \
         JMSG ( ctx, M_ERROR, "pg_res: %s\n", PQresStatus(status) ); \
         JMSG ( ctx, M_ERROR, "pg_err: %s\n", PQresultErrorMessage(result) );

/*
 * Plugin called here when it is first loaded
 */
bRC loadPlugin ( bInfo *lbinfo, bFuncs *lbfuncs, pInfo **pinfo, pFuncs **pfuncs )
{
   bfuncs = lbfuncs;                  /* set Bacula funct pointers */
   binfo  = lbinfo;

   printf ( PLUGIN_INFO "version %s %s (c) 2011 by Inteos\n", PLUGIN_VERSION, PLUGIN_DATE );
   printf ( PLUGIN_INFO "connected to Bacula version %d\n", binfo->version );

   *pinfo  = &pluginInfo;             /* return pointer to our info */
   *pfuncs = &pluginFuncs;            /* return pointer to our functions */

   return bRC_OK;
}

/*
 * Plugin called here when it is unloaded, normally when Bacula is going to exit.
 */
bRC unloadPlugin()
{
   return bRC_OK;
}

/*
 * Called here to make a new instance of the plugin -- i.e. when
 * a new Job is started.  There can be multiple instances of
 * each plugin that are running at the same time.  Your
 * plugin instance must be thread safe and keep its own
 * local data.
 */
static bRC newPlugin ( bpContext *ctx )
{
   pg_plug_inst * pinst;

   ASSERT_ctx;

   pinst = (pg_plug_inst *) malloc ( sizeof( pg_plug_inst ) );
   if ( pinst == NULL )
   {
      JMSG0 ( ctx, M_FATAL, "Error allocating plugin structures" );
      return bRC_Error;
   }

   ctx->pContext = pinst;

   /* initialize pinst contents */
   memset ( pinst, 0, sizeof ( pg_plug_inst ) );

   bfuncs->getBaculaValue ( ctx, bVarJobId, (void *)&pinst->JobId );
   DMSG1 ( ctx, D1, "newPlugin JobId=%d\n", pinst->JobId );

   return bRC_OK;
}

/*
 * Release everything concerning a particular instance of a
 *  plugin. Normally called when the Job terminates.
 */
static bRC freePlugin ( bpContext *ctx )
{
   int JobId = 0;
   pg_plug_inst *pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *) ctx->pContext;

   ASSERT_bfuncs;
   bfuncs->getBaculaValue ( ctx, bVarJobId, (void *)&JobId );
   if ( pinst->JobId != JobId )
   {
      JMSG0 ( ctx, M_ERROR, "freePlugin JobId mismatch" );
   }

   /* free pg_plug_inst private data */
   PQfinish ( pinst->catdb );
   if ( pinst->configfile )
      FREE ( pinst->configfile );
   keylist_free ( pinst->paramlist );
   keylist_free ( pinst->filelist );

   FREE ( pinst );

   DMSG1 ( ctx, D2, "freePlugin JobId=%d\n", JobId );

   return bRC_OK;
}

/*
 * Called by core code to get a variable from the plugin.
 *   Not currently used.
 */
static bRC getPluginValue ( bpContext *ctx, pVariable var, void *value )
{
   DMSG0 ( ctx, D3, "getPluginValue called.\n" );
   return bRC_OK;
}

/*
 * Called by core code to set a plugin variable.
 *  Not currently used.
 */
static bRC setPluginValue ( bpContext *ctx, pVariable var, void *value )
{
   DMSG0 ( ctx, D3, "setPluginValue called." );
   return bRC_OK;
}

/*
 * Parsing a plugin command.
 * 
 * in:
 *    ctx - plugin context
 *    parse_mode - indicate if we parse for backup or restore
 *    command - plugin command string to parse
 * out:
 *    ctx->pContext->configfile - name and path to config file
 *    ctx->pContext->mode - what we are doing (wal backup, db backup, db restore)
 *    ctx->pContext->paramlist - a list of parameters from config file
 *    bRC_OK - on success
 *    bRC_Error - on error
 */
bRC parse_plugin_command ( bpContext *ctx, const ParseMode parse_mode, const char * command )
{
   /* pgsql:/usr/local/bacula/etc/pgsql.phobos.conf:[wal,db] */
   char * s;
   char * n;
   pg_plug_inst * pinst;
   int len;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *) ctx->pContext;

   if ( ! ( pinst->restore_command_value != NULL && 
            strncmp ( pinst->restore_command_value, command, strlen ( command ) ) == 0 ) ){
      /* check for command correctness */
      s = strchr ( (char *)command, ':' );
      ASSERT_p ( s );
   
      s++;
      /* check for command correctness */
      n = strchr ( s, ':' );
      ASSERT_p ( n );
   
      ASSERT_bfuncs;
   
      /* 
       * s points to begin of the config file name, n points into last ':' sign
       * len indicate a length of config file name
       */
      len = n - s;
      /* if configfile was allocated then free it */
      if ( pinst->configfile ){
         FREE ( pinst->configfile );
      }
      if ( pinst->restore_command_value ){
         FREE ( pinst->restore_command_value );
      }
      if ( pinst->paramlist ){
         keylist_free ( pinst->paramlist );
      }
   
      /* new configfile allocation */
      pinst->configfile = MALLOC ( len + 1 );
      ASSERT_p ( pinst->configfile );
   
      /* copy config filename from plugin command */
      strncpy ( pinst->configfile, s, len );
      /* terminate config filename with null */
      pinst->configfile [len] = 0;
   
      n++;
      /* search for command/mode setting */
      if ( ! strncasecmp ( n, "wal", 3 ) ){
         if ( parse_mode == PARSE_BACKUP )
            pinst->mode = PGSQL_ARCH_BACKUP;
         else
         if ( parse_mode == PARSE_RESTORE )
            pinst->mode = PGSQL_ARCH_RESTORE;
      } else
      if ( ! strncasecmp ( n, "db", 2 ) ){
         if ( parse_mode == PARSE_BACKUP )
            pinst->mode = PGSQL_DB_BACKUP;
         else
         if ( parse_mode == PARSE_RESTORE )
            pinst->mode = PGSQL_DB_RESTORE;
      } else {
      /* unknown plugin command */
         FREE ( pinst->configfile );
         return bRC_Error;
      }
   
      pinst->paramlist = parse_config_file ( pinst->configfile );
      pinst->restore_command_value = bstrdup ( command );
   }
   return bRC_OK;
}

/*
 * connect to pgsql catalog database
 * 
 * in:
 *    ctx - plugin context
 * out:
 *    ctx->pContext->catdb - working catalog connection
 *    0 - on success
 *    1 - on error
 */
bRC catdbconnect ( bpContext *ctx ){

   char * catdbconnstring;
   ConnStatusType status;
   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   catdbconnstring = MALLOC ( CONNSTRLEN );
   ASSERT_p ( catdbconnstring );

   snprintf ( catdbconnstring, CONNSTRLEN, "host=%s port=%s dbname=%s user=%s password=%s",
         search_key ( pinst->paramlist, "CATDBHOST" ),
         search_key ( pinst->paramlist, "CATDBPORT" ),
         search_key ( pinst->paramlist, "CATDB" ),
         search_key ( pinst->paramlist, "CATUSER" ),
         search_key ( pinst->paramlist, "CATPASSWD" ));

   pinst->catdb = PQconnectdb ( catdbconnstring );
   FREE ( catdbconnstring );

   status = PQstatus ( pinst->catdb );
   if ( status == CONNECTION_BAD ){
      return bRC_Error;
   }
   return bRC_OK;
}

/*
 * perform an internal connection to the database in separate process and execute sql
 * statement
 * 
 * in:
 *    sql - SQL statement
 * out:
 *    bRC_OK - OK
 *    bRC_ERROR - Error
 *    bRC_More - 
 */
bRC pg_internal_conn ( bpContext *ctx, const char * sql ){

   PGconn * db;
   ConnStatusType status;
   ExecStatusType resstatus;
   PGresult * result;
   int pid = 0;
   struct stat st;
   uid_t pguid;
   int err;
   char connstring[CONNSTRLEN];
   pg_plug_inst * pinst;
   int exitstatus = 0;

   /* check input data */
   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   /* dynamic production database owner verification, we use it do connect to production
    * database. Required PGDATA from config file */
   ASSERT_p ( pinst->paramlist );
   err = stat ( search_key ( pinst->paramlist, "PGDATA" ) , &st );
   if ( err ){
      /* error, invalid PGDATA in config file */
      JMSG0 ( ctx, M_ERROR, "invalid 'PGDATA' variable in config file." );
      return bRC_Error;
   }

   /* we will fork to the owner of PGDATA database cluster */
   pguid = st.st_uid;

   /* switch pg xlog in different process, so make a fork */
   /* 
    * TODO: test if switching is possible only with seteuid to postgres and return to root after that 
    * it was possible with pg_ctl shutdown ... but what about possible security flaw?
    */
   pid = fork ();
   if ( pid == 0 ){
      DMSG0 ( ctx, D3, "forked process\n" );
      /* sleep used for debuging forked process in gdb */
      // sleep ( 60 );

      /* we are in forked process, do a real job */
      /* switch to pgcluster owner (postgres) */
      err = seteuid ( pguid );
      if ( err ){
         /* error switching uid, report a problem */
         /* TODO: add an errorlog display */
         JMSG ( ctx, M_ERROR, "seteuid to uid=%i failed!\n", pguid );
         exit (bRC_Error);
      }
      /* host is a socket directory, port is a socket 'port', we perform an 'internal'
       * connection through a postgresql socket, which is required by plugin
       * it require in pg_hba.conf a following line:
       *    local    all      postgres    trust
       * without it internall connection will not work */
      snprintf ( connstring, CONNSTRLEN, "host=%s port=%s",
            search_key ( pinst->paramlist, "PGHOST" ),
            search_key ( pinst->paramlist, "PGPORT" ) );

      db = PQconnectdb ( connstring );
      status = PQstatus ( db );
      if ( status == CONNECTION_BAD ){
         DMSG0 ( ctx, D1, "pg_internal_conn.conndb failed!\n" );
         JMSG0 ( ctx, M_WARNING, "pg_internal_conn.conndb failed!\n" );
         /* not all goes ok so we have to raise it, but it is not a critical error,
          * it should be handled by calling function */
         exit (bRC_More);
      }

      /* we have a successful production database connection, so execute sql */
      result = PQexec ( db, sql );
      resstatus = PQresultStatus ( result );
      if ( !(resstatus == PGRES_TUPLES_OK || resstatus == PGRES_COMMAND_OK) ){
         /* TODO: add an errorlog display */
         PGERROR ("pg_internal_conn.pqexec failed!", sql, result, resstatus);
         exit (bRC_Error);
      }

      /* finish database connection */
      PQfinish ( db );

      /* finish forked process */
      exit (bRC_OK);
   } else {
      /* we are waiting for background process to finish */
      waitpid ( pid, &exitstatus, 0 );
   }

   /* XXX: we should check for forked process exit status */
   DMSG1 ( ctx, D2, "pg_internal_conn.exit: %i\n", WEXITSTATUS( exitstatus ) );
   return (bRC) WEXITSTATUS( exitstatus );
}

/*
 * it makes a connection to production database in separate process and perform a
 * transaction log switching
 */
bRC switch_pg_xlog ( bpContext *ctx ){

   bRC err;

   err = pg_internal_conn ( ctx, "select pg_switch_xlog()" );

   if ( err == bRC_OK ) {
      /* wait a few seconds to switch log to finish
       * FIXME: we have to consider how to perform interprocess synchronization
       * one of the option is to use message passing (pgsql_msg_send,pgsql_msg_recv) */
      DMSG0 ( ctx, D3, "waiting for switch log");
      sleep ( 3 );
      DMSG0 ( ctx, D3, "waiting finish");
   } else
   if ( err == bRC_More ){
      /* we have got a connection error for log switching, possibly database temporary unavailable
       * maintanance shutdown? */
      JMSG0 ( ctx, M_INFO, "switch log failed. Current wal wasn't archived\n" );
      err = bRC_OK;
   }

   return err;
}

/*
 * start backup procedure
 * 
 * in:
 *    ctx - plugin context
 * out:
 *    bRC_OK - success
 *    bRC_Error - error
 */
bRC start_pg_backup ( bpContext *ctx ){

   pg_plug_inst * pinst;
   char * sql;
   bRC err = bRC_OK;

   /* check input data */
   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   sql = MALLOC ( SQLLEN );
   ASSERT_p ( sql );
#if 0
   /* catalog database connection */
   err = catdbconnect ( ctx );
   if ( err ){
      return bRC_Error;
   }

   /* insert status in catalog */
   snprintf ( sql, SQLLEN, "insert into pgsql_pgsql_backupdbs (client, status, blevel) values ('%s', '%i', '%i')",
         search_key ( pdata->paramlist, "ARCHCLIENT" ),
         status, 0 );

   result = PQexec ( pdata->catdb, sql );
   FREE ( sql );

   if ( PQresultStatus ( result ) != PGRES_COMMAND_OK ){
      abortprg ( pdata, EXITCATDBEINS, "CATDB: SQL insert error!" );
   }

   pgid = get_walid_from_catalog ( pdata );
#endif

   snprintf ( sql, SQLLEN, "select pg_start_backup ('%s:%i')",
         search_key ( pinst->paramlist, "ARCHCLIENT" ),
         pinst->JobId );

   err = pg_internal_conn ( ctx, sql );

   /* error database connection in this case it is a problem,
    * rise en error for calling function */
   if ( err == bRC_More )
      err = bRC_Error;

   FREE ( sql );

   return err;
}

/*
 * end backup procedure
 * 
 * in:
 *    ctx - plugin context
 * out:
 *    bRC_OK - success
 *    bRC_Error - error
 */
bRC stop_pg_backup ( bpContext *ctx ){

   bRC err = bRC_OK;

   err = pg_internal_conn ( ctx, "select pg_stop_backup ()" );

   /* error database connection in this case it is a problem,
    * rise en error for calling function */
   if ( err == bRC_More )
      err = bRC_Error;

   return err;
}

/* 
 * grab an arhivelogs list for backup
 * 
 * in:
 *    ctx - plugin context
 * out:
 *    bRC_OK - success
 *    bRC_Error - error
 */
bRC get_wal_list ( bpContext *ctx ){

   PGresult * result;
   char * sql;
   pg_plug_inst * pinst;
   char * filename;
   char * pgid;
   int nr;
   ExecStatusType resstatus;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   ASSERT_p ( pinst->paramlist );

   sql = MALLOC ( SQLLEN );
   ASSERT_p ( sql );
   
   /* PGSQL_STATUS_WAL_ARCH_FINISH, PGSQL_STATUS_WAL_ARCH_MULTI */
   snprintf ( sql, SQLLEN, "select * from pgsql_archivelogs where client='%s' and status in (3,5) order by mod_date",
         search_key ( pinst->paramlist, "ARCHCLIENT" ) );

   result = PQexec ( pinst->catdb, sql );
   resstatus = PQresultStatus ( result );
   if ( resstatus != PGRES_TUPLES_OK ){
      PGERROR ( "get_wal_list.pqexec failed!", sql, result, resstatus );
      return bRC_Error;
   }
//   FREE ( sql );

   nr = PQntuples ( result );
   if ( nr ){
      for (int a = 0; a < nr; a++ ){
         pgid =  (char *) PQgetvalue ( result, a, PQfnumber ( result, "id") );
         filename =  (char *) PQgetvalue ( result, a, PQfnumber ( result, "filename") );
         pinst->filelist = add_keylist ( pinst->filelist, pgid, filename );
      }
      /* first node of the list will be our current file for backup */
      pinst->curfile = (keyitem *)pinst->filelist->first();

      //print_keylist ( pinst->filelist );
   } else {
      /* no files to backup, startBackupFile will fill required structs */
      pinst->filelist = NULL;
      pinst->curfile = NULL;
   }

   return bRC_OK;
}

/*
 * function performs pg_tblspc directory analyzis in case of defined tablespaces - symbolic links
 * into a directories where a tablespaces are located. we have to insert both: pg_tblspc directory 
 * with links and tablespaces location directories. 
 * 
 * pg_tblspc directory and its links will have a pgsqldb namespace but every tablespace location
 * will have a pgsqltbs namespace instead
 *
 * for PostgreSQL version 9.x we have to handle a different tablespace directory hierarchy
 * postgres version is not discovered currently and we hanle it through PGVERSION parameter
 * 
 * in:
 *    ctx - plugin context
 *    list - current filename list
 *    base - indicate if filename path is relative or absolute
 *           '$ROOT$' - path is absolute, no need to concatenate both
 *           other => absolute filename path = $base/$path
 *    path - filename path (file or directory)
 * out:
 *    list - updated filename list
 */
keylist * get_tablespace_dir ( bpContext * ctx, keylist * list, const char * base, const char * path )
{

   DIR * dirp_tab;
   DIR * dirp_tablink;
   char * bpath = NULL;
   char * link = NULL;
   keylist * nlist = list;
   struct dirent * filedir_tab;
   struct dirent * filedir_tablink;
   int dl;
   char * pgver;
   pg_plug_inst * pinst;

   pinst = (pg_plug_inst *)ctx->pContext;

   DMSG1 ( ctx, D2, "perform tablespace dir at: %s/pg_tblspc\n", base );
   /* we add to the file list a contents of tablespace directory pg_tplspc at
    * pgsqldb namespace */
   nlist = get_file_list ( ctx, nlist, base, "pg_tblspc" );

   /* most important variable allocation on heap */
   bpath = MALLOC ( PATH_MAX );
   if ( ! bpath ){
      JMSG0 ( ctx, M_ERROR, "error allocating memory." );
      return list;
   }

   /* building absolute path into a tablespaces directory for symbolic links analyzis */
   snprintf ( bpath, PATH_MAX, "%s/pg_tblspc/", base );

   dirp_tab = opendir ( bpath );

   if ( !dirp_tab ){
      JMSG ( ctx, M_ERROR, "error opening dir: %s", bpath );
      FREE ( bpath );
      return nlist;
   }

   /* all tablespaces locations are symbolic links */
   while ( (filedir_tab = readdir ( dirp_tab )) ){
      if ( strcmp ( filedir_tab->d_name, "."  ) != 0 &&
           strcmp ( filedir_tab->d_name, ".." ) != 0 ){

         DMSG1 ( ctx, D2, "tablespace found: %s\n", filedir_tab->d_name );

         link = MALLOC ( PATH_MAX );
         if ( ! link ){
            JMSG0 ( ctx, M_ERROR, "error allocating memory." );
            FREE ( bpath );
            return nlist;
         }

         /* building absolute path into a links */
         snprintf ( bpath, PATH_MAX, "%s/pg_tblspc/%s", base, filedir_tab->d_name );

         /* get a link contents */
         dl = readlink ( bpath, link, PATH_MAX - 1 );
         if ( dl < 0 ){
            JMSG ( ctx, M_ERROR, "error reading link: %s", strerror ( errno ) );
            FREE ( bpath );
            FREE ( link );
            closedir ( dirp_tab );
            return nlist;
         }
         link [ dl ] = 0;
         DMSG2 ( ctx, D3, "symlink dl=%i %s\n", dl, link );

         /* check if a link is absolute or not */
         if ( link [ 0 ] == '/' ){
            /* absolute - it is good */
            strncpy ( bpath, link, PATH_MAX );
         } else {
            /* according to PostgreSQL a tablespace location has to be an absolute,
             * if not we have an error, raise it. */
            JMSG0 ( ctx, M_ERROR, "Relative link Error!" );
            FREE ( bpath );
            FREE ( link );
            closedir ( dirp_tab );
            return nlist;
         }

         /* all tablespaces are backuped with absolute path */
         realpath ( bpath, link );

         pgver = search_key ( pinst->paramlist, "PGVERSION" );
         if ( !pgver || strcmp ( pgver, "8.x" ) == 0 ){
            /* PostgreSQL 8.x */
            DMSG0(ctx, D3, "PGVERSION=8.x\n");
            nlist = get_file_list ( ctx, nlist, "$ROOT$", link );
         } else
         if ( strcmp (pgver, "9.x") == 0){
            /* PostgreSQL 9.x */
            dirp_tablink = opendir ( link );
   
            if ( !dirp_tablink ){
               JMSG ( ctx, M_ERROR, "error opening dir: %s", bpath );
               FREE ( bpath );
               FREE ( link );
               return nlist;
            }
   
            while ( (filedir_tablink = readdir ( dirp_tablink )) ){
               if ( strcmp ( filedir_tablink->d_name, "."  ) != 0 &&
                    strcmp ( filedir_tablink->d_name, ".." ) != 0 ){
                  if ( strncmp ( filedir_tablink->d_name, "PG_9.", 5 ) == 0){
                     DMSG0(ctx, D3, "PGVERSION=9.x\n");
                     snprintf ( bpath, PATH_MAX, "%s/%s", link, filedir_tablink->d_name );
                     DMSG1(ctx,D3,"->%s\n",bpath);
                     nlist = get_file_list ( ctx, nlist, "$ROOT$", bpath );
                  }
               }
            }
         /* end postgres 9.x */
         } else {
            JMSG ( ctx, M_ERROR, "unsupported PostgreSQL version: %s",pgver);
            FREE ( bpath );
            FREE ( link );
            return nlist;
         }

         FREE ( link );
      }
   }

   closedir ( dirp_tab );
   DMSG0 ( ctx, D2, "end perform tablespace\n" );

   return nlist;
}


/*
 * adds a directory entry into a filename list
 * 
 * in:
 *    ctx - plugin context
 *    list - current filename list
 *    base - indicate if filename path is relative or absolute
 *           '$ROOT$' - path is absolute, no need to concatenate both
 *           other => absolute filename path = $base/$path
 *    path - filename path (file or directory)
 *    direntry - 
 * out:
 *    list - updated filename list
 */
keylist * get_dir_content ( bpContext *ctx, keylist * list, const char * base, const char * path, char * direntry ){

   char * npath = NULL;
   keylist * nlist = list;
   int plen = strlen ( path );

   npath = MALLOC ( PATH_MAX );
   if ( ! npath ){
      JMSG0 ( ctx, M_ERROR, "error allocating memory." );
      return nlist;
   }
   /* XXX: required fix, we concatenate directory content with relative path into npath */
   snprintf ( npath, PATH_MAX, plen ? "%s/%s" : "%s%s",
              path, direntry );

   if ( strcmp ( direntry, "pg_xlog" ) == 0 ){
      /* we have to add pg_xlog and pg_xlog/archive_status directories into a list but
       * without its contents */
      int len = strlen ( npath );
      npath [ len ] = '/';
      npath [ len + 1 ] = '\0';
      nlist = add_keylist_attr ( nlist, base, npath, PG_DIR );
      snprintf ( npath, PATH_MAX, "%s%s/archive_status", path, direntry );
      nlist = add_keylist_attr ( nlist, base, npath, PG_DIR );
   } else {
      /* recursive add a file/directory */
      nlist = get_file_list ( ctx, nlist, base, npath );
   }
   FREE ( npath );

   return nlist;
}

/*
 * adds a dir filetype into a files list
 * 
 * in:
 *    ctx - plugin context
 *    list - current filename list
 *    base - indicate if filename path is relative or absolute
 *           '$ROOT$' - path is absolute, no need to concatenate both
 *           other => absolute filename path = $base/$path
 *    path - filename path (file or directory)
 *    bpath - absolute path
 * out:
 *    list - updated filename list
 */
keylist * add_dir_list ( bpContext *ctx, keylist * list, const char * base, const char * path, char * bpath ){

   char * npath = NULL;
   keylist * nlist = list;
   int plen = strlen ( path );

   npath = MALLOC ( PATH_MAX );
   if ( ! npath ){
      JMSG0 ( ctx, M_ERROR, "Error allocating memory." );
      return NULL;
   }

   DMSG3 ( ctx, D3, "add dir to the list base=%s path=%s bpath=%s\n", base, path, bpath );
   if ( strncmp ( base, "$ROOT$", PATH_MAX ) == 0 ){
      /* means that we backup directory with absolute path */
      snprintf ( npath, PATH_MAX, "%s/", bpath );
      nlist = add_keylist_attr ( nlist, base, npath, PG_DIR );
   } else {
      /* relative directory path */
      snprintf ( npath, PATH_MAX, plen ? "%s/":"%s", path );
      nlist = add_keylist_attr ( nlist, bpath, npath, PG_DIR );
   }

   FREE ( npath );

   return nlist;
}

char * get_check_bpath ( bpContext *ctx, keylist * list, const char * base, const char * path, struct stat * st ){

   char * bpath = NULL;
   int err;

   bpath = MALLOC ( PATH_MAX );
   if ( ! bpath ){
      JMSG0 ( ctx, M_ERROR, "Error allocating memory." );
      return NULL;
   }

   if ( strncmp ( base, "$ROOT$", PATH_MAX ) == 0 ){
      /* indicate absolute path, copy plain path into bpath */
      strncpy ( bpath, path, PATH_MAX );
   } else {
      /* bpath is a real file path as $base/$path, path is relative file path
       * first we have to concatenate it for later normalization */
      snprintf ( bpath, PATH_MAX, "%s/%s", base, path );
   }
   /* bpath has a full path into analized file/directory, we check it with lstat */
   err = lstat ( bpath, st );

   if ( err ){
      JMSG ( ctx, M_ERROR, "stat error on %s.", path );
      FREE ( bpath );
      return NULL;
   }

   return bpath;
}

/*
 * adds a link filetype into a files list
 * 
 * in:
 *    list - file list
 *    base - absolute or relative file path
 *    path - filename path
 *    bpath - absolute path
 * out:
 *    list - updated filename list
 */
keylist * add_link_list ( bpContext *ctx, keylist * list, const char * base, const char * path, char * bpath ){

   keylist * nlist = list;

   if ( strncmp ( base, "$ROOT$", PATH_MAX ) == 0 ){
      nlist = add_keylist_attr ( nlist, base, path, PG_LINK );
   } else {
      nlist = add_keylist_attr ( nlist, bpath, path, PG_LINK );
   }

   return nlist;
}

/*
 * adds a regular file type into a files list
 * 
 * in:
 *    list - file list
 *    base - absolute or relative file path
 *    path - filename path
 *    bpath - absolute path
 * out:
 *    list - updated filename list
 */
keylist * add_file_list ( bpContext *ctx, keylist * list, const char * base, const char * path, char * bpath ){

   keylist * nlist = list;

   if ( strncmp ( base, "$ROOT$", PATH_MAX ) == 0 ){
      nlist = add_keylist_attr ( nlist, base, path, PG_FILE );
   } else {
      nlist = add_keylist_attr ( nlist, bpath, path, PG_FILE );
   }

   return nlist;
}

/*
 * recurence function for building file list
 *
 * in:
 *    ctx - plugin context
 *    list - filename list
 *    base - relative or absolute file path
 *    path - filename (dir,link,file)
 * out:
 *    list->key - full pathname to backup file
 *    list->value - relative filename
 */
keylist * get_file_list ( bpContext *ctx, keylist * list, const char * base, const char * path ){

   struct stat st;
   DIR * dirp;
   char * bpath = NULL;
   keylist * nlist = list;
   struct dirent * filedir;
   int plen = strlen ( path );

   bpath = get_check_bpath ( ctx, list, base, path, &st );

   if ( S_ISDIR (st.st_mode) ){
      DMSG1 ( ctx, D3, "dir found: %s\n", bpath );
      /* directory performance */

      dirp = opendir ( bpath );
      if ( !dirp ){
         FREE ( bpath );
         return nlist;
      }

      while ( ( filedir = readdir ( dirp ) ) ){
         /* avoid ".", ".." and PG_9.xxxxxxxxxxx directories in our scan */
         if ( strcmp ( filedir->d_name, "." ) != 0 &&
               strcmp ( filedir->d_name, ".." ) != 0 &&
               strncmp ( filedir->d_name, "PG_9.", 5 ) != 0 ){
            /* check if we got tablespaces directory pg_tblspc, then path is empty and
             * base has absolute filename path */
            if ( strcmp ( filedir->d_name, "pg_tblspc" ) == 0 && plen == 0 ){
               nlist = get_tablespace_dir ( ctx, nlist, base, path );
            } else {
               /* no tablespaces dir, other entity */
               DMSG1 ( ctx, D3, "found: %s\n", filedir->d_name );
               nlist = get_dir_content ( ctx, nlist, base, path, filedir->d_name );
            }
         }
      }

      closedir ( dirp );

      /* finally at the end (?) we add en entry for directory
       * entry is required for Bacula to archive directory atributes */
      nlist = add_dir_list ( ctx, nlist, base, path, bpath );

   } else
   if ( S_ISLNK ( st.st_mode ) ){
      /* indicate filetype of link */
      nlist = add_link_list ( ctx, nlist, base, path, bpath );
   } else
   if ( S_ISREG ( st.st_mode ) ){
      /* indicate filetype of regular file */
      nlist = add_file_list ( ctx, nlist, base, path, bpath );
   }
   FREE ( bpath );
   return nlist;
}

/* 
 * generating a database files list for archiving
 * 
 * in:
 *    ctx - plugin context
 * out:
 *    bRC_OK - always success
 */
bRC get_dbf_list ( bpContext *ctx ){

   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   pinst->filelist = get_file_list ( ctx, NULL, search_key ( pinst->paramlist, "PGDATA" ), "" );

   /* first node of the list will be our current file for backup */
   pinst->curfile = (keyitem *)pinst->filelist->first();

   return bRC_OK;
}

/*
 * Called by Bacula when there are certain events that the
 *   plugin might want to know.  The value depends on the
 *   event.
 */
static bRC handlePluginEvent(bpContext *ctx, bEvent *event, void *value){

   int err;
   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   ASSERT_p ( event );
   DMSG1 ( ctx, D1, "handlePluginEvent: %i\n", event->eventType );

   switch (event->eventType) {
   case bEventJobStart:
   // unused in ur case, information only
      DMSG1 ( ctx, D2, "bEventJobStart value=%s\n", NPRT((char *)value) );
      break;
   case bEventJobEnd:
   // unused in ur case, information only
      DMSG1 ( ctx, D2, "bEventJobEnd value=%s\n", NPRT((char *)value) );
      break;
   case bEventStartBackupJob:
   // unused in ur case, information only
      DMSG1 ( ctx, D2, "bEventStartBackupJob value=%s\n", NPRT((char *)value) );
      break;
   case bEventEndBackupJob:
   // closing database connection
      DMSG1 ( ctx, D2, "bEventEndBackupJob value=%s\n", NPRT((char *)value));
      if ( pinst->mode == PGSQL_DB_BACKUP ){
         err = stop_pg_backup ( ctx );
         if ( err ){
            return bRC_Error;
         }
      }
      break;
   case bEventLevel:
   // unused in ur case, information only
   // XXX: it could be usefull
      DMSG1 ( ctx, D2, "bEventLevel=%c\n", (char) ( (intptr_t)value & 0xff) );
      break;
   case bEventSince:
   // unused in ur case, information only
      DMSG1 ( ctx, D2, "bEventSince=%ld\n", (intptr_t)value);
      break;
   case bEventStartRestoreJob:
   /* Test if restored database works or not, but we have very limited info about it,
    * it should be realized by external utility */
      DMSG1 ( ctx, D2, "StartRestoreJob value=%s\n", NPRT((char *)value));
      break;
   case bEventEndRestoreJob:
   // unused in ur case, information only
      DMSG0 ( ctx, D2, "bEventEndRestoreJob\n");
      break;

   /* Plugin command e.g. plugin = <plugin-name>:<name-space>:command */
   case bEventRestoreCommand:
      /* setup a plugin parameters: plugin = pgsql:<file_conf>:[wal,db] */
      DMSG1 ( ctx, D2, "bEventRestoreCommand value=%s\n", NPRT((char *)value) );
      err = parse_plugin_command ( ctx, PARSE_RESTORE, (char *) value );
      if ( err ){
         return bRC_Error;
      }
      break;

   case bEventBackupCommand:
      /* setup a plugin parameters: plugin = pgsql:<file_conf>:[wal,db] */
      DMSG1 ( ctx, D2, "bEventBackupCommand value=%s\n", NPRT((char *)value) );
      err = parse_plugin_command ( ctx, PARSE_BACKUP, (char *) value );
      if ( err ){
         return bRC_Error;
      }

      if ( pinst->mode == PGSQL_ARCH_BACKUP ){
         /* we force to switch a database log file, to do that we need to connect into a production
          * database instance and execute a pg_switch_xlog(); to handle it we have to switch a user
          * into a database cluster owner (in most cases it will be a postgres user) and perform a
          * required operations; as a user switching is unreversible for a particular process,
          * if we want to drop an admin permissions, we have to switch after a fork(); in different
          * process, it is performed in switch_pg_xlog(); */
         err = switch_pg_xlog ( ctx );

      /*
       * FIXME: We have to check how to handle possible error indicated by log switching function
       */
       //  if ( err ){
       //     return bRC_Error;
       //  }

         /* catalog database connection */
         err = catdbconnect ( ctx );
         if ( err ){
            return bRC_Error;
         }

         /* if we are backing up wal files get a list */
         err = get_wal_list ( ctx );
         if ( err ){
            return bRC_Error;
         }
      } else
      if ( pinst->mode == PGSQL_DB_BACKUP ) {
         /* database files backup is performed without explicit database log switch because
          * log switch is performed by pg_start_backup itself */
         err = start_pg_backup ( ctx );
         if ( err ){
            return bRC_Error;
         }

         /* if we are backing up db files then we have to get a list, it should be performed after
          * pg_start_backup because we'd like to get consistent list */
         err = get_dbf_list ( ctx );
         if ( err ){
            return bRC_Error;
         }
         //print_keylist ( pinst->filelist );
      }
      break;
   case bEventPluginCommand:
   // unused in ur case, information only
      DMSG1 ( ctx, D2, "bEventPluginCommand value=%s\n", NPRT((char *)value) );
      break;

   case bEventEndFileSet:
   // unused in ur case, information only
      DMSG1 ( ctx, D2, "bEventEndFileSet value=%s\n", NPRT((char *)value) );
      break;

   case bEventRestoreObject:
   // unused in ur case, information only
      DMSG1 ( ctx, D2, "bEventRestoreObject value=%s\n", NPRT((char *)value) );
      break;

   default:
   // enabled only for Debug
      DMSG1 ( ctx, D2, "unknown event=%d\n", event->eventType );
   }
   return bRC_OK;
}

/*
 * Called when starting to backup a file.  Here the plugin must
 *  return the "stat" packet for the directory/file and provide
 *  certain information so that Bacula knows what the file is.
 *  The plugin can create "Virtual" files by giving them a
 *  name that is not normally found on the file system.
 */
static bRC startBackupFile(bpContext *ctx, struct save_pkt *sp){

   DMSG1 ( ctx, D2, "startBackupFile with save_pkt=%p\n", sp );

   pg_plug_inst * pinst;
   char * buf;
   char * sql;
   PGresult * result;
   ExecStatusType resstatus;
   char * filename;
   char * vfilename;
   struct stat file_stat;
   int err;
   int len;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   if ( pinst->mode == PGSQL_ARCH_BACKUP ){
      /* a command was to backup wal files */
      if ( pinst->curfile ){
         buf = MALLOC ( PATH_MAX );
         ASSERT_p ( buf );
         /* pgsqlarch:<ARCHCLIENT>/<WAL_Filename> */
         /* above sentence will be splited in Bacula catalog on <path>/<file> */
         snprintf ( buf, PATH_MAX, "pgsqlarch:%s/%s",
               search_key ( pinst->paramlist, "ARCHCLIENT" ),
               pinst->curfile->value );

         len = strlen ( buf );
         vfilename = MALLOC ( len + 2 );
         ASSERT_p ( vfilename );
         strncpy ( vfilename, buf, len + 1 );

         snprintf ( buf, PATH_MAX, "%s/%s",
               search_key ( pinst->paramlist, "ARCHDEST" ),
               pinst->curfile->value );
         filename = bstrdup ( buf );

         FREE ( buf );

         /* we have get stat information about backuped wal files */
         err = stat ( filename, &file_stat );
         /* name of virtual file */
         sp->fname = vfilename;
         sp->type = FT_REG;
         /* sp->type = FT_PLUGIN; is not currently implemented */
         sp->portable = TRUE;

         /* copy all contents of stat struct */
         memcpy ( &sp->statp, &file_stat, sizeof (sp->statp) );

         /* we need a wal filename with pathname, store it for later use (endBackupFile)*/
         FREE ( pinst->curfile->value );
         pinst->curfile->value = filename;

         sql = MALLOC ( SQLLEN );
         ASSERT_p ( sql );
         /* update catalog information, set status = 6 -> starting of archivelogs backup */
         /* PGSQL_STATUS_WAL_BACK_START */
         snprintf ( sql, SQLLEN, "update pgsql_archivelogs set status=6 where id='%s'",
               pinst->curfile->key );

         result = PQexec ( pinst->catdb, sql );
         FREE ( sql );

         resstatus = PQresultStatus ( result );
         if ( resstatus != PGRES_COMMAND_OK ){
            PGERROR ( "startbackupfile.pqexec failed!", sql, result, resstatus );
            return bRC_Error;
         }
         DMSG2 ( ctx, D3, "filename=%s, vfilename=%s\n", filename, vfilename );
      } else {
         /* if we return a value different from bRC_OK then Bacula will finish
          * backup process, which at first call means no files to archive */
         return bRC_Max;
      }
   } else
   if ( pinst->mode == PGSQL_DB_BACKUP ) {
      if ( pinst->curfile ){
         buf = MALLOC ( PATH_MAX );
         ASSERT_p ( buf );

         /* real filename || $ROOT$ if vfilename is absolute, check this out */
         if ( strncmp ( pinst->curfile->key, "$ROOT$", PATH_MAX ) == 0 ){
            /* pgsqltbs:<ARCHCLIENT>/<TBS_Filename> */
            /* above sentence will be splited in Bacula catalog on <path>/<file> */
            snprintf ( buf, PATH_MAX, "pgsqltbs:%s%s",
                  search_key ( pinst->paramlist, "ARCHCLIENT" ),
                  pinst->curfile->value );
            /* filename should point to real file on fs */
            filename = pinst->curfile->value;
         } else {
            /* pgsqldb:<ARCHCLIENT>/<DB_Filename> */
            /* above sentence will be splited in Bacula catalog on <path>/<file> */
            snprintf ( buf, PATH_MAX, "pgsqldb:%s/%s",
                  search_key ( pinst->paramlist, "ARCHCLIENT" ),
                  pinst->curfile->value );
            filename = pinst->curfile->key;
         }

         vfilename = bstrdup ( buf );

         FREE ( buf );
         DMSG2 ( ctx, D3, "filename=%s, vfilename=%s\n", filename, vfilename );

         /* name of virtual file */
         sp->fname = vfilename;
         sp->portable = TRUE;

         switch ( pinst->curfile->attrs ) {
            case PG_DIR:
               /* FIXME: I have to add '/' on the end of the vfilename? */
               sp->type = FT_DIREND;
               sp->link = vfilename;
               break;
            case PG_LINK:
               sp->type = FT_LNK;
               buf = MALLOC ( PATH_MAX );
               ASSERT_p ( buf );

               err = readlink ( filename, buf, PATH_MAX - 1 );
               buf [ err ] = 0;
               len = strlen ( buf );
               sp->link = MALLOC ( len + 2 );
               ASSERT_p ( sp->link );
               strncpy ( sp->link, buf, len + 1 );
               FREE ( buf );
               break;
            case PG_FILE:
               sp->type = FT_REG;
               break;
            default:
               DMSG0 ( ctx, D2, "FT_NOSTAT\n" );
               sp->type = FT_NOSTAT;
         }
         /* we have got stat information about current file */
         err = lstat ( filename, &file_stat );
         /* copy all contents of stat struct */
         memcpy ( &sp->statp, &file_stat, sizeof (sp->statp) );
      }
   }

   return bRC_OK;
}

/*
 * Done backing up a file.
 */
static bRC endBackupFile ( bpContext *ctx ){

   pg_plug_inst * pinst;
   int err;
   char * sql;
   PGresult * result;
   ExecStatusType resstatus;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   if ( pinst->mode == PGSQL_ARCH_BACKUP ){
      if ( pinst->curfile ) {
         sql = MALLOC ( SQLLEN );
         ASSERT_p ( sql );
         /* update catalog information, set status = 8 -> backup of archivelogs finished */
         /* PGSQL_STATUS_WAL_BACK_DONE */
         snprintf ( sql, SQLLEN, "update pgsql_archivelogs set status=8 where id='%s'",
               pinst->curfile->key );

         result = PQexec ( pinst->catdb, sql );
         FREE ( sql );

         resstatus = PQresultStatus ( result );
         if ( resstatus != PGRES_COMMAND_OK ){
            PGERROR ( "endbackupfile.pqexec failed!", sql, result, resstatus );
            return bRC_Error;
         }

         /* when backup of particular file is done then unlink wal file */
         err = unlink ( pinst->curfile->value );
         if ( err ){
            return bRC_Error;
         }
         pinst->curfile = (keyitem *)pinst->filelist->next( pinst->curfile );
         if ( pinst->curfile ){
            return bRC_More;
         }
      }
   } else
   if ( pinst->mode == PGSQL_DB_BACKUP ){
      if ( pinst->curfile ) {
         pinst->curfile = (keyitem *)pinst->filelist->next( pinst->curfile );
         if ( pinst->curfile ){
            return bRC_More;
         }
      }
   }

   return bRC_OK;
}

/*
 * opens pinst->curfile and fill required data structures
 */
bRC perform_dbfile_open ( bpContext *ctx, struct io_pkt *io ) {

   pg_plug_inst * pinst;
   int dl;
   char * buf;
   char * link;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   /* perform backup of database files, including all directories and links */
   if ( pinst->curfile ){
      if ( ! pinst->curfd ){
         /* there is a info about file to open, go ahead */
         switch ( pinst->curfile->attrs ) {
            case PG_FILE:
               /* standard file to open */
               if ( strncmp ( pinst->curfile->key, "$ROOT$", PATH_MAX ) == 0 ){
                  /* filename to open is at value */
                  pinst->curfd = open ( pinst->curfile->value, io->flags );
               } else {
                  /* filename to open is at key */
                  pinst->curfd = open ( pinst->curfile->key, io->flags );
               }

               if ( ! pinst->curfd ){
                  /* there is a problem with opening file, raise an error */
                  io->io_errno = errno;
                  return bRC_Error;
               }
               /* XXX: anything else ? */

               break;
            case PG_LINK:
               if ( strncmp ( pinst->curfile->key, "$ROOT$", PATH_MAX ) == 0 ){
                  /* link to open is at node->value */
                  link = pinst->curfile->value;
               } else {
                  /* link to open is at node->key */
                  link = pinst->curfile->key;
               }
               buf = MALLOC ( PATH_MAX );
               ASSERT_p ( buf );
               dl = readlink ( link, buf, PATH_MAX - 1 );
               if ( dl < 0 ){
                  io->io_errno = errno;
                  JMSG ( ctx, M_ERROR, "ERR: %s\n", strerror ( io->io_errno ) );
                  FREE ( buf );
                  return bRC_Error;
               }

               /* terminate link string */
               buf [ dl ] = '\0';

               /* save link value in pinst->linkval */
               pinst->linkval = MALLOC ( dl + 2);
               ASSERT_p ( pinst->linkval );
               strncpy ( pinst->linkval, buf, dl + 1 );

               pinst->linkread = 0;

               FREE ( buf );
               break;
            case PG_DIR:
               pinst->diropen = 1;
               pinst->linkread = 0;
               /* its all for now */
               break;
            default:
               io->io_errno = EINVAL;
               return bRC_Error;
         }
      } // else we have fd already opened
   } else {
      io->io_errno = EINVAL;
      return bRC_Error;
   }

   return bRC_OK;
}

/*
 * perform a db file read into an iobuffer
 * 
 * in:
 *    ctx - plugin context
 *    io - io buffer
 * out:
 *    io - content read into a buffer
 */
bRC perform_dbfile_read ( bpContext *ctx, struct io_pkt *io ) {

   pg_plug_inst * pinst;
   int len;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   /* perform backup of database files, including all directories and links */
   if ( pinst->curfile ){
      /* there is a info about file to backup, go ahead */
      switch ( pinst->curfile->attrs ) {
         case PG_FILE:
            /* standard file to read */
            if ( pinst->curfd > 0 ){
               io->status = read ( pinst->curfd, io->buf, io->count );
               if ( io->status == 0 && errno ){
                  /* error occured, raide it upper */
                  io->io_errno = errno;
                  return bRC_Error;
               }
            }
            break;
         case PG_LINK:
            if ( pinst->linkread ) {
               /* link contents was previous read, so indicate end of file */
               io->status = 0;
               io->io_errno = 0;
            } else {
               /* save contents of a link stored in pinst->linkval */
               len = strlen ( pinst->linkval );
               if ( io->count < len ){
                  /* partial read of link contents is unsuported */
                  io->io_errno = EINVAL;
                  return bRC_Error;
               }
               strncpy ( io->buf, pinst->linkval, len + 1 );
               io->status = len;
               pinst->linkread = 1;
            }
            break;
         case PG_DIR:
            if ( pinst->linkread ) {
               /* link contents was previous read, so indicate end of file */
               io->status = 0;
               io->io_errno = 0;
            } else {
               if ( pinst->diropen ) {
                  snprintf ( io->buf, io->count, "DIR %s %s %i",
                        pinst->curfile->key,
                        pinst->curfile->value,
                        pinst->curfile->attrs );

                  io->status = strlen ( io->buf ) + 1;
                  pinst->linkread = 1;
               } else {
                  io->io_errno = EINVAL;
                  return bRC_Error;
               }
            }
            break;
         default:
            io->io_errno = EINVAL;
            return bRC_Error;
      }
   }
   return bRC_OK;
}

/*
 * perform a db file write from buffer
 * 
 * in:
 *    ctx - plugin context
 *    io - io buffer
 * out:
 *    io - content buffer to write
 */
bRC perform_dbfile_write ( bpContext *ctx, struct io_pkt *io ) {

   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   /* perform a restore of database files, including all directories and links */
   if ( pinst->curfile ){
      switch ( pinst->curfile->attrs ) {
         case PG_FILE:
            /* standard file to write */
            if ( pinst->curfd > 0 ){
               io->status = write ( pinst->curfd, io->buf, io->count );
               if ( io->status == 0 && errno ){
                  /* error occured, raise it upper */
                  io->io_errno = errno;
                  return bRC_Error;
               }
            }
            break;
         default:
            io->io_errno = EINVAL;
            return bRC_Error;
      }
   }
   return bRC_OK;
}

/*
 * 
 */
bRC perform_dbfile_close ( bpContext *ctx, struct io_pkt *io ) {

   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   if ( pinst->curfile ){
      /* there is a info about file to backup, go ahead */
      switch ( pinst->curfile->attrs ) {
         case PG_FILE:
            if ( pinst->curfd > 0){
               io->status = close ( pinst->curfd );
               pinst->curfd = 0;
            }
            break;
         case PG_LINK:
            if ( pinst->linkval ){
               FREE ( pinst->linkval );
            } else {
               io->io_errno = EINVAL;
               return bRC_Error;
            }
            break;
         case PG_DIR:
            if ( pinst->diropen ){
               pinst->diropen = 0;
            } else {
               io->io_errno = EINVAL;
               return bRC_Error;
            }
            break;
         default:
            io->io_errno = EINVAL;
            return bRC_Error;
      }
   } else {
      io->status = EINVAL;
      return bRC_Error;
   }

   return bRC_OK;
}

/*
 * opens archived pinst->curfile wal and fill required data structures
 */
bRC perform_arch_open ( bpContext *ctx, struct io_pkt *io ) {

   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   if ( pinst->curfile ){
      if ( ! pinst->curfd ){
         /* we are backing up a real wal file */
         pinst->curfd = open ( pinst->curfile->value, io->flags );

         if ( ! pinst->curfd ){
            io->io_errno = errno;
            return bRC_Error;
         }
      } // else we have already fd opened
   } else {
      /* FIXME: in general this will be an error because we had
       * fixed a virual empty file problem !!!*/
      /* we are backing up a virtual control file */
      pinst->curfd = 0;
   }

   return bRC_OK;
}

/*
 *
 */
bRC perform_arch_read ( bpContext *ctx, struct io_pkt *io ) {

   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   if ( pinst->curfd > 0 ){
      io->status = read ( pinst->curfd, io->buf, io->count );
      if ( io->status == 0 && errno ){
         /* error occured, raise it upper */
         io->io_errno = errno;
         return bRC_Error;
      }
   } else {
      return bRC_Error;
   }
   return bRC_OK;
}

/*
 *
 */
bRC perform_arch_write ( bpContext *ctx, struct io_pkt *io ) {

   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   if ( pinst->curfd > 0 ){
      io->status = write ( pinst->curfd, io->buf, io->count );
      if ( io->status == 0 && errno ){
         /* error occured, raide it upper */
         io->io_errno = errno;
         return bRC_Error;
      }
   } else {
      return bRC_Error;
   }
   return bRC_OK;
}

/*
 *
 */
bRC perform_arch_close ( bpContext *ctx, struct io_pkt *io ) {

   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   if ( pinst->curfd > 0){
         io->status = close ( pinst->curfd );
         pinst->curfd = 0;
   } else {
         return bRC_Error;
   }

   return bRC_OK;
}

/*
 * Do actual I/O.  Bacula calls this after startBackupFile
 *   or after startRestoreFile to do the actual file
 *   input or output.
 */
static bRC pluginIO ( bpContext *ctx, struct io_pkt *io ) {

   static int rw = 0;
   pg_plug_inst * pinst;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   io->status = 0;
   io->io_errno = 0;
   switch ( io->func ) {
   case IO_OPEN:
      DMSG1 ( ctx, D2, "IO_OPEN: (%s)\n", io->fname );
      switch ( pinst->mode ){
         case PGSQL_ARCH_BACKUP:
            return perform_arch_open ( ctx, io );
            break;
         case PGSQL_DB_BACKUP:
            return perform_dbfile_open ( ctx, io );
            break;
         case PGSQL_ARCH_RESTORE:
            return perform_arch_open ( ctx, io );
            break;
         case PGSQL_DB_RESTORE:
            return perform_dbfile_open ( ctx, io );
         default:
            return bRC_Error;
      }
   case IO_READ:
      if ( !rw ){
         rw = 1;
         DMSG2 ( ctx, D2, "IO_READ buf=%p len=%d\n", io->buf, io->count );
      }
      switch ( pinst->mode ){
         case PGSQL_ARCH_BACKUP:
            return perform_arch_read ( ctx, io );
            break;
         case PGSQL_DB_BACKUP:
            return perform_dbfile_read ( ctx, io );
            break;
         default:
            return bRC_Error;
      }

      break;
   case IO_WRITE:
      if ( !rw ){
         rw = 1;
         DMSG2 ( ctx, D2, "IO_WRITE buf=%p len=%d\n", io->buf, io->count );
      }
       switch ( pinst->mode ){
         case PGSQL_ARCH_RESTORE:
            return perform_arch_write ( ctx, io );
            break;
         case PGSQL_DB_RESTORE:
            return perform_dbfile_write ( ctx, io );
            break;
         default:
            return bRC_Error;
      }

      break;
   case IO_CLOSE:
      DMSG0 ( ctx, D2, "IO_CLOSE\n" );
      rw = 0;

      switch ( pinst->mode ){
         case PGSQL_ARCH_BACKUP:
            return perform_arch_close ( ctx, io );
            break;
         case PGSQL_DB_BACKUP:
            return perform_dbfile_close ( ctx, io );
            break;
         case PGSQL_ARCH_RESTORE:
            return perform_arch_close ( ctx, io );
            break;
         case PGSQL_DB_RESTORE:
            return perform_dbfile_close ( ctx, io );
            break;
         default:
            return bRC_Error;
      }
      break;
   }
   return bRC_OK;
}

static bRC startRestoreFile ( bpContext *ctx, const char *cmd )
{
   DMSG1 ( ctx, D2, PLUGIN_INFO "startRestoreFile cmd=%s\n", cmd );
   return bRC_OK;
}

static bRC endRestoreFile ( bpContext *ctx )
{
   DMSG0 ( ctx, D2, PLUGIN_INFO "endRestoreFile \n" );
   return bRC_OK;
}

/*
 * 
 */
int makepath ( bpContext *ctx, const char * filename ){

   char * dir;
   char * next;
   char * file; // allocated
   char * path; // allocated
   int len;
   int err;
   struct stat sp;

   if ( filename ){
      len = strlen ( filename ) + 2;
      file = MALLOC ( len );
      strncpy ( file, filename, len );

      /* 
       * Well, a dirname function returns the same pointer what we had provided
       * and modify input string - we can exploit this for simplyfing all work
       * - for later discussion !!!
       */
      dir = dirname ( file );

      /* 
       * Main dirname fuction formally returns unknown pointer, so if we'd like 
       * to use it we have to duplicate string returned, but we can free file var.
       */
      len = strlen ( dir ) + 2;
      path = MALLOC ( len );

      /* Now we have a copy of separated directory in path var. */
      strncpy ( path, dir, len );
      DMSG1 ( ctx, D3, "path: %s\n", path );
      file [0] = '\0';
      dir = path;
      next = dir + 1;

      while ( ( next = strchr ( next, '/' ) ) != NULL ){
         len = next - dir;
         strncpy ( file, dir, len );
         file [ len ] = '\0';
         DMSG1 ( ctx, D3, " DIR: %s\n", file );

         err = stat ( file, &sp );
         if ( err ) {
            /* This means that directory doesn't exist, so we create it */
            DMSG0 ( ctx, D3, "created\n");
            mkdir ( file, S_IRWXU );
         }

         next++;
      }
      DMSG1 ( ctx, D3, " DIR: %s\n", path );
      err = stat ( path, &sp );
      if ( err ) {
         DMSG0 ( ctx, D3, "created\n");
         mkdir ( path, S_IRWXU );
      }
      FREE ( file );
      FREE ( path );
      return 0;
   } else {
      return -1;
   }
}

/*
 * Check if 'Where' is set then we have to remove it from restored filename.
 * This is a very stupid Bacula BUG, but Kern doesn't see it, what a shame !!!
 * It works as designed and Bacula Team doesn't want to change it because 
 * they has to fix all existence plugins and perform a regression tests, but
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
 *
 */
char * check_archdest ( bpContext *ctx, struct restore_pkt *rp, const char * dest ){

   pg_plug_inst * pinst;
   char * archdest = NULL;

   //ASSERT_ctx_p;
   //TODO: change into an ASSERT_* definition
   if ( ! ctx || ! ctx->pContext ) {
      return NULL;
   }
   pinst = (pg_plug_inst *)ctx->pContext;

   /* For every database file we will overwrite always, no matter what user wants.
    * If user want will supply a 'Where' parameter we will use it, otherwise we will use
    * PGDATA variable */

   /* For every WAL file we should get an unarchive location. If not we will use a directory
    * defined by ARCHDEST variable */

   if ( rp->where ){
      if ( strlen ( rp->where ) > 0 ){
         /* user/pgsql-restore application supplied a path for database environment restoration */
         archdest = (char *)rp->where;
      } else {
         /* No 'where' parameter, we use a default path */
         archdest = search_key ( pinst->paramlist, dest );
      }
   }
   return archdest;
}

/*
 *
 */
bRC perform_add_filelist ( bpContext *ctx, struct restore_pkt *rp, char * file ){

   pg_plug_inst * pinst;
   char * buf;
   keyitem * node;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   /* fileindex to (char *) */
   buf = MALLOC ( 11 );
   snprintf ( buf, 10, "%i", rp->file_index );

   node = new_keyitem ( buf, file, PG_FILE );
   FREE ( buf );

   /* add current created file to the list */
   pinst->filelist = add_keylist_item ( pinst->filelist, node );
   /* set a curent file information in pinst->curfile */
   pinst->curfile = node;

   return bRC_OK;
}

/*
 * Checks if a file pointed by 'file' variable exist at filesystem; if file exist then
 * performs an unlink -> delete unless it is a directory, which is untouched
 */
int check_exist_rm ( char * file ){

   struct stat sp;
   int exist = 0;

   if ( stat ( file, &sp ) == 0 ){
      exist = 1;
      /* file exist, we shave to delete it */
      if ( ! S_ISDIR ( sp.st_mode ) ){
         /* unless it is a directory */
         unlink ( file );
      } else {
         /* it is a directory, what we should to do? */
      }
   }

   return exist;
}

/*
 * function realizes an empty file creation with proper attribs setup: owner,
 * permissions, access time, etc.
 */
bRC perform_create_file ( bpContext *ctx, struct restore_pkt *rp, char * file ){

   pg_plug_inst * pinst;
   int fd;
   int err;
   struct utimbuf ut;
   bRC rc = bRC_OK;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   DMSG1 ( ctx, D3, "creating file: %s\n", file );

   check_exist_rm ( file );

   /* creating a full path for a file */
   makepath ( ctx, file );

   /* file creation */
   fd = creat ( file, rp->statp.st_mode );
   if ( fd < 0 ){
      /* skasowaliśmy wcześniej ewentualny plik, więc błąd może wystąpić co
       * najwyżej ze względu na to że odtwarzamy plik na miejsce katalogu.
       * alternatywnie zabrakło inode'ów lub inny błąd */
      rp->create_status = CF_ERROR;
      JMSG ( ctx, M_ERROR, "create file error: %s\n", strerror ( errno ) );
      rc = bRC_Error;
   } else {
      pinst->curfd = fd;
      rp->create_status = CF_EXTRACT;

      /* set file owner */
      err = chown ( file, rp->statp.st_uid, rp->statp.st_gid );
      if ( err ){
         JMSG ( ctx, M_WARNING, "chown file error: %s\n", strerror ( errno ) );
      }

      /* set file permissions */
      err = chmod ( file, rp->statp.st_mode );
      if ( err ){
         JMSG ( ctx, M_WARNING, "chmod file error: %s\n", strerror ( errno ) );
      }

      /* set file times */
      ut.actime = rp->statp.st_atime;
      ut.modtime = rp->statp.st_mtime;
      utime ( file, &ut );

      perform_add_filelist ( ctx, rp, file );
   }

   return rc;
}

/*
 * perform link creation and fill rp structure
 * in:
 *  rp - restore_pkt struct
 *  file - name of link file
 * out:
 *  bRC_OK - OK
 *  bRC_ERROR - on Error
 */
bRC perform_create_link ( bpContext *ctx, struct restore_pkt *rp, char * file ){

   int err;
   bRC rc = bRC_OK;

   ASSERT_ctx_p;

   DMSG2 ( ctx, D3, "creating link: %s -> %s\n", file, rp->olname );

   /* sprawdza i ewentualnie kasuje link */
   check_exist_rm ( file );

   /* tworzymy do niego ścieżkę katalogów */
   makepath ( ctx, file );

   /* tworzymy symlink */
   err = symlink ( rp->olname, file );
   if ( err ){
      rp->create_status = CF_ERROR;
      rc = bRC_Error;
   }

   /* set link ownership */
   err = lchown ( file, rp->statp.st_uid, rp->statp.st_gid );
   if ( err ){
      JMSG ( ctx, M_WARNING, "chown link error: %s\n", strerror ( errno ) );
   }

   /* CF_CREATED; */
   rp->create_status = CF_CREATED;
   return rc;
}

/*
 * perform directory creation and fills rp structure
 * in:
 *  rp - restore_pkt struct
 *  file - name of directory
 * out:
 *  bRC_OK - OK
 *  bRC_ERROR - on Error
 */
bRC perform_create_dir ( bpContext *ctx, struct restore_pkt *rp, char * file ){

   int err;
   int exist = 0;
   struct utimbuf ut;

   ASSERT_ctx_p;

   DMSG1 ( ctx, D3, "creating dir: %s\n", file );

   /* sprawdza czy wskazywany plik istnieje. generalnie go nie kasuje, a może powinien? */
   exist = check_exist_rm ( file );

   if ( !exist ){
      makepath ( ctx, file );
      err = mkdir ( file, rp->statp.st_mode );
      if ( err ){
         JMSG ( ctx, M_WARNING, "creating directory error: %s\n", strerror ( errno ) );
      }
   }
   
   /* set directory ownership */
   err = chown ( file, rp->statp.st_uid, rp->statp.st_gid );
   if ( err ){
      JMSG ( ctx, M_WARNING, "chown directory error: %s\n", strerror ( errno ) );
   }

   /* set directory permissions */
   err = chmod ( file, rp->statp.st_mode );
   if ( err ){
      JMSG ( ctx, M_WARNING, "chmod directory error: %s\n", strerror ( errno ) );
   }

   /* set file times */
   ut.actime = rp->statp.st_atime;
   ut.modtime = rp->statp.st_mtime;
   utime ( file, &ut );

   rp->create_status = CF_CREATED;

   return bRC_OK;
}

/*
 *
 */
bRC perform_arch_restore ( bpContext *ctx, struct restore_pkt *rp ){

   char * client;
   char * filename;
   char * file;
   char * archdest;
   char * ofname;
   int out;
   bRC rc = bRC_OK;

   ASSERT_ctx_p;

   ofname = check_ofname ( rp );

   archdest = check_archdest ( ctx, rp, "ARCHDEST" );

   /* zmienną client możemy wykorzystać do weryfikacji czy odtwarzamy poprawnego klienta */
   client = MALLOC ( 64 );
   /* zmienna filename będzie zawierała wyextrachowaną nazwę pliku */
   filename = MALLOC ( PATH_MAX );

   out = sscanf ( ofname, "pgsqlarch:%64[^/]/%s", client, filename );
   DMSG3 ( ctx, D3, "out: %i client: %s file: %s\n", out, out > 0 ? client:"", out > 1 ? filename:"/" );

   if ( out < 2 ){
            /* odczytaliśmy mniej niż 2 parametry, niedobrze, zgłaszamy błąd */
            rp->create_status = CF_ERROR;
            rc = bRC_Error;
   } else {
      /* zmienna file będzie zawierała nazwę pliku/katalogu/linku do stworzenia */
      file = MALLOC ( PATH_MAX );
      ASSERT_p ( file );

      snprintf ( file, PATH_MAX, "/%s/%s", archdest, filename );

      FREE ( client );
      FREE ( filename );

      check_exist_rm ( file );

      /* pliki wal są tylko typu plik. inne rodzaje są błędem */
      switch ( rp->type ) {
         case FT_REG:
         case FT_REGE:
            rc = perform_create_file ( ctx, rp, file );
            break;
         default:
            /* tutaj zgłaszamy błąd */
            JMSG2 ( ctx, M_ERROR, "unknown file (%s) type %i\n", file, rp->type );
            break;
      }
   }

   return rc;
}

/*
 * 
 */
bRC perform_dbfile_restore ( bpContext *ctx, struct restore_pkt *rp ){

   char * client;
   char * filename;
   char * file;
   char * archdest;
   char * ofname;
   int out;
   bRC rc = bRC_OK;

   ASSERT_ctx_p;

   ofname = check_ofname ( rp );

   archdest = check_archdest ( ctx, rp, "PGDATA" );

   /* zmienną client możemy wykorzystać do weryfikacji czy odtwarzamy poprawnego klienta */
   client = MALLOC ( 64 );
   ASSERT_p ( client );
   /* zmienna filename będzie zawierała wyextrachowaną nazwę pliku */
   filename = MALLOC ( PATH_MAX );
   ASSERT_p ( filename );

   out = sscanf ( ofname, "pgsql%*[dbtbs]:%64[^/]/%s", client, filename );
   DMSG3 ( ctx, D3, "out: %i client: %s file: %s\n", out, out > 0 ? client:"",
            out > 1 ? filename:"'/'" );

   if ( out == 1 && rp->type == FT_DIREND ){
      /* zapisany jest główny katalog, obsłużmy to */
      filename [0] = '/';
      filename [1] = '\0';
      out = 2;
   }
   if ( out < 2 ){
      /* odczytaliśmy mniej niż 2 parametry, niedobrze, zgłaszamy błąd */
      DMSG1 ( ctx, D1, "restore filename error: %s\n", ofname );
      rp->create_status = CF_ERROR;
      rc = bRC_Error;
   } else {
      /* zmienna file będzie zawierała nazwę pliku/katalogu/linku do stworzenia */
      file = MALLOC ( PATH_MAX );
      ASSERT_p ( file );

      /* tworzymy nazwę pliku na filesystemie */
      snprintf ( file, PATH_MAX, "/%s/%s", archdest, filename );
      DMSG1 ( ctx, D3, "real filename: %s\n", file );

      /* TODO: normalizacja zmiennej file za pomocą realpath */

      /* zmienne pośrednie nie są już potrzebne */
      FREE ( client );
      FREE ( filename );

      check_exist_rm ( file );

      switch ( rp->type ) {
         case FT_REG:
         case FT_REGE:
            DMSG0 ( ctx, D3, "perform_create_file\n" );
            rc = perform_create_file ( ctx, rp, file );
            break;
         case FT_LNK:
            DMSG0 ( ctx, D3, "perform_create_link\n" );
            rc = perform_create_link ( ctx, rp, file );
            break;
         case FT_DIREND:
            DMSG0 ( ctx, D3, "perform_create_dir\n" );
            rc = perform_create_dir ( ctx, rp, file );
            break;
         default:
            DMSG0 ( ctx, D1, "rp->type error!\n" );
            rp->create_status = CF_ERROR;
            rc = bRC_Error;
      }

      FREE ( file );
   }

   return rc;
}

/*
 * Called here to give the plugin the information needed to
 *  re-create the file on a restore.  It basically gets the
 *  stat packet that was created during the backup phase.
 *  This data is what is needed to create the file, but does
 *  not contain actual file data.
 */
static bRC createFile ( bpContext *ctx, struct restore_pkt *rp ){

   pg_plug_inst * pinst;
   bRC rc = bRC_OK;

   ASSERT_ctx_p;
   pinst = (pg_plug_inst *)ctx->pContext;

   DMSG0 ( ctx, D3, PLUGIN_INFO "createFile" );
//   DMSG4 ( PLUGIN_INFO "type=%i file_index=%i linkfi=%i uid=%i\n",
//         rp->type, rp->file_index, rp->LinkFI, rp->uid );
//   DMSG4 ( PLUGIN_INFO "fname=%s lname=%s where=%s replace=%c\n",
//         rp->ofname, rp->olname, rp->where, (char)rp->replace );

   switch ( pinst->mode ){
      case PGSQL_ARCH_RESTORE:
         rc = perform_arch_restore ( ctx, rp );
         break;
      case PGSQL_DB_RESTORE:
         rc = perform_dbfile_restore ( ctx, rp );
         break;
      default:
         rc = bRC_Error;
   }

   return rc;
}

/*
 * checkFile used for accurate mode backup
 */
static bRC checkFile ( bpContext *ctx, char *fname ){

   int exist;
   struct stat statp;

   /* check input data */
   ASSERT_ctxp_RET_BRCERROR;

   DMSG1 ( ctx, D3, "checkFile for: %s\n", fname );
   exist = stat ( fname, &statp );
   DMSG1 ( ctx, D3, "seen: %i\n", exist );

   if ( exist ){
      return bRC_Seen;
   }

   return bRC_OK;
}

/*
 * Called after the file has been restored. This can be used to
 *  set directory permissions, ...
 * NOT YET IMPLEMENTED!!!
 */
static bRC setFileAttributes(bpContext *ctx, struct restore_pkt *rp)
{
   DMSG0 ( ctx, D3, PLUGIN_INFO "setFileAttributes called, strange." );
   return bRC_OK;
}

#ifdef __cplusplus
}
#endif
