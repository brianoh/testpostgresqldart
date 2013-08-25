/*
*  Program to test PostgrSQL using "postgresql" driver on Dart. License: BSD 
* 
*  02-Jul-2013   Brian OHara   Commenced initial version.
*                
*  16-Jul-2013   Brian OHara   Altered from async terminal-input to sync.
*                (brianoh)     Terminal-input was initially designed for
*                              async, so it is therefore now rather verbose
*                              as a result of initially writing for async
*                              console parameterized-input.
*                              
*  18-Jul-2013   Brian OHara   Wrote the minimalist ClassCcy for Currency.                             
*                 
*  31-Jul-2013   Brian OHara   Handle Db SocketException better. Handle read
*                              of fixed-length List from file.
*                              
*  01-Aug-2013   Brian OHara   Create a table "random" to store the list of
*                 (brianoh)    keys to be updated in order to test contention
*                              better. IE. all processes to use same keys for
*                              updates in order to cause more collisions.
*                              
*  02-Aug-2013   Brian OHara   Removed the start-time as a means of achieving
*                 (brianoh)    a near-synchronous state between instances and
*                              use 'control' table instead.
*                              
*  03-Aug-2013   Brian OHara   Added the 'totals' table as a means of comparing
*                 (brianoh)    with the selected totals on completion.
*                 
*  05-Aug-2013   Brian OHara   Introduced some errors in order to test the
*                 (brianoh)    error-handling, and made changes to improve
*                              the error-handling.
*                              
*  06-Aug-2013   Brian OHara   Display of totals and comparison to determine
*                              if inserts and updates balance with table
*                              values.
*  
*  08-Aug-2013   Brian OHara   Put in code for semaphore to detect first
*                              process to start.
*  
*  09-Aug-2013   Brian OHara   Test error-handling.
*  
*  14-Aug-2013   Brian OHara   Create Tables etc.
*  
*  15-Aug-2013   Brian OHara   Added option to use AutoInc or program-
*                              generated id.
*                              
*  18-Aug-2013   Brian OHara   Re-wrote the handling of totals to make it
*                              more streamlined.
*                              
*  22-Aug-2013   Brian OHara   Started testing on Linux (Ubuntu 13.04 64 bit).
*                              Results illustrate a problem somewhere.
*  
*  22-Aug-2013   Brian OHara   Reported on Github the problems with times
*                              on 64-bit Ubuntu (probably related to the VM
*                              or PostgreSQL RDBMS).
*  
*  22-Aug-2013   Brian OHara   Tested on 32-bit Linux (Ubuntu 13.04).
*                              Results good.
*  
*  23-Aug-2013   Brian OHara   Started testing on Win8 - results good.
*  
* ------------------------------------------------------------------------                         
*
* The purpose of this program is to test the database driver, it is not
* intended as an example of async programming. I've written this in my
* spare time to start learning Dart.
* 
* Thanks to Greg for writing the driver and thanks also to the architects
* and developers of Dart and the Dart team and to Google for creating a
* great programming language and environment. Assuming that the Ubuntu
* 64-bit problems relate to something other than Greg's code, I have not
* been able to fault Greg's driver to-date after a lot of trying,
* 
* Please advise any suggestions or problems via "issues". Any suggestions
* are very welcome. I'm very much still the student.
*  
* One purpose of this program is to enable the ability to put the "system"
* under stress and also stress the database and the driver and create
* collisions in the database in order to test contention. Hopefully, it will
* also help as an example, but as I am still very much a student of Dart,
* any help would be appreciated. I know that much of what has been done
* here can be done better.
* 
* With regard to the number of iterations, a larger number (1000+) likely
* gives a better indication of speed. With regard to the sample size, a
* smaller size likely tests contention better, and a larger sample likely
* gives a better indication of speed. A very-small sample-size will also
* likely not be a good indicator of speed in any situation. With multiple
* instances they will likely be queuing to update, and with a small number
* of instances the data will likely be in cache, but it still may not be
* super-fast with one user and a very small update sample.
* 
* I have written a virtually identical program to this test program for
* Sqljocky (mysql) which will be loaded onto Github also (when I
* incorporate latest changes).
*                          
* Perhaps someone could look at some of the async handling and error-
* handling for correctness or for 'better' ways to do it.
*                            
* The ClassConsole etc. may be overkill, however it was initially written
* for async console input and may have been of use then. However, I have
* found it quite simple to add or remove fields (being familiar with it).
* If of any use, it could be significantly enhanced, keeping in mind that
* HTML should probably be used for all "terminal" input in general.
*                          
* The layout of tables can be seen in Function fCreateTablesEtc. The
* database requires the following tables (which it creates) :
*    test01    - the main table that is updated
*    sequences - the table for generating Primary Keys
*    control   - table for synchronizing instances of program.
*    random    - the table used for list of keys to be updated.
*    totals    - the table used for storing and balancing totals             
*                 
*    Dart - postgresql - transactionStatus
*    -------------------------------------
*        TRANSACTION_UNKNOWN = 1;
*        TRANSACTION_NONE    = 2;
*        TRANSACTION_BEGUN   = 3;
*        TRANSACTION_ERROR   = 4;                              
*/

import 'dart:async' as async;
import 'dart:io';
import 'dart:math';
////import '../../postgresql/lib/postgresql.dart' as pg;
import 'package:postgresql/postgresql.dart' as pg;

const int    I_TOTAL_PROMPTS    = 11;   // the total count of prompts
const int    I_MAX_PROMPT       = 10;   // (data-entry 0 to 10)
const int    I_MAX_PARAM        = 9;    // maximum parameter number (0 to 9)
const int    I_DB_USER          = 0;    // prompt nr
const int    I_DB_PWD           = 1;    // prompt nr
const int    I_DB_NAME          = 2;    // prompt nr
const int    I_MAX_INSERTS      = 3;    // prompt nr
const int    I_USE_AUTOINC      = 4;    // prompt nr
const int    I_MAX_UPDATES      = 5;    // prompt nr
const int    I_SELECT_SIZE      = 6;    // prompt nr
const int    I_CLEAR_YN         = 7;    // prompt nr
const int    I_SAVE_YN          = 8;    // prompt nr
const int    I_INSTANCE_TOT     = 9;    // prompt nr
const int    I_CORRECT_YN       = 10;    // prompt nr

const int    I_DEC_PLACES       = 2;    // decimal places for currency
const int    I_TOT_OPEN         = 0;
const int    I_TOT_INSERT       = 1;
const int    I_TOT_UPDATE       = 2;

const String S_CCY_SYMBOL           = "\$";
const String S_CONNECT_TO_DB        = "Connect to Database";
const String S_CONTROL_KEY          = "1001";   // Key to control table
const String S_DEC_SEP              = ".";      // Decimal separator
const String S_SEQUENCE_KEY_MAIN    = "1001";   // Key to 'test01' table.
const String S_SEQUENCE_KEY_TOTALS  = "1002";   // key to 'totals' table
const String S_MAIN_TABLE           = "test01"; // the main table used
const String S_THOU_SEP             = ",";      // Thousands separator
const String S_PARAMS_FILE_NAME     = "testPg001.txt";

ClassCcy       ogCcy                = new ClassCcy(I_DEC_PLACES);
ClassTerminalInput ogTerminalInput  = new ClassTerminalInput();
ClassFormatCcy ogFormatCcy          = new ClassFormatCcy(I_DEC_PLACES,
                                           S_DEC_SEP, S_THOU_SEP,
                                           S_CCY_SYMBOL);
ClassPrintLine  ogPrintLine          = new ClassPrintLine(true);
ClassRandNames  ogRandNames          = new ClassRandNames();
ClassRandAmt    ogRandAmt            = new ClassRandAmt(I_DEC_PLACES);
pg.Connection   ogDb;                // postgres connection
RawServerSocket ogSocket;

void main() {
  /*
   * Get the user selections (program parameters)
   */
  List<String> lsInput = ogTerminalInput.fGetUserSelections();
 
  bool          tClearMain   = (lsInput[I_CLEAR_YN] == "y");
  bool          tFirstInstance;
  bool          tUseAutoInc  = (lsInput[I_USE_AUTOINC] == "y");  
  ClassTotals   oClassTotals = new ClassTotals();
  ClassWrapInt  oiClassInt   = new ClassWrapInt();
  ClassWrapList ollClassList = new ClassWrapList();
  int           iMaxUpdates  = int.parse(lsInput[I_MAX_UPDATES]);
  int           iInstanceMax = int.parse(lsInput[I_INSTANCE_TOT]);
  int           iInstanceNr  = 0;    // each instance has a unique number
  int           iMaxInserts  = int.parse(lsInput[I_MAX_INSERTS]);
  int           iSelectMax   = int.parse(lsInput[I_SELECT_SIZE]);
  
/*
 * Connect to database
 */   
  final String sUri = "postgres://${lsInput[I_DB_USER]}:"+
                       "${lsInput[I_DB_PWD]} @localhost:5432/"+
                       "${lsInput[I_DB_NAME]}";   

  pg.connect(sUri)
  .catchError((oError) {
    print ("Main:pg.connect - Database connection not active");
    fExit(1);
  })
  .then((pg.Connection oDb) {
    ogDb = oDb;      // assign to global database object or connection
    
    fCheckTransactionStatus("pg.connect", false);
  /*
   * Test database connection.
   */
    ogPrintLine.fPrintForce("Testing Db connection .....");
    
    return fTestConnection();

  }).then((bool tResult) {
    if (tResult != true && tResult != false)
      fFatal ("Main", "Result from fTestConnection invalid");
    if (tResult != true) {
      print ("Main: Database connection is not active");
      fExit(1);
    }
    ogPrintLine.fPrintForce("Database connection now tested");
        
  /*
   * Test if this is the first Instance of the program running
   */  
    return fCheckIfFirstInstance();
  }).then((tResult) {
    tFirstInstance = tResult;
    
    ogPrintLine.fPrintForce(tFirstInstance ? 
      "This is the first Instance of program" :
      "This is not the first instance of program");
    
    if (!tFirstInstance && iInstanceMax == 1)
      throw("This should be the first instance, however "+
      " there is an instance already running");    
    /*
     * create tables etc. if necessary
     */
    return (!tFirstInstance) ? true : fCreateTablesEtc(tClearMain);

  }).then((tResult) {
    if (tResult != true)
      fFatal("Main", "On return from fCreateTablesEtc. result not "+
              "'true' but ${tResult}");
    /*
     * Instances other than first instance will wait for 1st instance
     */
    return tFirstInstance ? true :
     fWaitForProcess(sColumn: "iCount1", sCompareType: ">",
                     iRequiredValue: 0, sWaitReason: "to initialize",
                     tStart:true); 

  }).then((tResult) {
    if (tResult != true)
      fFatal ("Main:fWaitForProcess", "(iCount1) Process failed. Result = ${tResult}");
    /*  
     * Update control row to increment count of instances started
     */
    int iRequiredCount = tFirstInstance ? 0 : -1;  // 1st instance must be first
    return fUpdateControlTableColumn(sColumn: "iCount1", 
                                     iRequiredCount: iRequiredCount,
                                     iMaxCount: iInstanceMax,
                                     oiResult: oiClassInt);
 
  }).then((bool tResult) {
    iInstanceNr = oiClassInt.iValue;    // unique instance number
    if (tFirstInstance && iInstanceNr != 1)
      fFatal("Main:fUpdateControlTableColumn",
              "First instance, but instance nr. = ${iInstanceNr}");
    /*
     * wait for All processes to Start
     */
    return fWaitForProcess(sColumn: "iCount1", sCompareType: "=",
            iRequiredValue: iInstanceMax, sWaitReason: "to start processing",
            tStart:true); 
  }).then((tResult) {
    if (tResult != true)
      fFatal("Main:fWaitForProcess", "Process failed. Result = ${tResult}");

    /*
     * Process Inserts
     */
    ogPrintLine.fPrintForce ("Main processing has commenced ......\n");

    return fProcessMainInserts(iInstanceNr, iMaxInserts,
                               tUseAutoInc, oClassTotals);

  }).then((bool tResult) {
    if (tResult != true)
      fFatal("main:fProcessMainInserts", "Process failed. Result = ${tResult}");
    /*
     * Display and Insert totals
     */    
    oClassTotals.fPrint();    // display totals
    
    return fInsertIntoTotalsTable(oClassTotals);
  })
  .catchError((oError) => fFatal ("Main", "fInsertIntoTotalsTable (Inserts) "+
                                 "Error = ${oError}"))
  .then((bool tResult) {
    if (tResult != true)
      fFatal("Main:fInsertIntoTotalsTable", "Procedd failed: result = ${tResult}");
    fCheckTransactionStatus("Main: after fInsertIntoTotalsTable", true);
    /*
     * Update 'control' to show inserts have completed
     */
    return fUpdateControlTableColumn(sColumn: "iCount2", 
                                     iRequiredCount: -1, 
                                     iMaxCount: iInstanceMax);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("Main:fUpdateControlTableColumn",
              "(iCount2) Process failed. Result = ${tResult}");
    fCheckTransactionStatus("Main: after fUpdateControlTableColumn (iCount2)", true);
    /*
     * wait for All processes to complete main Inserts
     */
    return fWaitForProcess(sColumn: "iCount2", sCompareType: "=",
        iRequiredValue: iInstanceMax, sWaitReason: "to complete inserts",
        tStart:true);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("Main:fWaitForProcess", "(iCount2) Process failed: result = ${tResult}");
    print ("");
    /*
     * Insert Random keys into 'random' table
     */
    ogPrintLine.fPrintForce("Insert table of random keys");
    return !tFirstInstance ? true : fInsertRandomKeys(iSelectMax);
  }).then((tResult) {
    if (tResult != true)
      fFatal("Main:fInsertRandomKeys", "failed to insert random keys");
    if (tFirstInstance)
      ogPrintLine.fPrint("Random keys table created");
    /*
     * first instance to update control table to show random keys created  
     */
    return !tFirstInstance ? true :    
     fUpdateControlTableColumn(sColumn: "iCount3", 
                                iRequiredCount: 0, 
                                iMaxCount: 1);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("Main:fUpdateControlTable", "(iCount3) Process failed. Result = ${tResult}");
   /*
    * Instances to wait for 'random' table to be completed
    */
    return fWaitForProcess(sColumn: "iCount3", sCompareType: ">",
        iRequiredValue: 0, sWaitReason: "to complete random key table propagation",
        tStart:true);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("Main:tWaitForProcess:", "(iCount3) Process failed. Result = ${tResult}");
    /*
     * Select random table for keys to use for updates
     */
    ogPrintLine.fPrintForce("Selecting random keys from random table");
    print("");
    String sSql = "SELECT ikey FROM random";
    return fProcessSqlSelect(sSql, false, ollClassList);    /////xxxx    
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("Main:fProcessSqlSelect:", "(random) Process failed. Result = ${tResult}");
 
    List<List> llRandKeys = ollClassList.llValue;
    if (llRandKeys == null)
      fFatal("Main:fProcessSqlSelect:", "Process failed to Select random keys");
    
    ogPrintLine.fPrintForce("Table of Random Keys selected\n");

    /*
     * Process Updates using random keys
     */
    return fProcessMainUpdates(iInstanceNr, iMaxUpdates, llRandKeys, oClassTotals);    
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("Main", "fProcessMainUpdates failed");
    /*
     * Inserts total for Updates
     */
    oClassTotals.fPrint();
    return fInsertIntoTotalsTable(oClassTotals);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("Main", "Insert into 'totals' (Updates) failed");
    print("");
    /*
     * Select table Main Table (unsorted)
     */
    String sSql = "SELECT * FROM ${S_MAIN_TABLE}";
    ogPrintLine.fPrintForce ("Processing Select (Unsorted)");
    return fProcessSqlSelect(sSql, true, null);    
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("FMain:fProcessSqlSelect",
              "${S_MAIN_TABLE} (unsorted) Process failed. Result = ${tResult}");
    /*
     * Select Main Table (sorted)
     */
    String sSql = "SELECT * FROM ${S_MAIN_TABLE} ORDER BY ikey";
    ogPrintLine.fPrintForce ("Processing Select (Sorted)");
    return fProcessSqlSelect(sSql, true, null);
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("FMain:fProcessSqlSelect",
              "${S_MAIN_TABLE} (sorted) Process failed. Result = ${tResult}");
  /*
   * Update control row to increment count of instances finished
   */
    return fUpdateControlTableColumn(sColumn: "iCount4", 
                                     iRequiredCount: -1, 
                                     iMaxCount: iInstanceMax);    
  }).then((bool tResult) {
    if (tResult != true)
      fFatal("Main:fUpdateControlTable", "(iCount4) Process failed. Result = ${tResult}");
    return fWaitForProcess(sColumn: "iCount4", sCompareType: "=",
        iRequiredValue: iInstanceMax, sWaitReason: "to complete updates",
        tStart:false); 
  }).then((bool tResult) {
    if (tResult != true)
      fFatal ("Main:fWaitForProcess", "(iCount1) Process failed. Result = ${tResult}");   
    /*
     * Select totals from main table
     */
    print ("");
    fDisplayTotals().then((_) {
      ogPrintLine.fPrintForce("Completed");
      fExit(0);
    });
  }).catchError((oError) {
    fFatal("Main", "Error = ${oError}");
  });
}

/*
 * Process Inserts To main Table
 */
async.Future<bool> fProcessMainInserts(int iInstanceNr, int iMaxIters,
                                       bool tUseAutoInc,
                                       ClassTotals oClassTotals) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  Function  fLoopMainInserts;
  int       iDiv           = fGetDivisor(iMaxIters);  // iDiv is divisor for progress indic.
  int       iInsertFailTot = 0;
  int       iCcyInsertTot  = 0;
  int       iInsertTotNr   = 0;
  int       iLastIdTot     = 0;  
  int       iLastLog       = 0;
  Stopwatch oStopwatch     = new Stopwatch();
  
  fLoopMainInserts = () {
    if (iInsertTotNr >= iMaxIters) {   // All Inserts have completed
      oStopwatch.stop;

      if (iInsertTotNr != iLastLog)
        stdout.write ("${iInsertTotNr}");
      
      print("");
      
      fCheckTransactionStatus("fProcessMainInserts - completed", false);
      ogPrintLine.fPrintForce("Failed Inserts (system stress) = "+
                               "${iInsertFailTot}");
      if (tUseAutoInc)
        ogPrintLine.fPrintForce("AutoInc Id's retrieved = ${iLastIdTot}");

      oClassTotals.fSetValues (iInstance    : iInstanceNr,
                               iTotType     : I_TOT_INSERT,
                               iInsertTotNr : iInsertTotNr,
                               iUpdateTotNr : 0,
                               iCcyTotAmt   : iCcyInsertTot,
                               iTotMillis   : oStopwatch.elapsedMilliseconds);

      oCompleter.complete(true);
      
      return;
    }
    if (iInsertFailTot >= 100 && iInsertFailTot > iInsertTotNr)
      fFatal("fProcessMainInserts", "Aborted - ${iInsertFailTot} failed inserts, "+
              " ${iInsertTotNr} succeeded");
    
    if (iInsertTotNr % iDiv == 0 && iInsertTotNr != iLastLog) {
      stdout.write("${iInsertTotNr}  ");
      iLastLog = iInsertTotNr;
    }
    
    bool tPositive  = (iInsertTotNr % 2 == 0);  // to generate pos or neg value
    int    iCcyBal  = ogRandAmt.fRandAmt(99999, tPositive);  // Generate random $cc
    String sCcyBal  = ogCcy.fCcyIntToString(iCcyBal);
    String sName    = ogRandNames.fGetRandName();
/////xxxx put code here to test errors
    
    if (!(tUseAutoInc)) {   // Don't use autoincrement
      String sSql     = "(iKey, sname, dbalance) "+
                         "VALUES (?, '$sName', $sCcyBal)";
    
      fInsertRowWithSequence(S_MAIN_TABLE, S_SEQUENCE_KEY_MAIN, sSql)
    
      .then((tResult) {
        if (!(tResult))
          iInsertFailTot++;
        else {  
          iCcyInsertTot += iCcyBal;
          iInsertTotNr++;
        }
        fLoopMainInserts();
      }).catchError((oError) {
        iInsertFailTot ++;
        fRollback("fProcessMainInserts");
        fLoopMainInserts();
      });
    } else {    // use autoincrement
      ClassWrapList ollClassList = new ClassWrapList();
      String sSql = "INSERT INTO ${S_MAIN_TABLE} (sname, dbalance)"+
          " VALUES ('$sName', $sCcyBal)";
      
      fExecuteSql(sSql, "S_MAIN_TABLE", "fProcessMainInserts", 1)
      
      .catchError((oError) {
        iInsertFailTot++;
        fLoopMainInserts();
      })
      .then((tResult) {
        if (!(tResult))
          iInsertFailTot++;
        else {
          iInsertTotNr++;
          iCcyInsertTot += iCcyBal;
          fProcessSqlSelect("SELECT LASTVAL()", false, ollClassList)
          .then((bool tResult) {
            List<List> llResult = ollClassList.llValue;
            if (llResult != null && llResult.length == 1) {
              int iKey = llResult[0][0];
              if (iKey > 0)
                iLastIdTot++;
            }
            fLoopMainInserts();
          }).catchError((_) => fLoopMainInserts);
        }  
      }).catchError((oError) {
        iInsertFailTot ++;
        fRollback("fProcessMainInserts").then((_) =>
          fLoopMainInserts());
      });
    }
  };
  
  ogPrintLine.fWriteForce ("Processing Inserts .... ");
  oStopwatch.reset();
  oStopwatch.start();
  
  fCheckTransactionStatus("fProcessMainInserts - commmencing", true);
  
  fLoopMainInserts();

  return oCompleter.future;
}  

/*
 * Process Updates for Main Table
 */
async.Future<bool> fProcessMainUpdates(int iInstanceNr, int iMaxIters,
                                       List<List<int>> lliRandKeys,
                                       ClassTotals oClassTotals) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();
  
  Function  fLoopMainUpdates;
  int       iCcyUpdateTot  = 0;
  int       iDiv           = fGetDivisor(iMaxIters);
  int       iUpdateTotNr   = 0;
  int       iUpdateFailTot = 0;
  int       iLastLog       = 0;
  Random    oRandom        = new Random();
  Stopwatch oStopwatch     = new Stopwatch();

  fCheckTransactionStatus("fProcessMainUpdates", true);

  if (iMaxIters > 0 && lliRandKeys == null)
    throw new Exception("fProcessMainUpdates: list of keys not created");   
  
  print ("");
 
  if (iMaxIters > 0 && lliRandKeys.isEmpty) {
    ogPrintLine.fPrintForce("Starting Updates - No Random Keys have been "+
                             "selected for Update");
    oCompleter.complete(true);
    return oCompleter.future;
  }
    
  ogPrintLine.fWriteForce ("Starting Updates .... ");
  
  int iLoopTot = 0;
  fLoopMainUpdates = () {
    if (iUpdateTotNr >= iMaxIters) {
      oStopwatch.stop;
      
      if (iUpdateTotNr != iLastLog)
        stdout.write ("${iUpdateTotNr}");
      print("");
      
      ogPrintLine.fPrintForce ("Failed updates = ${iUpdateFailTot}");
      fCheckTransactionStatus("fProcessMainUpdates - completed", false);
      
      oClassTotals.fSetValues (iInstance    : iInstanceNr,
                               iTotType     : I_TOT_UPDATE,
                               iInsertTotNr : 0,
                               iUpdateTotNr : iUpdateTotNr,
                               iCcyTotAmt   : iCcyUpdateTot,
                               iTotMillis   : oStopwatch.elapsedMilliseconds);

      oCompleter.complete(true);
      return;
    }
    
    if (iUpdateTotNr % iDiv == 0 && iUpdateTotNr != iLastLog) {
      stdout.write ("${iUpdateTotNr}  ");
      iLastLog = iUpdateTotNr;
    }
    
    if (iUpdateFailTot > 100 && iUpdateFailTot > iUpdateTotNr)
      fFatal("fProcessMainUpdates", "${iUpdateFailTot} Updates failed, "+
              "${iUpdateTotNr} succeeded");
    
    int iKey = lliRandKeys[oRandom.nextInt(lliRandKeys.length)][0];
    int    iCcyTranAmt = ogRandAmt.fRandAmt(999, (iUpdateTotNr % 2 == 0));
    
    fUpdateSingleMainRow(iKey, iCcyTranAmt).then((tResult) {
      if (tResult != true)
        iUpdateFailTot++;
      else {
        iUpdateTotNr ++;
        iCcyUpdateTot += iCcyTranAmt;
      }
      fLoopMainUpdates();
    }).catchError((oError) {
      iUpdateFailTot++;
      fLoopMainUpdates();
    });
  };
  oStopwatch.start();
  
  fCheckTransactionStatus("fProcessMainUpdates - commenced", true);
  
  fLoopMainUpdates(); 
  return oCompleter.future;
}  

/*
 * Update a Single Row of main table
 */
async.Future<bool> fUpdateSingleMainRow(int iKey, int iCcyTranAmt) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  fCheckTransactionStatus("fUpdateSingleMainRow", true);
  
  String sSql;
  ogDb.execute("begin").then((_) {
    sSql = "SELECT ikey, dbalance FROM ${S_MAIN_TABLE} "+
            "WHERE ikey = ${iKey} FOR UPDATE";    
    return ogDb.query(sSql).toList();
  }).then((lResult){
    if (lResult == null || lResult.isEmpty || lResult.length != 1)
      throw("fUpdateSingleMainRow: failed to Select ikey = ${iKey}");
 
    String sKey        = "${lResult[0][0]}";       
    String sBalOld     = lResult[0][1];
    double dBalOld     = double.parse(sBalOld);
    String sCcyTranAmt = ogCcy.fCcyIntToString(iCcyTranAmt);
    double dBalNew     = ogCcy.fAddCcyDoubles(dBalOld, double.parse(sCcyTranAmt));
    String sNewBal     = dBalNew.toStringAsFixed(I_DEC_PLACES);
    sSql = "UPDATE ${S_MAIN_TABLE} SET dBalance = $sNewBal WHERE ikey = $sKey";
    ogDb.execute(sSql).then((int iRowsAffected) {
      if (iRowsAffected != 1)
        throw("Rows affected = ${iRowsAffected}, should = 1");
      ogDb.execute("COMMIT");
    })
    .catchError((oError) => throw(oError))
    .then((_) {
      oCompleter.complete(true);
    });
  }).catchError((oError) {
    print ("fUpdateSingleMainRow: Update failed. Error = ${oError}");
    oCompleter.complete(false);
  });
  
  return oCompleter.future; 
}

/*
 * Update Control Table with counter to handle synchronization.
 */
async.Future<bool> fUpdateControlTableColumn({String sColumn,
                                            int iRequiredCount,
                                            int iMaxCount,
                                            ClassWrapInt oiResult}) {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();
 
  fCheckTransactionStatus("fUpdateControlTableColumn", true);
  
  int    iCount;
  String sSql;
  
  ogDb.execute("begin").then((_) {
    sSql ="SELECT ${sColumn} FROM control WHERE "+
           "ikey = $S_CONTROL_KEY FOR UPDATE";
    return ogDb.query(sSql).toList().catchError((oError) => throw(oError));
  }).then((llResult){
    if (llResult == null || llResult.isEmpty || llResult.length != 1)
      throw("fUpdateControlTableColumn: failed to Select "+
             "ikey = $S_CONTROL_KEY");
 
    iCount = llResult[0][0];
    if (iCount >= iMaxCount)
      throw ("fUpdateControlTableColumn: Fatal Error - Maximum count = "+
              "${iMaxCount}, current count = ${iCount}");
      
    if (iRequiredCount >= 0 && iCount != iRequiredCount)
      throw ("fUpdateControlTableColumn: Required count = ${iRequiredCount}, "+
              "Current count = ${iCount}");
    
    sSql = "UPDATE control SET ${sColumn} = ${++iCount} "+
            "WHERE ikey = $S_CONTROL_KEY";

    return ogDb.execute(sSql)
    .catchError((oError) => throw(oError))
    .then((int iRowsAffected) {
      if (iRowsAffected != 1)
        throw("Rows affected not 1, but ${iRowsAffected}");

      return (ogDb.execute("commit"))
      .catchError((oError) => throw(oError))
      .then((_) {
        fCheckTransactionStatus("fUpdateControlTableColumn-AfterCommit", true);
        if (oiResult != null)  // calling function wants result
          oiResult.iValue = iCount;   // set result for calling function
        oCompleter.complete(true);
      });
    });
  }).catchError((oError) {
    fFatal("fUpdateControlTableColumn", "${oError} - Sql = $sSql");
  });

  return oCompleter.future; 
}

/*
 * Insert the List of random keys into table for selection
 */
async.Future<bool> fInsertRandomKeys(int iSelectTot) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  ClassWrapList   ollClassList         = new ClassWrapList();
  Function        fLoopRandomInserts;
  int             iPos                 = 0;
  List<List<int>> lliKeys;
  
  fLoopRandomInserts = () {
    if (iPos >= lliKeys.length)
      return;
    
    int iKey = lliKeys[iPos][0];
    String sSql = "INSERT INTO random (iKey) VALUES (${iKey})";
    return fExecuteSql(sSql, "random", "fInsertRandomKeys", 1)
    .then((bool tResult){
      if (tResult == true)
        iPos++;
      fLoopRandomInserts();
    }).catchError((oError) =>
        throw ("fInsertRandomKeys: (${iPos}) ${oError}"));
  };
  
  String sSql = "SELECT ikey FROM ${S_MAIN_TABLE} ORDER BY RANDOM() " +
                 "LIMIT ${iSelectTot}";  

  fProcessSqlSelect(sSql, false, ollClassList)
  .then((bool tResult) {
    lliKeys = ollClassList.llValue;
    if (lliKeys == null)
      throw ("fInsertRandomKeys: Select of keys from "+
              "${S_MAIN_TABLE} failed");

    ogPrintLine.fPrintForce("${lliKeys.length} random row(s) selected");
    fLoopRandomInserts();
  
    oCompleter.complete(true);
    return;
    
  }).catchError((oError) {
    fFatal("fInsertRandomKeys", "${oError}");
  });
  
  return oCompleter.future;
}

/*
* Wait for other instances to complete. The "control" table
* handles synchronization by updating counters and then 
* waiting for a required number (of instances) to have completed
* their update.
*/

async.Future<bool> fWaitForProcess({String sColumn,
                                   String sCompareType,
                                   int iRequiredValue,
                                   String sWaitReason,
                                   bool   tStart}) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  ClassWrapList ollClassList  = new ClassWrapList();
  Function      fWaitLoop;
  int           iLastCount;
  int           iLoopCount    = 0;
 
  fWaitLoop = () {
    /*
     * The sleep is only one second in order that there is not an
     * excessive wait after all are "ready". This could be handled
     * better (using this methodology), but it would be a little more
     * complex. I'll likely change this.
     */
    new async.Timer(new Duration(seconds:1), () {
      String sSql = "Select ${sColumn} from control where ikey = "+
                     "$S_CONTROL_KEY";
      fProcessSqlSelect(sSql, false, ollClassList)
      .then((bool tResult) {
        bool tMatched = false;   // init
        List<List> llRow = ollClassList.llValue;   // get the list
        if (llRow == null || llRow.length != 1)
          fFatal("fWaitForProcess", "Failed to sSelect control row");
        int iCount = llRow[0][0];
        if (iCount != iLastCount) {
          iLastCount = iCount;
          String sInstances = iCount == 1 ? "instance has" :"instances have";
          String sPline = "${iCount} ${sInstances}";
          sPline += tStart ? " started." : " completed.";
          
          tMatched = ((sCompareType == "=" && iCount == iRequiredValue)
                      || (sCompareType == ">" && iCount > iRequiredValue));
          if (!tMatched) {
            sPline += " Waiting for $sCompareType ${iRequiredValue} instance(s) ";
            /////sPline += tStart ? "start " : "complete ";
            ogPrintLine.fPrintForce(sPline + "${sWaitReason}.");          
          }
          if (!tMatched && iCount > iRequiredValue)          
            throw ("instance count exceeded");
        }
        if (tMatched) {  // all processes are ready to go
          ogPrintLine.fPrintForce("Wait completed for ${iCount} instance(s) "+sWaitReason);
          oCompleter.complete(true);
          return;
        }
        iLoopCount++;
        fWaitLoop();
      }).catchError((oError) =>
          fFatal ("fWaitForProcess", "${oError}"));
    });
  };
  fWaitLoop();
  
  return oCompleter.future;
}

/*
 * General method to Clear a table.
 */
async.Future<bool> fClearTable(bool tShowMessage, String sTableName) {
 
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  fCheckTransactionStatus("fClearTable", true);
       
  if (tShowMessage)
    ogPrintLine.fPrintForce("Clearing ${sTableName} table");
  
  ogDb.execute("TRUNCATE TABLE ${sTableName}").then((oResult){
    fCheckTransactionStatus("fClearTable - after Truncate", false);
    oCompleter.complete(true);
    return;
  }).catchError((oError) {
    fFatal("fClearTable", "${oError}");
  });
  
  return oCompleter.future;
}

/*
 * Insert a Single Row into a table using sequence number
 * 'sequences' table holds last number used for Key.
 */
async.Future<bool> fInsertRowWithSequence (String sTableName,
    String sSequenceKey, String sSql1) {

  async.Completer<bool> oCompleter = new async.Completer<bool>();  

  fCheckTransactionStatus("fInsertRowWithSequence", true);
  
  String sNewKey;   // the new key to be inserted
  
  ogDb.execute("begin").then((_){
    String sSql2 = "SELECT iLastkey FROM sequences WHERE ikey = "+
                   "${sSequenceKey} FOR UPDATE";
    return ogDb.query(sSql2).toList();
  }).then((llResult){
    if (llResult.isEmpty) ///////xxxxxx test error here
      throw ("fInsertRowWithSequence: row '${sSequenceKey}' for table "+
              "${sTableName} is missing from 'sequences' table");
    sNewKey = (llResult[0][0] +1).toString();
       
    sSql1 = "INSERT INTO " +sTableName +sSql1.replaceFirst("?", sNewKey);
    
    return ogDb.execute(sSql1);
  }).then((int iRowsAffected){
    if (iRowsAffected != 1)   //////xxxxxxxxxxxxxxxxxx
      throw ("fInsertRowWithSequence: Insert ${sTableName}: "+
              "New Key = ${sNewKey}, rows affected Not 1 but "+
              "= ${iRowsAffected}");

    return ogDb.execute("UPDATE sequences SET ilastkey = "+
                        "${sNewKey} WHERE ikey = ${sSequenceKey}");      
  }).then((int iRowsAffected) {
     if (iRowsAffected != 1)
       throw ("fInsertRowWithSequence: Table ${sTableName}, Update "+
               "Sequences: Rows Not 1 but = ${iRowsAffected}");
    
    ogDb.execute("COMMIT").then((_) {
      oCompleter.complete(true);
      fCheckTransactionStatus("fInsertRowWithSequence - after COMMIT", false);
      return;
    });
  }).catchError((oError) {
    print ("fInsertRowWithSequence: Table: ${sTableName}, Error=${oError}");
    oCompleter.complete(false);
  });  
  
  return oCompleter.future;
}

/*
 * Test the Database connection
 */
async.Future<bool> fTestConnection() {
  async.Completer<bool> oCompleter = new async.Completer<bool>(); 
  String sSql = "DROP TABLE IF EXISTS testpg001";
  
  fExecuteSql(sSql, "Drop Table if Exists", "fTestConnection", -1)  
  .catchError((oError) {
    oCompleter.complete(false);
  })
  .then((bool tResult) {
    if (tResult != true && tResult != false)
      fFatal("fTestConnection", "Result from fExecuteSql is invalid");
    oCompleter.complete(tResult);
  });

  return oCompleter.future;
}

/*
 * create where necessary the tables and data used by this program.
 */
async.Future<bool> fCreateTablesEtc(bool tClearMain) {
  async.Completer<bool> oCompleter = new async.Completer<bool>();
 
  ClassWrapList ollClassList  = new ClassWrapList();
  
  ogPrintLine.fPrintForce("Creating tables etc. as required");
  fCheckTransactionStatus("fCreateTablesEtc", true);
  
  // Create 'control' table first
  String sSql = "CREATE TABLE IF NOT EXISTS control "+
      "(iKey int Primary Key not null unique, "+
      "icount1 int not null, icount2 int not null, "+
      "icount3 int not null, icount4 int not null, "+
      "icount5 int not null, icount6 int not null)";
  fExecuteSql(sSql, "Create 'control'", "fCreateTablesEtc", -1)
  .then((bool tResult) {
    if (tResult != true)
      fFatal("fCreateTablesEtc", "failed to create 'control' table");
    fCheckTransactionStatus("fCreateTablesEtc: after tables created", true);
    /*
     * Insert row into 'control' table
     */
    return fInsertControlRow();
  })
  .catchError((oError) => throw(oError))
  .then((bool tResult) {
    if (tResult != true)
      fFatal("fCreateTablesEtc", "failed to Insert 'control' row");          
    // Check that required sequences rows exist //
    fCheckTransactionStatus(
      "fCreateTablesEtc: Prior delete from sequences", true);
  
    // Create Main table //
    sSql = "CREATE TABLE IF NOT EXISTS ${S_MAIN_TABLE} "+
                 "(ikey SERIAL Primary Key, "+      
                 "sname varchar(22) not null, "+
                 "dbalance decimal(12,2) not null)  ";
    return fExecuteSql(sSql, "Create '${S_MAIN_TABLE}'", "fCreateTablesEtc", -1);
  }).catchError((oError) => throw(oError))
  .then((bool tResult) {
    if (tResult != true)
      fFatal("fCreateTablesEtc", "failed to create ${S_MAIN_TABLE}");
    // sequences table //
    sSql = "CREATE TABLE IF NOT EXISTS sequences "+
        "(iKey int Primary Key not null unique, "+
        "ilastkey int not null, stablename varchar(20) not null)";
    return fExecuteSql(sSql, "Create 'sequences'", "fCreateTablesEtc", -1)
    .catchError((oError) => throw(oError))    
    .then((bool tResult) {
      if (tResult != true)
        fFatal("fCreateTables", "Unable to create 'sequences' table ${tResult}");
      // random table //
      sSql = "CREATE TABLE IF NOT EXISTS random "+
          "(iKey int Primary Key not null unique)";    
      return fExecuteSql(sSql, "Create 'random'", "fCreateTablesEtc", -1);
    })
    .then((bool tResult) {
      if (tResult != true)
        fFatal("fCreateTablesEtc", "failed to create 'random' table");
      // totals table //
      sSql = "CREATE TABLE IF NOT EXISTS totals "+
              "(iKey int Primary Key not null unique, "+
              "iinstancenr int not null, itottype smallint not null,"
              "sdesc varchar(20) not null, "+
              "iinsertnr int not null, iupdatenr int not null, "+
              "dccyvalue decimal, iTotMillis int not null)";
      return fExecuteSql(sSql, "Create 'totals'", "fCreateTablesEtc", -1);
    })
    .then((bool tResult){
      if (tResult != true)
        fFatal("fCreateTablesEtc", "failed to create 'totals' table");      
      sSql = "DELETE FROM sequences WHERE ikey = "+
              "$S_SEQUENCE_KEY_MAIN OR iKey = $S_SEQUENCE_KEY_TOTALS";
      return fExecuteSql(sSql, "sequences Delete", "fCreateTablesEtc", -1);
    })
    .catchError((oError) => throw("Delete from 'sequences' ${oError}"))
    .then((bool tResult){
      if (tResult != true)
        fFatal("fCreateTablesEtc", "failed to Delete 'sequences' rows");       
      fCheckTransactionStatus(
       "fCreateTablesEtc: after Delete from sequences", true);
      // Insert 'sequences' rows
      sSql = "INSERT INTO sequences (ikey, iLastKey, stablename) "+
              "VALUES ($S_SEQUENCE_KEY_TOTALS, 1000, 'totals')";
      return fExecuteSql(sSql, "sequences", "fCreateTables", 1);
    })
    .catchError((oError) => throw("Insert Sequences. ${oError}"))
    .then((bool tResult) {
      if (tResult != true)
        fFatal("fCreateTablesEtc", "failed to Insert 'sequences' row");
      // Clear the 'random' table
      return fClearTable(true, "random");
    })
    .catchError((oError) => throw("Clear 'random' table ${oError}"))
    .then((bool tResult) {
      if (tResult != true)
        fFatal("fCreateTablesEtc", "failed to Clear 'random' table");
      // Clear 'totals' table
      return fClearTable(true, "totals");
    })
    .catchError((oError) => throw("Clear 'totals' - ${oError}"))
    .then((bool tResult) {
      if (tResult != true)
        fFatal("fCreateTablesEtc", "failed to Clear 'totals' table"); 
      // Clear the Main table
      return !(tClearMain) ? true : fClearTable(true, S_MAIN_TABLE);
    })
    .catchError((oError) => throw("Clear ${S_MAIN_TABLE} - ${oError}"))
    .then((bool tResult) {
      if (tResult != true)
        fFatal("fCreateTablesEtc", "failed to Clear '${S_MAIN_TABLE}' table"); 
      sSql = "SELECT MAX(iKey) FROM ${S_MAIN_TABLE}";
      return fProcessSqlSelect(sSql, false, ollClassList);
    })
    .catchError((oError) => throw(oError))
    .then((bool tResult) {
      List<List> lliResult = ollClassList.llValue;
      String sLastKey = "1001000";  // init
      if (!(lliResult == null || lliResult.isEmpty ||
       lliResult[0][0] == null))
        sLastKey = lliResult[0][0].toString();
      
      sSql = "INSERT INTO sequences (ikey, iLastKey, stablename) VALUES "+
              "($S_SEQUENCE_KEY_MAIN, ${sLastKey}, '${S_MAIN_TABLE}')";
      return fExecuteSql(sSql, "sequences", "fCreateTables", 1);
    })
    .catchError((oError) => throw(oError))      
    .then((bool tResult) {
      if (tResult != true)
        fFatal("fCreateTablesEtc", "failed to Insert 'sequences' row"); 
      return fInsertOpeningTotals(0);
    })
    .then((bool tResult) {
      if (tResult != true)
        fFatal("fCreateTablesEtc", "failed to Insert 'totals' row"); 
      ogPrintLine.fPrintForce("Tables etc. created");
      fCheckTransactionStatus("fCreateTablesEtc", true);
      oCompleter.complete(true);
    });
  })
  .catchError((oError) =>
    fFatal("fCreateTables", "Error = ${oError}"));
  return oCompleter.future;
}

/*
 * Insert row into control table for synchronization of processes.
 */
async.Future<bool> fInsertControlRow() {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  fCheckTransactionStatus("fInsertControlRow", true);
    
  // delete the 'control' item key "1" and re-insert.

  String sSql = "DELETE FROM control WHERE ikey = $S_CONTROL_KEY";
  
  fExecuteSql(sSql, "control", "fInsertControlRow", -1)
  .then((bool tResult) {
    if (tResult != true)
      oCompleter.complete(false);
    else {
      sSql = "INSERT INTO control (ikey, icount1, icount2, "+
              "icount3, icount4, icount5, icount6) "+
              "VALUES ($S_CONTROL_KEY,0,0,0,0,0,0)";    
      return fExecuteSql(sSql, "control", "fInsertControlRow", 1)
      .then((bool tResult) {
        oCompleter.complete(tResult == true);
      });
    }
  });

  return oCompleter.future;
}

/*
 * Execute Sql requiring Transaction.
 */
async.Future<bool> fExecuteSql(String sSql, String sTableName,
                               String sCalledBy, int iRequiredRows) {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  fCheckTransactionStatus("fExecuteSql:${sCalledBy}:", true);
  
  ogDb.execute("begin").then((oResult){
    return ogDb.execute(sSql);
  }).then((int iRowsAffected){
    if (iRequiredRows > -1 && iRowsAffected != iRequiredRows)
      throw ("${sTableName}: Rows affected not "+
             "${iRequiredRows} but = ${iRowsAffected}");

    ogDb.execute("COMMIT").then((_){
      fCheckTransactionStatus("fExecuteSql:${sCalledBy}", true);
      oCompleter.complete(true);
      return;
    });  
  }).catchError((oError) {
    print ("fExecuteSql:${sCalledBy} Error = ${oError}");
    fRollback("fExecuteSql:$sCalledBy");
    oCompleter.complete(false);
  });

  return oCompleter.future;
}

/*
 * Insert opening values into 'totals' table
 */
async.Future<bool> fInsertOpeningTotals(int iInstanceNr) {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  fCheckTransactionStatus("fInsertOpeningTotals", true);
  
  ogPrintLine.fPrintForce("Initializing totals table");

  ClassTotals   oClassTotals = new ClassTotals();
  ClassWrapList ollClassList = new ClassWrapList();
  
  String sSql = "SELECT count(*), sum(dbalance) FROM ${S_MAIN_TABLE}";
  fProcessSqlSelect(sSql, false, ollClassList)
  .then((bool tResult) {
    if (tResult != true)
      throw ("fInsertOpeningTotals: Select failed");
    List<List> llResult = ollClassList.llValue;
    int iInsertNr = 0;   // init
    int iUpdateNr = 0;   // init
    int iCcyTotal = 0;   // init
    
    if (llResult != null && llResult.length > 0) {
      iInsertNr        = llResult[0][0] == null ? 0      : llResult[0][0];
      String sCcyTotal = llResult[0][1] == null ? "0.00" : llResult[0][1];
     
      iCcyTotal = ogCcy.fCcyStringToInt(sCcyTotal);
    }
    
    oClassTotals.fSetValues(iInstance    : iInstanceNr,
                            iTotType     : I_TOT_OPEN,
                            iInsertTotNr : iInsertNr,
                            iUpdateTotNr : 0,
                            iCcyTotAmt   : iCcyTotal,
                            iTotMillis   : 0);
 
    oClassTotals.fPrint();
    
    fInsertIntoTotalsTable (oClassTotals)
    .then((bool tResult) {
      if (tResult != true)
        fFatal("fCreateTablesEtc", "Insert into totals (Opening) failed");
      oCompleter.complete(tResult);
      return;
    }).catchError((oError) => throw(oError));
  }).catchError((oError) {
    fFatal("fInsertOpeningTotals", "${oError}");
  });
       
  return oCompleter.future; 
}

/*
 * Insert into 'totals' table values from inserts and updates
 */
async.Future<bool> fInsertIntoTotalsTable(ClassTotals oClassTotals) {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  fCheckTransactionStatus("fInsertIntoTotalsTable", true);
  
  String sSql = oClassTotals.fFormatSql();
  
  fInsertRowWithSequence("totals", S_SEQUENCE_KEY_TOTALS, sSql)
  .catchError((oError) => throw(oError))
  .then((tResult) {
    if (tResult != true)
      throw("Insert failed");
    else {
      fCheckTransactionStatus("fInsertIntoTotalsTable-completion", true);
      oCompleter.complete(true);
    }
  }).catchError((oError) => fFatal("fInsertIntoTotalsTable", "${oError}"));
  
  return oCompleter.future;  
}

/*
 * Display on completion the totals from the
 * main table and also the 'totals' table 
 */
async.Future<bool> fDisplayTotals() {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();
 
  ClassWrapList ollClassList = new ClassWrapList();
  double        dCcyTot1     = 0.00;    // init
  double        dCcyTot2     = 0.00;    // init
  int           iRowTot1     = 0;       // init
  int           iRowTot2     = 0;       // init

  String sSql = "SELECT count(*), sum(dbalance) FROM ${S_MAIN_TABLE}";
  
  fProcessSqlSelect(sSql, false, ollClassList)
  .then((bool tResult) {
    if (tResult != true)
      fFatal(null, "FDisplayTotals: Select failed");
    /*
     * Display totals from select of main table
     */
    List<List> llRows = ollClassList.llValue;
    if (llRows == null || llRows.length < 1)
      throw ("fDisplayTotals: Select of '${S_MAIN_TABLE}' table failed");
    
    String sCcyTot1 = "0.0";
    
    if (llRows[0][0] != null)
      iRowTot1 = llRows[0][0];
    if (llRows[0][1] != null)
      sCcyTot1 = llRows[0][1];
    
    dCcyTot1 = double.parse(sCcyTot1);
    sCcyTot1 = ogFormatCcy.fFormatCcy(dCcyTot1);
    ogPrintLine.fPrintForce
    ("Current '${S_MAIN_TABLE}' table totals: Rows = ${iRowTot1}, "+
       "Balances = ${sCcyTot1}");
    
    return true;
  }).then((_) {   
    /*
     * / Display totals from select of totals table
     */

    sSql = "SELECT sum(iInsertNr), sum(dccyvalue) FROM totals"; ///xxxxxx
    fProcessSqlSelect(sSql, false, ollClassList)
    .then((bool tResult) {
      List<List> llRows = ollClassList.llValue;
      if (llRows == null || llRows.length < 1)
        throw ("fDisplayTotals: Select of 'totals' table failed");

      String sCcyTot2 = "0.0";
      
      if (llRows[0][0] != null)
        iRowTot2 = llRows[0][0];
      if (llRows[0][1] != null)
        sCcyTot2 = llRows[0][1];      
      
      dCcyTot2 = double.parse(sCcyTot2);
      sCcyTot2 = ogFormatCcy.fFormatCcy(dCcyTot2);
      ogPrintLine.fPrintForce
      ("Current 'totals' table totals: Rows = ${iRowTot2}, "+
          "Balances = ${sCcyTot2}");
    
      double dCcyDiff = ogCcy.fAddCcyDoubles(dCcyTot1, -dCcyTot2);
      int    iRowsDiff = iRowTot1 - iRowTot2;
      
      if (iRowsDiff == 0 && dCcyDiff == 0.00)
        ogPrintLine.fPrintForce("**** Totals DO Balance ****");
      else {
        String sDiffFormatted = ogFormatCcy.fFormatCcy(dCcyDiff); 
        ogPrintLine.fPrintForce("!!!! Totals DO NOT balance !!!!");
        ogPrintLine.fPrintForce (
            "Difference in Rows = ${iRowsDiff} "+
            "Difference in Value = ${sDiffFormatted}");
      }
      oCompleter.complete(true);
      return;
    });
  }).catchError((oError) {
    fFatal ("fDisplayTotals", "${oError}");
  });      
  
  return oCompleter.future;
}

/*
 * Process a general Select using string passed in
 */
async.Future<bool> fProcessSqlSelect(String sSql, bool tShowTime,
                                     ClassWrapList ollResult) {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();

  fCheckTransactionStatus("fProcessSqlSelect", true);
     
  Stopwatch oStopwatch = new Stopwatch();
  oStopwatch.start();

  ogDb.query(sSql).toList().then((llRows) {
    oStopwatch.stop();
    if (llRows == null || llRows.isEmpty) {
      if (tShowTime)
        ogPrintLine.fPrintForce ("No rows were selected");
    } else if (tShowTime) {
      int iTotRows = llRows.length;
      int iMillis = oStopwatch.elapsedMilliseconds;
      ogPrintLine.fPrintForce ("Total rows selected = $iTotRows, "+
       "elapsed = ${(iMillis/1000).toStringAsFixed(2)} seconds");
      ogPrintLine.fPrintForce ("Average elapsed milliseconds = "+
       "${(iMillis/iTotRows).toStringAsFixed(4)}");
    }
    fCheckTransactionStatus("fProcessSqlSelect - after Select", false);
    if (ollResult != null)
      ollResult.llValue = llRows;
    oCompleter.complete(true);
    return;
  }).catchError((oError) {
    fFatal ("fProcessSqlSelect", "${oError}");
  });

  return oCompleter.future;  
}

/*
 * Attempt to connect to specific port to determine if first process.
 */
async.Future<bool> fCheckIfFirstInstance() {
  async.Completer<bool> oCompleter = new async.Completer<bool>(); 
  RawServerSocket.bind("127.0.0.1", 8087).then((oSocket) {
    ogSocket = oSocket;         // assign to global
    oCompleter.complete(true);
  }).catchError((oError) {
    oCompleter.complete(false);
  });
  
  return oCompleter.future;
}

/*
 * Fatal Error Encountered
 */
void fFatal (String sCheckpoint, String oError) {
  print("\n\nFatal Error. $sCheckpoint\n${oError}");
  fRollback("fFatal").catchError((oError) => print (oError))
  .then((_) {
    fExit(1);
  });
}

/*
 *   Exit this program
 */
void fExit(int iExitCode) {
  if (ogDb != null) {
    fCheckTransactionStatus("fExit", false);
    ogDb.close();
    ogDb = null;
  }  
  if (ogSocket != null){
    ogSocket.close();
    ogSocket = null;
  }
  if (iExitCode != 0)
    print ("Terminating on error");
  
  exit(iExitCode);
}

/*
 * Rollback Transaction
 */
async.Future<bool> fRollback(String sMsg) {
  
  async.Completer<bool> oCompleter = new async.Completer<bool>();
 
  if (ogDb != null)
    print ("fRollback on entry: transaction status = ${ogDb.transactionStatus}");
  
  if (ogDb == null || (![1,3,4].contains(ogDb.transactionStatus))) {
    print ("Rollback - no action required");
    oCompleter.complete(false);
    return oCompleter.future;
  }
  
  print ("fRollback: Attempting Rollback from $sMsg");
  ogDb.execute("rollback").then((_) {
    print ("fRollback: Rollback completed");
    print ("fRollBack after rollback - Transaction status = ${ogDb.transactionStatus}");
    oCompleter.complete(true);
  }).catchError((oError) {
    print ("fRollback: Rollback failed. Error=\n${oError}");
    oCompleter.complete(false);
  });
  
  return oCompleter.future;
}

/*
 * Test for valid transaction status
 */
void fCheckTransactionStatus (String sFunctionName, bool tAbort) {
  String sErmes = (ogDb == null) ? "Database is null" :
                   [3,4].contains(ogDb.transactionStatus) ?
                   "transactionStatus = ${ogDb.transactionStatus}" :
                   null;
   
  if (sErmes != null) {                 
  
    print ("fCheckTransactionStatus: from ${sFunctionName} "+sErmes);
    
    if (tAbort)
      fFatal(sFunctionName, "Invalid transaction status");
  }
}

/*
 * Get Divisor For Number of Iterations - To Display Progress
 */
int fGetDivisor(int iMaxIters) =>
    iMaxIters >= 100000 ? 20000 :
    iMaxIters >   20000 ?  5000 :
    iMaxIters >    7000 ?  2000 :
    iMaxIters >    3000 ?  1000 :
    iMaxIters >    1000 ?   500 :
                            100;

/*
 * Get a Random Amount For Update or Insert
 */
class ClassRandAmt {
  Random _oRandom; 
  int _iDecPlaces;
  int _iScale = 1;

  ClassRandAmt(int iDecPlaces) {
    _iDecPlaces = iDecPlaces;
    _oRandom   = new Random();
    for (int i=0; i < iDecPlaces; i++, _iScale *= 10);   // set scale for cents
  }
  
  int fRandAmt(int iMaxDollars, bool tPositive) {
    int iCcyVal = _oRandom.nextInt((iMaxDollars * _iScale) + (_iScale-1))+1;  // ensure not zero
    if (! tPositive)   // then make negative
      iCcyVal = -iCcyVal;
    
    return iCcyVal;
  }
}

class ClassTotals{
  int          _iInstance;
  int          _iTotType;
  int          _iInsertNr;
  int          _iUpdateNr;
  int          _iCcyTotAmt;
  int          _iTotMillis;
  List<String> _lsDesc = ["Opening Values", "Inserted Values", "Updated Values"];
  
  ClassTotals() {}
  
  void fSetValues ({int iInstance, int iTotType, int iInsertTotNr, int iUpdateTotNr,
                    int iCcyTotAmt, int iTotMillis}) {
    _iInstance  = iInstance;
    _iTotType   = iTotType;
    _iInsertNr  = iInsertTotNr;
    _iUpdateNr  = iUpdateTotNr;
    _iCcyTotAmt = iCcyTotAmt;
    _iTotMillis = iTotMillis;
  }
  
  void fPrint() {
    String sDesc = _lsDesc[_iTotType];
    int iTotNr = _iInsertNr > 0 ? _iInsertNr : _iUpdateNr;
    String sAverageMillis = "";
    if (_iTotMillis > 0 && iTotNr > 0)
      sAverageMillis = ", Avg Millis = "
                        +(_iTotMillis / iTotNr).toStringAsFixed(2);
    String sValue = ogFormatCcy.fFormatCcy(ogCcy.fCcyIntToDouble(_iCcyTotAmt));
  
    String sPline = "${sDesc}, Nr: ${iTotNr}, Val: ${sValue}"+sAverageMillis;
    ogPrintLine.fPrintForce(sPline);
  }
  
  String fFormatSql() {
    String sDesc = _lsDesc[_iTotType];
    String sCcyValue = ogCcy.fCcyIntToString(_iCcyTotAmt);
    return    "(iKey, iinstancenr, iTotType, sdesc, "+
               "iinsertnr, iUpdateNr, dccyvalue, iTotMillis) VALUES ( "+
               "?, ${_iInstance}, ${_iTotType}, '${sDesc}', ${_iInsertNr}, "+
               "${_iUpdateNr}, ${sCcyValue}, ${_iTotMillis})";
  }  
}

/*
 * Class to handle currency as double, integer, and string 
 */
class ClassCcy {
  int    _iDecPlaces;
  int    _iScale     = 1;  // init

  ClassCcy (int iDecPlaces) {    
    _iDecPlaces = iDecPlaces;
    for (int i=0; i<iDecPlaces; i++, _iScale *= 10);
  }  
  
  double fAddCcyDoubles (double dAmt1, double dAmt2) {
    double dResult = ((fCcyDoubleToInt(dAmt1) + fCcyDoubleToInt(dAmt2)) / _iScale);
    return double.parse(dResult.toStringAsFixed(_iDecPlaces));
  }

  int fCcyStringToInt (String sAmt)
    => fCcyDoubleToInt(double.parse(sAmt));
  
  int fCcyDoubleToInt(double dAmt) =>
    int.parse((dAmt.toStringAsFixed(_iDecPlaces)).replaceFirst(".", ""));
  
  double fCcyIntToDouble(int iAmt) =>
    double.parse((iAmt/_iScale).toStringAsFixed(_iDecPlaces));
  
  String fCcyIntToString(int iAmt)
    => ((iAmt/_iScale).toStringAsFixed(_iDecPlaces));
}

/*
 * Format Currency Value with punctuation (probably not needed with intl)
*/
class ClassFormatCcy {
  int    _iDecPlaces;
  String _sCcySymbol;
  String _sDecSep;
  String _sThouSep;
  
  ClassFormatCcy (int iDecPlace, String sDecSep, String sThouSep, sCcySymbol) {
    _iDecPlaces = iDecPlace;
    _sCcySymbol = sCcySymbol;
    _sDecSep    = sDecSep;
    _sThouSep   = sThouSep;
  }
  
  String fFormatCcy(double dMoney) {
    String sMoney = dMoney.toStringAsFixed(_iDecPlaces);
    StringBuffer sbMoney = new StringBuffer();
    int iDecPos = sMoney.indexOf(_sDecSep);
    if (iDecPos < 0)   // no decimal place
      iDecPos = sMoney.length;
    for (int iPos = 0; iPos < sMoney.length; iPos++) {
      if ((iPos != 0) && (iPos < iDecPos) && (iDecPos -(iPos)) % 3 == 0)
        if (!(iPos == 1 && sMoney[0] == "-"))
          sbMoney.write(_sThouSep);
      sbMoney.write(sMoney[iPos]);
    }
    return _sCcySymbol +sbMoney.toString();
  }
}

class ClassWrapInt {
  int _iValue;
  
  ClassWrapInt() {}
  
  int  get iValue => _iValue;
  
  void set iValue(int iValue) {_iValue = iValue;}
}

class ClassWrapList {
  List<List> _llValue;
  
  ClassWrapList() {}
  
  List<List> get llValue => _llValue;
  
  void       set llValue(List<List> llValue) {_llValue = llValue;}
}

/*
 * Class To Print a line To Console (can probably go)
 */
class ClassPrintLine {
  bool   _tPrint;
  int    _iPrintSeq = 0;
  String _sNewline = "\n";
  
  ClassPrintLine(bool tPrint) {_tPrint = tPrint;}
  
  void set tPrint (bool tPrint) {_tPrint = tPrint;}
  
  void fWriteForce(String sPline) {
    _sNewline = "";
    fPrintForce(sPline);
    _sNewline = "\n";    
  }
  
  void fPrintForce (String sPline) {
    bool tPrintSave = _tPrint;
    tPrint = true;
    fPrint (sPline);
    _tPrint = tPrintSave;
  }
  
  void fPrint (String sPline) {
    if (_tPrint) {
      String sMsg = (++_iPrintSeq).toString();
      if (_iPrintSeq < 10)
        sMsg = "0"+sMsg;
      stdout.write(sMsg+"   "+sPline+_sNewline);
    }
  }
}

/*
 * Class to Create Random Names For Inserts
 */
class ClassRandNames {
  int     _iNamesUsed;
  List<String>    _lsNamesAvail = ['Lars', 'Kasper', 'Greg', 'James',
                  'William', 'Seth', 'Dermot', 'Monty', 'Dennis', 'Sue',
                  'Bill', 'Brendan', 'Drew', 'Ken', 'Ross', 'George',
                  'Anders', 'Rhonda', 'Kerry', 'Linus', 'Marilyn',
                  'Marcus', 'Rita', 'Barbara', 'Kevin', 'Brian'];
  List<bool> _ltNamesUsed;
  Random  _oRandom  = new Random();

  ClassRandNames() {
    _ltNamesUsed = new List<bool>(_lsNamesAvail.length); // list of used names
    _iNamesUsed = 0;
  }
  
  /*
   * Get a Single Random Name
   */
  String fGetRandName() {   // Get a new name from list
    int iPos;
    for (bool tUsed = true; tUsed;) {
      iPos = _oRandom.nextInt(_lsNamesAvail.length);
      tUsed = (_ltNamesUsed[iPos] == true);
      if (tUsed && _iNamesUsed >= _lsNamesAvail.length-7) {  // leave space for speed
        _ltNamesUsed = new List<bool>(_lsNamesAvail.length); // list of used names
        _iNamesUsed = 0;
        tUsed = false;
      }
    }
    _iNamesUsed++;
    _ltNamesUsed[iPos] = true;
    return _lsNamesAvail[iPos];
  }
}

/*
 * ClassTerminalInput : Class specific to this application for terminal input
 */
class ClassTerminalInput {
  List<String>   _lsHeading           = new List(6);
  List<String>   _lsPrompts           = new List(I_MAX_PROMPT+1);   // Prompt strings
  List<String>   _lsFieldNames        = new List(I_MAX_PROMPT);
  List<List>     _lliMinMax           = new List(I_MAX_PROMPT+1);
  List<List>     _llsAllowableEntries = new List(I_MAX_PROMPT+1);   // Allowable entries
  List<Function> _lfValidation        = new List(I_MAX_PROMPT+1);   // validation functions
  ClassConsole   _oClassConsole       = new ClassConsole();         // general class
   
  ClassTerminalInput() {
    _lsPrompts[I_DB_USER]      = "Database User Name .............. : ";
    _lsPrompts[I_DB_PWD]       = "Database User Password .......... : ";
    _lsPrompts[I_DB_NAME]      = "Database Name ................... : ";
    _lsPrompts[I_MAX_INSERTS]  = "Number of Insert Iterations ..... : ";
    _lsPrompts[I_USE_AUTOINC]  = "Use AutoInc for Inserts (y/n) ... : ";
    _lsPrompts[I_MAX_UPDATES]  = "Number of Update Iterations ..... : ";    
    _lsPrompts[I_SELECT_SIZE]  = "Select sample-size for updates .. : ";
    _lsPrompts[I_CLEAR_YN]     = "Clear Table? (y/n) .............. : ";
    _lsPrompts[I_SAVE_YN]      = "Save selections to file? (y/n) .. : ";
    _lsPrompts[I_INSTANCE_TOT] = "Nr. of instances you will run ... : ";
    _lsPrompts[I_CORRECT_YN]   = "Details Correct? (y/n/end) ...... : ";
    
    _lsFieldNames[I_DB_USER]      = "Database User Name";
    _lsFieldNames[I_DB_PWD]       = "Database User Password";
    _lsFieldNames[I_DB_NAME]      = "Database Name";
    _lsFieldNames[I_MAX_INSERTS]  = "Number of Insert Iterations";
    _lsFieldNames[I_MAX_INSERTS]  = "Use AutoInc for Inserts (y/n)";
    _lsFieldNames[I_MAX_UPDATES]  = "Number of Update Iterations";
    _lsFieldNames[I_SELECT_SIZE]  = "Select sample-size for Updates";
    _lsFieldNames[I_CLEAR_YN]     = "Clear Table? (y/n)";
    _lsFieldNames[I_SAVE_YN]      = "Save selections to file? (y/n)";
    _lsFieldNames[I_INSTANCE_TOT] = "Number of instances you will run";
 
    _lliMinMax[I_DB_USER]      = [1];
    _lliMinMax[I_DB_PWD]       = [1];
    _lliMinMax[I_DB_NAME]      = [1];
    _lliMinMax[I_MAX_INSERTS]  = [0, 1000000];
    _lliMinMax[I_USE_AUTOINC]  = [1];
    _lliMinMax[I_MAX_UPDATES]  = [0, 1000000];
    _lliMinMax[I_SELECT_SIZE]  = [1, 1000];
    _lliMinMax[I_CLEAR_YN]     = [1];
    _lliMinMax[I_SAVE_YN]      = [1];
    _lliMinMax[I_INSTANCE_TOT] = [1,999];
    _lliMinMax[I_CORRECT_YN]   = [1];

    _llsAllowableEntries[I_USE_AUTOINC]  = ["y", "n"];
    _llsAllowableEntries[I_CLEAR_YN]     = ["y", "n"];
    _llsAllowableEntries[I_SAVE_YN]      = ["y", "n"];
    _llsAllowableEntries[I_CORRECT_YN]   = ["y", "n"];
    
    List<String> _lsDataTypes = new List(I_MAX_PROMPT+1);
    _lsDataTypes[I_MAX_INSERTS]   = "int";
    _lsDataTypes[I_MAX_INSERTS]   = "int";
    _lsDataTypes[I_SELECT_SIZE]   = "int";
    _lsDataTypes[I_INSTANCE_TOT]  = "int";
       
    _lsHeading[0] = "";
    _lsHeading[1] = "";
    _lsHeading[2] = "Program to test Dart 'Postgresql' package by "+
                     "xxgreg (on Github)";
    _lsHeading[3] = "";
    _lsHeading[4] = "Enter required parameters as follows";
    _lsHeading[5] = "------------------------------------";
    
    _oClassConsole.fInit(I_MAX_PROMPT, _lsPrompts, _lsDataTypes,
                         _llsAllowableEntries, _lfValidation, _lsHeading,
                         _lliMinMax);
  }

  /*
   * ClassTerminalInput: Prompt User for parameters
   */
  List<String> fGetUserSelections() {
    int  iDisplayLine;
    String sOldStartTime;
       
    // Read parameters from file
    List<String> lsInput = _oClassConsole.fReadParamsFile(
                           S_PARAMS_FILE_NAME, I_TOTAL_PROMPTS);
    
    // save parameters for later comparison    
    List<String> lsSavedParams = new List(lsInput.length);
    for (int i=0; i < lsInput.length-1; lsSavedParams[i]=lsInput[i], i++);

    // determine validity of data on file and get first invalid line
    int iPromptNr = _oClassConsole.fValidateAllInput(lsInput, I_TOTAL_PROMPTS);  
    if (iPromptNr < I_MAX_PROMPT)  // if file data is invalid start at zero
      iPromptNr = 0;
    
    _oClassConsole.fDisplayInput(iPromptNr-1, lsInput);  // display previous lines

    bool tValid = false;    
    while (!tValid) {
      _oClassConsole.fGetUserInput(iPromptNr, lsInput, "end");
      
      print ("\n\n\n");     
      tValid = (lsInput[I_CORRECT_YN] == "y");
      lsInput[I_CORRECT_YN] = "";   // clear entry so not defaulted
      iPromptNr = 0;   // init
      if (tValid) {
        // check for first invalid line
        iPromptNr = _oClassConsole.fValidateAllInput(lsInput, I_TOTAL_PROMPTS);
        tValid = (iPromptNr >= I_CORRECT_YN);

        if (! tValid)
          print ("Entry is required for : "+ _lsFieldNames[iPromptNr]);
      } else {   
        // re-read file and if no changes made here, replace parameters
        bool tReplaced = _fCheckFileValues(lsInput, lsSavedParams);
        if (tReplaced)
          iPromptNr = I_CORRECT_YN;  // redisplay values
      }        
      if (tValid) {
        for (iPromptNr = 0; iPromptNr < I_INSTANCE_TOT && tValid;
        tValid = (lsInput[iPromptNr] != ""), iPromptNr++);
        
        if (! tValid)
          print ("\nParameter ${_lsFieldNames[iPromptNr]} "+
                 "must be entered");
      }
      
      if (tValid) {                  
        try {
          int iTemp = int.parse(lsInput[I_INSTANCE_TOT]);
          tValid = (iTemp > 0);
        } catch (oError) {tValid = false;}

        if (!tValid) {
          print ("Invalid - at least one instance must run");
        } else if ((int.parse(lsInput[I_MAX_INSERTS]) < 1) &&
         lsInput[I_CLEAR_YN] == "y" &&
         int.parse(lsInput[I_MAX_UPDATES]) > 0) {
          print ("Invalid - there will be nothing to update (zero inserts)");
          tValid = false;
          iPromptNr = I_CLEAR_YN;       
        }
      }
    }
      
    if (lsInput[I_SAVE_YN] == 'y')  // then save data
      _oClassConsole.fSaveParams(_lsPrompts, lsInput,
        I_MAX_PROMPT, S_PARAMS_FILE_NAME);
    
    return lsInput;
  }
  
  /*
   * ClassTerminalInput: Check if parameters entered are valid
   */
  int _fValidateParams(List<String> lsInput, bool tChangeTime) {    
    int iMax = I_INSTANCE_TOT;
    for (int iPos = 0; iPos <= I_INSTANCE_TOT; iPos++) {
      if (lsInput[iPos] == "")
        return iPos;
    }
    return I_MAX_PROMPT;
  }
  
  /*
   * ClassTerminalInput: if no alterations, re-read file
   */
  bool _fCheckFileValues (List<String> lsInput, List<String> lsSavedParams) {

    bool tAltered = false;

    for (int i=0; i<=(I_MAX_PARAM) && !tAltered;
     tAltered = (lsInput[i] != lsSavedParams[i]), i++);
 
    if (tAltered)     // there has been new input by operator
      return false;   // not replaced
     
    List<String> lsNewParams = _oClassConsole.fReadParamsFile(
                               S_PARAMS_FILE_NAME, I_TOTAL_PROMPTS);
 
    tAltered = false;   // init
    for (int i=0; i<=(I_MAX_PARAM) && !tAltered;
     tAltered = (lsInput[i] != lsNewParams[i]), i++);
    
    if (!tAltered)     // values on file are the same
      return false;

    print("");        
    String sInput;
    while (sInput != "y") {
      stdout.write ("\nParameter values on file have altered. "+
             "Do you wish to use them? (y/n)");
      sInput = stdin.readLineSync();
      if (sInput == "end")
        fExit(0);
      if (sInput == "n")
        return false ;    // not replaced;
    }

    // Replace the input parameters with the file parameters //
    for (int i=0; i <= I_MAX_PARAM; lsInput[i] = lsNewParams[i], i++);

    return true;   // parameters were replaced
  }  
}

/*
 * ClassConsole - Class to Handle Console Input
 */
class ClassConsole {
  int            _iMaxPrompt;
  List<Function> _lfValidation;
  List<List>     _llsValidEntries;
  List<List>     _lliMinMax;
  List<String>   _lsPrompts;
  List<String>   _lsDataTypes;
  List<String>   _lsHeading;

  /*
   * ClassConsole - Initialize
   */
  ClassConsole() {}
  
  fInit(int iMaxPrompt, List<String> lsPrompts, List<String> lsDataTypes,
        List<List> llsValidEntries, List<Function> lfValidation,
        List<String> lsHeading, List<List> lliMinMax) {
    _iMaxPrompt      = iMaxPrompt;
    _lsPrompts       = lsPrompts;
    _lsDataTypes     = lsDataTypes;
    _llsValidEntries = llsValidEntries;
    _lfValidation    = lfValidation;
    _lsHeading       = lsHeading;
    _lliMinMax       = lliMinMax;
  }
  
/*
 * ClassConsole - prompt for user input (all lines)
 */
  void fGetUserInput(int iPromptNr, List<String> lsInput, String sTerminate) {
    Function fLoop;      
     /*
     *  Loop prompting the user for each line
     */
    fLoop = (() {
      // Prompt for one line //
      iPromptNr = _fPromptOneLine(iPromptNr, lsInput, sTerminate); 

      if (iPromptNr > _iMaxPrompt)
        return;

      if (iPromptNr < 0) // 1st valid prompt is "0"
        iPromptNr = 0;

      return fLoop();
    });
    return fLoop();
  }  
  
/*
 * ClassConsole: Prompt User For Parameters (one line)
 */
  int _fPromptOneLine (int iPromptNr, List<String> lsInput, String sTerminate) {

    if (iPromptNr == _iMaxPrompt)
      fDisplayInput(_iMaxPrompt-1, lsInput);
    else if (iPromptNr == 0) {   // 1st line so display heading
      print("");
      for (int i = 0; i < _lsHeading.length; print(_lsHeading[i]), i++);
      for
        (int i=0; i<59; stdout.write(" "), i++);
      print ("Default");
      for
        (int i=0; i<59; stdout.write(" "), i++);
      print ("-------");
    }

    String sDefault = lsInput[iPromptNr] == null ? "" : lsInput[iPromptNr];
    
    String sInput = _fReadConsoleLine(iPromptNr, sDefault, "end");

    if (sTerminate != "" && sInput == sTerminate) {
      fExit(0);
    } else if (sInput == "<") {
      iPromptNr -= 2;
    } else {
      lsInput[iPromptNr] = sInput;   // save data entered
    }
    return ++iPromptNr;                  // increment prompt number
   }
  
  /*
   * ClassConsole - Read Single Console Line
   */
  String _fReadConsoleLine(int iPromptNr, String sDefault,
                            String sTerminate) {
    
    List<int> liCharCodes       = [];
    String sPrompt              = _lsPrompts[iPromptNr];
    
    while (true) {
      liCharCodes.clear();
      stdout.write("\n$sPrompt");
      if (sDefault != "") {
        // Display the default //
        for
          (int i=sPrompt.length; i<60; stdout.write(" "), i++);
        stdout.write("$sDefault");
        // backspace to position //
        for
          (int i = 58+sDefault.length+2; i>sPrompt.length; stdout.write("\b"), i--);
      }
      int iInput;
      while (iInput != 10) {   // 10 = newline
        // I'll alter this to read raw bytes when available //
        iInput = stdin.readByteSync();
        if (iInput > 31)  // then ascii printable char
          liCharCodes.add(iInput);
      }
      int iSecs = 0;    // init
      String sInput = new String.fromCharCodes(liCharCodes);
 
      if (sInput == "")
        sInput = sDefault;
      else
        sInput = sInput.trim();
      
      String sErrorLine = "";      
      try {
        if (sInput == sTerminate) {
        } else if (sInput == "<") {  // back one
        } else {
          sErrorLine = _fValidateSingleLine(iPromptNr, sInput);
        }
      } catch (oError) {
        sErrorLine = "$oError";
      }
      
      if (sErrorLine == "")
        return sInput;
      
      print (sErrorLine +"\n");       
    }
  }
   
  /*
   * ClassConsole - Validate Single input
   */
  String _fValidateSingleLine (int iPromptNr, String sInput) {
    int iMinVal;
    int iMaxVal;
    String sDataType            = _lsDataTypes[iPromptNr];  // default is String
    String sErrorLine;
    
    if (_lliMinMax[iPromptNr] != null && 
        _lliMinMax[iPromptNr].length > 0) {
      iMinVal = _lliMinMax[iPromptNr][0];
      if (_lliMinMax[iPromptNr].length > 1)
        iMaxVal = _lliMinMax[iPromptNr][1];
    }
    
    List<String> lsValidEntries = _llsValidEntries[iPromptNr];
    Function fValidation        = _lfValidation[iPromptNr];  
  
    if (fValidation != null) {
      sErrorLine = fValidation(sInput);
    } else if (sDataType == "int") {
      sErrorLine = _fValidateInt(sInput, iMinVal, iMaxVal, lsValidEntries);
    } else
      sErrorLine = _fValidateString(sInput, iMinVal, iMaxVal, lsValidEntries);          
 
    return sErrorLine;
  }
  
  /*
   * ClassConsole - Validate String Entered
   */
  String _fValidateString (String sInput, int iMinLgth, int iMaxLgth, List lAllowableEntries) {

    if (iMinLgth != null && sInput.length < iMinLgth) {
      if (iMinLgth == 1)
        return ("Invalid - field is compulsory");
      return ("Invalid - minimum length for field is ${iMinLgth}");
    }
    
    if (iMaxLgth != null && iMaxLgth > 0 && sInput.length > iMaxLgth)
      return ("Invalid - maximum length for field is ${iMaxLgth}");
      
    bool tValid = (lAllowableEntries == null || lAllowableEntries.length < 1);
    for (int iPos = 0; !tValid && iPos < lAllowableEntries.length; iPos++)
      tValid = (sInput == lAllowableEntries[iPos]);

    if (!tValid)
      return "Invalid - entry must be one of ${lAllowableEntries}";

    return "";
  }
 
  /*
   * ClassConsole - Validate Integer Entered
   */
  String _fValidateInt(String sInput, int iMinVal, int iMaxVal, List lAllowableEntries) {
    int iInput;
    try {
      iInput = int.parse(sInput);
    } catch (oError) {
      return "Invalid - non-numeric entry - '$sInput'";
    }
    if (iMinVal != null && iInput < iMinVal)
      return "Invalid - Entry must not be less than ${iMinVal}";
    if (iMaxVal != null && iInput > iMaxVal)
      return "Invalid - Entry must not exceed ${iMaxVal}";

    bool tValid = (lAllowableEntries == null || lAllowableEntries.length < 1);
    for (int iPos = 0; !tValid && iPos < lAllowableEntries.length; iPos++)
      tValid = (iInput == lAllowableEntries[iPos]);

    return ! tValid ? "Invalid - entry must be one of ${lAllowableEntries}" : "";
  }
    
  /*
   * ClassConsole: Save Selections to File
   */
  void fSaveParams(List<String> lsPrompts, List<String> lsInput,
    iMaxParams, sFileName) {
    String sFileBuffer = "";        // used to write to file
    for (int iPos1 = 0; iPos1 < iMaxParams; iPos1++) {
      sFileBuffer = sFileBuffer +"${_lsPrompts[iPos1]}";
      sFileBuffer = sFileBuffer +"${lsInput[iPos1]}\n";
    }
    ogPrintLine.fPrintForce ("Saving selections");
    new File(sFileName).writeAsStringSync(sFileBuffer);
    ogPrintLine.fPrintForce ("File written");
  }
  
  /*
   * ClassConsole: Read Parameters from File
   */
  List<String> fReadParamsFile(String sFileName, int iTotLines) {
    
    List<String> lsInput = new List(iTotLines);
    
    fReadFixedListSync(sFileName, lsInput);
    // extract the parameters for the file
    for (int iPos1 = 1; iPos1 <= iTotLines; iPos1++) {
      String sParam = lsInput[iPos1-1];
      int iPos2 = sParam.indexOf(" :");
      if (iPos2 < 1)  // no default
        lsInput[iPos1-1] = "";
      else
        lsInput[iPos1-1] = sParam.substring(iPos2+2).trim();
    }   
    
    return lsInput;
  }
  
  /*
   * ClassConsole: Read fixed-length String List from file
   */
  int fReadFixedListSync(String sFileName, List<String> lsData) {
    int iLinesOnFile = 0;
    int iByte;
    List<int> lCharCodes = [];
    try {
      var fileSync = new File(sFileName).openSync(mode: FileMode.READ); 
      do {  
        iByte = fileSync.readByteSync();
        if (iByte > 31)   // printable
          lCharCodes.add(iByte);
        else if (iByte == 10) {   // part of CR/LF
          iLinesOnFile++;
          if (iLinesOnFile <= lsData.length)
            lsData[iLinesOnFile-1] = new String.fromCharCodes(lCharCodes);
          lCharCodes.clear();
        }
      } while (iByte > -1);
    } catch(oError){
      if (!(oError is FileException))
        throw ("fReadFixedListSync: Error = \n${oError}");
    }
    // Make remaining lines (if any) Strings //
    for (int i=iLinesOnFile+1; i<=lsData.length; lsData[i-1] = "", i++);
    return iLinesOnFile;   // return actual line count
  }
  
  /*
   * Class Console : Validate all data in List
   */
  int fValidateAllInput(List lsData, int iMaxPrompt) {
    int iPromptNr;
    for (iPromptNr = 0; iPromptNr <= iMaxPrompt; iPromptNr++) {
      String sErmes = _fValidateSingleLine (iPromptNr, lsData[iPromptNr]);
      if (sErmes != "")   // invalid
        return iPromptNr;
    }
    return iMaxPrompt +1;   // all valid
  } 
  
  /*
   * Class Console: Display Selections made by Operator
   */
  void fDisplayInput(iMaxPrompt, List<String> lsInput) {
    for (int i = 0; i < _lsHeading.length; print(_lsHeading[i]), i++);
    for (int iPos = 0; iPos <= iMaxPrompt; iPos++){
      print ("");
      print (_lsPrompts[iPos] +"${lsInput[iPos]}");
    }   
  }
}