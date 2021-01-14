## IDAA Capture Replay

This project allows IDAA users to capture production IDAAV5 workloads and run the workloads on IDAAV7. Ideally this is done during migration from V5 to V7 before IDAAV7 goes to production.  The project has two parts:

> 1) **Capture:** From a default IDAAV5 server trace, all queries executed for the past 7 days are captured. Host variables or parameter markers in SQL are replaced with the actual value. A set of DSNTIAUL jobs are generated for all captured queries. Each SQL is appended with a QUERYNO clause to easily map the V5 query execution against the V7 query execution. A csv file is also generated to store query attributes and measurements. This csv file can be loaded into a Db2 table.
> 2) **Replay:** The DSNTIAUL jobs should then be executed on the V7 accelerator. From a default IDAAV7 trace a csv file is generated and loaded into a Db2 table.

### Pre-requisites
* A java compiler that includes the xmlparser.
* The programs require the Linux commands fold and unix2dos.
* Google protobuf. The latest version is here https://developers.google.com/protocol-buffers/docs/downloads . The latest version should work, the matching version of what was used, 3.10.0 is here https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.10.0/protobuf-java-3.10.0.jar  

### Setup
Make sure the java CLASSPATH enviroment variable includes the path toe both the com directory and the protobuf jar file. For example
CLASSPATH=/root/java:/root/java/protobuf-java-3.10.0.jar

Compile com/IDAA/SQLFormat/SQLStatementDetails.java
Compile both SQLHistoryGetHostvarValues.java and SQLHistorySummaryV75.java

On Db2z/OS create the following two tables in UNICODE to load the csv files
```
 CREATE TABLE DB2ADMN.ACCEL_QUERY_HIST_POC_ORIGINAL
     (
      TASKID               INTEGER WITH DEFAULT NULL,
      EXECTTIMESEC         DOUBLE WITH DEFAULT NULL,
      ELAPSEDTIMESEC       DOUBLE WITH DEFAULT NULL,
      WAITTIMESEC          DOUBLE WITH DEFAULT NULL,
      RESULTROWS           BIGINT WITH DEFAULT NULL,
      RESULTBYTES          BIGINT WITH DEFAULT NULL,
      FETCHTIMESEC         DOUBLE WITH DEFAULT NULL,
      INSERT_TSTAMP        TIMESTAMP (6) WITHOUT TIME ZONE NOT NULL
        WITH DEFAULT,
      "USER"               CHAR(8) FOR SBCS DATA WITH DEFAULT NULL,
      HASH_ORIGINAL        CHAR(40) FOR SBCS DATA WITH DEFAULT NULL,
      ORIGINAL_SQL         VARCHAR(30000) FOR SBCS DATA
        WITH DEFAULT NULL,
      DB2LOCATION          CHAR(8)  FOR SBCS DATA WITH DEFAULT NULL,
      ITERATIONNUM         INTEGER,
      SQLCODE              INTEGER);
```

```
 CREATE TABLE DB2ADMN.ACCEL_QUERY_HIST_POC_NEW
     (
      TASKID               INTEGER WITH DEFAULT NULL,
      V5TASKID               INTEGER WITH DEFAULT NULL,
      EXECTTIMESEC         DOUBLE WITH DEFAULT NULL,
      ELAPSEDTIMESEC       DOUBLE WITH DEFAULT NULL,
      WAITTIMESEC          DOUBLE WITH DEFAULT NULL,
      RESULTROWS           BIGINT WITH DEFAULT NULL,
      RESULTBYTES          BIGINT WITH DEFAULT NULL,
      FETCHTIMESEC         DOUBLE WITH DEFAULT NULL,
      INSERT_TSTAMP        TIMESTAMP (6) WITHOUT TIME ZONE NOT NULL
        WITH DEFAULT,
      "USER"               CHAR(8) FOR SBCS DATA WITH DEFAULT NULL,
      HASH_ORIGINAL        CHAR(40) FOR SBCS DATA WITH DEFAULT NULL,
      ORIGINAL_SQL         VARCHAR(30000) FOR SBCS DATA
        WITH DEFAULT NULL,
      DB2LOCATION          CHAR(8)  FOR SBCS DATA WITH DEFAULT NULL,
      ITERATIONNUM         INTEGER,
      SQLCODE              INTEGER);
```
### Running the tools
#### Capture
1) Run queries on V5, save trace. Extract all SQLHistory.<db2z location name> files under profiling to a separate folder. For example in TS002529484.PRODNZ01.TRC6.TGZ_unpack\profiling.tgz_unpack\nz\dwa\var\server\profiling\ copy SQLHistory.PROD1 and SQLHistory.PROD1.2021-01-11 to /root/java/PRODNZ1
2) Load the tables that are defined on the V5 accelerator to the V7 accelerator. The load to V7 should be done before tables are reloaded to V5 again. The data on the V5 accelerated tables must be the same as the data on the V7 accelerator to make valid comparisons.
3) Modify the JCL section of SQLHistoryGetHostvarValues in generateTIAULFile() to match the IDAAV7 accelerator name. Compile SQLHistoryGetHostvarValues.java
  Run the program SQLHistoryGetHostvarValues to generate both the csv file and DSNTIAUL jobs
  java SQLHistoryGetHostvarValues /root/java/PRODNZ1 1 sanitizeit
4) The generated DSNTIAUL files will have a maximum of 100 DSNTIAUL steps in each .TIAUL file. Each step contains 1 SQL statement. For example
```  
//RUNTIAUL JOB 'USER=$$USER','<USERNAME:JOBNAME>',CLASS=A,          
//         MSGCLASS=A,MSGLEVEL=(1,1),USER=SYSADM,REGION=4096K,      
//         PASSWORD=C0DESHOP                                        
/*ROUTE PRINT STLVM14.CHIHCHAN                                      
//*                                                                 
//*                                                                 
//JOBLIB  DD  DSN=DB2A.SDSNLOAD,DISP=SHR                            
//UNLOAD  EXEC PGM=IKJEFT01,DYNAMNBR=20,COND=(8,LT)                 
//SYSTSPRT  DD  SYSOUT=*                                            
//SYSTSIN   DD  *                                                   
DSN SYSTEM(DB2A)                                                    
RUN  PROGRAM(DSNTIAUL) PLAN(DSNTIBC1) PARMS('SQL','1') -            
LIB('DB2A.TESTLIB')                                                 
//SYSPRINT  DD SYSOUT=*                                             
//SYSUDUMP  DD SYSOUT=*                                             
//SYSREC00  DD DUMMY                                                
//SYSPUNCH DD SYSOUT=*                                              
//SYSIN     DD *                                                    
SET CURRENT QUERY ACCELERATION=ALL;
SET CURRENT ACCELERATOR=SIM143;
SELECT C1 FROM T1 WHERE C0 > 0 
WITH UR
QUERYNO 9472;
//*  
```
5) Load the csv file into DB2ADMN.ACCEL_QUERY_HIST_POC_ORIGINAL
  
#### Replay
1) Run the DSNTIAUL jobs sequentially against the V7 accelerator.
2) Save the default IDAAV7 trace. Copy the SQLHistory file(s) to a separate folder. For example TS000000000.accel-trace-archive-20200810-094235-642\Server\accel-trace-20200810-094134-618\accelerator\profiling\profiling\head\dwa\var\log\profiling\SQLHistory.PROD2
3) Run the program SQLHistorySummaryV75. The 1st input argument is the path to the folder with the SQLHistory file(s). The 2nd input argument is the iteration number that you define. The example below is the third execution of the workload on V7
```
java SQLHistorySummaryV75 /root/java/sql_historyV7.iteration3/ 3
Generating .sqlhistory files
Generated 47090 .sqlhistory files
Generating csv file
Generated csv file with 47090 query entries
Cleaning up .sqlhistory files
Cleanup of .sqlhistory files complete
```
4) Load the generated csv file into DB2ADMN.ACCEL_QUERY_HIST_POC_NEW

Here are some sample queries to compare the V7 workload runs against the V5 production run
Sample queries comparing the two
```
SELECT V7.TASKID AS V7TASKID
,V5.EXECTTIMESEC/V7.EXECTTIMESEC EXECTIME_SPEEDUP
,V5.ELAPSEDTIMESEC/V7.ELAPSEDTIMESEC ELAPSEDTIME_SPEEDUP
,V7.EXECTTIMESEC -  V5.EXECTTIMESEC EXECTIME_DIFF
,V7.ELAPSEDTIMESEC -  V5.ELAPSEDTIMESEC ELAPSEDTIME_DIFF
,V7.EXECTTIMESEC AS V7EXECTIME,V5.EXECTTIMESEC AS V5EXECTIME
,V7.ELAPSEDTIMESEC AS V7ELAPSEDTIME,V5.ELAPSEDTIMESEC AS V5ELAPSEDTIME
,V7.WAITTIMESEC AS V7WAITTIME,V5.WAITTIMESEC AS V5WAITTIME
,V7.FETCHTIMESEC AS V7FETCHTIME ,V5.FETCHTIMESEC AS V5FETCHTIME
,V7.RESULTROWS - V5.RESULTROWS AS RESULTROW_DIFF
,V7.RESULTBYTES - V5.RESULTBYTES AS RESULTBYTE_DIFF
from DB2ADMN.ACCEL_QUERY_HIST_POC_ORIGINAL V5,
DB2ADMN.ACCEL_QUERY_HIST_POC_NEW V7
WHERE V5.TASKID = V7.V5TASKID
AND V7.ITERATIONNUM = <ITERATIONNUMBER>;

SELECT
COUNT(V5.TASKID) AS NUM_EXECUTIONS
,AVG(V5.EXECTTIMESEC/V7.EXECTTIMESEC) AS AVG_EXECTIME_SPEEDUP
,AVG(V5.ELAPSEDTIMESEC/V7.ELAPSEDTIMESEC) AS AVG_ELAPSEDTIME_SPEEDUP
,AVG(V7.EXECTTIMESEC -  V5.EXECTTIMESEC) AS AVG_EXECTIME_DIFF
,AVG(V7.ELAPSEDTIMESEC -  V5.ELAPSEDTIMESEC) AS AVG_ELAPSEDTIME_DIFF
,AVG(V7.EXECTTIMESEC) AS AVG_V7EXECTIME
,AVG(V5.EXECTTIMESEC) AS AVG_V5EXECTIME
,AVG(V7.ELAPSEDTIMESEC) AS AVG_V7ELAPSEDTIME
,AVG(V5.ELAPSEDTIMESEC) AS AVG_V5ELAPSEDTIME
,AVG(V7.WAITTIMESEC) AS AVG_V7WAITTIME
,AVG(V5.WAITTIMESEC) AS AVG_V5WAITTIME
,AVG(V7.FETCHTIMESEC) AS AVG_V7FETCHTIME 
,AVG(V5.FETCHTIMESEC) AS AVG_V5FETCHTIME
,V5.HASH_ORIGINAL
from DB2ADMN.ACCEL_QUERY_HIST_POC_ORIGINAL V5,
DB2ADMN.ACCEL_QUERY_HIST_POC_NEW V7
WHERE V5.TASKID = V7.V5TASKID
AND V7.ITERATIONNUM = <ITERATIONNUMBER>
GROUP BY V5.HASH_ORIGINAL
ORDER BY AVG_V7ELAPSEDTIME;

SELECT SUM(AVG7ELAPSEDTIME) AS SUMAVG7ELAPSEDTIME,SUM(AVG5ELAPSEDTIME) AS SUMAVG5ELAPSEDTIME,COUNT(*) AS NUMUNIQUEQUERIES
FROM (
SELECT
AVG(V7.ELAPSEDTIMESEC) AS AVG7ELAPSEDTIME
,AVG(V5.ELAPSEDTIMESEC) AS AVG5ELAPSEDTIME
,COUNT(*) AS NUMUNIQUEQUERIES
FROM DB2ADMN.ACCEL_QUERY_HIST_POC_ORIGINAL V5,
DB2ADMN.ACCEL_QUERY_HIST_POC_NEW V7
WHERE V5.TASKID = V7.V5TASKID
AND V7.ITERATIONNUM = <ITERATIONNUMBER>
AND V7.DB2LOCATION='<DB2Z LOCATION NAME>'
AND V7. INSERT_TSTAMP > '<BEGINNING TIMESTAMP OF WHEN THE WORKLOAD WAS STARTED IN UTC0>'
GROUP BY V5.HASH_ORIGINAL 
) AS TX1;
```
