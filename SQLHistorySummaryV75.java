import com.google.protobuf.TextFormat;
import java.io.* ;
import java.nio.file.* ;
import java.lang.String;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.IDAA.SQLFormat.SQLStatementDetails.SQLStatementDetailsData;

//java SQLHistorySummaryV75 /root/java/sql_history 1
public class SQLHistorySummaryV75 {
  static final String xml10pattern = "[^" + "\u0009\r\n" + "\u0020-\uD7FF" + "\uE000-\uFFFD" + "\ud800\udc00-\udbff\udfff" + "]";
  static final int ORIGINALSQL = 0;
  static final int BACKENDSQL = 1;
  static final String[] attr = {
    "original_sql_statement_text: \"",
    "backend_sql_statement_text: \"",
    "client_accounting: \"",
    "backend_error_message: \""
  };
  private static int indexStringContainsItemFromList(String inputStr, String[] items) {
    for (int i = 0; i < items.length; i++) {
      if (inputStr.contains(items[i])) {
        return i;
      }
    }
    return - 1;
  }

  private static String replaceForProto(String inputStr) {
    String returnVal = inputStr.replace("\"", "\\\"");
    returnVal = returnVal.replace("\n", " ");
    return returnVal.replace("'", "\\'");
  }

  private static String processOriginalSQL(BufferedWriter writerSanitizedSQLHistory, String sanitizedline, BufferedReader in ) {
    String originalSQLString = replaceForProto(sanitizedline);
    int attrIndex = ORIGINALSQL;
    String line = "";

    try {
      while (attrIndex != BACKENDSQL && (line = in.readLine()) != null) {
        attrIndex = indexStringContainsItemFromList(line, attr);
        originalSQLString += replaceForProto(line);
      }
      originalSQLString = originalSQLString.substring(0, originalSQLString.length() - 2);
      originalSQLString = originalSQLString + "\"";
      String valueToWrite = attr[ORIGINALSQL] + originalSQLString;
      writerSanitizedSQLHistory.write(valueToWrite + "\n");
    } catch(Exception e) {
      e.printStackTrace();
    }
    return line;
  }

  public static void main(String[] args) throws IOException {

    FilenameFilter filterSQLHistory = new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.startsWith("SQLHistory.");
      }
    };
    FilenameFilter filterSanitized = new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".sanitized");
      }
    };
    FilenameFilter filterSQLHistoryProto = new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(".sqlhistory");
      }
    };

    try {
      String pathToSQLHistory = args[0];
      String iterationNum = "0";
      if (args.length > 1 ){
        iterationNum = args[1];
	  }
      String outputCSVfilename = "sql_history.csv";
      BufferedWriter writer = new BufferedWriter(new FileWriter(pathToSQLHistory + System.getProperty("file.separator") + outputCSVfilename));
      File directory = new File(pathToSQLHistory);

      if (args.length <= 2) {
        File[] listOfSQLHistoryFiles = directory.listFiles(filterSQLHistory);
        System.out.println("Generating .sqlhistory files");
        int stmtNo = 0;
        for (int i = 0; i < listOfSQLHistoryFiles.length; i++) {
          File file = listOfSQLHistoryFiles[i];
          BufferedReader in =new BufferedReader(new FileReader(file), 1024);
          BufferedWriter writerSanitizedSQLHistory = null;
          String line = "";
          boolean skipWriteToFile = false;
          while ((line = in.readLine()) != null) {
            if (line.contains("auxiliary_data:") || line.contains("</SQLHistory>")) {
              skipWriteToFile = true;
            } else if (line.contains("finished:")) {
              skipWriteToFile = false;
            } else if (line.contains("statement_id: ") || line.contains("<SQLHistory>")) {
              writerSanitizedSQLHistory = new BufferedWriter(new FileWriter(pathToSQLHistory + System.getProperty("file.separator") + Integer.toString(stmtNo) + ".sqlhistory"));
            }

            if (skipWriteToFile == false) {
              String sanitizedline = line.replaceAll("SQLEditor <Scrip", "SQLEditor Scrip");
              sanitizedline = sanitizedline.replaceAll("\t", "");
              sanitizedline = sanitizedline.replaceAll(xml10pattern, "");

              int attrIndex = indexStringContainsItemFromList(sanitizedline, attr);
              if (attrIndex > -1) {
                String value = sanitizedline.substring(sanitizedline.indexOf(attr[attrIndex]) + attr[attrIndex].length());

                if (attrIndex == ORIGINALSQL) {
                  String backendSQLString = processOriginalSQL(writerSanitizedSQLHistory, value, in);
                  attrIndex = BACKENDSQL;
                  value = backendSQLString.substring(backendSQLString.indexOf(attr[attrIndex]) + attr[attrIndex].length());
                }
                value = replaceForProto(value);

                value = value.substring(0, value.length() - 2);
                value = value + "\"";
                sanitizedline = attr[attrIndex] + value;
              }
              sanitizedline = sanitizedline.replaceAll("<SQLHistory>", "");
              sanitizedline = sanitizedline.replaceAll("</SQLHistory>", "");
              writerSanitizedSQLHistory.write(sanitizedline + "\n");
            } //if (skipWriteToFile == false){
            if (line.contains("backend_sql_application_id:") == true) {
              writerSanitizedSQLHistory.close();
              stmtNo += 1;
            }
          } //while ((line = in.readLine()) != null) {
          in .close();
        }
        System.out.println("Generated "+stmtNo+" .sqlhistory files");
      } //args.length==2

      File[] fileList = directory.listFiles((dir, name) ->name.endsWith(".sqlhistory"));
      System.out.println("Generating csv file");
      int numLines=0;
      writer.write("taskID,V5taskID,backendDBSExecTime,totalElapsedTime,backendWaitTime,numResultRows,numResultBytes,fetchTime,entryTimestamp,originalUserID,hash,SQL Text,location,iteration,sqlcode\n");
      for (File file: fileList) {
        SQLStatementDetailsData.Builder SQLStatementDetailsDataMessage = SQLStatementDetailsData.newBuilder();
        String contents = new String(Files.readAllBytes(file.toPath()));
        com.google.protobuf.TextFormat.getParser().merge(contents, SQLStatementDetailsDataMessage);

        SQLStatementDetailsData.Timings timingsData = SQLStatementDetailsDataMessage.getTimings();
        SQLStatementDetailsData.ExecutionResult executionResultData = SQLStatementDetailsDataMessage.getExecutionResult();

        String entryTimestamp = timingsData.getRealWorldEntryTimestamp();
        entryTimestamp = entryTimestamp.replace("T", " ");
        entryTimestamp = entryTimestamp.replace("Z", "");

        String sqlStmtText = SQLStatementDetailsDataMessage.getOriginalSqlStatementText();
        if (sqlStmtText.indexOf(";")>0){
          sqlStmtText = sqlStmtText.substring(0, sqlStmtText.indexOf(";") + 1);
		    }else {
          sqlStmtText = sqlStmtText.substring(0, sqlStmtText.indexOf("backend_sql_statement_text"));
		    }
        sqlStmtText = sqlStmtText.replaceAll(",  ", ",");
        sqlStmtText = sqlStmtText.replaceAll("\"", "");
        sqlStmtText = sqlStmtText.replaceAll("'", "''");
        sqlStmtText = sqlStmtText.replaceAll("\t", " ");
        sqlStmtText = sqlStmtText.replaceAll("\n", "");
        String v5TaskID = "";
        if (sqlStmtText.indexOf("QUERYNO") > -1) {
          v5TaskID = sqlStmtText.substring(sqlStmtText.indexOf("QUERYNO") + 8, sqlStmtText.indexOf(";"));
        }
        writer.write(SQLStatementDetailsDataMessage.getTaskId() + "," + v5TaskID + "," + timingsData.getBackendDBSExecTime() + "," + timingsData.getTotalElapsedTime() + "," + timingsData.getBackendWaitTime() + "," + executionResultData.getNumResultRows() + "," + executionResultData.getNumResultBytes() + "," + timingsData.getFetchTime() + ",'" + entryTimestamp + "','" + SQLStatementDetailsDataMessage.getOriginalUserId() + "','" + SQLStatementDetailsDataMessage.getOriginalSqlStatementTextHash() + "','" + sqlStmtText+ "','"+SQLStatementDetailsDataMessage.getDatabaseSystemLocationName()+"',"+iterationNum+","+executionResultData.getSqlCode()+"\n");
        numLines+=1;
      }
      writer.close();
      System.out.println("Generated csv file with "+numLines+ " query entries");
      System.out.println("Cleaning up .sqlhistory files");
	  File[] listOfsqlhistoryFiles = directory.listFiles(filterSQLHistoryProto);
 	  for (int sqlhistoryFileNum = 0; sqlhistoryFileNum < listOfsqlhistoryFiles.length; sqlhistoryFileNum++) {
 	 	  File sqlhistoryFile = listOfsqlhistoryFiles[sqlhistoryFileNum];
	      sqlhistoryFile.delete();
	  }
	  System.out.println("Cleanup of .sqlhistory files complete");
    } catch(Exception e) {
      e.printStackTrace();
    }

  }
}