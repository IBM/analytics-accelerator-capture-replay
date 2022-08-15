import java.math.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.namespace.QName;
import javax.xml.stream.*;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.InputSource;
import org.xml.sax.helpers.DefaultHandler;

//Generate DSNTIAUL files and a csv file
//java SQLHistoryGetHostvarValues /root/java/folderwithSQLHistoryfiles 1 sanitizeit

//Generate only a csv file
//java SQLHistoryGetHostvarValues /root/java/folderwithSQLHistoryfiles 1 sanitizeit csvonly

public class SQLHistoryGetHostvarValues {

	enum State {
		outsideComment, insideLineComment, insideblockComment, insideblockComment_noNewLineYet, insideString
	};

	private static void generateTIAULFile(List<String> sqlForDSNTIAUL, int sanitizedFileNum, int subSanitizedFileNum,
			String pathToSQLHistory) {

		String dsntiaulHeader = "//RUNTIAUL JOB 'USER=$$USER','<USERNAME:JOBNAME>',CLASS=A,\n"
				+ "//         MSGCLASS=A,MSGLEVEL=(1,1),USER=SYSADM,REGION=4096K,\n"
				+ "//         PASSWORD=xxxxxxxx                                  \n"
				+ "/*ROUTE PRINT STLVM14.CHIHCHAN                                \n"
				+ "//*                                                           \n"
				+ "//*                                                           \n"
				+ "//JOBLIB  DD  DSN=DB2A.SDSNLOAD,DISP=SHR                      \n";
		String dsntiaulStep = "//UNLOAD  EXEC PGM=IKJEFT01,DYNAMNBR=20,COND=(8,LT)            \n"
				+ "//SYSTSPRT  DD  SYSOUT=*                                      \n"
				+ "//SYSTSIN   DD  *                                             \n"
				+ "DSN SYSTEM(DB2A)                                              \n"
				+ "RUN  PROGRAM(DSNTIAUL) PLAN(DSNTIBC1) PARMS('SQL') -          \n"
				+ "LIB('DB2A.TESTLIB')                                           \n"
				+ "//SYSPRINT  DD SYSOUT=*                                       \n"
				+ "//SYSUDUMP  DD SYSOUT=*                                       \n"
				+ "//SYSREC00  DD DUMMY                                          \n"
				+ "//SYSPUNCH DD SYSOUT=*                                        \n"
				+ "//SYSIN     DD *                                              \n"
				+ "SET CURRENT QUERY ACCELERATION=ALL;                           \n"
				+ "SET CURRENT ACCELERATOR=SIM143;                               \n";

		int numOfFiles = (int) Math.ceil((double) sqlForDSNTIAUL.size() / 100);
		int numOfStmts = sqlForDSNTIAUL.size();
		int currentStmtNoSQL = 0;
		try {
			String outputTEP2filename = "S" + sanitizedFileNum + subSanitizedFileNum + ".TIAUL.WIDE";
			BufferedWriter writerTEP2 = new BufferedWriter(
					new FileWriter(pathToSQLHistory + System.getProperty("file.separator") + outputTEP2filename));
			writerTEP2.write(dsntiaulHeader);
			int lastStmtNoInFile = Math.min(currentStmtNoSQL + 100, numOfStmts);
			for (int lineNo = currentStmtNoSQL; lineNo < lastStmtNoInFile; lineNo++) {
				writerTEP2.write(dsntiaulStep);
				writerTEP2.write(sqlForDSNTIAUL.get(lineNo) + "\n");
			}
			currentStmtNoSQL = lastStmtNoInFile;
			writerTEP2.write("//*");
			writerTEP2.close();
			// for Linux only
			String tiaulFilename = "S" + sanitizedFileNum + subSanitizedFileNum + ".TIAUL";
			String command = "fold -w72 -s " + pathToSQLHistory + System.getProperty("file.separator")
					+ outputTEP2filename + " > " + pathToSQLHistory + System.getProperty("file.separator")
					+ tiaulFilename;
			String[] cmd = { "/bin/sh", "-c", command };
			Runtime.getRuntime().exec(cmd).waitFor();
			String unix2doscmd = "unix2dos " + pathToSQLHistory + System.getProperty("file.separator") + tiaulFilename;
			Runtime.getRuntime().exec(unix2doscmd).waitFor();
			Files.deleteIfExists(
					Paths.get(pathToSQLHistory + System.getProperty("file.separator") + outputTEP2filename));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String removeComments(String code) {
		State state = State.outsideComment;
		StringBuilder result = new StringBuilder();
		Scanner s = new Scanner(code);
		s.useDelimiter("");
		while (s.hasNext()) {
			String c = s.next();
			switch (state) {
			case outsideComment:
				if (c.equals("-") && s.hasNext()) {
					String c2 = s.next();
					if (c2.equals("-")) {
						state = State.insideLineComment;
					} else {
						result.append(c).append(c2);
					}
				} else if (c.equals("/") && s.hasNext()) {
					String c2 = s.next();
					if (c2.equals("/"))
						state = State.insideLineComment;
					else if (c2.equals("*")) {
						state = State.insideblockComment_noNewLineYet;
					} else {
						result.append(c).append(c2);
					}
				} else {
					result.append(c);
					if (c.equals("\"")) {
						state = State.insideString;
					}
				}
				break;
			case insideString:
				result.append(c);
				if (c.equals("\"")) {
					state = State.outsideComment;
				} else if (c.equals("\\") && s.hasNext()) {
					result.append(s.next());
				}
				break;
			case insideLineComment:
				if (c.equals("\n")) {
					state = State.outsideComment;
					result.append("\n");
				}
				break;
			case insideblockComment_noNewLineYet:
				if (c.equals("\n")) {
					result.append("\n");
					state = State.insideblockComment;
				}
			case insideblockComment:
				while (c.equals("*") && s.hasNext()) {
					String c2 = s.next();
					if (c2.equals("/")) {
						state = State.outsideComment;
						break;
					}
				}
			}
		}
		s.close();
		return result.toString();
	}

	private static String replacePMWithValues(String clientSQLQuery, String parameterMarkerValues) {
		String newDb2zSQLStmt = new String();
		StringTokenizer pmTokenizer = new StringTokenizer(parameterMarkerValues, ",");

		if (!parameterMarkerValues.equals("N/A")) {
			int numPM = pmTokenizer.countTokens();
			long countPM = clientSQLQuery.chars().filter(ch -> ch == '?').count();
			if (numPM != countPM) {
				throw new IllegalArgumentException("Number of ? in SQL does not match number of parameter marker values in\n parameterMarkerValues="+parameterMarkerValues+"\n in SQLHistory ");
			} else {
				for (int i = 0; i < numPM; i++) {
					String pmVal = pmTokenizer.nextToken();
					clientSQLQuery = clientSQLQuery.replaceFirst("\\?", pmVal);
				}		
			}
		}
		newDb2zSQLStmt = clientSQLQuery;
		return newDb2zSQLStmt;
	}

	private static boolean queryHasDateorTimestampLiteral(String db2zSQLStmt) {
		String dateRegex = "'\\d{4}\\-(0?[1-9]|1[012])\\-(0?[1-9]|[12][0-9]|3[01])*'";
		String timestampRegex = "'\\d{4}\\-(0?[1-9]|1[012])\\-(0?[1-9]|[12][0-9]|3[01])-([0-2][0-9].[0-5][0-9].[0-5][0-9].[0-9]{6})*'";

		Pattern datePattern = Pattern.compile(dateRegex);
		Pattern timestampPattern = Pattern.compile(timestampRegex);
		Matcher dateMatcher = datePattern.matcher(db2zSQLStmt);
		Matcher timestampMatcher = timestampPattern.matcher(db2zSQLStmt);
		return (dateMatcher.find() || timestampMatcher.find());

	}

	public static void main(String[] args) throws IOException {
		int numSQLperTIAUL = 100; // number of SQL statements per DSNTIAUL file, max is 100. CHANGE WITH CAUTION
		BigInteger maxInt = new BigInteger("2147483647");
		String ExcelDelimiter ="\"";
		String defaultDelimiter ="'";
		String xml10pattern = "[^" + "\u0009\r\n" + "\u0020-\uD7FF" + "\uE000-\uFFFD" + "\ud800\udc00-\udbff\udfff"
				+ "]";
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

		try {
			String pathToSQLHistory = args[0];
			String iterationNum = args[1];
			File folder = new File(pathToSQLHistory);
			FilenameFilter filterToUse = null;
			boolean csvOnly = false;
			boolean forExcel = false;
			String sqlDelimiter = defaultDelimiter;

			if (args.length >= 4 && args[3].equals("csvonly")) {
				csvOnly = true;
			}
			if (args.length >= 5 && args[4].equals("forexcel")) {
				forExcel = true;
			}

			if (args.length == 2) {
				filterToUse = filterSQLHistory;
			} else if (args.length >= 3 && args[2].equals("sanitizeit")) { // use this option if the default execution
																			// shows XML parser invalid character errors

				File[] listOfSQLHistoryFiles = folder.listFiles(filterSQLHistory);
				for (int i = 0; i < listOfSQLHistoryFiles.length; i++) {
					File file = listOfSQLHistoryFiles[i];
					String outputSanitizedSQLHistoryfilename = file.getName() + ".sanitized";
					System.out.println("Generating sanitized file " + outputSanitizedSQLHistoryfilename);
					BufferedWriter writerSanitizedSQLHistory = new BufferedWriter(new FileWriter(pathToSQLHistory
							+ System.getProperty("file.separator") + outputSanitizedSQLHistoryfilename));
					BufferedReader in = new BufferedReader(new FileReader(file), 1024);
					String line = "";
					while ((line = in.readLine()) != null) {
						String sanitizedline = line.replaceAll("SQLEditor <", "SQLEditor ");
						sanitizedline = sanitizedline.replaceAll(xml10pattern, "");
						sanitizedline = sanitizedline.replaceAll("[^\\x00-\\x7F]", "");
						sanitizedline = sanitizedline.replaceAll("\"'\"", "\"");
						if (sanitizedline.contains("correlationToken") == false
								&& sanitizedline.contains("clientApplication") == false
								&& sanitizedline.contains("clientAccounting") == false
								&& sanitizedline.contains("externalName") == false
								&& sanitizedline.contains("CLIENT ACCTNG") == false
								&& sanitizedline.contains("CLIENT CORR_TOKEN") == false
								&& sanitizedline.contains("CURRENT CLIENT APPLNAME") == false) {
							writerSanitizedSQLHistory.write(sanitizedline + "\n");
						} else if (sanitizedline.contains("parameterMarkerValues") == true) // parameter marker value
																							// could be %A & %B, the &
																							// character messes up the
																							// xml parser, ok to ignore
																							// here
						{
							writerSanitizedSQLHistory.write(">\n");
						}
					}
					in.close();
					writerSanitizedSQLHistory.close();
				}
				filterToUse = filterSanitized;
			} // else

			File[] listOfSanitizedFiles = folder.listFiles(filterToUse);
			for (int sanitizedFileNum = 0; sanitizedFileNum < listOfSanitizedFiles.length; sanitizedFileNum++) {
				File sanitizedFile = listOfSanitizedFiles[sanitizedFileNum];
				System.out.println("Processing " + sanitizedFile.getName());
				String outputCSVfilename = sanitizedFile.getName() + ".csv";
				outputCSVfilename = outputCSVfilename.replaceAll("SQLHistory", "SQLSummary");
				outputCSVfilename = outputCSVfilename.replaceAll(".sanitized", "");
				BufferedWriter writer = new BufferedWriter(
						new FileWriter(pathToSQLHistory + System.getProperty("file.separator") + outputCSVfilename));
				FileInputStream fis = null;
				List<String> sqlForDSNTIAUL = new ArrayList<String>();
				int subSanitizedFileNum = 0;

				fis = new FileInputStream(sanitizedFile);
				XMLInputFactory xmlInFact = XMLInputFactory.newInstance();
				XMLStreamReader reader = xmlInFact.createXMLStreamReader(fis);
				String queryno = new String();
				String mSummaryString = new String();
				String location = new String();
				String finishState = new String();
				String clientUserID = new String();
				String parameterMarkerValues = new String();
				String clientSQLQuery = new String();
				String sqlcode = new String();
				String taskID = new String();
				while (reader.hasNext()) {
					switch (reader.next()) {
					case XMLStreamConstants.START_ELEMENT:
						if (reader.getLocalName().equals("SQLStatementDetails")) {
							if (reader.getAttributeCount() > 0) {
								taskID = reader.getAttributeValue(null, "taskID");
								String entryTimestamp = reader.getAttributeValue(null, "entryTimestamp");
								entryTimestamp = entryTimestamp.replace("T", " ");
								entryTimestamp = entryTimestamp.replace("Z", "");
								String totalWaitTime = reader.getAttributeValue(null, "totalWaitTime");
								String fetchTime = reader.getAttributeValue(null, "fetchTime");
								String netezzaExecTime = reader.getAttributeValue(null, "netezzaExecTime");
								String totalElapsedTime = reader.getAttributeValue(null, "totalElapsedTime");
								String originalUserID = reader.getAttributeValue(null, "originalUserID");
								String numResultRows = reader.getAttributeValue(null, "numResultRows");
								String numResultBytes = reader.getAttributeValue(null, "numResultBytes");
								location = reader.getAttributeValue(null, "dbmsIdentifier");
								finishState = reader.getAttributeValue(null, "finishState");
								clientUserID = reader.getAttributeValue(null, "clientUserID");
								sqlcode = reader.getAttributeValue(null, "sqlcode");

								BigInteger taskID_bigint = new BigInteger(taskID);
								Integer taskID_int = -1;
								if (taskID_bigint.compareTo(maxInt) == 1) {
									taskID_int = taskID_bigint.subtract(maxInt).intValue();
									queryno = Integer.toString(taskID_int);
								} else {
									queryno = taskID;
								}
								mSummaryString = taskID + "," + netezzaExecTime + "," + totalElapsedTime + ","
										+ totalWaitTime + "," + numResultRows + "," + numResultBytes + "," + fetchTime
										+ ",'" + entryTimestamp + "','" + originalUserID + "',";
							} // if(reader.getAttributeCount()
						} else if (reader.getLocalName().equals("SQLStatementInstance")) {
							if (reader.getAttributeCount() > 0) {
								parameterMarkerValues = reader.getAttributeValue(null, "parameterMarkerValues");
								String newDb2zSQLStmt = replacePMWithValues(clientSQLQuery, parameterMarkerValues);
								String db2zSQLStmt = newDb2zSQLStmt.replaceAll("\t", "        ");

								Pattern patternWithUR = Pattern.compile("WITH\\s*UR", Pattern.CASE_INSENSITIVE);
								Pattern patternNewlineWithUR = Pattern.compile("\nWITH\\s*UR",
										Pattern.CASE_INSENSITIVE);
								if (patternWithUR.matcher(db2zSQLStmt).find() == false) {
									sqlForDSNTIAUL.add(db2zSQLStmt + "\nWITH UR QUERYNO " + queryno + ";");
								} else {
									if (patternNewlineWithUR.matcher(db2zSQLStmt).find() == false) {
										db2zSQLStmt = db2zSQLStmt.replaceAll("WITH UR", "\nWITH UR\n");
									}
									sqlForDSNTIAUL.add(db2zSQLStmt + "\nQUERYNO " + queryno + ";");
								}										
							}
						} else if (reader.getLocalName().equals("ClientSQLQuery")) {
							if (reader.getAttributeCount() > 0) {
								String hash = reader.getAttributeValue(null, "hash");
								mSummaryString += "'" + hash + "',";
							} // hash
							try {
								int clientSQLQueryEventType = reader.next();

								if (clientSQLQueryEventType == XMLStreamConstants.CHARACTERS
										|| clientSQLQueryEventType == XMLStreamConstants.CDATA) {
									clientSQLQuery = removeComments(reader.getText());
								}

								if (clientSQLQueryEventType == XMLStreamConstants.CHARACTERS
										|| clientSQLQueryEventType == XMLStreamConstants.CDATA) {
									String sqlStmt = reader.getText();
									sqlStmt = sqlStmt.replaceAll(",  ", ",");
									sqlStmt = sqlStmt.replaceAll("\"", "");
									if (forExcel == false){
										sqlStmt = sqlStmt.replaceAll("'", "''");
									}else{
										sqlDelimiter = ExcelDelimiter;
									}
									sqlStmt = sqlStmt.replaceAll("\t", " ");
									sqlStmt = sqlStmt.replaceAll("(\r|\n)", "");
									mSummaryString += sqlDelimiter + sqlStmt + sqlDelimiter+",'" + location + "'," + iterationNum + ","
											+ sqlcode;
									writer.write(mSummaryString + "\n");
								}
							} catch (XMLStreamException e) {
								System.out.println("Error processing taskID " + taskID);
								System.out.println(e.getMessage() + " at " + reader.getLocation().getLineNumber() + ":"
										+ reader.getLocation().getColumnNumber());
							}
						} 
						break;
					default:
						break;
					}// switch ( reader.next()
					if (!csvOnly && sqlForDSNTIAUL.size() == numSQLperTIAUL) {
						generateTIAULFile(sqlForDSNTIAUL, sanitizedFileNum, subSanitizedFileNum, pathToSQLHistory);
						sqlForDSNTIAUL = new ArrayList<String>();
						subSanitizedFileNum += 1;
					} // if (sqlForDSNTIAUL){
				} // while(reader.hasNext())
				reader.close();
				if (!csvOnly && sqlForDSNTIAUL.size() > 0) {
					generateTIAULFile(sqlForDSNTIAUL, sanitizedFileNum, subSanitizedFileNum, pathToSQLHistory);
				}
				System.out.println("Finished processing " + sanitizedFile.getName());
				writer.close();
				if (args.length >= 3 && args[2].equals("sanitizeit")) {
					sanitizedFile.delete();
				}
			} // for (int i = 0; i < listOfSanitizedFiles.length; i++) {
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
