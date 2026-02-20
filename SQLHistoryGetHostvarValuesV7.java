import com.google.protobuf.TextFormat;
import java.math.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.IDAA.SQLFormat.SQLStatementDetails.SQLStatementDetailsData;
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
//java SQLHistoryGetHostvarValuesV7 /root/java/folderwithSQLHistoryfiles 1 sanitizeit

//Generate only a csv file
//java SQLHistoryGetHostvarValuesV7 /root/java/folderwithSQLHistoryfiles 1 sanitizeit csvonly

public class SQLHistoryGetHostvarValuesV7 {

	static final int ORIGINALSQL = 0;

	enum State {
		outsideComment,
		insideString,
		insideLineComment,
		insideBlockComment
	};

	private static File[] getAllFilesSortedByName(Path dir, FilenameFilter filter) throws IOException {
		List<Path> paths = Files.walk(dir)
				.filter(Files::isRegularFile)
				.filter(p -> {
					if (filter == null)
						return true;
					File f = p.toFile();
					return filter.accept(f.getParentFile(), f.getName());
				})
				.sorted(Comparator.comparing(
						p -> p.getFileName().toString(),
						String.CASE_INSENSITIVE_ORDER))
				.collect(Collectors.toList());

		return paths.stream()
				.map(Path::toFile)
				.toArray(File[]::new);
	}

	private static File[] getAllFilesSortedByName(File dir, FilenameFilter filter) throws IOException {
		return getAllFilesSortedByName(dir.toPath(), filter);
	}

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
				+ "RUN  PROGRAM(DSNTIAUL) PLAN(DSNTIBD1) PARMS('SQL') -          \n"
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
			String outputTEP2filename = "S" + sanitizedFileNum + subSanitizedFileNum + ".TIAUL";
			BufferedWriter writerTEP2 = new BufferedWriter(
					new FileWriter(pathToSQLHistory + System.getProperty("file.separator") + outputTEP2filename));
			writerTEP2.write(dsntiaulHeader);
			int lastStmtNoInFile = Math.min(currentStmtNoSQL + 100, numOfStmts);
			for (int lineNo = currentStmtNoSQL; lineNo < lastStmtNoInFile; lineNo++) {
				writerTEP2.write(dsntiaulStep);
				writerTEP2.write(sqlForDSNTIAUL.get(lineNo) + "\n");
			}
			currentStmtNoSQL = lastStmtNoInFile;
			writerTEP2.write("//*\n");
			writerTEP2.write("//* Path: " + pathToSQLHistory + "\n");
			writerTEP2.write("//* File: " + outputTEP2filename + "\n");
			writerTEP2.write("//*-+----1----+----2----+----3----+----4----+----5----+----6----+----7----+----8");
			writerTEP2.close();
			// for Linux only
			String unix2doscmd = "unix2dos " + pathToSQLHistory + System.getProperty("file.separator")
					+ outputTEP2filename;
			Runtime.getRuntime().exec(unix2doscmd).waitFor();
			System.out.println("Generated " + outputTEP2filename);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String removeComments(String code) {
		StringBuilder result = new StringBuilder();
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;

		int i = 0;
		while (i < code.length()) {
			char c = code.charAt(i);

			// Toggle single-quoted string literal
			if (c == '\'' && !inDoubleQuote) {
				result.append(c);
				inSingleQuote = !inSingleQuote;
				i++;
				continue;
			}

			// Toggle double-quoted identifier
			if (c == '"' && !inSingleQuote) {
				result.append(c);
				inDoubleQuote = !inDoubleQuote;
				i++;
				continue;
			}

			// Only check for comments when outside literals
			if (!inSingleQuote && !inDoubleQuote) {
				// Hybrid: -- /* ... */, still a line comment
				if (code.startsWith("--", i)) {
					int j = i + 2;
					while (j < code.length() && Character.isWhitespace(code.charAt(j)))
						j++;
					if (j < code.length() && code.startsWith("/*", j)) {
						int end = code.indexOf("\n", j + 2);
						if (end == -1)
							return result.toString(); // unterminated
						i = end + 2;
						continue;
					}
				}

				// Block comment /* ... */
				if (code.startsWith("/*", i)) {
					int end = code.indexOf("*/", i + 2);
					if (end == -1)
						return result.toString(); // unterminated
					i = end + 2;
					continue;
				}

				// Line comment -- ...
				if (code.startsWith("--", i)) {
					int end = code.indexOf("\n", i + 2);
					if (end == -1)
						return result.toString(); // to end of string
					i = end; // keep newline
					continue;
				}
			}

			// Normal character
			result.append(c);
			i++;
		}

		return result.toString();
	}

	private static String replacePMWithValues(String clientSQLQuery, String parameterMarkerValues) {
		String newDb2zSQLStmt = new String();

		if (parameterMarkerValues != null && !parameterMarkerValues.equals("N/A")) {
			parameterMarkerValues = parameterMarkerValues.replaceAll(",\"$", ""); // remove any trailing comma
			String[] pmTokens = parameterMarkerValues.split(",(?=(?:[^']*'[^']*')*[^']*$)");
			int numPM = pmTokens.length;
			long countPM = clientSQLQuery.chars().filter(ch -> ch == '?').count();
			if (numPM != countPM) {
				throw new IllegalArgumentException(
						"Number of ? in SQL "+countPM+" does not match number of parameter marker values "+numPM+" in\n parameterMarkerValues="
								+ parameterMarkerValues + "\n in SQLHistory");
			} else {
				for (String pmVal:pmTokens ) {
					clientSQLQuery = clientSQLQuery.replaceFirst("\\?", pmVal.trim());
				}
			}
		}
		newDb2zSQLStmt = clientSQLQuery;
		return newDb2zSQLStmt;
	}

	private static String formatSQLForDSNTIAUL(String originalSqlStmtText) {

		boolean isDMLStatement = false;

		originalSqlStmtText = originalSqlStmtText.trim();

		String prefix = originalSqlStmtText.substring(0, Math.min(6, originalSqlStmtText.length())).toUpperCase();
		if ((prefix.equals("INSERT") || prefix.equals("DELETE") || prefix.equals("UPDATE"))
				&& (prefix.length() == 6 || !Character.isLetter(prefix.charAt(6)))) {
			isDMLStatement = true;
		}

		if (originalSqlStmtText.endsWith(";")) {
			originalSqlStmtText = originalSqlStmtText.substring(0, originalSqlStmtText.length() - 1).trim();
		}

		StringBuilder formattedSql = new StringBuilder();
		int currentLineLength = 0;
		boolean spaceNext = false;

		Pattern pattern = Pattern.compile(
				"[Xx]\\s*'([0-9A-Fa-f]+)'" + // hex literal
						"|'([^']|'')*'" + // single-quoted string literals
						"|\"([^\"]|\"\")*\"" + // double-quoted identifiers
						"|\\|\\|" + // ||
						"|<=|>=|<>|!=|\\^=" + // multi-char operators
						"|\\d*\\.\\d+([Ee][+-]?\\d+)?" + // decimals
						"|\\d+\\.([Ee][+-]?\\d+)?" + // numbers ending with dot
						"|\\d+" + // integers
						"|[A-Za-z_][A-Za-z0-9_]*\\.\\*" + // table.* pattern
						"|[(),=<>]" + // single-char operators/punctuation
						"|[+\\-*/]" + // math operators
						"|\\." + // dot as separate punctuation
						"|[^\\s(),=<>|!+'\"*\\-/]+" // words/identifiers
		);

		Matcher matcher = pattern.matcher(originalSqlStmtText);
		List<String> tokens = new ArrayList<>();
		while (matcher.find()) {
			tokens.add(matcher.group());
		}

		for (String token : tokens) {
			token = token.replaceFirst("(?i:X)\\s*'", "X'");
			boolean isHex = token.startsWith("X'");
			boolean isLiteral = token.startsWith("'") || isHex;

			boolean isOperator = token.equals("||") || token.equals("<>") || token.equals("!=") || token.equals("^=")
					|| token.equals("<=") || token.equals(">=") || token.equals("<")
					|| token.equals(">") || token.equals("=")
					|| token.equals("+") || token.equals("-")
					|| token.equals("*") || token.equals("/");

			// Wrap long literals (string and hex)
			if (isLiteral && token.length() > 72) {
				String content = token.substring(1, token.length() - 1);
				String chunkPrefix = "'";
				boolean addConcat = false;

				if (isHex) {
					int quoteIndex = token.indexOf("'");
					chunkPrefix = "X'";
					content = token.substring(quoteIndex + 1, token.length() - 1);
					addConcat = true;
				}

				int pos = 0;
				int lineMaxLength = 72;

				while (pos < content.length()) {
					int remaining = content.length() - pos;
					int chunkSize;

					if (isHex) {
						if (remaining + chunkPrefix.length() + 1 <= lineMaxLength) {
							chunkSize = remaining;
						} else {
							chunkSize = lineMaxLength - chunkPrefix.length() - 1 - 3; // leave room for " ||"
							if (chunkSize % 2 != 0)
								chunkSize--;
							if (chunkSize <= 0)
								chunkSize = 2;
						}
					} else {
						if (remaining + chunkPrefix.length() + 1 <= lineMaxLength) {
							chunkSize = remaining;
						} else {
							chunkSize = lineMaxLength - chunkPrefix.length() - 1;
							if (chunkSize <= 0)
								chunkSize = 1;
						}
					}

					String chunk = content.substring(pos, pos + chunkSize);
					String chunkStr = chunkPrefix + chunk + "'";
					if (addConcat && pos + chunkSize < content.length()) {
						chunkStr += " ||";
					}

					if (currentLineLength + chunkStr.length() > 72) {
						formattedSql.append("\n");
						currentLineLength = 0;
					}

					formattedSql.append(chunkStr);
					currentLineLength += chunkStr.length();
					pos += chunkSize;

					if (pos < content.length()) {
						formattedSql.append("\n");
						currentLineLength = 0;
					}
				}
				spaceNext = true;
				continue;
			}

			// Handle operators with spacing
			if (isOperator && formattedSql.length() > 0 && formattedSql.charAt(formattedSql.length() - 1) != ' ') {
				if (currentLineLength + token.length() > 71) {
					formattedSql.append("\n");
					currentLineLength = 0;
				}
				formattedSql.append(" ");
				currentLineLength++;
				formattedSql.append(token);
				currentLineLength += token.length();
				spaceNext = true;
				continue;
			}

			// Line wrapping for normal tokens
			if (currentLineLength + token.length() + (spaceNext ? 1 : 0) > 71) {
				formattedSql.append("\n");
				currentLineLength = 0;
			}

			if (spaceNext && !token.equals(",") && !token.equals(")") && !token.equals("(") && !token.equals(".")
					&& !(token.length() > 0 && token.charAt(0) == '.')) {
				formattedSql.append(" ");
				currentLineLength++;
			}

			formattedSql.append(token);
			currentLineLength += token.length();

			if (token.equals("(") || token.equals(")") || token.equals(",") || token.equals(".")) {
				spaceNext = false;
			} else {
				spaceNext = true;
			}
		}

		// Append WITH UR if missing
		String normalizedOriginal = originalSqlStmtText.replaceAll("\\s+", " ").trim();
		boolean hasWithUr = normalizedOriginal.matches("(?i).*\\bWITH\\s+UR\\b.*;?\\s*$");
		boolean hasForFetchOnly = normalizedOriginal.matches("(?i).*\\bFOR\\s+FETCH\\s+ONLY\\b.*$");

		if (!isDMLStatement && !hasWithUr && !hasForFetchOnly) {
			if (currentLineLength != 0)
				formattedSql.append("\n");
			formattedSql.append("WITH UR");
		}

		return formattedSql.toString().trim();
	}

	private static String escapeStringLiterals(String text) {
		StringBuilder out = new StringBuilder(text.length() + 32);
		boolean inString = false;
		char quote = 0;
		boolean prevBackslash = false;

		for (int i = 0; i < text.length(); i++) {
			char c = text.charAt(i);

			if (!inString) {
				out.append(c);
				if (c == '"' || c == '\'') {
					inString = true;
					quote = c;
					prevBackslash = false;
				}
			} else {
				if (prevBackslash) {
					out.append(c);
					prevBackslash = false;
				} else if (c == '\\') {
					out.append('\\');
					prevBackslash = true;
				} else if (c == quote) {
					out.append(c);
					inString = false;
				} else if (c == '\n') {
					out.append("\\n");
				} else if (c == '\r') {
					out.append("\\r");
				} else if (c == '\t') {
					out.append("\\t");
				} else {
					out.append(c);
				}
			}
		}
		return out.toString();
	}

	private static String addSpaceBeforeQuoteOutsideLiterals(String sql) {
		StringBuilder result = new StringBuilder();
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;

		int i = 0;
		while (i < sql.length()) {
			char c = sql.charAt(i);

			// Handle single-quoted literals
			if (c == '\'' && !inDoubleQuote) {
				// If previous char was part of a word/identifier, add space
				if (!inSingleQuote && result.length() > 0) {
					char prev = result.charAt(result.length() - 1);
					if (Character.isLetterOrDigit(prev) || prev == '_') {
						result.append(' ');
					}
				}

				result.append(c);

				// Handle escaped single quote ('')
				if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
					result.append('\'');
					i += 2;
					continue;
				} else {
					inSingleQuote = !inSingleQuote;
				}
			}
			// Handle double-quoted identifiers
			else if (c == '"' && !inSingleQuote) {
				result.append(c);
				inDoubleQuote = !inDoubleQuote;
			}
			// Normal character
			else {
				result.append(c);
			}

			i++;
		}

		return result.toString();
	}

	private static String removeExtraSpacesAroundPunctuation(String sql, char punctuationChar) {
		StringBuilder result = new StringBuilder();
		boolean inSingleQuote = false;
		boolean inDoubleQuote = false;

		int i = 0;
		while (i < sql.length()) {
			char c = sql.charAt(i);

			// Handle single-quoted literals
			if (c == '\'' && !inDoubleQuote) {
				result.append(c);
				if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') { // escaped ''
					result.append('\'');
					i += 2;
					continue;
				} else {
					inSingleQuote = !inSingleQuote;
				}
			}
			// Handle double-quoted identifiers
			else if (c == '"' && !inSingleQuote) {
				result.append(c);
				inDoubleQuote = !inDoubleQuote;
			}
			// Handle punctuation outside quotes
			else if (!inSingleQuote && !inDoubleQuote && c == punctuationChar) {
				boolean isNumeric = false;

				if (punctuationChar == '.') {
					char prev = (result.length() > 0) ? result.charAt(result.length() - 1) : '\0';
					char next = (i + 1 < sql.length()) ? sql.charAt(i + 1) : '\0';

					boolean prevIsDigit = Character.isDigit(prev);
					boolean nextIsDigit = Character.isDigit(next);

					// Rule 1: standard decimal like 123.456
					if (prevIsDigit && nextIsDigit) {
						isNumeric = true;
					}
					// Rule 2: leading decimal like .999
					else if (!prevIsDigit && nextIsDigit) {
						isNumeric = true;
					}
					// Rule 3: scientific notation like 1.23E+4
					else if (prevIsDigit && (next == 'E' || next == 'e')) {
						isNumeric = true;
					}
				}

				if (!isNumeric) {
					// Remove spaces immediately before punctuation
					while (result.length() > 0 && result.charAt(result.length() - 1) == ' ') {
						result.deleteCharAt(result.length() - 1);
					}

					result.append(punctuationChar);

					// Skip spaces immediately after punctuation
					i++;
					while (i < sql.length() && sql.charAt(i) == ' ') {
						i++;
					}
					continue;
				} else {
					result.append(c); // decimal or numeric point, just append
				}
			}
			// Normal character
			else {
				result.append(c);
			}

			i++;
		}

		return result.toString();
	}

	public static void main(String[] args) throws IOException {
		int numSQLperTIAUL = 100; // number of SQL statements per DSNTIAUL file, max is 100. CHANGE WITH CAUTION
		Long maxInt = 2147483647L;
		String ExcelDelimiter = "\"";
		String defaultDelimiter = "'";
		String xml10pattern = "[^" + "\u0009\r\n" + "\u0020-\uD7FF" + "\uE000-\uFFFD" + "\ud800\udc00-\udbff\udfff"
				+ "]";
		FilenameFilter filterSQLHistory = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith(".sqlhistory");
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
				// File[] listOfSQLHistoryFiles = folder.listFiles(filterSQLHistory);
				File[] listOfSQLHistoryFiles = getAllFilesSortedByName(folder, filterSQLHistory);
				for (int i = 0; i < listOfSQLHistoryFiles.length; i++) {
					File file = listOfSQLHistoryFiles[i];
					String outputSanitizedSQLHistoryfilename = file.getName() + ".sanitized";
					System.out.println("file " + file.getName());
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

			// File[] listOfSanitizedFiles = folder.listFiles(filterToUse);
			File[] listOfSanitizedFiles = getAllFilesSortedByName(folder, filterToUse);
			BufferedWriter writer = new BufferedWriter(
					new FileWriter(
							pathToSQLHistory + System.getProperty("file.separator") + "sql_history.captured.csv"));

			int numLines = 0;
			for (int sanitizedFileNum = 0; sanitizedFileNum < listOfSanitizedFiles.length; sanitizedFileNum++) {
				File sanitizedFile = listOfSanitizedFiles[sanitizedFileNum];
				System.out.println("Processing " + sanitizedFile.getName());

				SQLStatementDetailsData.Builder SQLStatementDetailsDataMessage = SQLStatementDetailsData.newBuilder();
				String contents = new String(Files.readAllBytes(sanitizedFile.toPath()));
				contents = escapeStringLiterals(contents);
				try {
					com.google.protobuf.TextFormat.getParser().merge(contents, SQLStatementDetailsDataMessage);
				} catch (Exception e) {
					System.out.println("Error processing " + sanitizedFile.getName());
					System.out.println(contents);
					e.printStackTrace();
				}
				SQLStatementDetailsData.Timings timingsData = SQLStatementDetailsDataMessage.getTimings();
				SQLStatementDetailsData.ExecutionResult executionResultData = SQLStatementDetailsDataMessage
						.getExecutionResult();

				XMLInputFactory xmlInFact = XMLInputFactory.newInstance();
				XMLStreamReader reader = xmlInFact
						.createXMLStreamReader(new StringReader(SQLStatementDetailsDataMessage.getAuxiliaryData()));

				List<String> sqlForDSNTIAUL = new ArrayList<String>();
				int subSanitizedFileNum = 0;

				String queryno = new String();
				String mSummaryString = new String();
				String location = new String();
				String finishState = new String();
				String clientUserID = new String();
				String parameterMarkerValues = new String();
				String originalSqlStmtText = SQLStatementDetailsDataMessage.getOriginalSqlStatementText();
				String sqlcode = new String();
				long taskID = SQLStatementDetailsDataMessage.getTaskId();
				String entryTimestamp = timingsData.getRealWorldEntryTimestamp();
				entryTimestamp = entryTimestamp.replace("T", " ");
				entryTimestamp = entryTimestamp.replace("Z", "");

				Long taskID_long = taskID;
				if (taskID_long.compareTo(maxInt) == 1) {
					taskID_long = (taskID_long - maxInt);
					queryno = Integer.toString(taskID_long.intValue());
				} else {
					queryno = Long.toString(taskID);
				}

				while (reader.hasNext()) {
					switch (reader.next()) {
						case XMLStreamConstants.START_ELEMENT:
							if (reader.getLocalName().equals("SQLStatementInstance")) {
								if (reader.getAttributeCount() > 0) {
									parameterMarkerValues = reader.getAttributeValue(null, "parameterMarkerValues");
								}
							}
							break;
						default:
							break;
					}// switch ( reader.next()
				} // while(reader.hasNext())

				originalSqlStmtText = originalSqlStmtText.replace('\u00A0', ' ');
				originalSqlStmtText = removeComments(originalSqlStmtText);
				originalSqlStmtText = removeExtraSpacesAroundPunctuation(originalSqlStmtText, '.');
				originalSqlStmtText = removeExtraSpacesAroundPunctuation(originalSqlStmtText, ',');
				originalSqlStmtText = addSpaceBeforeQuoteOutsideLiterals(originalSqlStmtText);
				originalSqlStmtText = replacePMWithValues(originalSqlStmtText, parameterMarkerValues);
				originalSqlStmtText = formatSQLForDSNTIAUL(originalSqlStmtText);
				sqlForDSNTIAUL.add(originalSqlStmtText + "\nQUERYNO " + queryno + ";");
				String v5TaskID = "";
				if (originalSqlStmtText.indexOf("QUERYNO") > -1) {
					v5TaskID = originalSqlStmtText.substring(originalSqlStmtText.indexOf("QUERYNO") + 8,
							originalSqlStmtText.indexOf(";"));
				}
				writer.write(SQLStatementDetailsDataMessage.getTaskId() + "," + v5TaskID + ","
						+ timingsData.getBackendDBSExecTime() + "," + timingsData.getTotalElapsedTime() + ","
						+ timingsData.getBackendWaitTime() + "," + executionResultData.getNumResultRows() + ","
						+ executionResultData.getNumResultBytes() + "," + timingsData.getFetchTime() + ",'"
						+ entryTimestamp + "','"
						+ SQLStatementDetailsDataMessage.getOriginalUserId() + "','"
						+ SQLStatementDetailsDataMessage.getOriginalSqlStatementTextHash() + "'," + sqlDelimiter
						+ originalSqlStmtText
						+ sqlDelimiter + ",'" + SQLStatementDetailsDataMessage.getDatabaseSystemLocationName() + "',"
						+ iterationNum
						+ "," + executionResultData.getSqlCode() + "\n");
				numLines += 1;

				if (!csvOnly && sqlForDSNTIAUL.size() > 0) {
					generateTIAULFile(sqlForDSNTIAUL, sanitizedFileNum, subSanitizedFileNum, pathToSQLHistory);
				}
				System.out.println("Finished processing " + sanitizedFile.getName());
				if (args.length >= 3 && args[2].equals("sanitizeit")) {
					sanitizedFile.delete();
				}
			} // for (int i = 0; i < listOfSanitizedFiles.length; i++) {
			System.out.println("Generated csv file with " + numLines + " query entries");
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

