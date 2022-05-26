/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.UpdateApplicationCatalog;
import org.voltdb.compiler.DDLParserCallback;
import org.voltdb.parser.SQLLexer;
import org.voltdb.parser.SQLParser;
import org.voltdb.parser.SQLParser.FileInfo;
import org.voltdb.parser.SQLParser.FileOption;
import org.voltdb.parser.SQLParser.ParseRecallResults;

import com.google_voltpatches.common.collect.ImmutableMap;

import jline.console.CursorBuffer;
import jline.console.KeyMap;
import jline.console.history.FileHistory;

public class SQLCommand {
    private static boolean m_stopOnError = true;
    private static boolean m_debug = false;
    private static boolean m_interactive;
    private static boolean m_versionCheck = true;
    private static boolean m_returningToPromptAfterError = false;
    private static int m_exitCode = 0;

    private static boolean m_hasBatchTimeout = true;
    private static int m_batchTimeout = BatchTimeoutOverrideType.DEFAULT_TIMEOUT;

    private static final String m_readme = "SQLCommandReadme.txt";

    public static String getReadme() {
        return m_readme;
    }

    private static List<String> RecallableSessionLines = new ArrayList<>();
    private static boolean m_testFrontEndOnly;
    private static String m_testFrontEndResult;

    // Special exception type to inform main to return m_exitCode and stop SQLCmd
    @SuppressWarnings("serial")
    private static class SQLCmdEarlyExitException extends RuntimeException {}

    private static String patchErrorMessageWithFile(String batchFileName, String message) {

        Pattern errorMessageFilePrefix = Pattern.compile("\\[.*:([0-9]+)\\]");

        Matcher matcher = errorMessageFilePrefix.matcher(message);
        if (matcher.find()) {
            // This won't work right if the filename contains a "$"...
            message = matcher.replaceFirst("[" + batchFileName + ":$1]");
        }
        return message;
    }

    private static ClientResponse callProcedureHelper(String procName, Object... parameters)
            throws IOException, ProcCallException {
        final ClientResponse response;
        if (m_hasBatchTimeout) {
            response = m_client.callProcedureWithTimeout(m_batchTimeout, procName, parameters);
        } else {
            response = m_client.callProcedure(procName, parameters);
        }
        return response;
    }

    private static void executeDDLBatch(
            String batchFileName, String statements, DDLParserCallback callback, int batchEndLineNumber) {
        try {
            if (callback != null) {
                callback.batch(statements, batchEndLineNumber);
                return;
            }

            if ( ! m_interactive ) {
                System.out.println();
                System.out.println(statements);
            }

            if (! SQLParser.appearsToBeValidDDLBatch(statements)) {
                throw new Exception("Error: This batch begins with a non-DDL statement.  "
                        + "Currently batching is only supported for DDL.");
            }


            if (m_testFrontEndOnly) {
                m_testFrontEndResult += statements;
                return;
            }
            ClientResponse response = m_client.callProcedure("@AdHoc", statements);

            if (response.getStatus() != ClientResponse.SUCCESS) {
                throw new Exception("Execution Error: " + response.getStatusString());
            }
            // Assert the current DDL AdHoc batch call behavior
            assert(response.getResults().length == 1);
            System.out.println("Batch command succeeded.");
            loadStoredProcedures();
        } catch (ProcCallException ex) {
            String fixedMessage = patchErrorMessageWithFile(batchFileName, ex.getMessage());
            stopOrContinue(new Exception(fixedMessage));
        } catch (Exception ex) {
            stopOrContinue(ex);
        }
    }

    // The main loop for interactive mode.
    private static void interactWithTheUser() throws Exception {
        final SQLConsoleReader interactiveReader =
                new SQLConsoleReader(new FileInputStream(FileDescriptor.in), System.out);
        interactiveReader.setBellEnabled(false);
        FileHistory historyFile = null;
        try {
            // Maintain persistent history in ~/.sqlcmd_history.
            historyFile = new FileHistory(new File(System.getProperty("user.home"), ".sqlcmd_history"));
            interactiveReader.setHistory(historyFile);

            // Make Ctrl-D (EOF) exit if on an empty line, otherwise delete the next character.
            KeyMap keyMap = interactiveReader.getKeys();
            keyMap.bind(Character.toString(KeyMap.CTRL_D), new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    CursorBuffer cursorBuffer = interactiveReader.getCursorBuffer();
                    if (cursorBuffer.length() == 0) {
                        // tells caller to stop (basically a goto)
                        throw new SQLCmdEarlyExitException();
                    } else {
                        try {
                            interactiveReader.delete();
                        } catch (IOException ignored) {
                        }
                    }
                }
            });

            getInteractiveQueries(interactiveReader);
        } finally {
            // Flush input history to a file.
            if (historyFile != null) {
                try {
                    historyFile.flush();
                } catch (IOException e) {
                    System.err.printf("* Unable to write history to \"%s\" *\n", historyFile.getFile().getPath());
                    if (m_debug) {
                        e.printStackTrace();
                    }
                }
            }
            // Clean up jline2 resources.
            if (interactiveReader != null) {
                interactiveReader.shutdown();
            }
        }
    }

    private static void getInteractiveQueries(SQLConsoleReader interactiveReader) throws Exception {
        // Reset the error state to avoid accidentally ignoring future FILE content
        // after a file had runtime errors (ENG-7335).
        m_returningToPromptAfterError = false;
        StringBuilder statement = new StringBuilder();
        boolean isRecall = false;

        while (true) {
            String stmtContinuationStr = statement.length() > 0 ? "  " : "";
            String prompt = isRecall ? "" : (stmtContinuationStr + (RecallableSessionLines.size() + 1) + "> ");
            isRecall = false;
            String line = interactiveReader.readLine(prompt);
            if (line == null) {
                // This used to occur in an edge case when trying to pipe an
                // empty file into stdin and ending up in interactive mode by
                // mistake. That case works differently now, so this code path
                // MAY be dead. If not, cut our losses by rigging a quick exit.
                statement.setLength(0);
                line = "EXIT;";
            }

            // Was there a line-ending semicolon typed at the prompt?
            // This mostly matters for "non-directive" statements.
            boolean executeImmediate = SQLParser.isSemiColonTerminated(line);


            // When we are tracking the progress of a multi-line statement,
            // avoid coincidentally recognizing mid-statement SQL content as sqlcmd
            // "directives".
            if (statement.length() == 0) {

                if (line.trim().equals("") || SQLParser.isWholeLineComment(line)) {
                    // We don't strictly have to execute or append or recall
                    // a blank line or whole-line comment when no statement is in progress.
                    continue;
                }

                // EXIT command - exit immediately
                if (SQLParser.isExitCommand(line)) {
                    return;
                }

                // RECALL command
                ParseRecallResults recallParseResults = SQLParser.parseRecallStatement(line, RecallableSessionLines.size() - 1);
                if (recallParseResults != null) {
                    if (recallParseResults.getError() == null) {
                        line = RecallableSessionLines.get(recallParseResults.getLine());
                        interactiveReader.putString(line);
                        interactiveReader.flush();
                        isRecall = true;
                    }
                    else {
                        System.out.println(recallParseResults.getError());
                    }
                    executeImmediate = false; // let user edit the recalled line.
                    continue;
                }

                // Queue up the line to the recall stack
                //TODO: In the future, we may not want to have simple directives count as recallable
                // lines, so this call would move down a ways.
                RecallableSessionLines.add(line);

                if (executesAsSimpleDirective(line)) {
                    executeImmediate = false; // return to prompt.
                    continue;
                }

                // If the line is a FILE command - execute the content of the file
                List<FileInfo> filesInfo = null;
                try {
                    filesInfo = SQLParser.parseFileStatement(line);
                } catch (SQLParser.Exception e) {
                    stopOrContinue(e);
                    continue;
                }

                if (filesInfo != null && filesInfo.size() != 0) {
                    executeScriptFiles(filesInfo, interactiveReader, null);

                    if (m_returningToPromptAfterError) {
                        // executeScriptFile stopped because of an error. Wipe the slate clean.
                        m_returningToPromptAfterError = false;
                    }
                    continue;
                }

                // else treat the input line as a regular database command
                if (executeImmediate) {
                    executeStatements(line + "\n", null, 0);
                    if (m_testFrontEndOnly) {
                        break; // test mode expects this early return before end of input.
                    }
                    continue;
                }
            } else {
                // With a multi-line statement pending,
                // queue up the line continuation to the recall list.
                //TODO: arguably, it would be more useful to append continuation
                // lines to the last existing Lines entry to build each complete
                // statement as a single recallable unit. Experiments indicated
                // that joining the lines with a single space, while not as pretty
                // as a newline for very long statements, behaved perfectly for
                // line editing (cursor positioning).
                RecallableSessionLines.add(line);
                if (executeImmediate) {
                    statement.append(line + "\n");
                    String incompleteSt = executeStatements(statement.toString(), null, 0);
                    if (incompleteSt != null) {
                        statement = new StringBuilder(incompleteSt);
                    } else {
                        if (m_testFrontEndOnly) {
                            break; // test mode expects this early return before end of input.
                        }
                        statement.setLength(0);
                    }
                    continue;
                }
            }

            // Collect lines ...
            statement.append(line + "\n");
            //TODO: Here's where we might append to a separate buffer that uses
            // a single space rather than a newline as its separator to build up
            // a recallable multi-line statement.
        }
    }


    /// A stripped down variant of the processing in "interactWithTheUser" suitable for
    /// applying to a command script. It skips all the interactive-only options.
    /// It uses the same logic as the FILE directive but gets its input from stdin.
    private static void executeNoninteractive() throws Exception {
        SQLCommandLineReader stdinReader = new LineReaderAdapter(new InputStreamReader(System.in));
        FileInfo fileInfo = FileInfo.forSystemIn();
        executeScriptFromReader(fileInfo, stdinReader, null);
    }


    /// Simple directives require only the input line and no other context from the input loop.
    /// Return true if the line is a directive that has been completely handled here, so that the
    /// input loop can proceed to the next line.
    //TODO: There have been suggestions that some or all of these directives could be made
    // available in non-interactive contexts. This function is available to enable that.
    private static boolean executesAsSimpleDirective(String line) throws Exception {

        // SHOW or LIST <blah> statement
        String subcommand = SQLParser.parseShowStatementSubcommand(line);
        if (subcommand != null) {
            String[] modifiers = subcommand.split(" ", 2);
            switch (modifiers[0].toLowerCase()) {
                case "proc":
                case "procedures":
                    execListProcedures();
                    break;
                case "functions":
                    execListFunctions();
                    break;
                case "tables":
                    String filter = null;
                    if (modifiers.length > 1) {
                        filter = modifiers[1];
                        if (filter.endsWith(";")) {
                            filter = filter.substring(0, filter.length() - 1);
                        }
                    }
                    execListTables(filter);
                    break;
                case "streams":
                    execListStreams();
                    break;
                case "classes":
                    execListClasses();
                    break;
                /*
                * This undocumented argument, CONFIG, is broken: returns an error that
                * @SystemCatalog has no selector CONFIGURATION.
                * Commenting out for now.

                case "config":
                case "configuration":
                    execListConfigurations();
                    break;
                */

                case "tasks":
                    executeListTasks();
                    break;
                case "topics":
                    executeListTopics();
                    break;
                default:
                    String errorCase = (modifiers[0].equals("") || modifiers[0].equals(";")) ?
                            ("Incomplete SHOW command.\n") :
                            ("Invalid SHOW command completion: '" + modifiers[0] + "'.\n");
                    System.out.println(errorCase +
                            "The valid SHOW command completions are classes, procedures, streams, tables, or tasks.");
                    break;
            }
            // Consider it handled here, whether or not it was a good SHOW statement.
            return true;
        }

        // HELP commands - ONLY in interactive mode, close batch and parse for execution
        // Parser returns null if it isn't a HELP command. If no arguments are specified
        // the returned string will be empty.
        String helpSubcommand = SQLParser.parseHelpStatement(line);
        if (helpSubcommand != null) {
            // Ignore the arguments for now.
            if (!helpSubcommand.isEmpty()) {
                System.out.printf("Ignoring extra HELP argument(s): %s\n", helpSubcommand);
            }
            printHelp(System.out); // Print readme to the screen
            return true;
        }

        String echoArgs = SQLParser.parseEchoStatement(line);
        if (echoArgs != null) {
            System.out.println(echoArgs);
            return true;
        }

        String echoErrorArgs = SQLParser.parseEchoErrorStatement(line);
        if (echoErrorArgs != null) {
            System.err.println(echoErrorArgs);
            return true;
        }

        // DESCRIBE table
        String describeArgs = SQLParser.parseDescribeStatement(line);
        if (describeArgs != null) {

            // Check if table exists
            String tableName = "";
            String type = "";
            VoltTable tableData = m_client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
            while (tableData.advanceRow()) {
                String t = tableData.getString(2);
                if (t.equalsIgnoreCase(describeArgs)) {
                    tableName = t;
                    type = tableData.getString(3);
                    String remarks = tableData.getString(4);
                    if (isDRTable(remarks)) {
                        type = "DR TABLE";
                    }
                    break;
                }
            }
            if (tableName.equals("")) {
                System.err.println("Table does not exist");
                return true;
            }

            // Class to temporarily store and sort column attributes
            class Column implements Comparable {
                String name;
                String type;
                long size;
                String remarks;
                long position;
                String notNull;

                @Override
                public String toString() {return "";}

                @Override
                public int compareTo(Object obj) {
                    Column col = (Column) obj;
                    return (int)(this.position - col.position);
                }
            }

            // sort the columns as they are inserted
            SortedSet set = new TreeSet();

            // Retrieve Column attributes
            VoltTable columnData = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
            int maxNameWidth = 10;
            while (columnData.advanceRow()) {
                if (tableName.equalsIgnoreCase(columnData.getString(2))) {

                    Column c = new Column();
                    c.name = columnData.getString(3);
                    if (c.name.length() > maxNameWidth) {
                        maxNameWidth = c.name.length();
                    }

                    c.type = columnData.getString(5);

                    c.size = columnData.getLong(6);
                    if (columnData.wasNull()) {
                        if (c.type.equals("GEOGRAPHY")) {
                            c.size = 32768;
                        }
                        if (c.type.equals("GEOGRAPHY_POINT")) {
                            c.size = 16;
                        }
                    } else if (c.type.equals("TINYINT")) {
                        c.size = 1;
                    } else if (c.type.equals("SMALLINT")) {
                        c.size = 2;
                    } else if (c.type.equals("INTEGER")) {
                        c.size = 4;
                    } else if (c.type.equals("BIGINT")) {
                        c.size = 8;
                    } else if (c.type.equals("FLOAT")) {
                        c.size = 8;
                    } else if (c.type.equals("DECIMAL")) {
                        c.size = 16;
                    } else if (c.type.equals("TIMESTAMP")) {
                        c.size = 8;
                    }

                    c.remarks = columnData.getString(11);
                    if (columnData.wasNull()) {
                        c.remarks = "";
                    }

                    c.position = columnData.getLong(16);

                    String nullableYesNo = columnData.getString(17);
                    c.notNull = "";
                    if (nullableYesNo.equals("NO")) {
                        c.notNull = "NOT NULL";
                    }

                    set.add(c);
                }
            }

            // print output
            String headerFormat = "%-" + maxNameWidth + "s|%-16s|%-11s|%-9s|%-16s\n";
            String rowFormat = "%-" + maxNameWidth + "s|%-16s|%11d|%-9s|%-16s\n";
            System.out.printf(headerFormat,"COLUMN","DATATYPE","SIZE","NULLABLE","REMARKS");
            String DASHES = new String(new char[56+maxNameWidth]).replace("\0", "-");
            System.out.println(DASHES);

            for (Object o : set) {
                Column c = (Column) o;
                System.out.printf(rowFormat, c.name, c.type, c.size, c.notNull, c.remarks);
            }
            System.out.println(DASHES);

            // primary key
            String primaryKey = "";
            VoltTable keyData = m_client.callProcedure("@SystemCatalog", "PRIMARYKEYS").getResults()[0];
            while (keyData.advanceRow()) {
                if (tableName.equalsIgnoreCase(keyData.getString(2))) {
                    String colName = keyData.getString(3);
                    if (!primaryKey.equals("")) {
                        primaryKey += ",";
                    }
                    primaryKey += colName;
                }
            }
            if (!primaryKey.equals("")) {
                System.out.println("Type: " + type + ", Primary Key (" + primaryKey + ")");
            } else {
                System.out.println("Type: " + type);
            }
            return true;
        }

        // QUERYSTATS
        String queryStatsArgs = SQLParser.parseQueryStatsStatement(line);
        if (queryStatsArgs != null) {
            m_startTime = System.nanoTime();    // needs to reset timer here
            printResponse(m_client.callProcedure("@QueryStats", queryStatsArgs), false);
            return true;
        }

        // It wasn't a locally-interpreted directive.
        return false;
    }

    private static void executeListTasks() throws Exception {
        VoltTable schedules = m_client.callProcedure("@SystemCatalog", "TASKS").getResults()[0];
        System.out.println("--- Tasks ----------------------------------------------------");
        while (schedules.advanceRow()) {
            System.out.println(schedules.getString(0));
        }
    }

    private static void executeListTopics() throws Exception {
        ColumnInfo[] schema = new ColumnInfo[] {
                new ColumnInfo("TOPIC_NAME", VoltType.STRING),
                new ColumnInfo("STREAM_NAME", VoltType.STRING),
                new ColumnInfo("PROCEDURE_NAME", VoltType.STRING)
        };
        VoltTable topicsTable = new VoltTable(schema);
        StringBuffer opaque = new StringBuffer("\n--- Opaque Topics ----------------------------------------------");
        VoltTable topics = m_client.callProcedure("@SystemCatalog", "TOPICS").getResults()[0];
        while (topics.advanceRow()) {
            if ("true".equalsIgnoreCase(topics.getString("IS_OPAQUE"))) {
                opaque.append("\n" + topics.getString("TOPIC_NAME"));
            } else {
                topicsTable.addRow(topics.getString("TOPIC_NAME"), topics.getString("STREAM_NAME"), topics.getString("PROCEDURE_NAME"));
            }
        }
        System.out.println("--- Topics ----------------------------------------------------");
        if (topicsTable.getRowCount() > 0) {
            System.out.println(topicsTable.toFormattedString());
        }
        System.out.println(opaque);
    }

    private static void execListClasses() {
        //TODO: since sqlcmd makes no intrinsic use of the Classlist, it would be more
        // efficient to load the Classlist only "on demand" from here and to cache a
        // complete formatted String result rather than the complex map representation.
        // This would save churn on startup and on DDL update.
        if (Classlist.isEmpty()) {
            System.out.println();
            printCatalogHeader("Empty Class List");
            System.out.println();
        }
        List<String> list = new LinkedList<>(Classlist.keySet());
        Collections.sort(list);
        int padding = 0;
        for (String classname : list) {
            padding = Math.max(padding, classname.length());
        }
        String format = " %1$-" + padding + "s";
        String[] categoryHeader = new String[] {
                "Potential Procedure Classes", "Active Procedure Classes", "Non-Procedure Classes"
        };
        for (int i = 0; i<3; i++) {
            boolean firstInCategory = true;
            for (String classname : list) {
                List<Boolean> stuff = Classlist.get(classname);
                // Print non-active procs first
                if (i == 0 && !(stuff.get(0) && !stuff.get(1))) {
                    continue;
                } else if (i == 1 && !(stuff.get(0) && stuff.get(1))) {
                    continue;
                } else if (i == 2 && stuff.get(0)) {
                    continue;
                }
                if (firstInCategory) {
                    firstInCategory = false;
                    System.out.println();
                    printCatalogHeader(categoryHeader[i]);
                }
                System.out.printf(format, classname);
                System.out.println();
            }
        }
        System.out.println();
    }

    private static void execListTables(String typeFilter) throws Exception {
        //TODO: since sqlcmd makes no intrinsic use of the tables list, it would be more
        // efficient to load the list only "on demand" from here and to cache a
        // complete formatted String result rather than the multiple lists.
        // This would save churn on startup and on DDL update.

        // list all tables
        Tables tables = getTables();
        if (typeFilter != null) {
            switch (typeFilter) {
            case "dr":
                printTables("DR Tables", tables.drs);
                break;
            case "view":
                printTables("Views", tables.views);
                break;
            default:
                System.out.println("Unrecognized table type. The valid types are \"dr\" and \"view\".");
            }
            // Handled all type filer
            return;
        }
        printTables("Tables", tables.tables);
        printTables("DR Tables", tables.drs);
        printTables("Streams", tables.exports);
        printTables("Views", tables.views);
        System.out.println();
    }

    private static void execListStreams() throws Exception {
        // list all tables
        Tables tables = getTables();
        printTables("Streams", tables.exports);
    }

    private static void execListFunctions() throws Exception {
        System.out.println();
        printCatalogHeader("User-defined Functions");
        String outputFormat = "%-20s%-20s%-50s";
        VoltTable tableData = m_client.callProcedure("@SystemCatalog", "FUNCTIONS").getResults()[0];
        while (tableData.advanceRow()) {
            String functionType = tableData.getString("FUNCTION_TYPE");
            String functionName = tableData.getString("FUNCTION_NAME");
            String className = tableData.getString("CLASS_NAME");
            String methodName = tableData.getString("METHOD_NAME");
            if (methodName != null) {
                System.out.println(String.format(outputFormat, functionName,
                        functionType + " function", className + "." + methodName));
            } else {
                System.out.println(String.format(outputFormat, functionName,
                        functionType + " function", className));
            }
        }
        System.out.println();
    }

    private static void execListProcedures() {
        List<String> list = new LinkedList<>(Procedures.keySet());
        Collections.sort(list);
        int padding = 0;
        for (String procedure : list) {
            if (padding < procedure.length()) {
                padding = procedure.length();
            }
        }
        padding++;
        String format = "%1$-" + padding + "s";
        boolean firstSysProc = true;
        boolean firstUserProc = true;
        for (String procedure : list) {
            //TODO: it would be much easier over all to maintain sysprocs and user procs in
            // in two separate maps.
            if (procedure.startsWith("@")) {
                if (firstSysProc) {
                    firstSysProc = false;
                    System.out.println();
                    printCatalogHeader("System Procedures");
                }
            }
            else {
                if (CompoundProcedures.contains(procedure)) {
                    continue;
                }
                if (firstUserProc) {
                    firstUserProc = false;
                    System.out.println();
                    printCatalogHeader("User Procedures");
                }
            }
            printProcedure(format, procedure);
        }
        if (!CompoundProcedures.isEmpty()) {
            System.out.println();
            printCatalogHeader("User Compound Procedures");
            CompoundProcedures.forEach(p -> printProcedure(format, p));
        }
        System.out.println();
    }

    private static void printProcedure(String format, String procedure) {
        for (List<String> parameterSet : Procedures.get(procedure).values()) {
            System.out.printf(format, procedure);
            String sep = "\t";
            for (String paramType : parameterSet) {
                System.out.print(sep + paramType);
                sep = ", ";
            }
            System.out.println();
        }
    }

    private static void printConfig(VoltTable configData) {
        System.out.println();
        System.out.println(String.format("%-20s%-20s%-60s", "NAME", "VALUE", "DESCRIPTION"));
        for (int i=0; i<100; i++) {
            System.out.print('-');
        }
        System.out.println();
        while (configData.advanceRow()) {
            System.out.println(String.format("%-20s%-20s%-60s",
                    configData.getString(0), configData.getString(1),
                    configData.getString(2)));
        }
    }

    private static void printCatalogHeader(final String name) {
        System.out.println("--- " + name + " " +
                String.join("", Collections.nCopies(57 - name.length(), "-")));
    }

    private static void printTables(final String name, final Collection<String> tables) {
        System.out.println();
        printCatalogHeader(name);
        for (String table : tables) {
            System.out.println(table);
        }
        System.out.println();
    }

    /**
     * Reads a script file and executes its content.
     * Note that the "script file" could be an inline batch,
     * i.e., a "here document" that is coming from the same input stream
     * as the "file" directive.
     *
     * @param filesInfo    Info on the file directive being processed
     * @param parentLineReader  The current input stream, to be used for "here documents".
     * @throws IOException
     */

    static void executeScriptFiles(
            List<FileInfo> filesInfo, SQLCommandLineReader parentLineReader, DDLParserCallback callback)
            throws IOException {
        LineReaderAdapter adapter;
        SQLCommandLineReader reader;
        StringBuilder statements = new StringBuilder();

        if ( ! m_interactive && callback == null) {
            // We have to check for the callback to avoid spewing to System.out in the "init --classes" filtering codepath.
            // Better logging/output handling in general would be nice to have here -- output on System.out will be consumed
            // by the test generators (build_eemakefield) and cause build failures.
            System.out.println();

            StringBuilder commandString = new StringBuilder();
            commandString.append(filesInfo.get(0).toString());
            for (int ii = 1; ii < filesInfo.size(); ii++) {
                    commandString.append(" " + filesInfo.get(ii).getFile().toString());
            }
            System.out.println(commandString.toString());
        }

        for (int ii = 0; ii < filesInfo.size(); ii++) {

            FileInfo fileInfo = filesInfo.get(ii);
            adapter = null;

            if (fileInfo.getOption() == FileOption.INLINEBATCH) {
                // File command is a "here document" so pass in the current
                // input stream.
                reader = parentLineReader;
            } else {
                try {
                    reader = adapter = new LineReaderAdapter(new FileReader(fileInfo.getFile()));
                } catch (FileNotFoundException e) {
                    System.err.println("Script file '" + fileInfo.getFile() + "' could not be found.");
                    stopOrContinue(e);
                    return; // continue to the next line after the FILE command
                }

                // if it is a batch option, get all contents from all the files and send it as a string
                if (fileInfo.getOption() == FileOption.BATCH) {
                    String line;
                    // use the current reader we obtained to read from the file
                    // and append to existing statements
                    while ((line = reader.readBatchLine()) != null) {
                        statements.append(line).append("\n");
                    }
                    // set reader to null since we finish reading from the file
                    reader = null;

                    // if it is the last file, create a reader to read from the string of all files contents
                    if ( ii == filesInfo.size() - 1 ) {
                        String allStatements = statements.toString();
                        byte[] bytes = allStatements.getBytes("UTF-8");
                        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                        // reader LineReaderAdapter needs an input stream reader
                        reader = adapter = new LineReaderAdapter(new InputStreamReader( bais ) );
                    }
                    // NOTE - fileInfo has the last file info for batch with multiple files
                }
            }
            try {
                executeScriptFromReader(fileInfo, reader, callback);
            } catch (SQLCmdEarlyExitException e) {
                throw e;
            } catch (Exception x) {
                stopOrContinue(x);
            } finally {
                if (adapter != null) {
                    adapter.close();
                }
            }
        }
    }

    /**
     *
     * @param fileInfo  The FileInfo object describing the file command (or stdin)
     * @throws Exception
     */

    public static void executeScriptFromReader(FileInfo fileInfo, SQLCommandLineReader reader, DDLParserCallback callback)
            throws Exception {

        // return back in case of multiple batch files
        if (reader == null) {
            return;
        }

        StringBuilder statement = new StringBuilder();
        // non-interactive modes need to be more careful about discarding blank lines to
        // keep from throwing off diagnostic line numbers. So "statement" may be non-empty even
        // when a sql statement has not yet started (?)
        boolean statementStarted = false;
        StringBuilder batch = fileInfo.isBatch() ? new StringBuilder() : null;

        String delimiter = (fileInfo.getOption() == FileOption.INLINEBATCH) ?
                fileInfo.getDelimiter() : null;

        while (true) {

            String line = reader.readBatchLine();

            if (delimiter != null) {
                if (line == null) {
                    // We only print this nice message if the inline batch is
                    // being executed non-interactively. For an inline batch
                    // entered from the command line, SQLConsoleReader catches
                    // ctrl-D and exits the process before this code can execute,
                    // even if this code is in a "finally" block.
                    throw new Exception("ERROR: Failed to find delimiter \"" + delimiter +
                             "\" indicating end of inline batch.  No batched statements were executed.");
                }
                if (delimiter.equals(line)) {
                    line = null;
                }
            }

            if (line == null) {
                // No more lines.  Execute whatever we got.
                if (batch == null) {
                    String statementString = statement.toString();
                    // Trim here avoids a "missing statement" error from adhoc in an edge case
                    // like a blank line from stdin.
                    if ( ! statementString.trim().isEmpty()) {
                        //* enable to debug */if (m_debug) System.out.println("DEBUG QUERY:'" + statementString + "'");
                        executeStatements(statementString, callback, reader.getLineNumber());
                    }
                } else {
                    batch.append(statement);
                    if (batch.length() > 0) {
                        executeDDLBatch(fileInfo.getFilePath(), batch.toString(), callback, reader.getLineNumber());
                    }
                }
                return;
            }

            if ( ! statementStarted) {
                if (line.trim().isEmpty() || SQLParser.isWholeLineComment(line)) {
                    // We don't strictly have to include a blank line or whole-line
                    // comment at the start of a statement, but when we want to preserve line
                    // numbers (in a batch), we should at least append a newline.
                    // Whether to echo comments or blank lines from a batch is
                    // a grey area.
                    if (batch != null && callback == null) {
                        statement.append(line).append("\n");
                    }
                    continue;
                }

                // Recursively process FILE commands, any failure will cause a recursive failure
                List<FileInfo> nestedFilesInfo = SQLParser.parseFileStatement(fileInfo, line);

                if (nestedFilesInfo != null) {
                    // Guards must be added for FILE Batch containing batches.
                    if (batch != null) {
                        stopOrContinue(new RuntimeException(
                                "A FILE command is invalid in a batch."));
                        continue; // continue to the next line after the FILE command
                    }

                    // Execute the file content or fail to but only set m_returningToPromptAfterError
                    // if the intent is to cause a recursive failure, stopOrContinue decided to stop.

                    executeScriptFiles(nestedFilesInfo, reader, callback);

                    if (m_returningToPromptAfterError) {
                        // The recursive readScriptFile stopped because of an error.
                        // Escape to the outermost readScriptFile caller so it can exit or
                        // return to the interactive prompt.
                        return;
                    } else {
                        // Continue after a bad nested file command by processing the next line
                        // in the current file.
                        continue;
                    }
                }

                // process other non-interactive directives
                if (executesAsSimpleDirective(line)) {
                    continue;
                }

                // TODO: This would be a reasonable place to validate that the line
                // starts with a SQL command keyword, exec/execute or one of the other
                // known commands.
                // According to the current parsing rules that allow multi-statement
                // stacking on a line (as an undocumented feature),
                // this work would also have to be repeated after each
                // non-quoted non-commented statement-splitting semicolon.
                // See executeStatements.
            }

            // Process normal @AdHoc commands which may be
            // multi-line-statement continuations.
            statement.append(line).append("\n");

            // Check if the current statement ends here and now.
            // if it is an incomplete multi statement procedure, it is returned back
            if (SQLParser.isSemiColonTerminated(line)) {
                String statementString = statement.toString();
                if (batch == null) {
                    //* enable to debug */ if (m_debug) System.out.println("DEBUG QUERY:'" + statementString + "'");
                    String incompleteStmt = executeStatements(statementString, callback, reader.getLineNumber());
                    if (incompleteStmt != null) {
                        statement = new StringBuilder(incompleteStmt);
                    } else {
                        statement.setLength(0);
                        statementStarted = false;
                    }
                } else { // when in a batch:
                    SplitStmtResults splitResults = SQLLexer.splitStatements(statementString);
                    if (splitResults.getIncompleteStmt() == null) {
                        // not in the middle of a statement.
                        statementStarted = false;
                        batch.append(statement);
                        statement.setLength(0);
                    } else {
                        int incompleteStmtOffset = splitResults.getIncompleteStmtOffset();
                        statementStarted = true;
                        if (incompleteStmtOffset != 0) {
                            batch.append(statementString.substring(0, incompleteStmtOffset));
                            statement = new StringBuilder(statementString.substring(incompleteStmtOffset));
                        }
                    }
                }
            } else {
                // Disable directive processing until end of statement.
                statementStarted = true;
            }
        }
    }

    private static long m_startTime;
    // executeQueuedStatements is called instead of executeStatement because
    // multiple semicolon-separated statements are allowed on a line and because
    // using "line ends with semicolon" is not foolproof as a means of detecting
    // the end of a statement. It could give a false negative for something as
    // simple as an end-of-line comment.
    //
    private static String executeStatements(String statements, DDLParserCallback callback, int lineNum) {
        SplitStmtResults parsedOutput = SQLLexer.splitStatements(statements);
        List<String> parsedStatements = parsedOutput.getCompletelyParsedStmts();
        for (String statement: parsedStatements) {
            executeStatement(statement, callback, lineNum);
        }
        return parsedOutput.getIncompleteStmt();
    }

    @SuppressWarnings("deprecation")
    private static void executeStatement(String statement, DDLParserCallback callback, int lineNum) {
        if (m_testFrontEndOnly) {
            m_testFrontEndResult += statement + ";\n";
            return;
        }
        if ( !m_interactive && m_outputShowMetadata && callback == null) {
            System.out.println();
            System.out.println(statement + ";");
        }
        try {

            if (callback != null) {
                callback.statement(statement, lineNum);
                return;
            }

            // EXEC <procedure> <params>...
            m_startTime = System.nanoTime();
            SQLParser.ExecuteCallResults execCallResults = SQLParser.parseExecuteCall(statement, Procedures);
            if (execCallResults != null) {
                String procName = execCallResults.procedure;
                Object[] objectParams = execCallResults.getParameterObjects();

                if (procName.equals("@UpdateApplicationCatalog")) {
                    File catfile = null;
                    if (objectParams[0] != null) {
                        catfile = new File((String)objectParams[0]);
                    }
                    File depfile = null;
                    if (objectParams[1] != null) {
                        depfile = new File((String)objectParams[1]);
                    }
                    printDdlResponse(UpdateApplicationCatalog.update(m_client, catfile, depfile));

                    // Need to update the stored procedures after a catalog change (could have added/removed SPs!).  ENG-3726
                    loadStoredProcedures();
                } else if (procName.equals("@UpdateClasses")) {
                    File jarfile = null;
                    if (objectParams[0] != null) {
                        jarfile = new File((String)objectParams[0]);
                    }
                    printDdlResponse(m_client.updateClasses(jarfile, (String)objectParams[1]));
                    // Need to reload the procedures and classes
                    loadStoredProcedures();
                } else {
                    // @SnapshotDelete needs array parameters.
                    if (procName.equals("@SnapshotDelete")) {
                        objectParams[0] = new String[] { (String)objectParams[0] };
                        objectParams[1] = new String[] { (String)objectParams[1] };
                    }

                    boolean suppressTableOutputForDML = ! procName.equals("@SwapTables");

                    printResponse(callProcedureHelper(execCallResults.procedure, objectParams), suppressTableOutputForDML);
                }
                return;
            }

            String explainStatement = SQLParser.parseExplainCall(statement);
            if (explainStatement != null) {
                // We've got a statement that starts with "explain", send the statement to
                // @Explain (after parseExplainCall() strips "explain").
                printResponse(m_client.callProcedure("@Explain", explainStatement), false);
                return;
            }

            // explainjson => @ExplainJSON
            String explainStatementInJSON = SQLParser.parseExplainJSONCall(statement);
            if (explainStatementInJSON != null) {
                printResponse(m_client.callProcedure("@ExplainJSON", explainStatementInJSON), false);
                return;
            } else if (SQLParser.parseExplainCatalogCall(statement)) {
                // explaincatalog => @ExplainCatalog
                printResponse(m_client.callProcedure("@ExplainCatalog"), false);
                return;
            }

            String explainProcName = SQLParser.parseExplainProcCall(statement);
            if (explainProcName != null) {
                // We've got a statement that starts with "explainproc", send the statement to
                // @ExplainProc (now that parseExplainProcCall() has stripped out "explainproc").
                printResponse(m_client.callProcedure("@ExplainProc", explainProcName), false);
                return;
            }

            String explainViewName = SQLParser.parseExplainViewCall(statement);
            if (explainViewName != null) {
                // We've got a statement that starts with "explainview", send the statement to
                // @ExplainView (now that parseExplainViewCall() has stripped out "explainview").
                printResponse(m_client.callProcedure("@ExplainView", explainViewName), false);
                return;
            }

            // LOAD CLASS <jar>?
            String loadPath = SQLParser.parseLoadClasses(statement);
            if (loadPath != null) {
                File jarfile = new File(loadPath);
                printDdlResponse(m_client.updateClasses(jarfile, null));
                loadStoredProcedures();
                return;
            }

            // REMOVE CLASS <class-selector>?
            String classSelector = SQLParser.parseRemoveClasses(statement);
            if (classSelector != null) {
                printDdlResponse(m_client.updateClasses(null, classSelector));
                loadStoredProcedures();
                return;
            }

            // DDL statements get forwarded to @AdHoc,
            // but get special post-processing.
            if (SQLParser.queryIsDDL(statement)) {
                // if the query is DDL, reload the stored procedures.
                printDdlResponse(m_client.callProcedure("@AdHoc", statement));
                loadStoredProcedures();
                return;
            }
            // All other commands get forwarded to @AdHoc
            printResponse(callProcedureHelper("@AdHoc", statement), true);
        } catch (Exception exc) {
            stopOrContinue(exc);
        }
    }

    private static int stopOrContinue(Exception exc) {
        System.err.println(exc.getMessage());
        if (m_debug) {
            exc.printStackTrace(System.err);
        }
        // Let the final exit code reflect any error(s) in the run.
        // This is useful for debugging a script that may have multiple errors
        // and multiple valid statements.
        m_exitCode = -1;
        if (m_stopOnError) {
            if (! m_interactive ) {
                throw new SQLCmdEarlyExitException();
            }
            // Setting this member to drive a fast stack unwind from
            // recursive readScriptFile requires explicit checks in that code,
            // but still seems easier than a "throw" here from a catch block that
            // would require additional exception handlers in the caller(s)
            m_returningToPromptAfterError = true;
        }
        return 0;
    }

    // Output generation
    private static SQLCommandOutputFormatter m_outputFormatter = new SQLCommandOutputFormatterDefault();
    private static boolean m_outputShowMetadata = true;

    private static boolean isUpdateResult(VoltTable table) {
        return (table.getColumnName(0).isEmpty() || table.getColumnName(0).equals("modified_tuples")) &&
                table.getRowCount() == 1 && table.getColumnCount() == 1 && table.getColumnType(0) == VoltType.BIGINT;
    }

    private static void printResponse(ClientResponse response, boolean suppressTableOutputForDML) throws Exception {
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new Exception("Execution Error: " + response.getStatusString());
        }

        long elapsedTime = System.nanoTime() - m_startTime;
        for (VoltTable t : response.getResults()) {
            long rowCount;
            if (suppressTableOutputForDML && isUpdateResult(t)) {
                rowCount = t.fetchRow(0).getLong(0);
            } else {
                rowCount = t.getRowCount();
                // Run it through the output formatter.
                m_outputFormatter.printTable(System.out, t, m_outputShowMetadata);
                //System.out.println("printable");
            }
            if (m_outputShowMetadata) {
                System.out.printf("(Returned %d rows in %.2fs)\n", rowCount, elapsedTime / 1000000000.0);
            }
        }
    }

    private static void printDdlResponse(ClientResponse response) throws Exception {
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new Exception("Execution Error: " + response.getStatusString());
        } else {
            //TODO: In the future, if/when we change the prompt when waiting for the remainder of an unfinished command,
            // successful DDL commands may just silently return to a normal prompt without this verbose feedback.
            System.out.println("Command succeeded.");
        }
    }

    // VoltDB connection support
    private static Client m_client;
    // Default visibility is for test purposes.
    static Map<String,Map<Integer, List<String>>> Procedures = Collections.synchronizedMap(new HashMap<>());
    private static Map<String, List<Boolean>> Classlist = Collections.synchronizedMap(new HashMap<>());
    private static Set<String> CompoundProcedures = Collections.synchronizedSet(new HashSet<>());

    private static void loadSystemProcedures() {
        Procedures.put("@Pause",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<>()).build());
        Procedures.put("@JStack",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("int")).build());
        Procedures.put("@Quiesce",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<>()).build());
        Procedures.put("@Resume",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<>()).build());
        Procedures.put("@Shutdown",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<>()).build());
        Procedures.put("@StopNode",
                ImmutableMap.<Integer, List<String>>builder().put(1, Arrays.asList("int")).build());
        Procedures.put("@SnapshotDelete",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar")).build()
                );
        Procedures.put("@SnapshotRestore",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar"))
                        .put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@SnapshotSave",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<>())
                        .put( 1, Arrays.asList("varchar"))
                        .put( 3, Arrays.asList("varchar", "varchar", "bit")).build());
        Procedures.put("@SnapshotScan",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@Statistics",
                ImmutableMap.<Integer, List<String>>builder()
                       .put( 1, Arrays.asList("statisticscomponent"))
                       .put( 2, Arrays.asList("statisticscomponent", "bit"))
                       .build());
        Procedures.put("@SystemCatalog",
                ImmutableMap.<Integer, List<String>>builder().put( 1,Arrays.asList("metadataselector")).build());
        Procedures.put("@SystemInformation",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("sysinfoselector")).build());
        Procedures.put("@UpdateApplicationCatalog",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar")).build());
        Procedures.put("@UpdateClasses",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar")).build());
        Procedures.put("@UpdateLogging",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@UpdateLicense",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@Ping",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<>()).build());
        Procedures.put("@Promote",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<>()).build());
        Procedures.put("@Explain",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@ExplainProc",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@ExplainView",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@ElasticRemoveNT",
                ImmutableMap.<Integer, List<String>>builder().put( 4, Arrays.asList("tinyint", "varchar", "varchar", "bigint")).build());
        Procedures.put("@ValidatePartitioning",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("int", "varbinary")).build());
        Procedures.put("@GetPartitionKeys",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@GC",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<>()).build());
        Procedures.put("@ResetDR",
                ImmutableMap.<Integer, List<String>>builder().put( 3, Arrays.asList("tinyint", "tinyint", "tinyint")).build());
        Procedures.put("@SwapTables",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar")).build());
        Procedures.put("@QueryStats",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@Trace",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<>())
                        .put(1, Arrays.asList("varchar"))
                        .put(2, Arrays.asList("varchar", "varchar")).build());
        Procedures.put("@Note",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
    }

    private static Client getClient(ClientConfig config, String[] servers, int port) throws Exception {
        final Client client = ClientFactory.createClient(config);

        // Only fail if we can't connect to any servers
        boolean connectedAnyServer = false;
        StringBuilder connectionErrorMessages = new StringBuilder();

        for (String server : servers) {
            try {
                client.createConnection(server.trim(), port);
                connectedAnyServer = true;
            } catch (UnknownHostException e) {
                connectionErrorMessages.append("\n    ")
                        .append(server.trim())
                        .append(":").append(port)
                        .append(" - UnknownHostException");
            } catch (IOException e) {
                connectionErrorMessages.append("\n    ")
                        .append(server.trim()).append(":")
                        .append(port).append(" - ").append(e.getMessage());
            }
        }

        if (!connectedAnyServer) {
            throw new IOException("Unable to connect to VoltDB cluster" + connectionErrorMessages);
        } else {
            return client;
        }
    }

    // General application support
    private static void printUsage(String msg) {
        System.out.print(msg);
        System.out.println("\n");
        m_exitCode = -1;
        printUsage();
    }
    private static void printUsage() {
        System.out.println(
        "Usage: sqlcmd --help\n"
        + "   or  sqlcmd [--servers=comma_separated_server_list]\n"
        + "              [--port=port_number]\n"
        + "              [--user=user]\n"
        + "              [--password=password]\n"
        + "              [--credentials=file_spec]\n"
        + "              [--kerberos=jaas_login_configuration_entry_key]\n"
        + "              [--ssl or --ssl=ssl-configuration-file]\n"
        + "              [--query=query]\n"
        + "              [--output-format=(fixed|csv|tab)]\n"
        + "              [--output-skip-metadata]\n"
        + "              [--stop-on-error=(true|false)]\n"
        + "              [--query-timeout=number_of_milliseconds]\n"
        + "\n"
        + "[--servers=comma_separated_server_list]\n"
        + "  List of servers to connect to.\n"
        + "  Default: localhost.\n"
        + "\n"
        + "[--port=port_number]\n"
        + "  Client port to connect to on cluster nodes.\n"
        + "  Default: 21212.\n"
        + "\n"
        + "[--user=user]\n"
        + "  Name of the user for database login.\n"
        + "  Default: (not defined - connection made without credentials).\n"
        + "\n"
        + "[--password=password]\n"
        + "  Password of the user for database login.\n"
        + "  Default: (not defined - connection made without credentials).\n"
        + "\n"
        + "[--credentials=credentials]\n"
        + "  File that contains username and password information.\n"
        + "  Default: (not defined - connection made without credentials).\n"
        + "\n"
        + "[--kerberos=jaas_login_configuration_entry_key]\n"
        + "  Enable kerberos authentication for user database login by specifying\n"
        + "  the JAAS login configuration file entry name"
        + "  Default: (not defined - connection made without credentials).\n"
        + "\n"
        + "[--query=query]\n"
        + "  Execute a non-interactive query. Multiple query options are allowed.\n"
        + "  Default: (runs the interactive shell when no query options are present).\n"
        + "\n"
        + "[--output-format=(fixed|csv|tab)]\n"
        + "  Format of returned resultset data (Fixed-width, CSV or Tab-delimited).\n"
        + "  Default: fixed.\n"
        + "\n"
        + "[--output-skip-metadata]\n"
        + "  Removes metadata information such as column headers and row count from\n"
        + "  produced output. Default: metadata output is enabled.\n"
        + "\n"
        + "[--stop-on-error=(true|false)]\n"
        + "  Causes the utility to stop immediately or continue after detecting an error.\n"
        + "  In interactive mode, a value of \"true\" discards any unprocessed input\n"
        + "  and returns to the command prompt. Default: true.\n"
        + "\n"
        + "[--query-timeout=millisecond_number]\n"
        + "  Read-only queries that take longer than this number of milliseconds will abort. Default: " + BatchTimeoutOverrideType.DEFAULT_TIMEOUT/1000.0 + " seconds.\n"
        + "\n"
        );
    }

    // printHelp() can print readme either to a file or to the screen
    // depending on the argument passed in
    // Default visibility is for test purposes.
    static void printHelp(OutputStream prtStr) {
        try {
            InputStream is = SQLCommand.class.getResourceAsStream(m_readme);
            while (is.available() > 0) {
                byte[] bytes = new byte[is.available()]; // Fix for ENG-3440
                is.read(bytes, 0, bytes.length);
                prtStr.write(bytes); // For JUnit test
            }
        } catch (Exception x) {
            System.err.println(x.getMessage());
            m_exitCode = -1;
        }
    }

    private static class Tables {
        TreeSet<String> tables = new TreeSet<>();
        TreeSet<String> exports = new TreeSet<>();
        TreeSet<String> drs = new TreeSet<>();
        TreeSet<String> views = new TreeSet<>();
    }

    private static Tables getTables() throws Exception {
        Tables tables = new Tables();
        VoltTable tableData = m_client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        while (tableData.advanceRow()) {
            String tableName = tableData.getString("TABLE_NAME");
            String tableType = tableData.getString("TABLE_TYPE");
            String tableRemark = tableData.getString("REMARKS");
            if (tableType.equalsIgnoreCase("EXPORT")) {
                tables.exports.add(tableName);
            } else if (tableType.equalsIgnoreCase("VIEW")) {
                tables.views.add(tableName);
            } else if (isDRTable(tableRemark)) {
                tables.drs.add(tableName);
            } else {
                tables.tables.add(tableName);
            }
        }
        return tables;
    }

    // Load stored procedures and update globals Procedures, Classlist, CompoundProcedures
    private static void loadStoredProcedures() {
        VoltTable procs = null;
        VoltTable params = null;
        VoltTable classes = null;
        try {
            procs = m_client.callProcedure("@SystemCatalog", "PROCEDURES").getResults()[0];
            params = m_client.callProcedure("@SystemCatalog", "PROCEDURECOLUMNS").getResults()[0];
            classes = m_client.callProcedure("@SystemCatalog", "CLASSES").getResults()[0];
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
            return;
        }
        Map<String, Integer> proc_param_counts = Collections.synchronizedMap(new HashMap<String, Integer>());
        while (params.advanceRow()) {
            String this_proc = params.getString("PROCEDURE_NAME");
            Integer curr_val = proc_param_counts.get(this_proc);
            if (curr_val == null) {
                curr_val = 1;
            } else {
                ++curr_val;
            }
            proc_param_counts.put(this_proc, curr_val);
        }
        params.resetRowPosition();
        Set<String> userProcs = new HashSet<>();
        CompoundProcedures.clear();
        while (procs.advanceRow()) {
            String proc_name = procs.getString("PROCEDURE_NAME");
            userProcs.add(proc_name);
            Integer param_count = proc_param_counts.get(proc_name);
            ArrayList<String> this_params = new ArrayList<>();
            // prepopulate it to make sure the size is right
            if (param_count != null) {
                for (int i = 0; i < param_count; i++) {
                    this_params.add(null);
                }
            } else {
                param_count = 0;
            }
            HashMap<Integer, List<String>> argLists = new HashMap<>();
            argLists.put(param_count, this_params);
            Procedures.put(proc_name, argLists);
            // Detect compound procedures
            String remarks = procs.getString("REMARKS");
            if (isCompound(remarks)) {
                CompoundProcedures.add(proc_name);
            }
        }
        for (String proc_name : new ArrayList<>(Procedures.keySet())) {
            if (!proc_name.startsWith("@") && !userProcs.contains(proc_name)) {
                Procedures.remove(proc_name);
            }
        }
        Classlist.clear();
        while (classes.advanceRow()) {
            String classname = classes.getString("CLASS_NAME");
            boolean isProc = (classes.getLong("VOLT_PROCEDURE") == 1L);
            boolean isActive = (classes.getLong("ACTIVE_PROC") == 1L);
            if (!Classlist.containsKey(classname)) {
                List<Boolean> stuff = Collections.synchronizedList(new ArrayList<Boolean>());
                stuff.add(isProc);
                stuff.add(isActive);
                Classlist.put(classname, stuff);
            }
        }

        // Retrieve the parameter types.  Note we have to do some special checking
        // for array types.  ENG-3101
        params.resetRowPosition();
        while (params.advanceRow()) {
            Map<Integer, List<String>> argLists = Procedures.get(params.getString("PROCEDURE_NAME"));
            assert(argLists.size() == 1);
            List<String> this_params = argLists.values().iterator().next();
            int idx = (int)params.getLong("ORDINAL_POSITION") - 1;
            String param_type = params.getString("TYPE_NAME").toLowerCase();
            // Detect if this parameter is supposed to be an array.  It's kind of clunky, we have to
            // look in the remarks column...
            String param_remarks = params.getString("REMARKS");
            if (null != param_remarks) {
                param_type += (param_remarks.equalsIgnoreCase("ARRAY_PARAMETER") ? "_array" : "");
            }
            this_params.set(idx, param_type);
        }
    }

    private static boolean isCompound(String remarks) {
        boolean ret = false;
        try {
            JSONObject json = new JSONObject(remarks);
            ret = json.optBoolean("compound");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    /// Parser unit test entry point
    ///TODO: it would be simpler if this testing entry point could just set up some mocking
    /// of io and statement "execution" -- mocking with statement capture instead of actual execution
    /// to better isolate the parser.
    /// Then it could call a new simplified version of interactWithTheUser().
    /// But the current parser tests expect to call a SQLCommand function that can return for
    /// some progress checking before its input stream has been permanently terminated.
    /// They would need to be able to check parser progress in one thread while
    /// SQLCommand.interactWithTheUser() was awaiting further input on another thread
    /// (or in its own process).
    static List<String> getParserTestQueries(InputStream inmocked, OutputStream outmocked) {
        testFrontEndOnly();
        try {
            SQLConsoleReader reader = new SQLConsoleReader(inmocked, outmocked);
            getInteractiveQueries(reader);
            return SQLLexer.splitStatements(m_testFrontEndResult).getCompletelyParsedStmts();
        } catch (Exception ignored) {}
        return null;
    }

    static void testFrontEndOnly() {
        m_testFrontEndOnly = true;
        m_testFrontEndResult = "";
    }

    static String getTestResult() { return m_testFrontEndResult; }

    private static String extractArgInput(String arg) {
        // the input arguments has "=" character when this function is called
        String[] splitStrings = arg.split("=", 2);
        if (splitStrings[1].isEmpty()) {
            printUsage("Missing input value for " + splitStrings[0]);
            return null;
        }
        return splitStrings[1];
    }

    /**
     * Wraps the main routine. Is callable from other code without fear of it
     * calling System.exit(..).
     */
    public static int mainWithReturnCode(String args[]) {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
        // Initialize parameter defaults
        String serverList = "localhost";
        int port = 21212;
        String user = "";
        String password = "";
        String credentials = "";
        String kerberos = "";
        FileReader fr = null;
        List<String> queries = null;
        String ddlFileText = "";
        String sslConfigFile = null;
        boolean enableSSL = false;

        // Parse out parameters
        for (String arg : args) {
            if (arg.startsWith("--servers=")) {
                serverList = extractArgInput(arg);
                if (serverList == null) {
                    return -1;
                }
            } else if (arg.startsWith("--port=")) {
                String portStr = extractArgInput(arg);
                if (portStr == null) {
                    return -1;
                }
                port = Integer.parseInt(portStr);
            } else if (arg.startsWith("--user=")) {
                user = extractArgInput(arg);
                if (user == null) {
                    return -1;
                }
            } else if (arg.startsWith("--password=")) {
                password = extractArgInput(arg);
                if (password == null) {
                    return -1;
                }
            } else if (arg.startsWith("--credentials")) {
                credentials = extractArgInput(arg);
                if (credentials == null) {
                    return -1;
                }
            } else if (arg.startsWith("--kerberos=")) {
                kerberos = extractArgInput(arg);
                if (kerberos == null) {
                    return -1;
                }
            } else if (arg.startsWith("--kerberos")) {
                kerberos = "VoltDBClient";
            } else if (arg.startsWith("--query=")) {
                List<String> argQueries = SQLLexer.splitStatements(arg.substring(8)).getCompletelyParsedStmts();
                if (!argQueries.isEmpty()) {
                    if (queries == null) {
                        queries = argQueries;
                    } else {
                        queries.addAll(argQueries);
                    }
                }
            } else if (arg.startsWith("--output-format=")) {
                String formatName = extractArgInput(arg);
                if (formatName == null) {
                    return -1;
                }
                formatName = formatName.toLowerCase();
                switch (formatName) {
                    case "fixed":
                        m_outputFormatter = new SQLCommandOutputFormatterDefault();
                        break;
                    case "csv":
                        m_outputFormatter = new SQLCommandOutputFormatterCSV();
                        break;
                    case "tab":
                        m_outputFormatter = new SQLCommandOutputFormatterTabDelimited();
                        break;
                    default:
                        printUsage("Invalid value for --output-format");
                        return -1;
                }
            } else if (arg.startsWith("--stop-on-error=")) {
                String optionName = extractArgInput(arg);
                if (optionName == null) {
                    return -1;
                }
                optionName = optionName.toLowerCase();
                if (optionName.equals("true")) {
                    m_stopOnError = true;
                } else if (optionName.equals("false")) {
                    m_stopOnError = false;
                } else {
                    printUsage("Invalid value for --stop-on-error");
                    return -1;
                }
            } else if (arg.startsWith("--ddl-file=")) {
                String ddlFilePath = extractArgInput(arg);
                if (ddlFilePath == null) {
                    return -1;
                }
                try {
                    File ddlJavaFile = new File(ddlFilePath);
                    Scanner scanner = new Scanner(ddlJavaFile);
                    ddlFileText = scanner.useDelimiter("\\Z").next();
                    scanner.close();
                } catch (FileNotFoundException e) {
                    printUsage("DDL file not found at path:" + ddlFilePath);
                    return -1;
                }
            } else if (arg.startsWith("--query-timeout=")) {
                m_hasBatchTimeout = true;
                String batchTimeoutStr = extractArgInput(arg);
                if (batchTimeoutStr == null) {
                    return -1;
                }
                m_batchTimeout = Integer.parseInt(batchTimeoutStr);
            } else if (arg.equals("--output-skip-metadata")) { // equals check starting here
                m_outputShowMetadata = false;
            } else if (arg.startsWith("--ssl=")) {
                enableSSL = true;
                sslConfigFile = extractArgInput(arg);
                if (sslConfigFile == null) {
                    return -1;
                }
            } else if (arg.startsWith("--ssl")) {
                enableSSL = true;
                sslConfigFile = null;
            } else if (arg.equals("--debug")) {
                m_debug = true;
            } else if (arg.equals("--help")) {
                printHelp(System.out); // Print readme to the screen
                System.out.println("\n\n");
                printUsage();
                return -1;
            } else if (arg.equals("--no-version-check")) {
                m_versionCheck = false; // Disable new version phone home check
            } else if ((arg.equals("--usage")) || (arg.equals("-?"))) {
                printUsage();
                return -1;
            } else {
                printUsage("Invalid Parameter: " + arg);
                return -1;
            }
        }

        // Split server list
        String[] servers = serverList.split(",");

        // Phone home to see if there is a newer version of VoltDB
        if (m_versionCheck) {
            openURLAsync();
        }

        // read username and password from txt file
        if (credentials != null && !credentials.trim().isEmpty()) {
            Properties props = MiscUtils.readPropertiesFromCredentials(credentials);
            user = props.getProperty("username");
            password = props.getProperty("password");
        }

        try {
            // If we need to prompt the user for a password, do so.
            password = CLIConfig.readPasswordIfNeeded(user, password, "Enter password: ");
        } catch (IOException ex) {
            printUsage("Unable to read password: " + ex);
        }

        // Create connection
        ClientConfig config = new ClientConfig(user, password, null);
        if (enableSSL) {
            if (sslConfigFile != null && !sslConfigFile.trim().isEmpty()) {
                config.setTrustStoreConfigFromPropertyFile(sslConfigFile);
            } else {
                config.setTrustStoreConfigFromDefault();
            }
            config.enableSSL();
        }
        config.setProcedureCallTimeout(0);  // Set procedure all to infinite timeout, see ENG-2670
        try {
            // if specified enable kerberos
            if (!kerberos.isEmpty()) {
                config.enableKerberosAuthentication(kerberos);
            }
            m_client = getClient(config, servers, port);
        } catch (Exception exc) {
            System.err.println(exc.getMessage());
            return -1;
        }

        try {
            if (! ddlFileText.equals("")) {
                // fast DDL Loader mode
                // System.out.println("fast DDL Loader mode with DDL input:\n" + ddlFile);
                m_client.callProcedure("@AdHoc", ddlFileText);
                return m_exitCode;
            }

            // Load system procedures
            loadSystemProcedures();

            // Load user stored procs
            loadStoredProcedures();

            // Removed code to prevent Ctrl-C from exiting. The original code is visible
            // in Git history hash 837df236c059b5b4362ffca7e7a5426fba1b7f20.

            m_interactive = true;
            if (queries != null && !queries.isEmpty()) {
                // If queries are provided via command line options run them in
                // non-interactive mode.
                //TODO: Someday we should honor batching.
                m_interactive = false;
                for (String query : queries) {
                    executeStatement(query, null, 0);
                }
            }
            // This test for an interactive environment is mostly
            // reliable. See stackoverflow.com/questions/1403772.
            // It accurately detects when data is piped into the program
            // but it fails to distinguish the case when data is ONLY piped
            // OUT of the command -- that's a possible but very strange way
            // to run an interactive session, so it's OK that we don't support
            // it. Instead, in that edge case, we fall back to non-interactive
            // mode but IN THAT MODE, we wait on and process user input as if
            // from a slow pipe. Strange, but acceptable, and preferable to the
            // check used here in the past (System.in.available() > 0)
            // which would fail in the opposite direction, when a 0-length
            // file was piped in, showing an interactive greeting and prompt
            // before quitting.
            if (System.console() == null && m_interactive) {
                m_interactive = false;
                executeNoninteractive();
            }
            if (m_interactive) {
                // Print out welcome message
                System.out.printf("SQL Command :: %s%s:%d\n", (user.isEmpty() ? "" : user + "@"), serverList, port);
                interactWithTheUser();
            }
        } catch (SQLCmdEarlyExitException e) {
            return m_exitCode;
        } catch (Exception x) {
            try { stopOrContinue(x); } catch (SQLCmdEarlyExitException e) { return m_exitCode; }
        } finally {
            try { m_client.close(); } catch (Exception ignored) { }
        }
        // Processing may have been continued after one or more errors.
        // Reflect them in the exit code.
        // This might be a little unconventional for an interactive session,
        // but it's also likely to be ignored in that case, so "no great harm done".
        //* enable to debug */ System.err.println("Exiting with code " + m_exitCode);
        return m_exitCode;
    }

    // Application entry point
    public static void main(String args[]) {
        int exitCode = mainWithReturnCode(args);
        System.exit(exitCode);
    }

    // The following two methods implement a "phone home" version check for VoltDB.
    // Asynchronously ping VoltDB to see what the current released version is.
    // If it is newer than the one running here, then notify the user in some manner TBD.
    // Note that this processing should not impact utility use in any way.  Ignore all
    // errors.
    private static void openURLAsync() {
        final Thread t = new Thread(SQLCommand::openURL);
        // Set the daemon flag so that this won't hang the process if it runs into difficulty
        t.setDaemon(true);
        t.start();
    }

    private static void openURL() {
        URL url;
        try {
            // Read the response from VoltDB
            String a = "http://community.voltdb.com/versioncheck?app=sqlcmd&ver=" +
                    org.voltdb.VoltDB.instance().getVersionString();
            url = new URL(a);
            URLConnection conn = url.openConnection();

            // open the stream and put it into BufferedReader
            try(BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                while (br.readLine() != null) {
                    // At this time do nothing, just drain the stream.
                    // In the future we'll notify the user that a new version of VoltDB is available.
                }
            }
        } catch (Throwable ignored) { }
    }

    private static boolean isDRTable(String remark) {
        if (remark != null) {
            try {
                JSONObject json = new JSONObject(remark);
                if (json != null && Boolean.valueOf((String)json.get("drEnabled"))) {
                    return true;
                }
            } catch (JSONException e) {/* swallow it */}
        }
        return false;
    }
}
