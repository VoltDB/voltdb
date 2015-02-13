/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.TreeSet;

import jline.console.CursorBuffer;
import jline.console.KeyMap;
import jline.console.history.FileHistory;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.parser.SQLParser;
import org.voltdb.parser.SQLParser.ParseRecallResults;

import com.google_voltpatches.common.collect.ImmutableMap;

public class SQLCommand
{
    private static boolean m_stopOnError = true;
    private static boolean m_debug = false;
    private static boolean m_interactive;
    private static boolean m_returningToPromptAfterError = false;
    private static int m_exitCode = 0;

    private static final String readme = "SQLCommandReadme.txt";

    public static String getReadme() {
        return readme;
    }

    // Command line interaction
    private static SQLConsoleReader lineInputReader = null;
    private static FileHistory historyFile = null;

    private static List<String> RecallableSessionLines = new ArrayList<String>();

   /// The main loop for interactive mode.
    public static void interactWithTheUser() throws Exception
    {
        List<String> parsedQueries = null;
        while ((parsedQueries = getInteractiveQueries()) != null) {
            for (String parsedQuery : parsedQueries) {
                executeQuery(parsedQuery);
            }
        }
    }

    //TODO: If we can rework the interactive mode unit test framework, we can eliminate this
    // unit test entry point and inline this code into interactWithTheUser.
    // This would eliminate an extra layer of looping and needless bouncing
    // out of and back into getInteractiveQueries for some kinds of input
    // but not others.
    public static List<String> getInteractiveQueries() throws Exception
    {
        // Reset the error state to avoid accidentally ignoring future FILE content
        // after a file had runtime errors (ENG-7335).
        m_returningToPromptAfterError = false;
        //TODO: add to this multiLineStatementBuffer to disable processing of "directives"
        // while there is a multi-line statement in progress.
        // For now, for backward compatibility, keep this empty. This undesirably allows the
        // directives at the start of any line to temporarily interrupt statements in progress.
        List<String> multiLineStatementBuffer = new ArrayList<>();
        List<String> parsedQueries = new ArrayList<>();
        StringBuilder query = new StringBuilder();
        boolean isRecall = false;

        boolean executeImmediate = false;
        while ( ! executeImmediate) {
            String prompt = isRecall ? "" : ((RecallableSessionLines.size() + 1) + "> ");
            isRecall = false;
            String line = lineInputReader.readLine(prompt);

            assert(line != null);

            // Was there a line-ending semicolon typed at the prompt?
            // This mostly matters for "non-directive" statements, but, for
            // now, for backward compatibility, it needs to be noted for FILE
            // commands prior to their processing.
            executeImmediate = SQLParser.isSemiColonTerminated(line);

            // When we are tracking the progress of a multi-line statement,
            // avoid coincidentally recognizing mid-statement SQL content as sqlcmd
            // "directives".
            if (multiLineStatementBuffer.isEmpty()) {

                // EXIT command - exit immediately
                if (SQLParser.isExitCommand(line)) {
                    return null;
                }

                // RECALL command
                ParseRecallResults recallParseResults = SQLParser.parseRecallStatement(line, RecallableSessionLines.size() - 1);
                if (recallParseResults != null) {
                    if (recallParseResults.error == null) {
                        line = RecallableSessionLines.get(recallParseResults.line);
                        lineInputReader.putString(line);
                        lineInputReader.flush();
                        isRecall = true;
                    }
                    else {
                        System.out.printf("%d> %s\n%d", RecallableSessionLines.size(), recallParseResults.error);
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

                // GO commands - signal the end of any pending multi-line statements.
                //TODO: to be deprecated in favor of just typing a semicolon on its own line to finalize
                // a multi-line statement.
                if (SQLParser.isGoCommand(line)) {
                    executeImmediate = true;
                    line = ";";
                }

                // handle statements that are converted to regular database commands
                line = handleTranslatedCommands(line);
                if (line == null) {
                    // something bad happened interpreting the translated command
                    executeImmediate = false; // return to prompt
                    continue;
                }

                // If the line is a FILE command - include the content of the file into the query queue
                //TODO: executing statements (from files) as they are read rather than queuing them
                // would improve performance and error handling.
                File file = SQLParser.parseFileStatement(line);
                if (file != null) {
                    // Get the line(s) from the file(s) to queue as regular database commands
                    // or get back a null if, in the recursive call, stopOrContinue decided to continue.
                    line = readScriptFile(file);
                    if (m_returningToPromptAfterError) {
                        // readScriptFile stopped because of an error. Wipe the slate clean.
                        query = new StringBuilder();
                        // Until we execute statements as they are read, there will always be a
                        // chance that errors in queued statements are still waiting to be detected,
                        // so, this reset is not 100% effective (as discovered in ENG-7335).
                        m_returningToPromptAfterError = false;
                        executeImmediate = false; // return to prompt.
                        continue;
                    }
                    // else treat the line(s) from the file(s) as regular database commands
                }

                // else treat the input line as a regular database command
            }
            else {
                // With a multi-line statement pending, queue up the line continuation to the recall list.
                //TODO: arguably, it would be more useful to append continuation lines to the last
                // existing Lines entry to build each complete statement as a single recallable
                // unit. Experiments indicated that joining the lines with a single space, while not
                // very pretty for very long statements, behaved best for line editing (cursor synch)
                // purposes.
                // The multiLineStatementBuffer MAY become useful here.
                RecallableSessionLines.add(line);
            }

            //TODO: Here's where we might use multiLineStatementBuffer to note a sql statement
            // in progress -- if the line(s) so far contained anything more than whitespace.

            // Collect lines ...
            query.append(line);
            query.append("\n");
        }
        parsedQueries = SQLParser.parseQuery(query.toString());
        return parsedQueries;
    }

    /// Returns the original command, a replacement command, or null (on error).
    private static String handleTranslatedCommands(String lineIn)
    {
        try {
            return SQLParser.translateStatement(lineIn);
        }
        catch(SQLParser.Exception e) {
            System.out.printf("%d> %s\n", RecallableSessionLines.size(), e.getMessage());
        }

        //* enable to debug */ if (lineOut != null && !lineOut.equals(lineIn)) System.err.printf("Translated: %s -> %s\n", lineIn, lineOut);

        return lineIn;
    }

    /// A stripped down variant of the processing in "interactWithTheUser" suitable for
    /// applying to a command script. It skips all the interactive-only options.
    public static void executeNoninteractive() throws Exception
    {
        //TODO: increase code reuse between the processing of stdin (piped file) here
        // and the processing of explicitly opened files in readScriptFile.
        // Both of these methods should be using more of an execute-as-you-go approach rather than
        // so much statement queueing.
        StringBuilder query = new StringBuilder();
        while (true) {
            String line = lineInputReader.readLine();
            if (line == null) {
                //* enable to debug */     System.err.println("Read null batch line.");
                List<String> parsedQueries = SQLParser.parseQuery(query.toString());
                for (String parsedQuery : parsedQueries) {
                    executeQuery(parsedQuery);
                }
                return;
            }
            //* enable to debug */ else System.err.println("Read non-null batch line: (" + line + ")");

            // handle statements that are converted to regular database commands
            line = handleTranslatedCommands(line);
            if (line == null) {
                continue;
            }

            // If the line is a FILE command - include the content of the file into the query queue
            File file = SQLParser.parseFileStatement(line);
            if (file != null) {
                // Get the line(s) from the file(s) to queue as regular database commands,
                // or get back a null if in the recursive call, stopOrContinue decided to continue.
                line = readScriptFile(file);
                if (line == null) {
                    continue;
                }
            }

            // else treat the input line as a regular database command

            // Collect the lines ...
            query.append(line);
            query.append("\n");
        }
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
            if (subcommand.equals("proc") || subcommand.equals("procedure")) {
               execListProcedures();
            }
            else if (subcommand.equals("tables")) {
                execListTables();
            }
            else if (subcommand.equals("classes")) {
                execListClasses();
            }
            else {
                System.out.printf("%d> Bad SHOW target: %s\n%d", RecallableSessionLines.size(), subcommand);
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

        // It wasn't a locally-interpreted directive.
        return false;
    }

    private static void execListClasses() {
        //TODO: since sqlcmd makes no intrinsic use of the Classlist, it would be more
        // efficient to load the Classlist only "on demand" from here and to cache a
        // complete formatted String result rather than the complex map representation.
        // This would save churn on startup and on DDL update.
        if (Classlist.isEmpty()) {
            System.out.println();
            System.out.println("--- Empty Class List -----------------------");
            System.out.println();
        }
        List<String> list = new LinkedList<String>(Classlist.keySet());
        Collections.sort(list);
        int padding = 0;
        for (String classname : list) {
            padding = Math.max(padding, classname.length());
        }
        String format = " %1$-" + padding + "s";
        String categoryHeader[] = new String[] {
                "--- Potential Procedure Classes ----------------------------",
                "--- Active Procedure Classes  ------------------------------",
                "--- Non-Procedure Classes ----------------------------------"};
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
                    System.out.println(categoryHeader[i]);
                }
                System.out.printf(format, classname);
                System.out.println();
            }
        }
        System.out.println();
    }

    private static void execListTables() throws Exception {
        //TODO: since sqlcmd makes no intrinsic use of the tables list, it would be more
        // efficient to load the list only "on demand" from here and to cache a
        // complete formatted String result rather than the multiple lists.
        // This would save churn on startup and on DDL update.
        Tables tables = getTables();
        printTables("User Tables", tables.tables);
        printTables("User Views", tables.views);
        printTables("User Export Streams", tables.exports);
        System.out.println();
    }

    private static void execListProcedures() {
        List<String> list = new LinkedList<String>(Procedures.keySet());
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
                    System.out.println("--- System Procedures --------------------------------------");
                }
            }
            else {
                if (firstUserProc) {
                    firstUserProc = false;
                    System.out.println();
                    System.out.println("--- User Procedures ----------------------------------------");
                }
            }
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
        System.out.println();
    }

    private static void printTables(final String name, final Collection<String> tables)
    {
        System.out.println();
        System.out.println("--- " + name + " --------------------------------------------");
        for (String table : tables) {
            System.out.println(table);
        }
        System.out.println();
    }

    public static String readScriptFile(File file)
    {
        BufferedReader script = null;
        try {
            script = new BufferedReader(new FileReader(file));
        }
        catch (FileNotFoundException e) {
            System.err.println("Script file '" + file + "' could not be found.");
            stopOrContinue(e);
            return null; // continue to the next line after the FILE command
        }
        try {
            StringBuilder query = new StringBuilder();
            String line;
            while ((line = script.readLine()) != null) {
                // Strip out RECALL, EXIT and GO commands
                //TODO: There is inconsistent handling of other "interactive mode" commands
                // between batch commands in a file and batch commands from stdin or "--query=".
                // The LIST commands are not covered here in particular, causing them to get
                // piled onto the query string to mix with any statements or statement fragments
                // currently being queued there. We COULD add them to this filter.
                // But if INSTEAD we removed the filter completely both here and in the other input
                // reader in "non-interactive" mode, then the user would soon learn not to
                // put these garbage lines uselessly into their batch inputs.
                // This would have the advantage of simplicity and would avoid possible
                // edge case confusion when one of these "commands" like EXIT or GO happened to be
                // a name in the user's schema that fell on a line of its own in the input and so
                // got ignored.
                // Maybe we should bypass these potential snafus in non-interactive mode by
                // taking all of the user's command input more "literally".
                // FILE is arguably the only useful one -- it could be improved by giving it a name
                // less likely to be accidentally used in database commands like @File or #include.
                if (SQLParser.parseRecallStatement(line, RecallableSessionLines.size() - 1) != null ||
                    SQLParser.isExitCommand(line) ||
                    SQLParser.isGoCommand(line)) {
                    continue;
                }

                // handle statements that are converted to regular database commands
                line = handleTranslatedCommands(line);
                if (line == null) {
                    continue;
                }

                // Recursively process FILE commands, any failure will cause a recursive failure
                File nestedFile = SQLParser.parseFileStatement(line);
                if (nestedFile != null) {
                    // Get the line(s) from the file(s) to queue as regular database commands
                    // or get back a null if in the recursive call, stopOrContinue decided to continue.
                    line = readScriptFile(nestedFile);
                    if (line == null) {
                        if (m_returningToPromptAfterError) {
                            // The recursive readScriptFile stopped because of an error.
                            // Escape to the outermost readScriptFile caller so it can exit or
                            // return to the interactive prompt.
                            return null;
                        }
                        // Continue after a bad nested file command by processing the next line
                        // in the current file.
                        continue;
                    }
                }

                query.append(line);
                query.append("\n");
            }
            return query.toString().trim();
        }
        catch (Exception x) {
            stopOrContinue(x);
            return null;
        }
        finally {
            if (script != null) {
                try {
                    script.close();
                } catch (IOException e) { }
            }
        }
    }

    private static long m_startTime;
    private static void executeQuery(String statement)
    {
        try {
            // EXEC <procedure> <params>...
            m_startTime = System.nanoTime();
            SQLParser.ExecuteCallResults execCallResults = SQLParser.parseExecuteCall(statement, Procedures);
            if (execCallResults != null) {
                Object[] objectParams = execCallResults.getParameterObjects();

                if (execCallResults.procedure.equals("@UpdateApplicationCatalog")) {
                    File catfile = null;
                    if (objectParams[0] != null) {
                        catfile = new File((String)objectParams[0]);
                    }
                    File depfile = null;
                    if (objectParams[1] != null) {
                        depfile = new File((String)objectParams[1]);
                    }
                    printDdlResponse(VoltDB.updateApplicationCatalog(catfile, depfile));

                    // Need to update the stored procedures after a catalog change (could have added/removed SPs!).  ENG-3726
                    loadStoredProcedures(Procedures, Classlist);
                }
                else if (execCallResults.procedure.equals("@UpdateClasses")) {
                    File jarfile = null;
                    if (objectParams[0] != null) {
                        jarfile = new File((String)objectParams[0]);
                    }
                    printDdlResponse(VoltDB.updateClasses(jarfile, (String)objectParams[1]));
                    // Need to reload the procedures and classes
                    loadStoredProcedures(Procedures, Classlist);
                }
                else {
                    // @SnapshotDelete needs array parameters.
                    if (execCallResults.procedure.equals("@SnapshotDelete")) {
                        objectParams[0] = new String[] { (String)objectParams[0] };
                        objectParams[1] = new String[] { (String)objectParams[1] };
                    }
                    printResponse(VoltDB.callProcedure(execCallResults.procedure, objectParams));
                }
                return;
            }

            String explainQuery = SQLParser.parseExplainCall(statement);
            if (explainQuery != null) {
                // We've got a query that starts with "explain", send the query to
                // @Explain (after parseExplainCall() strips "explain").
                printResponse(VoltDB.callProcedure("@Explain", explainQuery));
                return;
            }

            String explainProcQuery = SQLParser.parseExplainProcCall(statement);
            if (explainProcQuery != null) {
                // We've got a query that starts with "explainproc", send the query to
                // @Explain (after parseExplainCall() strips "explainproc").
                // Clean up any extra spaces from between explainproc and the proc name.
                explainProcQuery = explainProcQuery.trim();
                printResponse(VoltDB.callProcedure("@ExplainProc", explainProcQuery));
                return;
            }

            // All other commands get forwarded to @AdHoc
            if (SQLParser.queryIsDDL(statement)) {
                // if the query is DDL, reload the stored procedures.
                printDdlResponse(VoltDB.callProcedure("@AdHoc", statement));
                loadStoredProcedures(Procedures, Classlist);
            }
            else {
                printResponse(VoltDB.callProcedure("@AdHoc", statement));
            }
        } catch(Exception exc) {
            stopOrContinue(exc);
        }
    }

    private static void stopOrContinue(Exception exc) {
        System.err.println(exc.getMessage());
        if (m_debug) {
            exc.printStackTrace(System.err);
        }
        // Let the final exit code reflect any error(s) in the run.
        // This is useful for debugging a script that may have multiple errors
        // and multiple valid statements.
        m_exitCode = -1;
        if (m_stopOnError) {
            if ( ! m_interactive ) {
                System.exit(m_exitCode);
            }
            // Setting this member to drive a fast stack unwind from
            // recursive readScriptFile requires explicit checks in that code,
            // but still seems easier than a "throw" here from a catch block that
            // would require additional exception handlers in the caller(s)
            m_returningToPromptAfterError = true;
        }
    }

    // Output generation
    private static SQLCommandOutputFormatter m_outputFormatter = new SQLCommandOutputFormatterDefault();
    private static boolean m_outputShowMetadata = true;

    private static boolean isUpdateResult(VoltTable table)
    {
        return ((table.getColumnName(0).isEmpty() || table.getColumnName(0).equals("modified_tuples")) &&
                 table.getRowCount() == 1 && table.getColumnCount() == 1 && table.getColumnType(0) == VoltType.BIGINT);
    }

    private static void printResponse(ClientResponse response) throws Exception
    {
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new Exception("Execution Error: " + response.getStatusString());
        }

        long elapsedTime = System.nanoTime() - m_startTime;
        for (VoltTable t : response.getResults()) {
            long rowCount;
            if (!isUpdateResult(t)) {
                rowCount = t.getRowCount();
                // Run it through the output formatter.
                m_outputFormatter.printTable(System.out, t, m_outputShowMetadata);
            }
            else {
                rowCount = t.fetchRow(0).getLong(0);
            }
            if (m_outputShowMetadata) {
                System.out.printf("(Returned %d rows in %.2fs)\n",
                        rowCount, elapsedTime / 1000000000.0);
            }
        }
    }

    private static void printDdlResponse(ClientResponse response) throws Exception {
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new Exception("Execution Error: " + response.getStatusString());
        }
        //TODO: In the future, if/when we change the prompt when waiting for the remainder of an unfinished command,
        // successful DDL commands may just silently return to a normal prompt without this verbose feedback.
        System.out.println("Command succeeded.");
    }

    // VoltDB connection support
    private static Client VoltDB;
    static Map<String,Map<Integer, List<String>>> Procedures =
            Collections.synchronizedMap(new HashMap<String,Map<Integer, List<String>>>());
    private static Map<String, List<Boolean>> Classlist =
        Collections.synchronizedMap(new HashMap<String, List<Boolean>>());
    private static void loadSystemProcedures()
    {
        Procedures.put("@Pause",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@Quiesce",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@Resume",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@Shutdown",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@StopNode",
                ImmutableMap.<Integer, List<String>>builder().put(1, Arrays.asList("int")).build());
        Procedures.put("@SnapshotDelete",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar")).build()
                );
        Procedures.put("@SnapshotRestore",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar"))
                                                             .put( 1, Arrays.asList("varchar")).build()
                );
        Procedures.put("@SnapshotSave",
                ImmutableMap.<Integer, List<String>>builder().put( 3, Arrays.asList("varchar", "varchar", "bit")).
                put( 1, Arrays.asList("varchar")).build()
                );
        Procedures.put("@SnapshotScan",
                ImmutableMap.<Integer, List<String>>builder().put( 1,
                Arrays.asList("varchar")).build());
        Procedures.put("@Statistics",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("statisticscomponent", "bit")).build());
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
        Procedures.put("@Promote",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@SnapshotStatus",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@Explain",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@ExplainProc",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@ValidatePartitioning",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("int", "varbinary")).build());
        Procedures.put("@GetPartitionKeys",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@GC",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@ApplyBinaryLogSP",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varbinary", "varbinary")).build());
    }

    public static Client getClient(ClientConfig config, String[] servers, int port) throws Exception
    {
        final Client client = ClientFactory.createClient(config);

        for (String server : servers) {
            client.createConnection(server.trim(), port);
        }
        return client;
    }

    // General application support
    private static void printUsage(String msg)
    {
        System.out.print(msg);
        System.out.println("\n");
        printUsage(-1);
    }
    private static void printUsage(int exitCode)
    {
        System.out.println(
        "Usage: sqlcmd --help\n"
        + "   or  sqlcmd [--servers=comma_separated_server_list]\n"
        + "              [--port=port_number]\n"
        + "              [--user=user]\n"
        + "              [--password=password]\n"
        + "              [--kerberos=jaas_login_configuration_entry_key]\n"
        + "              [--query=query]\n"
        + "              [--output-format=(fixed|csv|tab)]\n"
        + "              [--output-skip-metadata]\n"
        + "              [--stop-on-error=(true|false)]\n"
        + "              [--debug]\n"
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
        + "  produced output.\n"
        + "\n"
        + "[--stop-on-error=(true|false)]\n"
        + "  Causes the utility to stop immediately or continue after detecting an error.\n"
        + "  In interactive mode, a value of \"true\" discards any unprocessed input\n"
        + "  and returns to the command prompt.\n"
        + "\n"
        + "[--debug]\n"
        + "  Causes the utility to print out stack traces for all exceptions.\n"
        );
        System.exit(exitCode);
    }

    // printHelp() can print readme either to a file or to the screen
    // depending on the argument passed in
    public static void printHelp(OutputStream prtStr)
    {
        try {
            InputStream is = SQLCommand.class.getResourceAsStream(readme);
            while (is.available() > 0) {
                byte[] bytes = new byte[is.available()]; // Fix for ENG-3440
                is.read(bytes, 0, bytes.length);
                prtStr.write(bytes); // For JUnit test
            }
        }
        catch (Exception x) {
            System.err.println(x.getMessage());
            System.exit(-1);
        }
    }

    private static class Tables
    {
        TreeSet<String> tables = new TreeSet<String>();
        TreeSet<String> exports = new TreeSet<String>();
        TreeSet<String> views = new TreeSet<String>();
    }

    private static Tables getTables() throws Exception
    {
        Tables tables = new Tables();
        VoltTable tableData = VoltDB.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        while (tableData.advanceRow()) {
            String tableName = tableData.getString("TABLE_NAME");
            String tableType = tableData.getString("TABLE_TYPE");
            if (tableType.equalsIgnoreCase("EXPORT")) {
                tables.exports.add(tableName);
            }
            else if (tableType.equalsIgnoreCase("VIEW")) {
                tables.views.add(tableName);
            }
            else {
                tables.tables.add(tableName);
            }
        }
        return tables;
    }

    private static void loadStoredProcedures(Map<String,Map<Integer, List<String>>> procedures,
            Map<String, List<Boolean>> classlist)
    {
        VoltTable procs = null;
        VoltTable params = null;
        VoltTable classes = null;
        try {
            procs = VoltDB.callProcedure("@SystemCatalog", "PROCEDURES").getResults()[0];
            params = VoltDB.callProcedure("@SystemCatalog", "PROCEDURECOLUMNS").getResults()[0];
            classes = VoltDB.callProcedure("@SystemCatalog", "CLASSES").getResults()[0];
        }
        catch (NoConnectionsException e) {
            e.printStackTrace();
            return;
        }
        catch (IOException e) {
            e.printStackTrace();
            return;
        }
        catch (ProcCallException e) {
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
        while (procs.advanceRow()) {
            String proc_name = procs.getString("PROCEDURE_NAME");
            Integer param_count = proc_param_counts.get(proc_name);
            ArrayList<String> this_params = new ArrayList<String>();
            // prepopulate it to make sure the size is right
            if (param_count != null) {
                for (int i = 0; i < param_count; i++) {
                    this_params.add(null);
                }
            }
            else {
                param_count = 0;
            }
            HashMap<Integer, List<String>> argLists = new HashMap<Integer, List<String>>();
            argLists.put(param_count, this_params);
            procedures.put(proc_name, argLists);
        }
        classlist.clear();
        while (classes.advanceRow()) {
            String classname = classes.getString("CLASS_NAME");
            boolean isProc = (classes.getLong("VOLT_PROCEDURE") == 1L);
            boolean isActive = (classes.getLong("ACTIVE_PROC") == 1L);
            if (!classlist.containsKey(classname)) {
                List<Boolean> stuff = Collections.synchronizedList(new ArrayList<Boolean>());
                stuff.add(isProc);
                stuff.add(isActive);
                classlist.put(classname, stuff);
            }
        }

        // Retrieve the parameter types.  Note we have to do some special checking
        // for array types.  ENG-3101
        params.resetRowPosition();
        while (params.advanceRow()) {
            Map<Integer, List<String>> argLists = procedures.get(params.getString("PROCEDURE_NAME"));
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
    public static List<String> getParserTestQueries(InputStream inmocked, OutputStream outmocked)
    {
        try {
            SQLConsoleReader reader = new SQLConsoleReader(inmocked, outmocked);
            lineInputReader = reader;
            return getInteractiveQueries();
        } catch (Exception ioe) {}
        return null;
    }

    private static InputStream in = null;
    private static OutputStream out = null;


    private static String extractArgInput(String arg) {
        // the input arguments has "=" character when this function is called
        String[] splitStrings = arg.split("=", 2);
        if (splitStrings[1].isEmpty()) {
            printUsage("Missing input value for " + splitStrings[0]);
        }
        return splitStrings[1];
    }

    // Application entry point
    public static void main(String args[])
    {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
        // Initialize parameter defaults
        String serverList = "localhost";
        int port = 21212;
        String user = "";
        String password = "";
        String kerberos = "";
        List<String> queries = null;
        String ddlFile = "";

        // Parse out parameters
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--servers=")) {
                serverList = extractArgInput(arg);
            } else if (arg.startsWith("--port=")) {
                port = Integer.valueOf(extractArgInput(arg));
            } else if (arg.startsWith("--user=")) {
                user = extractArgInput(arg);
            } else if (arg.startsWith("--password=")) {
                password = extractArgInput(arg);
            } else if (arg.startsWith("--kerberos=")) {
                kerberos = extractArgInput(arg);
            } else if (arg.startsWith("--kerberos")) {
                kerberos = "VoltDBClient";
            } else if (arg.startsWith("--query=")) {
                List<String> argQueries = SQLParser.parseQuery(arg.substring(8));
                if (!argQueries.isEmpty()) {
                    if (queries == null) {
                        queries = argQueries;
                    }
                    else {
                        queries.addAll(argQueries);
                    }
                }
            }
            else if (arg.startsWith("--output-format=")) {
                String formatName = extractArgInput(arg).toLowerCase();
                if (formatName.equals("fixed")) {
                    m_outputFormatter = new SQLCommandOutputFormatterDefault();
                }
                else if (formatName.equals("csv")) {
                    m_outputFormatter = new SQLCommandOutputFormatterCSV();
                }
                else if (formatName.equals("tab")) {
                    m_outputFormatter = new SQLCommandOutputFormatterTabDelimited();
                }
                else {
                    printUsage("Invalid value for --output-format");
                }
            }
            else if (arg.equals("--output-skip-metadata")) {
                m_outputShowMetadata = false;
            }
            else if (arg.equals("--debug")) {
                m_debug = true;
            }
            else if (arg.startsWith("--stop-on-error=")) {
                String optionName = extractArgInput(arg).toLowerCase();
                if (optionName.equals("true")) {
                    m_stopOnError = true;
                }
                else if (optionName.equals("false")) {
                    m_stopOnError = false;
                }
                else {
                    printUsage("Invalid value for --stop-on-error");
                }
            }
            else if (arg.startsWith("--ddl-file=")) {
                String ddlFilePath = extractArgInput(arg);
                try {
                    ddlFile = new Scanner(new File(ddlFilePath)).useDelimiter("\\Z").next();
                } catch (FileNotFoundException e) {
                    printUsage("DDL file not found at path:" + ddlFilePath);
                }
            }
            else if (arg.equals("--help")) {
                printHelp(System.out); // Print readme to the screen
                System.out.println("\n\n");
                printUsage(0);
            }
            else if ((arg.equals("--usage")) || (arg.equals("-?"))) {
                printUsage(0);
            }
            else {
                printUsage("Invalid Parameter: " + arg);
            }
        }

        // Split server list
        String[] servers = serverList.split(",");

        // Phone home to see if there is a newer version of VoltDB
        openURLAsync();

        // Create connection
        ClientConfig config = new ClientConfig(user, password);
        config.setProcedureCallTimeout(0);  // Set procedure all to infinite timeout, see ENG-2670

        try {
            // if specified enable kerberos
            if (!kerberos.isEmpty()) {
                config.enableKerberosAuthentication(kerberos);
            }
            VoltDB = getClient(config, servers, port);
        } catch (Exception exc) {
            System.err.println(exc.getMessage());
            System.exit(-1);
        }

        try {
            if (! ddlFile.equals("")) {
                // fast DDL Loader mode
                // System.out.println("fast DDL Loader mode with DDL input:\n" + ddlFile);
                VoltDB.callProcedure("@AdHoc", ddlFile);
                System.exit(m_exitCode);
            }

            // Load system procedures
            loadSystemProcedures();

            // Load user stored procs
            loadStoredProcedures(Procedures, Classlist);

            in = new FileInputStream(FileDescriptor.in);
            out = System.out;
            lineInputReader = new SQLConsoleReader(in, out);

            lineInputReader.setBellEnabled(false);

            // Maintain persistent history in ~/.sqlcmd_history.
            historyFile = new FileHistory(new File(System.getProperty("user.home"), ".sqlcmd_history"));
            lineInputReader.setHistory(historyFile);

            // Make Ctrl-D (EOF) exit if on an empty line, otherwise delete the next character.
            KeyMap keyMap = lineInputReader.getKeys();
            keyMap.bind(new Character(KeyMap.CTRL_D).toString(), new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e)
                {
                    CursorBuffer cursorBuffer = lineInputReader.getCursorBuffer();
                    if (cursorBuffer.length() == 0) {
                        System.exit(m_exitCode);
                    }
                    else {
                        try {
                            lineInputReader.delete();
                        }
                        catch (IOException e1) {}
                    }
                }
            });

            // Removed code to prevent Ctrl-C from exiting. The original code is visible
            // in Git history hash 837df236c059b5b4362ffca7e7a5426fba1b7f20.

            m_interactive = true;
            if (queries != null && !queries.isEmpty()) {
                // If queries are provided via command line options run them in
                // non-interactive mode.
                //TODO: Someday we should honor batching.
                m_interactive = false;
                for (String query : queries) {
                    executeQuery(query);
                }
            }
            if (System.in.available() > 0) {
                // If Standard input comes loaded with data, run in non-interactive mode
                m_interactive = false;
                executeNoninteractive();
            }
            if (m_interactive) {
                // Print out welcome message
                System.out.printf("SQL Command :: %s%s:%d\n", (user == "" ? "" : user + "@"), serverList, port);
                interactWithTheUser();
            }
        }
        catch (Exception x) {
            stopOrContinue(x);
        }
        finally {
            try { VoltDB.close(); } catch (Exception x) { }
            // Flush input history to a file.
            if (historyFile != null) {
                try {
                    historyFile.flush();
                }
                catch (IOException e) {
                    System.err.printf("* Unable to write history to \"%s\" *\n",
                                      historyFile.getFile().getPath());
                    if (m_debug) {
                        e.printStackTrace();
                    }
                }
            }
            // Clean up jline2 resources.
            if (lineInputReader != null) {
                lineInputReader.shutdown();
            }
        }
        // Processing may have been continued after one or more errors.
        // Reflect them in the exit code.
        // This might be a little unconventional for an interactive session,
        // but it's also likely to be ignored in that case, so "no great harm done".
        //* enable to debug */ System.err.println("Exiting with code " + m_exitCode);
        System.exit(m_exitCode);
    }

    // The following two methods implement a "phone home" version check for VoltDB.
    // Asynchronously ping VoltDB to see what the current released version is.
    // If it is newer than the one running here, then notify the user in some manner TBD.
    // Note that this processing should not impact utility use in any way.  Ignore all
    // errors.
    private static void openURLAsync()
    {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                 openURL();
            }
        });

        // Set the daemon flag so that this won't hang the process if it runs into difficulty
        t.setDaemon(true);
        t.start();
    }

    private static void openURL()
    {
        URL url;

        try {
            // Read the response from VoltDB
            String a="http://community.voltdb.com/versioncheck?app=sqlcmd&ver=" + org.voltdb.VoltDB.instance().getVersionString();
            url = new URL(a);
            URLConnection conn = url.openConnection();

            // open the stream and put it into BufferedReader
            BufferedReader br = new BufferedReader(
                               new InputStreamReader(conn.getInputStream()));

            while (br.readLine() != null) {
                // At this time do nothing, just drain the stream.
                // In the future we'll notify the user that a new version of VoltDB is available.
            }
            br.close();
        } catch (Throwable e) {
            // ignore any error
        }
    }
}
