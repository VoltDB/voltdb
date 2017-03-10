/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.parser.SQLParser;
import org.voltdb.parser.SQLParser.FileInfo;
import org.voltdb.parser.SQLParser.FileOption;
import org.voltdb.parser.SQLParser.ParseRecallResults;

import com.google_voltpatches.common.collect.ImmutableMap;

import jline.console.CursorBuffer;
import jline.console.KeyMap;
import jline.console.history.FileHistory;

public class SQLCommand
{
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

    private static List<String> RecallableSessionLines = new ArrayList<String>();
    private static boolean m_testFrontEndOnly;
    private static String m_testFrontEndResult;


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
            throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse response = null;
        if (m_hasBatchTimeout) {
            response = m_client.callProcedureWithTimeout(m_batchTimeout, procName, parameters);
        } else {
            response = m_client.callProcedure(procName, parameters);
        }
        return response;
    }

    private static void executeDDLBatch(String batchFileName, String statements) {
        try {
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
            loadStoredProcedures(Procedures, Classlist);
        }
        catch (ProcCallException ex) {
            String fixedMessage = patchErrorMessageWithFile(batchFileName, ex.getMessage());
            stopOrContinue(new Exception(fixedMessage));
        }
        catch (Exception ex) {
            stopOrContinue(ex);
        }
    }

    // The main loop for interactive mode.
    public static void interactWithTheUser() throws Exception
    {
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
            keyMap.bind(new Character(KeyMap.CTRL_D).toString(), new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    CursorBuffer cursorBuffer = interactiveReader.getCursorBuffer();
                    if (cursorBuffer.length() == 0) {
                        System.exit(m_exitCode);
                    } else {
                        try {
                            interactiveReader.delete();
                        } catch (IOException e1) {
                        }
                    }
                }
            });

            getInteractiveQueries(interactiveReader);
        }
        finally {
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
            if (interactiveReader != null) {
                interactiveReader.shutdown();
            }
        }
    }

    public static void getInteractiveQueries(SQLConsoleReader interactiveReader) throws Exception
    {
        // Reset the error state to avoid accidentally ignoring future FILE content
        // after a file had runtime errors (ENG-7335).
        m_returningToPromptAfterError = false;
        final StringBuilder statement = new StringBuilder();
        boolean isRecall = false;

        while (true) {
            String prompt = isRecall ? "" : ((RecallableSessionLines.size() + 1) + "> ");
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
                FileInfo fileInfo = null;
                try {
                    fileInfo = SQLParser.parseFileStatement(line);
                }
                catch (SQLParser.Exception e) {
                    stopOrContinue(e);
                    continue;
                }
                if (fileInfo != null) {
                    executeScriptFile(fileInfo, interactiveReader);
                    if (m_returningToPromptAfterError) {
                        // executeScriptFile stopped because of an error. Wipe the slate clean.
                        m_returningToPromptAfterError = false;
                    }
                    continue;
                }

                // else treat the input line as a regular database command
                if (executeImmediate) {
                    executeStatements(line + "\n");
                    if (m_testFrontEndOnly) {
                        break; // test mode expects this early return before end of input.
                    }
                    continue;
                }
            }
            else {
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
                    executeStatements(statement.toString());
                    if (m_testFrontEndOnly) {
                        break; // test mode expects this early return before end of input.
                    }
                    statement.setLength(0);
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
    public static void executeNoninteractive() throws Exception
    {
        SQLCommandLineReader stdinReader = new LineReaderAdapter(new InputStreamReader(System.in));
        FileInfo fileInfo = SQLParser.FileInfo.forSystemIn();
        executeScriptFromReader(fileInfo, stdinReader);
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
            if (subcommand.equals("proc") || subcommand.equals("procedures")) {
                execListProcedures();
            }
            else if (subcommand.equals("tables")) {
                execListTables();
            }
            else if (subcommand.equals("classes")) {
                execListClasses();
            }
            else if (subcommand.equals("config") || subcommand.equals("configuration")) {
                execListConfigurations();
            }
            else {
                String errorCase = (subcommand.equals("") || subcommand.equals(";")) ?
                        ("Incomplete SHOW command.\n") :
                        ("Invalid SHOW command completion: '" + subcommand + "'.\n");
                System.out.println(errorCase +
                        "The valid SHOW command completions are proc, procedures, tables, or classes.");
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

        // It wasn't a locally-interpreted directive.
        return false;
    }

    private static void execListConfigurations() throws Exception {
        VoltTable configData = m_client.callProcedure("@SystemCatalog", "CONFIG").getResults()[0];
        if (configData.getRowCount() != 0) {
            printConfig(configData);
        }
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

    private static void printConfig(VoltTable configData) {
        System.out.println();
        System.out.println(String.format("%-20s%-20s%-60s", "NAME", "VALUE", "DESCRIPTION"));
        for (int i=0; i<100; i++) {
            System.out.print('-');
        }
        System.out.println();
        while (configData.advanceRow()) {
            System.out.println(String.format("%-20s%-20s%-60s",
                    configData.getString(0), configData.getString(1), configData.getString(2)));
        }
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

    /** Adapt BufferedReader into a SQLCommandLineReader */
    private static class LineReaderAdapter implements SQLCommandLineReader {
        private final BufferedReader m_reader;

        LineReaderAdapter(InputStreamReader reader) {
            m_reader = new BufferedReader(reader);
        }

        @Override
        public String readBatchLine() throws IOException {
            return m_reader.readLine();
        }

        void close() {
            try {
                m_reader.close();
            } catch (IOException e) { }
        }
    }

    /**
     * Reads a script file and executes its content.
     * Note that the "script file" could be an inline batch,
     * i.e., a "here document" that is coming from the same input stream
     * as the "file" directive.
     *
     * @param fileInfo    Info on the file directive being processed
     * @param parentLineReader  The current input stream, to be used for "here documents".
     */
    static void executeScriptFile(FileInfo fileInfo, SQLCommandLineReader parentLineReader)
    {
        LineReaderAdapter adapter = null;
        SQLCommandLineReader reader = null;

        if ( ! m_interactive) {
            System.out.println();
            System.out.println(fileInfo.toString());
        }

        if (fileInfo.getOption() == FileOption.INLINEBATCH) {
            // File command is a "here document" so pass in the current
            // input stream.
            reader = parentLineReader;
        }
        else {
            try {
                reader = adapter = new LineReaderAdapter(new FileReader(fileInfo.getFile()));
            }
            catch (FileNotFoundException e) {
                System.err.println("Script file '" + fileInfo.getFile() + "' could not be found.");
                stopOrContinue(e);
                return; // continue to the next line after the FILE command
            }
        }
        try {
            executeScriptFromReader(fileInfo, reader);
        }
        catch (Exception x) {
            stopOrContinue(x);
        }
        finally {
            if (adapter != null) {
                adapter.close();
            }
        }
    }

    /**
     *
     * @param fileInfo  The FileInfo object describing the file command (or stdin)
     * @param script    The line reader object to read from
     * @throws Exception
     */
    private static void executeScriptFromReader(FileInfo fileInfo, SQLCommandLineReader reader)
            throws Exception {

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
                if (statement.length() > 0) {
                    if (batch == null) {
                        String statementString = statement.toString();
                        // Trim here avoids a "missing statement" error from adhoc in an edge case
                        // like a blank line from stdin.
                        if ( ! statementString.trim().isEmpty()) {
                            //* enable to debug */if (m_debug) System.out.println("DEBUG QUERY:'" + statementString + "'");
                            executeStatements(statementString);
                        }
                    }
                    else {
                        // This means that batch did not end with a semicolon.
                        // Maybe it ended with a comment.
                        // For now, treat the final semicolon as optional and
                        // assume that we are not just adding a partial statement to the batch.
                        batch.append(statement);
                        executeDDLBatch(fileInfo.getFilePath(), batch.toString());
                    }
                }
                return;
            }

            if ( ! statementStarted) {
                if (line.trim().equals("") || SQLParser.isWholeLineComment(line)) {
                    // We don't strictly have to include a blank line or whole-line
                    // comment at the start of a statement, but when we want to preserve line
                    // numbers (in a batch), we should at least append a newline.
                    // Whether to echo comments or blank lines from a batch is
                    // a grey area.
                    if (batch != null) {
                        statement.append(line).append("\n");
                    }
                    continue;
                }
                // Recursively process FILE commands, any failure will cause a recursive failure
                FileInfo nestedFileInfo = SQLParser.parseFileStatement(fileInfo, line);
                if (nestedFileInfo != null) {
                    // Guards must be added for FILE Batch containing batches.
                    if (batch != null) {
                        stopOrContinue(new RuntimeException(
                                "A FILE command is invalid in a batch."));
                        continue; // continue to the next line after the FILE command
                    }

                    // Execute the file content or fail to but only set m_returningToPromptAfterError
                    // if the intent is to cause a recursive failure, stopOrContinue decided to stop.
                    executeScriptFile(nestedFileInfo, reader);
                    if (m_returningToPromptAfterError) {
                        // The recursive readScriptFile stopped because of an error.
                        // Escape to the outermost readScriptFile caller so it can exit or
                        // return to the interactive prompt.
                        return;
                    }
                    // Continue after a bad nested file command by processing the next line
                    // in the current file.
                    continue;
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
            if (SQLParser.isSemiColonTerminated(line)) {
                if (batch == null) {
                    String statementString = statement.toString();
                    // Trim here avoids a "missing statement" error from adhoc in an edge case
                    // like a blank line from stdin.
                    if ( ! statementString.trim().isEmpty()) {
                        //* enable to debug */ if (m_debug) System.out.println("DEBUG QUERY:'" + statementString + "'");
                        executeStatements(statementString);
                    }
                    statement.setLength(0);
                }
                statementStarted = false;
            }
            else {
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
    private static void executeStatements(String statements)
    {
        List<String> parsedStatements = SQLParser.parseQuery(statements);
        for (String statement: parsedStatements) {
            executeStatement(statement);
        }
    }

    private static void executeStatement(String statement)
    {
        if (m_testFrontEndOnly) {
            m_testFrontEndResult += statement + ";\n";
            return;
        }
        if ( !m_interactive && m_outputShowMetadata) {
            System.out.println();
            System.out.println(statement + ";");
        }
        try {
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
                    printDdlResponse(m_client.updateApplicationCatalog(catfile, depfile));

                    // Need to update the stored procedures after a catalog change (could have added/removed SPs!).  ENG-3726
                    loadStoredProcedures(Procedures, Classlist);
                }
                else if (procName.equals("@UpdateClasses")) {
                    File jarfile = null;
                    if (objectParams[0] != null) {
                        jarfile = new File((String)objectParams[0]);
                    }
                    printDdlResponse(m_client.updateClasses(jarfile, (String)objectParams[1]));
                    // Need to reload the procedures and classes
                    loadStoredProcedures(Procedures, Classlist);
                }
                else {
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
                loadStoredProcedures(Procedures, Classlist);
                return;
            }

            // REMOVE CLASS <class-selector>?
            String classSelector = SQLParser.parseRemoveClasses(statement);
            if (classSelector != null) {
                printDdlResponse(m_client.updateClasses(null, classSelector));
                loadStoredProcedures(Procedures, Classlist);
                return;
            }

            // DDL statements get forwarded to @AdHoc,
            // but get special post-processing.
            if (SQLParser.queryIsDDL(statement)) {
                // if the query is DDL, reload the stored procedures.
                printDdlResponse(m_client.callProcedure("@AdHoc", statement));
                loadStoredProcedures(Procedures, Classlist);
                return;
            }

            // All other commands get forwarded to @AdHoc
            printResponse(callProcedureHelper("@AdHoc", statement), true);

        } catch (Exception exc) {
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

    private static void printResponse(ClientResponse response, boolean suppressTableOutputForDML) throws Exception
    {
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new Exception("Execution Error: " + response.getStatusString());
        }

        long elapsedTime = System.nanoTime() - m_startTime;
        for (VoltTable t : response.getResults()) {
            long rowCount;
            if (suppressTableOutputForDML && isUpdateResult(t)) {
                rowCount = t.fetchRow(0).getLong(0);
            }
            else {
                rowCount = t.getRowCount();
                // Run it through the output formatter.
                m_outputFormatter.printTable(System.out, t, m_outputShowMetadata);
                //System.out.println("printable");
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
    private static Client m_client;
    // Default visibility is for test purposes.
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
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>())
                                                             .put( 1, Arrays.asList("varchar"))
                                                             .put( 3, Arrays.asList("varchar", "varchar", "bit")).build());
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
        Procedures.put("@ExplainView",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@ValidatePartitioning",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("int", "varbinary")).build());
        Procedures.put("@GetPartitionKeys",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@GC",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@ResetDR",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@SwapTables",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar")).build());
    }

    private static Client getClient(ClientConfig config, String[] servers, int port) throws Exception
    {
        final Client client = ClientFactory.createClient(config);

        // Only fail if we can't connect to any servers
        boolean connectedAnyServer = false;
        String connectionErrorMessages = "";

        for (String server : servers) {
            try {
                client.createConnection(server.trim(), port);
                connectedAnyServer = true;
            }
            catch (UnknownHostException e) {
                connectionErrorMessages += "\n    " + server.trim() + ":" + port + " - UnknownHostException";
            }
            catch (IOException e) {
                connectionErrorMessages += "\n    " + server.trim() + ":" + port + " - " + e.getMessage();
            }
        }

        if (!connectedAnyServer) {
            throw new IOException("Unable to connect to VoltDB cluster" + connectionErrorMessages);
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
        System.exit(exitCode);
    }

    // printHelp() can print readme either to a file or to the screen
    // depending on the argument passed in
    // Default visibility is for test purposes.
    static void printHelp(OutputStream prtStr)
    {
        try {
            InputStream is = SQLCommand.class.getResourceAsStream(m_readme);
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
        VoltTable tableData = m_client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
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
            procs = m_client.callProcedure("@SystemCatalog", "PROCEDURES").getResults()[0];
            params = m_client.callProcedure("@SystemCatalog", "PROCEDURECOLUMNS").getResults()[0];
            classes = m_client.callProcedure("@SystemCatalog", "CLASSES").getResults()[0];
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
            }
            else {
                ++curr_val;
            }
            proc_param_counts.put(this_proc, curr_val);
        }
        params.resetRowPosition();
        Set<String> userProcs = new HashSet<String>();
        while (procs.advanceRow()) {
            String proc_name = procs.getString("PROCEDURE_NAME");
            userProcs.add(proc_name);
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
        for (String proc_name : new ArrayList<String>(procedures.keySet())) {
            if (!proc_name.startsWith("@") && !userProcs.contains(proc_name)) {
                procedures.remove(proc_name);
            }
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
        testFrontEndOnly();
        try {
            SQLConsoleReader reader = new SQLConsoleReader(inmocked, outmocked);
            getInteractiveQueries(reader);
            return SQLParser.parseQuery(m_testFrontEndResult);
        } catch (Exception ioe) {}
        return null;
    }

    public static void testFrontEndOnly() {
        m_testFrontEndOnly = true;
        m_testFrontEndResult = "";
    }

    public static String getTestResult() { return m_testFrontEndResult; }

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
        System.setProperty("voltdb_no_logging", "true");
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
            }
            else if (arg.startsWith("--port=")) {
                port = Integer.valueOf(extractArgInput(arg));
            }
            else if (arg.startsWith("--user=")) {
                user = extractArgInput(arg);
            }
            else if (arg.startsWith("--password=")) {
                password = extractArgInput(arg);
            }
            else if (arg.startsWith("--kerberos=")) {
                kerberos = extractArgInput(arg);
            }
            else if (arg.startsWith("--kerberos")) {
                kerberos = "VoltDBClient";
            }
            else if (arg.startsWith("--query=")) {
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
            else if (arg.startsWith("--query-timeout=")) {
                m_hasBatchTimeout = true;
                m_batchTimeout = Integer.valueOf(extractArgInput(arg));
            }

            // equals check starting here
            else if (arg.equals("--output-skip-metadata")) {
                m_outputShowMetadata = false;
            }
            else if (arg.equals("--debug")) {
                m_debug = true;
            }
            else if (arg.equals("--help")) {
                printHelp(System.out); // Print readme to the screen
                System.out.println("\n\n");
                printUsage(0);
            }
            else if (arg.equals("--no-version-check")) {
                m_versionCheck = false; // Disable new version phone home check
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
        if (m_versionCheck) {
            openURLAsync();
        }

        try
        {
            // If we need to prompt the user for a password, do so.
            password = CLIConfig.readPasswordIfNeeded(user, password, "Enter password: ");
        }
        catch (IOException ex)
        {
            printUsage("Unable to read password: " + ex);
        }

        // Create connection
        ClientConfig config = new ClientConfig(user, password);
        config.setProcedureCallTimeout(0);  // Set procedure all to infinite timeout, see ENG-2670

        try {
            // if specified enable kerberos
            if (!kerberos.isEmpty()) {
                config.enableKerberosAuthentication(kerberos);
            }
            m_client = getClient(config, servers, port);
        } catch (Exception exc) {
            System.err.println(exc.getMessage());
            System.exit(-1);
        }

        try {
            if (! ddlFile.equals("")) {
                // fast DDL Loader mode
                // System.out.println("fast DDL Loader mode with DDL input:\n" + ddlFile);
                m_client.callProcedure("@AdHoc", ddlFile);
                System.exit(m_exitCode);
            }

            // Load system procedures
            loadSystemProcedures();

            // Load user stored procs
            loadStoredProcedures(Procedures, Classlist);

            // Removed code to prevent Ctrl-C from exiting. The original code is visible
            // in Git history hash 837df236c059b5b4362ffca7e7a5426fba1b7f20.

            m_interactive = true;
            if (queries != null && !queries.isEmpty()) {
                // If queries are provided via command line options run them in
                // non-interactive mode.
                //TODO: Someday we should honor batching.
                m_interactive = false;
                for (String query : queries) {
                    executeStatement(query);
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
                System.out.printf("SQL Command :: %s%s:%d\n", (user == "" ? "" : user + "@"), serverList, port);
                interactWithTheUser();
            }
        }
        catch (Exception x) {
            stopOrContinue(x);
        }
        finally {
            try { m_client.close(); } catch (Exception x) { }
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
