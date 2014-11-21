/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.console.CursorBuffer;
import jline.console.KeyMap;
import jline.console.completer.Completer;
import jline.console.history.FileHistory;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import com.google_voltpatches.common.collect.ImmutableMap;

public class SQLCommand
{
    private static final Map<String, String> FRIENDLY_TYPE_NAMES =
            ImmutableMap.<String, String>builder().put("tinyint", "byte numeric")
                                                  .put("smallint", "short numeric")
                                                  .put("int", "numeric")
                                                  .put("integer", "numeric")
                                                  .put("bigint", "long numeric")
                                                  .build();
    private static boolean m_stopOnError = true;
    private static boolean m_debug = false;
    private static boolean m_interactive;
    private static boolean m_returningToPromptAfterError = false;
    private static int m_exitCode = 0;

    // SQL Parsing
    private static final Pattern EscapedSingleQuote = Pattern.compile("''", Pattern.MULTILINE);
    private static final Pattern SingleLineComments = Pattern.compile("^\\s*(\\/\\/|--).*$", Pattern.MULTILINE);
    private static final Pattern Extract = Pattern.compile("'[^']*'", Pattern.MULTILINE);
    private static final Pattern AutoSplit = Pattern.compile("(\\s|((\\(\\s*)+))(alter|create|drop|select|insert|update|upsert|delete|truncate|exec|execute|explain|explainproc)\\s", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    private static final Pattern SetOp = Pattern.compile("(\\s|\\))\\s*(union|except|intersect)(\\s\\s*all)?((\\s*\\({0,1}\\s*)*)select", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    private static final Pattern Subquery =
            Pattern.compile("(\\s*)(,|(?:\\s(?:from|in|exists|join)))((\\s*\\(\\s*)*)select",
                            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);

    private static final String quotedIdPattern = "\"(?:[^\"]|\"\")+\""; // double-quoted ID, " escaped with ""
                                                                   // question: is 0-length name allowed?
    private static final String unquotedIdPattern = "[a-z][a-z0-9_]*";
    private static final String idPattern = "(?:" + unquotedIdPattern + "|" + quotedIdPattern + ")";

    // This pattern consumes no input itself but ensures that the next
    // character is either whitespace or a double quote. This is handy
    // when a keyword is followed by an identifier:
    //   INSERT INTO"Foo"SELECT ...
    // HSQL doesn't require whitespace between keywords and quoted
    // identifiers.
    private static String followedBySpaceOrQuote = "(?=\"|\\s)";

    // Ugh, these are all fragile.
    private static final Pattern CreateView =
        Pattern.compile(
                "(\\s*)" + // 0 or more spaces
                "(" + // start group 2
                "create\\s+view" + // 'create view'
                "\\s+" + // 1 or more spaces
                "((?!create\\s+(view|procedure)).)*" + // any string that doesn't contain 'create view'
                                                       // or 'create procedure' again
                "\\s+" + // 1 or more spaces
                "as" +
                "\\s+" + // 1 or more spaces
                ")" + // end group 2
                "select",
                Pattern.MULTILINE + Pattern.CASE_INSENSITIVE + Pattern.DOTALL);
    private static final Pattern CreateProcedureSelect =
        Pattern.compile(
                "(\\s*)" + // 0 or more spaces
                "(" + // start group 2
                "create\\s+procedure" + // 'create procedure'
                "\\s+" + // 1 or more spaces
                "((?!create\\s+(view|procedure)).)*" + // any string that doesn't contain 'create view'
                                                       // or 'create procedure' again
                "\\s+" + // 1 or more spaces
                "as" +
                "\\s+" + // 1 or more spaces
                ")" + // end group 2
                "select",
                Pattern.MULTILINE + Pattern.CASE_INSENSITIVE + Pattern.DOTALL);
    private static final Pattern CreateProcedureInsert =
        Pattern.compile(
                "(\\s*)" + // 0 or more spaces
                "(" + // start group 2
                "create\\s+procedure" + // 'create procedure'
                "\\s+" + // 1 or more spaces
                "((?!create\\s+(view|procedure)).)*" + // any string that doesn't contain 'create view'
                                                       // or 'create procedure' again
                "\\s+" + // 1 or more spaces
                "as" +
                "\\s+" + // 1 or more spaces
                ")" + // end group 2
                "insert",
                Pattern.MULTILINE + Pattern.CASE_INSENSITIVE + Pattern.DOTALL);
    private static final Pattern CreateProcedureUpdate =
        Pattern.compile(
                "(\\s*)" + // 0 or more spaces
                "(" + // start group 2
                "create\\s+procedure" + // 'create procedure'
                "\\s+" + // 1 or more spaces
                "((?!create\\s+(view|procedure)).)*" + // any string that doesn't contain 'create view'
                                                       // or 'create procedure' again
                "\\s+" + // 1 or more spaces
                "as" +
                "\\s+" + // 1 or more spaces
                ")" + // end group 2
                "update",
                Pattern.MULTILINE + Pattern.CASE_INSENSITIVE + Pattern.DOTALL);
    private static final Pattern CreateProcedureDelete =
        Pattern.compile(
                "(\\s*)" + // 0 or more spaces
                "(" + // start group 2
                "create\\s+procedure" + // 'create procedure'
                "\\s+" + // 1 or more spaces
                "((?!create\\s+(view|procedure)).)*" + // any string that doesn't contain 'create view'
                                                       // or 'create procedure' again
                "\\s+" + // 1 or more spaces
                "as" +
                "\\s+" + // 1 or more spaces
                ")" + // end group 2
                "delete",
                Pattern.MULTILINE + Pattern.CASE_INSENSITIVE + Pattern.DOTALL);

    // For HSQL's purposes, the optional column list that may appear
    // in an INSERT statement is recognized as:
    // - a left parenthesis
    // - 1 or more of the following items:
    //   - quoted identifiers (which may contain right parentheses)
    //   - characters which are not double quotes or right parentheses
    // - a right parenthesis
    // This should recognize whatever may appear inside the column
    // list, including quoted column names with embedded parentheses.
    private static final String optionalColumnList = "(?:\\((?:" + quotedIdPattern + "|[^\")])+\\))?";
    private static final Pattern InsertIntoSelect =
            Pattern.compile(
                    "(" +                  // start capturing group
                    "\\s*" +               // leading whitespace
                    "(?:insert|upsert)\\s+into" + followedBySpaceOrQuote + "\\s*" +
                    idPattern + "\\s*" +   // <tablename>
                    optionalColumnList +   // (column, "anotherColumn", ...)
                    "[(\\s]*" +            // 0 or more spaces or left parentheses
                    ")" +                  // end capturing group
                    "select",
                    Pattern.MULTILINE + Pattern.CASE_INSENSITIVE + Pattern.DOTALL);

    // the common prefix for both ALTER TABLE <table> DROP
    // and ALTER TABLE <table> ALTER
    private static String alterTableCommonPrefix =
            "\\s*alter\\s*table" + followedBySpaceOrQuote + "\\s*" +
            idPattern + "\\s*";
    private static final Pattern AlterTableAlter =
            Pattern.compile(
                    "(" + alterTableCommonPrefix + ")alter",
                    Pattern.MULTILINE + Pattern.CASE_INSENSITIVE + Pattern.DOTALL);
    private static final Pattern AlterTableDrop =
            Pattern.compile(
                    "(" + alterTableCommonPrefix + ")drop",
                    Pattern.MULTILINE + Pattern.CASE_INSENSITIVE + Pattern.DOTALL);

    private static final Pattern AutoSplitParameters = Pattern.compile("[\\s,]+", Pattern.MULTILINE);
    /**
     * Matches a command followed by and SQL CRUD statement verb
     */
    private static final Pattern ParserStringKeywords = Pattern.compile(
            "\\s*" + // 0 or more spaces
            "(" + // start group 1
              "exec|execute|explain|explainproc" + // command
            ")" +  // end group 1
            "\\s+" + // one or more spaces
            "(" + // start group 2
              "select|insert|update|upsert|delete|truncate" + // SQL CRUD statement verb
            ")" + // end group 2
            "\\s+", // one or more spaces
            Pattern.MULTILINE|Pattern.CASE_INSENSITIVE
    );
    private static final String readme = "SQLCommandReadme.txt";

    public static String getReadme() {
        return readme;
    }

    public static Pattern getExecuteCall() {
        return ExecuteCall;
    }

    public static List<String> parseQuery(String query)
    {
        if (query == null) {
            return null;
        }

        //* enable to debug */ System.err.println("Parsing command queue:\n" + query);
        /*
         * Mark any parser string keyword matches by interposing the #SQL_PARSER_STRING_KEYWORD#
         * tag. Which is later stripped at the end of this procedure. This tag is here to
         * aide the evaluation of SetOp and AutoSplit REGEXPs, meaning that an
         * 'explain select foo from bar will cause SetOp and AutoSplit match on the select as
         * is prefixed with the #SQL_PARSER_STRING_KEYWORD#
         *
         * For example
         *     'explain select foo from bar'
         *  becomes
         *     'explain #SQL_PARSER_STRING_KEYWORD#select foo from bar'
         */
        query = ParserStringKeywords.matcher(query).replaceAll(" $1 #SQL_PARSER_STRING_KEYWORD#$2 ");
        /*
         * strip out single line comments
         */
        query = SingleLineComments.matcher(query).replaceAll("");
        /*
         * replace all escaped single quotes with the #(SQL_PARSER_ESCAPE_SINGLE_QUOTE) tag
         */
        query = EscapedSingleQuote.matcher(query).replaceAll("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)");

        /*
         * move all single quoted strings into the string fragments list, and do in place
         * replacements with numbered instances of the #(SQL_PARSER_STRING_FRAGMENT#[n]) tag
         *
         */
        Matcher stringFragmentMatcher = Extract.matcher(query);
        ArrayList<String> stringFragments = new ArrayList<String>();
        int i = 0;
        while (stringFragmentMatcher.find()) {
            stringFragments.add(stringFragmentMatcher.group());
            query = stringFragmentMatcher.replaceFirst("#(SQL_PARSER_STRING_FRAGMENT#" + i + ")");
            stringFragmentMatcher = Extract.matcher(query);
            i++;
        }

        /*
         * Mark all SQL keywords that are part of another statement so they don't get auto-split
         */
        query = SetOp.matcher(query).replaceAll("$1$2$3$4SQL_PARSER_SAME_SELECT");
        query = Subquery.matcher(query).replaceAll("$1$2$3SQL_PARSER_SAME_SELECT");
        query = CreateView.matcher(query).replaceAll("$1$2SQL_PARSER_SAME_CREATEVIEW");
        query = CreateProcedureSelect.matcher(query).replaceAll("$1$2SQL_PARSER_SAME_CREATESELECT");
        query = CreateProcedureInsert.matcher(query).replaceAll("$1$2SQL_PARSER_SAME_CREATEINSERT");
        query = CreateProcedureUpdate.matcher(query).replaceAll("$1$2SQL_PARSER_SAME_CREATEUPDATE");
        query = CreateProcedureDelete.matcher(query).replaceAll("$1$2SQL_PARSER_SAME_CREATEDELETE");
        query = InsertIntoSelect.matcher(query).replaceAll("$1SQL_PARSER_SAME_INSERTINTOSELECT");
        query = AlterTableAlter.matcher(query).replaceAll("$1SQL_PARSER_SAME_ALTERTABLEALTER");
        query = AlterTableDrop.matcher(query).replaceAll("$1SQL_PARSER_SAME_ALTERTABLEDROP");
        query = AutoSplit.matcher(query).replaceAll(";$2$4 "); // there be dragons here
        query = query.replaceAll("SQL_PARSER_SAME_SELECT", "select");
        query = query.replaceAll("SQL_PARSER_SAME_CREATEVIEW", "select");
        query = query.replaceAll("SQL_PARSER_SAME_CREATESELECT", "select");
        query = query.replaceAll("SQL_PARSER_SAME_CREATEINSERT", "insert");
        query = query.replaceAll("SQL_PARSER_SAME_CREATEUPDATE", "update");
        query = query.replaceAll("SQL_PARSER_SAME_CREATEDELETE", "delete");
        query = query.replaceAll("SQL_PARSER_SAME_INSERTINTOSELECT", "select");
        query = query.replaceAll("SQL_PARSER_SAME_ALTERTABLEALTER", "alter");
        query = query.replaceAll("SQL_PARSER_SAME_ALTERTABLEDROP", "drop");
        String[] sqlFragments = query.split("\\s*;+\\s*");

        ArrayList<String> queries = new ArrayList<String>();
        for (String fragment : sqlFragments) {
            fragment = fragment.trim();
            if (fragment.isEmpty()) {
                continue;
            }
            if (fragment.indexOf("#(SQL_PARSER_STRING_FRAGMENT#") > -1) {
                int k = 0;
                for (String strFrag : stringFragments) {
                    fragment = fragment.replace("#(SQL_PARSER_STRING_FRAGMENT#" + k + ")", strFrag);
                    k++;
                }
            }
            fragment = fragment.replace("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)", "''");
            fragment = fragment.replace("#SQL_PARSER_STRING_KEYWORD#","");
            queries.add(fragment);
        }
        return queries;
    }

    public static List<String> parseQueryProcedureCallParameters(String query)
    {
        if (query == null) {
            return null;
        }

        query = SingleLineComments.matcher(query).replaceAll("");
        query = EscapedSingleQuote.matcher(query).replaceAll("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)");
        Matcher stringFragmentMatcher = Extract.matcher(query);
        ArrayList<String> stringFragments = new ArrayList<String>();
        int i = 0;
        while (stringFragmentMatcher.find()) {
            stringFragments.add(stringFragmentMatcher.group());
            query = stringFragmentMatcher.replaceFirst("#(SQL_PARSER_STRING_FRAGMENT#" + i + ")");
            stringFragmentMatcher = Extract.matcher(query);
            i++;
        }
        query = AutoSplitParameters.matcher(query).replaceAll(",");
        String[] sqlFragments = query.split("\\s*,+\\s*");
        ArrayList<String> queries = new ArrayList<String>();
        for(int j = 0;j<sqlFragments.length;j++)
        {
            sqlFragments[j] = sqlFragments[j].trim();
            if (sqlFragments[j].length() != 0)
            {
                if(sqlFragments[j].indexOf("#(SQL_PARSER_STRING_FRAGMENT#") > -1) {
                    for(int k = 0;k<stringFragments.size();k++) {
                        sqlFragments[j] = sqlFragments[j].replace("#(SQL_PARSER_STRING_FRAGMENT#" + k + ")", stringFragments.get(k));
                    }
                }
                sqlFragments[j] = sqlFragments[j].replace("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)", "''");
                sqlFragments[j] = sqlFragments[j].trim();
                queries.add(sqlFragments[j]);
            }
        }
        return queries;
    }

    // Command line interaction
    private static SQLConsoleReader lineInputReader = null;
    private static FileHistory historyFile = null;

    private static final Pattern HelpToken = Pattern.compile("^\\s*help;*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern GoToken = Pattern.compile("^\\s*go;*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ExitToken = Pattern.compile("^\\s*(exit|quit);*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ListProceduresToken = Pattern.compile("^\\s*((?:list|show)\\s+proc|(?:list|show)\\s+procedures);*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ListTablesToken = Pattern.compile("^\\s*((?:list|show)\\s+tables);*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ListClassesToken = Pattern.compile("^\\s*((?:list|show)\\s+classes);*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SemicolonToken = Pattern.compile("^.*\\s*;+\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern RecallToken = Pattern.compile("^\\s*recall\\s*([^;]+)\\s*;*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern FileToken = Pattern.compile("^\\s*file\\s*['\"]*([^;'\"]+)['\"]*\\s*;*\\s*", Pattern.CASE_INSENSITIVE);
    private static int LineIndex = 1;
    private static List<String> Lines = new ArrayList<String>();

    /**
     * The list of recognized basic tab-complete-able SQL command prefixes.
     * Comparisons are done in uppercase.
     */
    static final String[] m_commandPrefixes = new String[] {
        "DELETE",
        "EXEC",
        "EXIT",
        "EXPLAIN",
        "EXPLAINPROC",
        "FILE",
        "GO",
        "HELP",
        "INSERT",
        "LIST PROCEDURES",
        "LIST TABLES",
        "LIST CLASSES",
        "SHOW PROCEDURES",
        "SHOW TABLES",
        "SHOW CLASSES",
        "QUIT",
        "RECALL",
        "SELECT",
        "UPDATE",
    };

    public static List<String> getQuery(boolean interactive) throws Exception
    {
        StringBuilder query = new StringBuilder();
        boolean isRecall = false;
        String line = null;

        do
        {
            if (interactive)
            {
                if (isRecall)
                {
                    isRecall = false;
                    line = lineInputReader.readLine("");

                } else {
                    line = lineInputReader.readLine((LineIndex++) + "> ");
                }
            } else {
                line = lineInputReader.readLine();
                //* enable to debug */ if (line == null) {
                //* enable to debug */     System.err.println("Read null batch line.");
                //* enable to debug */ }
                //* enable to debug */ else {
                //* enable to debug */     System.err.println("Read non-null batch line: (" + line + ")");
                //* enable to debug */ }
            }

            if (line == null)
            {
                if (query == null) {
                    return null;
                } else {
                    return parseQuery(query.toString());
                }
            }

            // Process recall commands - ONLY in interactive mode
            if (interactive && RecallToken.matcher(line).matches())
            {
                    Matcher m = RecallToken.matcher(line);
                    if (m.find())
                    {
                        int recall = -1;
                        try { recall = Integer.parseInt(m.group(1))-1; } catch(Exception x){}
                        if (recall > -1 && recall < Lines.size())
                        {
                            line = Lines.get(recall);
                            lineInputReader.putString(line);
                            lineInputReader.flush();
                            isRecall = true;
                            continue;
                        } else {
                            System.out.printf("%s> Invalid RECALL reference: '" + m.group(1) + "'.\n", LineIndex-1);
                        }
                    } else {
                        System.out.printf("%s> Invalid RECALL command: '" + line + "'.\n", LineIndex-1);
                    }
            }

            // Strip out invalid recall commands
            if (RecallToken.matcher(line).matches()) {
                line = "";
            }

            // Queue up the line to the recall stack - ONLY in interactive mode
            if (interactive) {
                Lines.add(line);
            }

            // EXIT command - ONLY in interactive mode, exit immediately (without running any queued statements)
            if (ExitToken.matcher(line).matches())
            {
                if (interactive) {
                    return null;
                }
            }
            // LIST PROCEDURES command
            else if (ListProceduresToken.matcher(line).matches())
            {
                if (interactive)
                {
                    List<String> list = new LinkedList<String>(Procedures.keySet());
                    Collections.sort(list);
                    int padding = 0;
                    for(String procedure : list) {
                        if (padding < procedure.length()) {
                            padding = procedure.length();
                        }
                    }
                    padding++;
                    String format = "%1$-" + padding + "s";
                    for(int i = 0;i<2;i++)
                    {
                        int j = 0;
                        for(String procedure : list)
                        {
                            if (i == 0 && procedure.startsWith("@")) {
                                continue;
                            } else if (i == 1 && !procedure.startsWith("@")) {
                                continue;
                            }
                            if (j == 0)
                            {
                                if (i == 0) {
                                    System.out.println("\n--- User Procedures ----------------------------------------");
                                } else {
                                    System.out.println("\n--- System Procedures --------------------------------------");
                                }
                            }
                            for (List<String> parameterSet : Procedures.get(procedure).values()) {
                                System.out.printf(format, procedure);
                                System.out.print("\t");
                                int pidx = 0;
                                for(String paramType : parameterSet)
                                {
                                    if (pidx > 0) {
                                        System.out.print(", ");
                                    }
                                    System.out.print(paramType);
                                    pidx++;
                                }
                                System.out.print("\n");
                            }
                            j++;
                        }
                    }
                    System.out.print("\n");
                }
            }
            // LIST TABLES command
            else if (ListTablesToken.matcher(line).matches())
            {
                if (interactive)
                {
                    Tables tables = getTables();
                    printTables("User Tables", tables.tables);
                    printTables("User Views", tables.views);
                    printTables("User Export Streams", tables.exports);
                    System.out.print("\n");
                }
            }
            // SHOW CLASSES
            else if (ListClassesToken.matcher(line).matches()) {
                if (interactive) {
                    if (Classlist.isEmpty()) {
                        System.out.println("\n--- Empty Class List -----------------------\n");
                    }
                    List<String> list = new LinkedList<String>(Classlist.keySet());
                    Collections.sort(list);
                    int padding = 0;
                    for(String classname : list) {
                        if (padding < classname.length()) {
                            padding = classname.length();
                        }
                    }
                    padding++;
                    String format = "%1$-" + padding + "s";
                    for(int i = 0;i<3;i++)
                    {
                        int j = 0;
                        for(String classname : list)
                        {
                            List<Boolean> stuff = Classlist.get(classname);
                            // Print non-active procs first
                            if (i == 0 && !(stuff.get(0) && !stuff.get(1))) {
                                continue;
                            } else if (i == 1 && !(stuff.get(0) && stuff.get(1))) {
                                continue;
                            } else if (i == 2 && stuff.get(0)) {
                                continue;
                            }
                            if (j == 0)
                            {
                                if (i == 0) {
                                    System.out.println("\n--- Potential Procedure Classes ----------------------------");
                                } else if (i == 1) {
                                    System.out.println("\n--- Active Procedure Classes  ------------------------------");
                                } else {
                                    System.out.println("\n--- Non-Procedure Classes ----------------------------------");
                                }
                            }
                            System.out.printf(format, classname);
                            System.out.print("\n");
                            j++;
                        }
                    }
                    System.out.print("\n");
                }
            }
            // GO commands - ONLY in interactive mode, close batch and parse for execution
            else if (GoToken.matcher(line).matches())
            {
                if (interactive) {
                    return parseQuery(query.toString().trim());
                }
            }
            // HELP commands - ONLY in interactive mode, close batch and parse for execution
            else if (HelpToken.matcher(line).matches())
            {
                if (interactive) {
                    printHelp(System.out); // Print readme to the screen
                }
            }
            else {
                // Was there a line-ending semicolon typed at the prompt?
                boolean executeImmediate =
                        interactive && SemicolonToken.matcher(line).matches();
                // If the line is a FILE command - include the content of the file into the query queue
                Matcher fileMatcher = FileToken.matcher(line);
                if (fileMatcher.matches()) {
                    // Get the line(s) from the file(s) to queue as regular database commands
                    // or get back a null if in the recursive call, stopOrContinue decided to continue.
                    line = readScriptFile(fileMatcher.group(1));
                    if (m_returningToPromptAfterError) {
                        // readScriptFile stopped because of an error. Wipe the slate clean.
                        query = new StringBuilder();
                        line = null;
                        m_returningToPromptAfterError = false;
                    }
                    // else treat the line(s) from the file(s) as regular database commands
                }
                // else treat the input line as a regular database command

                // Collect the lines ...
                query.append(line);
                query.append("\n");

                // ... until there was a line-ending semicolon typed at the prompt.
                if (executeImmediate) {
                    return parseQuery(query.toString().trim());
                }
            }
            line = null;
        }
        while(true);
    }

    private static void printTables(final String name, final Collection<String> tables)
    {
        System.out.printf("\n--- %s --------------------------------------------\n", name);
        for (String table : tables) {
            System.out.println(table);
        }
        System.out.print("\n");
    }

    public static String readScriptFile(String filePath)
    {
        BufferedReader script = null;
        try {
            script = new BufferedReader(new FileReader(filePath));
        }
        catch (FileNotFoundException e) {
            System.err.println("Script file '" + filePath + "' could not be found.");
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
                if (RecallToken.matcher(line).matches() ||
                      ExitToken.matcher(line).matches() ||
                      GoToken.matcher(line).matches()) {
                    continue;
                }
                // Recursively process FILE commands, any failure will cause a recursive failure
                Matcher fileMatcher = FileToken.matcher(line);
                if (fileMatcher.matches()) {
                    // Get the line(s) from the file(s) to queue as regular database commands
                    // or get back a null if in the recursive call, stopOrContinue decided to continue.
                    line = readScriptFile(fileMatcher.group(1));
                    if (m_returningToPromptAfterError) {
                        // The recursive readScriptFile stopped because of an error.
                        // Escape to the outermost readScriptFile caller so it can exit or
                        // return to the interactive prompt.
                        return null;
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

    // Query Execution
    private static final Pattern ExecuteCall = Pattern.compile("^(exec|execute) ", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explain" (case insensitive).  We'll convert them to @Explain invocations.
    private static final Pattern ExplainCall = Pattern.compile("^explain ", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explainproc" (case insensitive).  We'll convert them to @ExplainProc invocations.
    private static final Pattern ExplainProcCall = Pattern.compile("^explainProc ", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    private static final Pattern StripCRLF = Pattern.compile("[\r\n]+", Pattern.MULTILINE);
    private static final SimpleDateFormat DateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Pattern Unquote = Pattern.compile("^'|'$", Pattern.MULTILINE);

    private static long m_startTime;
    private static void executeQuery(String query)
    {
        try {
            m_startTime = System.nanoTime();
            Matcher matcher = ExecuteCall.matcher(query);
            if (matcher.find()) {
                query = matcher.replaceFirst("");
                List<String> params = parseQueryProcedureCallParameters(query);
                String procedure = params.remove(0);
                Map<Integer, List<String>> signature = Procedures.get(procedure);
                if (signature == null) {
                    throw new Exception("Undefined procedure: " + procedure);
                }

                List<String> paramTypes = signature.get(params.size());
                if (paramTypes == null || params.size() != paramTypes.size()) {
                    String expectedSizes = "";
                    for (Integer expectedSize : signature.keySet()) {
                        expectedSizes += expectedSize + ", ";
                    }
                    throw new Exception("Invalid parameter count for procedure: " + procedure + "(expected: " + expectedSizes + " received: " + params.size() + ")");
                }
                Object[] objectParams = new Object[params.size()];
                if (procedure.equals("@SnapshotDelete")) {
                    objectParams[0] = new String[] { Unquote.matcher(params.get(0)).replaceAll("").replace("''","'") };
                    objectParams[1] = new String[] { Unquote.matcher(params.get(1)).replaceAll("").replace("''","'") };
                }
                else {
                    int i = 0;
                    try {
                        for (; i < params.size(); i++) {
                            String paramType = paramTypes.get(i);
                            String param = params.get(i);
                            Object objParam = null;
                            // For simplicity, handle first the types that don't allow null as a special value.
                            if (paramType.equals("bit")) {
                                //TODO: upper/mixed case Yes and True should be treated as "1"?
                                //TODO: non-0 integers besides 1 should be treated as "1"?
                                //TODO: garbage values and null should be rejected, not accepted as "0":
                                //      (case-insensitive) "no"/"false"/"0" should be required for "0"?
                                if (param.equals("yes") || param.equals("true") || param.equals("1")) {
                                    objParam = (byte)1;
                                } else {
                                    objParam = (byte)0;
                                }
                            }
                            else if (paramType.equals("statisticscomponent") ||
                                     paramType.equals("sysinfoselector") ||
                                     paramType.equals("metadataselector")) {
                                objParam = preprocessParam(param);
                            }
                            else if ( ! "null".equalsIgnoreCase(param)) {
                                if (paramType.equals("tinyint")) {
                                    objParam = Byte.parseByte(param);
                                }
                                else if (paramType.equals("smallint")) {
                                    objParam = Short.parseShort(param);
                                }
                                else if (paramType.equals("int") || paramType.equals("integer")) {
                                    objParam = Integer.parseInt(param);
                                }
                                else if (paramType.equals("bigint")) {
                                    objParam = Long.parseLong(param);
                                }
                                else if (paramType.equals("float")) {
                                    objParam = Double.parseDouble(param);
                                }
                                else if (paramType.equals("varchar")) {
                                    objParam = Unquote.matcher(param).replaceAll("").replace("''","'");
                                }
                                else if (paramType.equals("decimal")) {
                                    objParam = new BigDecimal(param);
                                }
                                else if (paramType.equals("timestamp")) {
                                    // Remove any quotes around the timestamp value.  ENG-2623
                                    objParam = DateParser.parse(param.replaceAll("^\"|\"$", "").replaceAll("^'|'$", ""));
                                }
                                else if (paramType.equals("varbinary") || paramType.equals("tinyint_array")) {
                                    String val = Unquote.matcher(param).replaceAll("");
                                    objParam = Encoder.hexDecode(val);
                                    // Make sure we have an even number of characters, otherwise it is an invalid byte string
                                    if (param.length() % 2 == 1) {
                                        throw new RuntimeException("Invalid varbinary value (" + param + ") (param " + (i+1) +
                                                ") :  must have an even number of hex characters to be valid.");
                                    }
                                }
                                else {
                                    throw new Exception("Unsupported Data Type: " + paramType);
                                }
                            } // else param is keyword "null", so leave objParam as null.
                            objectParams[i] = objParam;
                        }
                    } catch (NumberFormatException nfe) {
                        throw new RuntimeException("Invalid parameter:  Expected a " +
                                friendlyTypeDescription(paramTypes.get(i)) +
                                " value, got '" + params.get(i) + "' (param " + (i+1) + ").", nfe);
                    }
                }
                if (procedure.equals("@UpdateApplicationCatalog")) {
                    File catfile = null;
                    if (objectParams[0] != null) {
                        catfile = new File((String)objectParams[0]);
                    }
                    File depfile = null;
                    if (objectParams[1] != null) {
                        depfile = new File((String)objectParams[1]);
                    }
                    printResponse(VoltDB.updateApplicationCatalog(catfile, depfile));

                    // Need to update the stored procedures after a catalog change (could have added/removed SPs!).  ENG-3726
                    loadStoredProcedures(Procedures, Classlist);
                }
                else if (procedure.equals("@UpdateClasses")) {
                    File jarfile = null;
                    if (objectParams[0] != null) {
                        jarfile = new File((String)objectParams[0]);
                    }
                    printResponse(VoltDB.updateClasses(jarfile, (String)objectParams[1]));
                    // Need to reload the procedures and classes
                    loadStoredProcedures(Procedures, Classlist);
                }
                else {
                    printResponse(VoltDB.callProcedure(procedure, objectParams));
                }
            }
            else if (ExplainCall.matcher(query).find()) {
                // We've got a query that starts with "explain", send the query to
                // @Explain (after stripping "explain").
                // This all could probably be done more elegantly via a group extracted
                // from a more comprehensive regexp.
                query = query.substring("explain ".length());
                query = StripCRLF.matcher(query).replaceAll(" ");
                printResponse(VoltDB.callProcedure("@Explain", query));
            }
            else if (ExplainProcCall.matcher(query).find()) {
                // We've got a query that starts with "explainproc", send the proc name
                // to @ExplainPlan (after stripping "explainproc").
                // This all could probably be done more elegantly via a group extracted
                // from a more comprehensive regexp.
                query = query.substring("explainProc ".length());
                query = StripCRLF.matcher(query).replaceAll(" ");
                // Clean up any extra spaces from between explainproc and the proc name.
                query = query.trim();
                printResponse(VoltDB.callProcedure("@ExplainProc", query));
            }
            else { // All other commands get forwarded to @AdHoc
                query = StripCRLF.matcher(query).replaceAll(" ");
                printResponse(VoltDB.callProcedure("@AdHoc", query));
                // if the query was DDL, reload the stored procedures.
                if (SQLLexer.extractDDLToken(query) != null) {
                    loadStoredProcedures(Procedures, Classlist);
                }
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

    private static String friendlyTypeDescription(String paramType) {
        String friendly = FRIENDLY_TYPE_NAMES.get(paramType);
        if (friendly != null) {
            return friendly;
        }
        return paramType;
    }

    // Uppercase param.
    // Remove any quotes.
    // Trim
    private static String preprocessParam(String param)
    {
        if ((param.charAt(0) == '\'' && param.charAt(param.length()-1) == '\'') ||
                (param.charAt(0) == '"' && param.charAt(param.length()-1) == '"')) {
            // The position of the closing quote, param.length()-1 is where to end the substring
            // to get a result with two fewer characters.
            param = param.substring(1, param.length()-1);
        }
        param = param.trim();
        param = param.toUpperCase();
        return param;
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
                System.out.printf("\n\n(Returned %d rows in %.2fs)\n",
                        rowCount, elapsedTime / 1000000000.0);
            }
        }
    }

    // VoltDB connection support
    private static Client VoltDB;
    private static Map<String,Map<Integer, List<String>>> Procedures =
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

    static public void mockVoltDBForTest(Client testVoltDB) { VoltDB = testVoltDB; }

    static public void mockLineReaderForTest(SQLConsoleReader reader) { lineInputReader = reader; }

    private static InputStream in = null;
    private static OutputStream out = null;

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

        // Parse out parameters
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--servers=")) {
                serverList = arg.split("=")[1];
            } else if (arg.startsWith("--port=")) {
                port = Integer.valueOf(arg.split("=")[1]);
            } else if (arg.startsWith("--user=")) {
                user = arg.split("=")[1];
            } else if (arg.startsWith("--password=")) {
                password = arg.split("=")[1];
            } else if (arg.startsWith("--kerberos=")) {
                kerberos = arg.split("=")[1];
            } else if (arg.startsWith("--kerberos")) {
                kerberos = "VoltDBClient";
            } else if (arg.startsWith("--query=")) {
                List<String> argQueries = parseQuery(arg.substring(8));
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
                String formatName = arg.split("=")[1].toLowerCase();
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
                String optionName = arg.split("=")[1].toLowerCase();
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

        // Don't ask... Java is such a crippled language!
        DateParser.setLenient(true);

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
            // Load system procedures
            loadSystemProcedures();

            // Load user stored procs
            loadStoredProcedures(Procedures, Classlist);

            in = new FileInputStream(FileDescriptor.in);
            out = System.out;
            lineInputReader = new SQLConsoleReader(in, out);

            lineInputReader.setBellEnabled(false);

            // Provide a custom completer.
            Completer completer = new SQLCompleter(m_commandPrefixes);
            lineInputReader.addCompleter(completer);

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

            boolean interactive = true;
            if (queries != null && !queries.isEmpty()) {
                // If queries are provided via command line options run them in
                // non-interactive mode.
                //TODO: Someday we should honor batching.
                interactive = false;
                for (String query : queries) {
                    executeQuery(query);
                }
            }
            if (System.in.available() > 0) {
                // If Standard input comes loaded with data, run in non-interactive mode
                interactive = false;
                queries = getQuery(false);
                if (queries != null) {
                    for (String query : queries) {
                        executeQuery(query);
                    }
                }
            }
            if (interactive) {
                // Print out welcome message
                System.out.printf("SQL Command :: %s%s:%d\n", (user == "" ? "" : user + "@"), serverList, port);

                while ((queries = getQuery(true)) != null) {
                    for (String query : queries) {
                        executeQuery(query);
                    }
                }
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
