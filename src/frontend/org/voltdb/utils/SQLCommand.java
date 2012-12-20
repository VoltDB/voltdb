/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.ConsoleReader;
import jline.SimpleCompletor;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import com.google.common.collect.ImmutableMap;

public class SQLCommand
{
    // SQL Parsing
    private static final Pattern EscapedSingleQuote = Pattern.compile("''", Pattern.MULTILINE);
    private static final Pattern SingleLineComments = Pattern.compile("^\\s*(\\/\\/|--).*$", Pattern.MULTILINE);
    private static final Pattern Extract = Pattern.compile("'[^']*'", Pattern.MULTILINE);
    private static final Pattern AutoSplit = Pattern.compile("\\s(select|insert|update|delete|exec|execute|explain|explainproc)\\s", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    private static final Pattern AutoSplitParameters = Pattern.compile("[\\s,]+", Pattern.MULTILINE);
    private static final String readme = "SQLCommandReadme.txt";

    public static String getReadme() {
        return readme;
    }

    public static Pattern getExecuteCall() {
        return ExecuteCall;
    }

    public static List<String> parseQuery(String query)
    {
        if (query == null)
            return null;

        String[] command = new String[] {"exec", "execute", "explain", "explainproc"};
        String[] keyword = new String[] {"select", "insert", "update", "delete"};
        for(int i = 0;i<command.length;i++)
        {
            for(int j = 0;j<keyword.length;j++)
            {
                Pattern r = Pattern.compile("\\s*(" + command[i].replace(" ","\\s+") + ")\\s+(" + keyword[j] + ")\\s*", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
                query = r.matcher(query).replaceAll(" $1 #SQL_PARSER_STRING_KEYWORD#$2 ");
            }
        }

        query = SingleLineComments.matcher(query).replaceAll("");
        query = EscapedSingleQuote.matcher(query).replaceAll("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)");
        Matcher stringFragmentMatcher = Extract.matcher(query);
        ArrayList<String> stringFragments = new ArrayList<String>();
        int i = 0;
        while(stringFragmentMatcher.find())
        {
            stringFragments.add(stringFragmentMatcher.group());
            query = stringFragmentMatcher.replaceFirst("#(SQL_PARSER_STRING_FRAGMENT#" + i + ")");
            stringFragmentMatcher = Extract.matcher(query);
            i++;
        }
        query = AutoSplit.matcher(query).replaceAll(";$1 ");
        String[] sqlFragments = query.split("\\s*;+\\s*");
        ArrayList<String> queries = new ArrayList<String>();
        for(int j = 0;j<sqlFragments.length;j++)
        {
            sqlFragments[j] = sqlFragments[j].trim();
            if (sqlFragments[j].length() != 0)
            {
                if(sqlFragments[j].indexOf("#(SQL_PARSER_STRING_FRAGMENT#") > -1)
                    for(int k = 0;k<stringFragments.size();k++)
                        sqlFragments[j] = sqlFragments[j].replace("#(SQL_PARSER_STRING_FRAGMENT#" + k + ")", stringFragments.get(k));
                sqlFragments[j] = sqlFragments[j].replace("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)", "''");
                sqlFragments[j] = sqlFragments[j].replace("#SQL_PARSER_STRING_KEYWORD#","");
                queries.add(sqlFragments[j]);
            }
        }
        return queries;
    }
    public static List<String> parseQueryProcedureCallParameters(String query)
    {
        if (query == null)
            return null;

        query = SingleLineComments.matcher(query).replaceAll("");
        query = EscapedSingleQuote.matcher(query).replaceAll("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)");
        Matcher stringFragmentMatcher = Extract.matcher(query);
        ArrayList<String> stringFragments = new ArrayList<String>();
        int i = 0;
        while(stringFragmentMatcher.find())
        {
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
                if(sqlFragments[j].indexOf("#(SQL_PARSER_STRING_FRAGMENT#") > -1)
                    for(int k = 0;k<stringFragments.size();k++)
                        sqlFragments[j] = sqlFragments[j].replace("#(SQL_PARSER_STRING_FRAGMENT#" + k + ")", stringFragments.get(k));
                sqlFragments[j] = sqlFragments[j].replace("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)", "''");
                sqlFragments[j] = sqlFragments[j].trim();
                queries.add(sqlFragments[j]);
            }
        }
        return queries;
    }

    // Command line interaction
    private static ConsoleReader Input = null;
    private static final Pattern GoToken = Pattern.compile("^\\s*go;*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ExitToken = Pattern.compile("^\\s*(exit|quit);*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ListToken = Pattern.compile("^\\s*(list proc|list procedures);*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ListTablesToken = Pattern.compile("^\\s*(list tables);*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SemicolonToken = Pattern.compile("^.*\\s*;+\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern RecallToken = Pattern.compile("^\\s*recall\\s*([^;]+)\\s*;*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern FileToken = Pattern.compile("^\\s*file\\s*['\"]*([^;'\"]+)['\"]*\\s*;*\\s*", Pattern.CASE_INSENSITIVE);
    private static int LineIndex = 1;
    private static List<String> Lines = new ArrayList<String>();
    private static List<String> getQuery(boolean interactive) throws Exception
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
                    line = Input.readLine("");

                }
                else
                    line = Input.readLine((LineIndex++) + "> ");
            }
            else
                line = Input.readLine();

            if (line == null)
            {
                if (query == null)
                    return null;
                else
                    return parseQuery(query.toString());
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
                            Input.putString(line);
                            out.flush();
                            isRecall = true;
                            continue;
                        }
                        else
                            System.out.printf("%s> Invalid RECALL reference: '" + m.group(1) + "'.\n", LineIndex-1);
                    }
                    else
                        System.out.printf("%s> Invalid RECALL command: '" + line + "'.\n", LineIndex-1);
            }

            // Strip out invalid recall commands
            if (RecallToken.matcher(line).matches())
                line = "";

            // Queue up the line to the recall stack - ONLY in interactive mode
            if (interactive)
                Lines.add(line);

            // EXIT command - ONLY in interactive mode, exit immediately (without running any queued statements)
            if (ExitToken.matcher(line).matches())
            {
                if (interactive)
                    return null;
            }
            // EXIT command - ONLY in interactive mode, exit immediately (without running any queued statements)
            else if (ListToken.matcher(line).matches())
            {
                if (interactive)
                {
                    List<String> list = new LinkedList<String>(Procedures.keySet());
                    Collections.sort(list);
                    int padding = 0;
                    for(String procedure : list)
                        if (padding < procedure.length()) padding = procedure.length();
                    padding++;
                    String format = "%1$-" + padding + "s";
                    for(int i = 0;i<2;i++)
                    {
                        int j = 0;
                        for(String procedure : list)
                        {
                            if (i == 0 && procedure.startsWith("@"))
                                continue;
                            else if (i == 1 && !procedure.startsWith("@"))
                                continue;
                            if (j == 0)
                            {
                                if (i == 0)
                                    System.out.println("\n--- User Procedures ----------------------------------------");
                                else
                                    System.out.println("\n--- System Procedures --------------------------------------");
                            }
                            for (List<String> parameterSet : Procedures.get(procedure).values()) {
                                System.out.printf(format, procedure);
                                System.out.print("\t");
                                int pidx = 0;
                                for(String paramType : parameterSet)
                                {
                                    if (pidx > 0)
                                        System.out.print(", ");
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
            // EXIT command - ONLY in interactive mode, exit immediately (without running any queued statements)
            else if (ListTablesToken.matcher(line).matches())
            {
                if (interactive)
                {
                    Object[] lists = GetTableList();
                    for(int i=0;i<3;i++)
                    {
                        if (i == 0)
                            System.out.println("\n--- User Tables --------------------------------------------");
                        else if (i == 1)
                            System.out.println("\n--- User Views ---------------------------------------------");
                        else
                            System.out.println("\n--- User Export Streams ------------------------------------");
                        @SuppressWarnings("unchecked")
                        Iterator<String> list = ((TreeSet<String>)lists[i]).iterator();
                        while(list.hasNext())
                            System.out.println(list.next());
                        System.out.print("\n");
                    }
                    System.out.print("\n");
                }
            }
            // GO commands - ONLY in interactive mode, close batch and parse for execution
            else if (GoToken.matcher(line).matches())
            {
                if (interactive)
                    return parseQuery(query.toString().trim());
            }
            // FILE command - include the content of the file into the query
            else if (FileToken.matcher(line).matches())
            {
                boolean executeImmediate = false;
                if (interactive && SemicolonToken.matcher(line).matches())
                    executeImmediate = true;
                Matcher m = FileToken.matcher(line);
                if (m.find())
                {
                    line = readScriptFile(m.group(1));
                    if (line == null)
                    {
                        if (!interactive)
                            return null;
                    }
                    else
                    {
                        query.append(line);
                        query.append("\n");

                        if (executeImmediate)
                            return parseQuery(query.toString().trim());
                    }
                }
                else
                {
                    System.err.print("Invalid FILE command: '" + line + "'.");
                    // In non-interactive mode, a failure aborts the entire batch
                    // In interactive mode, we'll just ignore that specific failed command.
                    if (!interactive)
                        return null;
                }
            }
            else
            {
                query.append(line);
                query.append("\n");
                if (interactive && SemicolonToken.matcher(line).matches())
                    return parseQuery(query.toString().trim());
            }
            line = null;
        }
        while(true);
    }

    public static String readScriptFile(String filePath)
    {
        try
        {
            StringBuilder query = new StringBuilder();
            BufferedReader script = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = script.readLine()) != null)
            {
                // Strip out RECALL, EXIT and GO commands
                if (!(RecallToken.matcher(line).matches() || ExitToken.matcher(line).matches() || GoToken.matcher(line).matches()))
                {
                    // Recursively process FILE commands, any failure will cause a recursive failure
                    if (FileToken.matcher(line).matches())
                    {
                        Matcher m = FileToken.matcher(line);
                        if (m.find())
                        {
                            line = readScriptFile(m.group(1));
                            if (line == null)
                                return null;
                            query.append(line);
                            query.append("\n");
                        }
                        else
                        {
                            System.err.print("Invalid FILE command: '" + line + "'.");
                            return null;
                        }
                    }
                    else
                    {
                        query.append(line);
                        query.append("\n");
                    }
                }
            }
            script.close();
            return query.toString().trim();
        }
        catch(FileNotFoundException e)
        {
            System.err.println("Script file '" + filePath + "' could not be found.");
            return null;
        }
        catch(Exception x)
        {
            System.err.println(x.getMessage());
            return null;
        }
    }

    // Query Execution
    private static final Pattern ExecuteCall = Pattern.compile("^(exec|execute) ", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explain" (case insensitive).  We'll convert them to @Explain invocations.
    private static final Pattern ExplainCall = Pattern.compile("^explain ", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explainproc" (case insensitive).  We'll convert them to @ExplainProc invocations.
    private static final Pattern ExplainProcCall = Pattern.compile("^explainProc ", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    private static final Pattern StripCRLF = Pattern.compile("[\r\n]+", Pattern.MULTILINE);
    private static final Pattern IsNull = Pattern.compile("null", Pattern.CASE_INSENSITIVE);
    private static final SimpleDateFormat DateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Pattern Unquote = Pattern.compile("^'|'$", Pattern.MULTILINE);
    private static void executeQuery(String query) throws Exception
    {
        if (ExecuteCall.matcher(query).find())
        {
            query = ExecuteCall.matcher(query).replaceFirst("");
            List<String> params = parseQueryProcedureCallParameters(query);
            String procedure = params.remove(0);
            if (!Procedures.containsKey(procedure))
                throw new Exception("Undefined procedure: " + procedure);

            List<String> paramTypes = Procedures.get(procedure).get(params.size());
            if (paramTypes == null || params.size() != paramTypes.size()) {
                String expectedSizes = "";
                for (Integer expectedSize : Procedures.get(procedure).keySet()) {
                    expectedSizes += expectedSize + ", ";
                }
                throw new Exception("Invalid parameter count for procedure: " + procedure + "(expected: " + expectedSizes + " received: " + params.size() + ")");
            }
            Object[] objectParams = new Object[params.size()];
            if (procedure.equals("@SnapshotDelete"))
            {
                objectParams[0] = new String[] { Unquote.matcher(params.get(0)).replaceAll("").replace("''","'") };
                objectParams[1] = new String[] { Unquote.matcher(params.get(1)).replaceAll("").replace("''","'") };
            }
            else
            {
                for(int i = 0;i<params.size();i++)
                {
                    String paramType = paramTypes.get(i);
                    String param = params.get(i);
                    if (paramType.equals("bit"))
                    {
                        if(param.equals("yes") || param.equals("true") || param.equals("1"))
                            objectParams[i] = (byte)1;
                        else
                            objectParams[i] = (byte)0;
                    }
                    else if (paramType.equals("tinyint"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_TINYINT;
                        else
                        {
                            try
                            {
                                objectParams[i] = Byte.parseByte(param);
                            }
                            catch (NumberFormatException nfe)
                            {
                                throw new Exception("Invalid parameter:  Expected a byte numeric value, got '" + param + "' (param " + (i+1) + ").");
                            }
                        }
                    }
                    else if (paramType.equals("smallint"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_SMALLINT;
                        else
                        {
                            try
                            {
                                objectParams[i] = Short.parseShort(param);
                            }
                            catch (NumberFormatException nfe)
                            {
                                throw new Exception("Invalid parameter:  Expected a short numeric value, got '" + param + "' (param " + (i+1) + ").");
                            }
                        }
                    }
                    else if (paramType.equals("int") || paramType.equals("integer"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_INTEGER;
                        else
                        {
                            try
                            {
                                objectParams[i] = Integer.parseInt(param);
                            }
                            catch (NumberFormatException nfe)
                            {
                                throw new Exception("Invalid parameter:  Expected a numeric value, got '" + param + "' (param " + (i+1) + ").");
                            }
                        }
                    }
                    else if (paramType.equals("bigint"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_BIGINT;
                        else
                        {
                            try
                            {
                                objectParams[i] = Long.parseLong(param);
                            }
                            catch (NumberFormatException nfe)
                            {
                                throw new Exception("Invalid parameter:  Expected a numeric value, got '" + param + "' (param " + (i+1) + ").");
                            }
                        }
                    }
                    else if (paramType.equals("float"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_FLOAT;
                        else
                        {
                            try
                            {
                                objectParams[i] = Double.parseDouble(param);
                            }
                            catch (NumberFormatException nfe)
                            {
                                throw new Exception("Invalid parameter:  Expected a float value, got '" + param + "' (param " + (i+1) + ").");
                            }
                        }
                    }
                    else if (paramType.equals("varchar"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_STRING_OR_VARBINARY;
                        else
                            objectParams[i] = Unquote.matcher(param).replaceAll("").replace("''","'");
                    }
                    else if (paramType.equals("decimal"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_DECIMAL;
                        else
                            objectParams[i] = new BigDecimal(param);
                    }
                    else if (paramType.equals("timestamp"))
                    {
                        if (IsNull.matcher(param).matches())
                        {
                            objectParams[i] = VoltType.NULL_TIMESTAMP;
                        }
                        else
                        {
                            // Remove any quotes around the timestamp value.  ENG-2623
                            objectParams[i] = DateParser.parse(param.replaceAll("^\"|\"$", "").replaceAll("^'|'$", ""));
                        }
                    }
                    else if (paramType.equals("statisticscomponent"))
                    {
                        String p = preprocessParam(param);
                        if (!StatisticsComponents.contains(p))
                            throw new Exception("Invalid Statistics Component: " + param);
                        objectParams[i] = p;
                    }
                    else if (paramType.equals("sysinfoselector"))
                    {
                        String p = preprocessParam(param);
                        if (!SysInfoSelectors.contains(p))
                            throw new Exception("Invalid SysInfo Selector: " + param);
                        objectParams[i] = p;
                    }
                    else if (paramType.equals("metadataselector"))
                    {
                        String p = preprocessParam(param);
                        if (!MetaDataSelectors.contains(p))
                            throw new Exception("Invalid Meta-Data Selector: " + param);
                        objectParams[i] = p;
                    }
                    else if (paramType.equals("varbinary") || paramType.equals("tinyint_array"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_STRING_OR_VARBINARY;
                        else
                        {
                            // Make sure we have an even amount of characters, otherwise it is an invalid hex string
                            if (param.length() % 2 == 1)
                                throw new Exception("Invalid varbinary value: input must have an even amount of characters to be a valid hex string.");
                            String val = Unquote.matcher(param).replaceAll("").replace("''","'");
                            objectParams[i] = hexStringToByteArray(val);
                        }
                    }
                    else
                        throw new Exception("Unsupported Data Type: " + paramType);
                }
            }
            if (procedure.equals("@UpdateApplicationCatalog"))
            {
                printResponse(VoltDB.updateApplicationCatalog(new File((String) objectParams[0]),
                                                              new File((String) objectParams[1])));

                // Need to update the stored procedures after a catalog change (could have added/removed SPs!).  ENG-3726
                Procedures.clear();
                loadSystemProcedures();
                loadStoredProcedures(Procedures);
            }
            else
            {
                printResponse(VoltDB.callProcedure(procedure, objectParams));
            }
        }
        else if (ExplainCall.matcher(query).find())
        {
            // We've got a query that starts with "explain", pre-pend
            // the @Explain sp invocatino ahead of the query (after stripping "explain").
            query = query.substring("explain ".length());
            query = StripCRLF.matcher(query).replaceAll(" ");
            printResponse(VoltDB.callProcedure("@Explain", query));
        }
        else if (ExplainProcCall.matcher(query).find())
        {
            // We've got a query that starts with "explainplan", pre-pend
            // the @ExplainPlan sp invocation ahead of the query (after stripping "explainplan").
            query = query.substring("explainProc ".length());
            query = StripCRLF.matcher(query).replaceAll(" ");
            printResponse(VoltDB.callProcedure("@ExplainProc", query));
        }
        else  // Ad hoc query
        {
            query = StripCRLF.matcher(query).replaceAll(" ");
            printResponse(VoltDB.callProcedure("@AdHoc", query));
        }
        return;
    }

    // Uppercase param.
    // Remove any quotes.
    // Trim
    private static String preprocessParam(String param)
    {
        param = param.toUpperCase();
        if (param.startsWith("'") && param.endsWith("'"))
            param = param.substring(1, param.length()-1);
        if (param.charAt(0)=='"' && param.charAt(param.length()-1)=='"')
            param = param.substring(1, param.length()-1);
        param = param.trim();
        return param;
    }

    private static String byteArrayToHexString(byte[] data)
    {
        StringBuffer hexString = new StringBuffer();
        for (int i=0;i<data.length;i++)
        {
            String hex = Integer.toHexString(0xFF & data[i]);
            if (hex.length() == 1)
                hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private static byte[] hexStringToByteArray(String s)
    {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2)
            data[i/2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i+1), 16));
        return data;
    }

    // Output generation
    private static String OutputFormat = "fixed";
    private static boolean OutputShowMetadata = true;

    private static boolean isUpdateResult(VoltTable table)
    {
        return ((table.getColumnName(0).length() == 0 || table.getColumnName(0).equals("modified_tuples"))&& table.getRowCount() == 1 && table.getColumnCount() == 1 && table.getColumnType(0) == VoltType.BIGINT);
    }
    private static void printResponse(ClientResponse response) throws Exception
    {
        if (response.getStatus() != ClientResponse.SUCCESS)
            throw new Exception("Execution Error: " + response.getStatusString());
        if (OutputFormat.equals("fixed"))
        {
            for(VoltTable t : response.getResults())
            {
                if (isUpdateResult(t))
                {
                    if(OutputShowMetadata)
                        System.out.printf("\n\n(%d row(s) affected)\n", t.fetchRow(0).getLong(0));
                    continue;
                }

                // Use the VoltTable pretty printer to display formatted output.
                System.out.println(t.toFormattedString());

                if (OutputShowMetadata)
                    System.out.printf("\n\n(%d row(s) affected)\n", t.getRowCount());
            }
        }
        else
        {
            String separator = OutputFormat.equals("csv") ? "," : "\t";
            for(VoltTable t : response.getResults())
            {
                if (isUpdateResult(t))
                {
                    if(OutputShowMetadata)
                        System.out.printf("\n\n(%d row(s) affected)\n", t.fetchRow(0).getLong(0));
                    continue;
                }
                int columnCount = t.getColumnCount();
                if (OutputShowMetadata)
                {
                    for (int i = 0; i < columnCount; i++)
                    {
                        if (i > 0) System.out.print(separator);
                        System.out.print(t.getColumnName(i));
                    }
                    System.out.print("\n");
                }
                t.resetRowPosition();
                while(t.advanceRow())
                {
                    for (int i = 0; i < columnCount; i++)
                    {
                        if (i > 0) System.out.print(separator);
                        Object v = t.get(i, t.getColumnType(i));
                        if (t.wasNull())
                            v = "NULL";
                        else if (t.getColumnType(i) == VoltType.VARBINARY)
                            v = byteArrayToHexString((byte[])v);
                        else
                            v = v.toString();
                        System.out.print(v);
                    }
                    System.out.print("\n");
                }
                if (OutputShowMetadata)
                    System.out.printf("\n\n(%d row(s) affected)\n", t.getRowCount());
            }
        }
    }

    // VoltDB connection support
    private static Client VoltDB;
    private static final List<String> StatisticsComponents = Arrays.asList("INDEX","INITIATOR","IOSTATS","MANAGEMENT","MEMORY","PROCEDURE","TABLE","PARTITIONCOUNT","STARVATION","LIVECLIENTS", "DR", "TOPO", "PLANNER");
    private static final List<String> SysInfoSelectors = Arrays.asList("OVERVIEW","DEPLOYMENT");
    private static final List<String> MetaDataSelectors =
        Arrays.asList("TABLES", "COLUMNS", "INDEXINFO", "PRIMARYKEYS",
                      "PROCEDURES", "PROCEDURECOLUMNS");
    private static Map<String,Map<Integer, List<String>>> Procedures =
            Collections.synchronizedMap(new HashMap<String,Map<Integer, List<String>>>());
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
        Procedures.put("@SnapshotDelete",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar")).build()
                );
        Procedures.put("@SnapshotRestore",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "varchar")).build()
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
        Procedures.put("@UpdateLogging",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@Promote",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@SnapshotStatus",
                ImmutableMap.<Integer, List<String>>builder().put( 0, new ArrayList<String>()).build());
        Procedures.put("@AdHoc_RO_MP",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@AdHoc_RO_SP",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "bigint")).build());
        Procedures.put("@AdHoc_RW_MP",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@AdHoc_RW_SP",
                ImmutableMap.<Integer, List<String>>builder().put( 2, Arrays.asList("varchar", "bigint")).build());
        Procedures.put("@Explain",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
        Procedures.put("@ExplainProc",
                ImmutableMap.<Integer, List<String>>builder().put( 1, Arrays.asList("varchar")).build());
    }

    public static Client getClient(ClientConfig config, String[] servers, int port) throws Exception
    {
        final Client client = ClientFactory.createClient(config);

        for (String server : servers)
            client.createConnection(server.trim(), port);
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
        + "              [--output-format=(fixed|csv|tab)]\n"
        + "              [--output-skip-metadata]\n"
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
        + "[--debug]\n"
        + "  Causes the utility to print out stack traces for all exceptions.\n"
        );
        System.exit(exitCode);
    }

    // printHelp() can print readme either to a file or to the screen
    // depending on the argument passed in
    public static void printHelp(OutputStream prtStr)
    {
        try
        {
            InputStream is = SQLCommand.class.getResourceAsStream(readme);
            while(is.available() > 0) {
                byte[] bytes = new byte[is.available()]; // Fix for ENG-3440
                is.read(bytes, 0, bytes.length);
                prtStr.write(bytes); // For JUnit test
            }
        }
        catch(Exception x)
        {
            System.err.println(x.getMessage());
            System.exit(-1);
        }
    }

    private static Object[] GetTableList() throws Exception
    {
        VoltTable tableData = VoltDB.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        TreeSet<String> tables = new TreeSet<String>();
        TreeSet<String> exports = new TreeSet<String>();
        TreeSet<String> views = new TreeSet<String>();
        for(int i = 0; i < tableData.getRowCount(); i++)
        {
            String tableName = tableData.fetchRow(i).getString("TABLE_NAME");
            String tableType = tableData.fetchRow(i).getString("TABLE_TYPE");
            if (tableType.equalsIgnoreCase("EXPORT"))
            {
                exports.add(tableName);
            }
            else if (tableType.equalsIgnoreCase("VIEW"))
            {
                views.add(tableName);
            }
            else
            {
                tables.add(tableName);
            }
        }
        return new Object[] {tables, views, exports};
    }

    private static void loadStoredProcedures(Map<String,Map<Integer, List<String>>> procedures)
    {
        VoltTable procs = null;
        VoltTable params = null;
        try
        {
            procs = VoltDB.callProcedure("@SystemCatalog", "PROCEDURES").getResults()[0];
            params = VoltDB.callProcedure("@SystemCatalog", "PROCEDURECOLUMNS").getResults()[0];
        }
        catch (NoConnectionsException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
        catch (ProcCallException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
        Map<String, Integer> proc_param_counts =
            Collections.synchronizedMap(new HashMap<String, Integer>());
        while (params.advanceRow())
        {
            String this_proc = params.getString("PROCEDURE_NAME");
            if (!proc_param_counts.containsKey(this_proc))
            {
                proc_param_counts.put(this_proc, 0);
            }
            int curr_val = proc_param_counts.get(this_proc);
            proc_param_counts.put(this_proc, ++curr_val);
        }
        params.resetRowPosition();
        while (procs.advanceRow())
        {
            String proc_name = procs.getString("PROCEDURE_NAME");
            ArrayList<String> this_params = new ArrayList<String>();
            // prepopulate it to make sure the size is right
            if (proc_param_counts.get(proc_name) != null)
            {
                for (int i = 0; i < proc_param_counts.get(procs.getString("PROCEDURE_NAME")); i++) {
                    this_params.add(null);
                }
            }
            HashMap<Integer, List<String>> argLists = new HashMap<Integer, List<String>>();
            if (proc_param_counts.containsKey(proc_name)) {
                argLists.put(proc_param_counts.get(proc_name), this_params);
            } else {
                argLists.put(0, this_params);

            }
            procedures.put(procs.getString("PROCEDURE_NAME"), argLists);
        }

        // Retrieve the parameter types.  Note we have to do some special checking
        // for array types.  ENG-3101
        params.resetRowPosition();
        while (params.advanceRow())
        {
            Map<Integer, List<String>> argLists = procedures.get(params.getString("PROCEDURE_NAME"));
            assert(argLists.size() == 1);
            List<String> this_params = argLists.values().iterator().next();
            int idx = (int)params.getLong("ORDINAL_POSITION") - 1;
            String param_type = params.getString("TYPE_NAME").toLowerCase();
            // Detect if this parameter is supposed to be an array.  It's kind of clunky, we have to
            // look in the remarks column...
            String param_remarks = params.getString("REMARKS");
            if (null != param_remarks)
            {
                param_type += (param_remarks.equalsIgnoreCase("ARRAY_PARAMETER") ? "_array" : "");
            }
            this_params.set(idx, param_type);
        }
    }

    static public void mockVoltDBForTest(Client testVoltDB) {
        VoltDB = testVoltDB;
    }

    private static InputStream in = null;
    private static Writer out = null;
    // Application entry point
    public static void main(String args[])
    {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+0"));
        boolean debug = false;
        try
        {
            // Initialize parameter defaults
            String serverList = "localhost";
            int port = 21212;
            String user = "";
            String password = "";
            List<String> queries = null;

            // Parse out parameters
            for(int i = 0; i < args.length; i++)
            {
                String arg = args[i];
                if (arg.startsWith("--servers="))
                    serverList = arg.split("=")[1];
                else if (arg.startsWith("--port="))
                    port = Integer.valueOf(arg.split("=")[1]);
                else if (arg.startsWith("--user="))
                    user = arg.split("=")[1];
                else if (arg.startsWith("--password="))
                    password = arg.split("=")[1];
                else if (arg.startsWith("--query="))
                {
                    List<String> argQueries = parseQuery(arg.substring(8));
                    if (!argQueries.isEmpty()) {
                        if (queries == null)
                        {
                            queries = argQueries;
                        }
                        else
                        {
                            queries.addAll(argQueries);
                        }
                    }
                }
                else if (arg.startsWith("--output-format="))
                {
                    if (Pattern.compile("(fixed|csv|tab)", Pattern.CASE_INSENSITIVE).matcher(arg.split("=")[1].toLowerCase()).matches())
                        OutputFormat = arg.split("=")[1].toLowerCase();
                    else
                        printUsage("Invalid value for --output-format");
                }
                else if (arg.equals("--output-skip-metadata"))
                    OutputShowMetadata = false;
                else if (arg.equals("--debug"))
                    debug = true;
                else if (arg.equals("--help"))
                {
                    printHelp(System.out); // Print readme to the screen
                    System.out.println("\n\n");
                    printUsage(0);
                }
                else if ((arg.equals("--usage")) || (arg.equals("-?")))
                    printUsage(0);
                else
                    printUsage("Invalid Parameter: " + arg);
            }

            // Split server list
            String[] servers = serverList.split(",");

            // Load system procedures
            loadSystemProcedures();

            // Don't ask... Java is such a crippled language!
            DateParser.setLenient(true);

            // Create connection
            ClientConfig config = new ClientConfig(user, password);
            config.setProcedureCallTimeout(0);  // Set procedure all to infinite timeout, see ENG-2670
            VoltDB = getClient(config, servers, port);

            // Load user stored procs
            loadStoredProcedures(Procedures);

            in = new FileInputStream(FileDescriptor.in);
            out = new PrintWriter(new OutputStreamWriter(System.out, System.getProperty("jline.WindowsTerminal.output.encoding", System.getProperty("file.encoding"))));
            Input = new ConsoleReader(in, out);

            Input.setBellEnabled(false);
            Input.addCompletor(new SimpleCompletor(new String[] {"select", "update", "insert", "delete", "exec", "file", "recall", "SELECT", "UPDATE", "INSERT", "DELETE", "EXEC", "FILE", "RECALL" }));

            boolean interactive = true;
            if (queries != null && !queries.isEmpty())
            {
                // If queries are provided via command line options run them in
                // non-interactive mode.
                //TODO: Someday we should honor batching.
                interactive = false;
                for(int i = 0;i<queries.size();i++)
                    executeQuery(queries.get(i));
            }
            if (System.in.available() > 0)
            {
                // If Standard input comes loaded with data, run in non-interactive mode
                interactive = false;
                queries = getQuery(false);
                if (queries == null)
                    System.exit(0);
                else
                    for(int i = 0;i<queries.size();i++)
                        executeQuery(queries.get(i));
            }
            if (interactive)
            {
                // Print out welcome message
                System.out.printf("SQL Command :: %s%s:%d\n", (user == "" ? "" : user + "@"), serverList, port);

                while((queries = getQuery(true)) != null)
                {
                    try
                    {
                        for(int i = 0;i<queries.size();i++)
                            executeQuery(queries.get(i));
                    }
                    catch(Exception x)
                    {
                        System.err.println(x.getMessage());
                        if (debug) x.printStackTrace(System.err);
                    }
                }
            }

       }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            if (debug) e.printStackTrace(System.err);
            System.exit(-1);
        }
        finally
        {
            try { VoltDB.close(); } catch(Exception _) {}
        }
    }

}
