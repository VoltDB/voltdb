/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.*;
import java.lang.StringBuilder;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.text.SimpleDateFormat;
import java.math.BigDecimal;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import jline.*;

public class SQLCommand
{
    // SQL Parsing
    private static final Pattern EscapedSingleQuote = Pattern.compile("''", Pattern.MULTILINE);
    private static final Pattern SingleLineComments = Pattern.compile("^\\s*(\\/\\/|--).*$", Pattern.MULTILINE);
    private static final Pattern Extract = Pattern.compile("'[^']*'", Pattern.MULTILINE);
    private static final Pattern AutoSplit = Pattern.compile("\\s(select|insert|update|delete|exec|execute)\\s", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    private static final Pattern AutoSplitParameters = Pattern.compile("[\\s,]+", Pattern.MULTILINE);
    public static List<String> parseQuery(String query)
    {
        if (query == null)
            return null;

        String[] command = new String[] {"exec", "execute"};
        String[] keyword = new String[] {"select", "insert", "update", "delete"};
        for(int i = 0;i<command.length;i++)
        {
            for(int j = 0;j<command.length;j++)
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
                            System.out.printf(format, procedure);
                            System.out.print("\t");
                            int pidx = 0;
                            for(String paramType : Procedures.get(procedure))
                            {
                                if (pidx > 0)
                                    System.out.print(", ");
                                System.out.print(paramType);
                                pidx++;
                            }
                            System.out.print("\n");
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

    private static String readScriptFile(String filePath)
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

            List<String> paramTypes = Procedures.get(procedure);
            if (params.size() != paramTypes.size())
                throw new Exception("Invalid parameter count for procedure: " + procedure + "(expected: " + paramTypes.size() + ", received: " + params.size() + ")");
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
                            objectParams[i] = Byte.parseByte(param);
                    }
                    else if (paramType.equals("smallint"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_SMALLINT;
                        else
                            objectParams[i] = Short.parseShort(param);
                    }
                    else if (paramType.equals("int") || paramType.equals("integer"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_INTEGER;
                        else
                            objectParams[i] = Integer.parseInt(param);
                    }
                    else if (paramType.equals("bigint"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_BIGINT;
                        else
                            objectParams[i] = Long.parseLong(param);
                    }
                    else if (paramType.equals("float"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_FLOAT;
                        else
                            objectParams[i] = Double.parseDouble(param);
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
                            objectParams[i] = VoltType.NULL_TIMESTAMP;
                        else
                            objectParams[i] = DateParser.parse(param);
                    }
                    else if (paramType.equals("statisticscomponent"))
                    {
                        if (!StatisticsComponents.contains(param.toUpperCase()))
                            throw new Exception("Invalid Statistics Component: " + param);
                        objectParams[i] = param.toUpperCase();
                    }
                    else if (paramType.equals("sysinfoselector"))
                    {
                        if (!SysInfoSelectors.contains(param.toUpperCase()))
                            throw new Exception("Invalid SysInfo Selector: " + param);
                        objectParams[i] = param.toUpperCase();
                    }
                    else if (paramType.equals("metadataselector"))
                    {
                        if (!MetaDataSelectors.contains(param.toUpperCase()))
                            throw new Exception("Invalid Meta-Data Selector: " + param);
                        objectParams[i] = param.toUpperCase();
                    }
                    else if (paramType.equals("varbinary"))
                    {
                        if (IsNull.matcher(param).matches())
                            objectParams[i] = VoltType.NULL_STRING_OR_VARBINARY;
                        else
                            objectParams[i] = hexStringToByteArray(Unquote.matcher(param).replaceAll("").replace("''","'"));
                    }
                    else
                        throw new Exception("Unsupported Data Type: " + paramType);
                }
            }
            if (procedure.equals("@UpdateApplicationCatalog"))
            {
                printResponse(VoltDB.updateApplicationCatalog(new File((String) objectParams[0]),
                                                              new File((String) objectParams[1])));
            }
            else
            {
                printResponse(VoltDB.callProcedure(procedure, objectParams));
            }
        }
        else
        {
            query = StripCRLF.matcher(query).replaceAll(" ");
            printResponse(VoltDB.callProcedure("@AdHoc", query));
        }
        return;
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
    public static String paddingString(String s, int n, char c, boolean paddingLeft)
    {
        if (s == null)
            return s;

        int add = n - s.length();

        if(add <= 0)
            return s;

        StringBuffer str = new StringBuffer(s);
        char[] ch = new char[add];
        Arrays.fill(ch, c);
        if(paddingLeft)
            str.insert(0, ch);
        else
            str.append(ch);


       return str.toString();
    }
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
                int columnCount = t.getColumnCount();
                int[] padding = new int[columnCount];
                String[] fmt = new String[columnCount];
                for (int i = 0; i < columnCount; i++)
                    padding[i] = OutputShowMetadata ? t.getColumnName(i).length() : 0;
                t.resetRowPosition();
                while(t.advanceRow())
                {
                    for (int i = 0; i < columnCount; i++)
                    {
                        Object v = t.get(i, t.getColumnType(i));
                        if (v == null) v = "null";
                        int l = t.getColumnType(i) == VoltType.VARBINARY ? ((byte[])v).length*2 : v.toString().length();
                        if (padding[i] < l)
                            padding[i] = l;
                    }
                }
                for (int i = 0; i < columnCount; i++)
                {
                    padding[i] += 1;
                    fmt[i] = "%1$" + ((t.getColumnType(i) == VoltType.STRING || t.getColumnType(i) == VoltType.TIMESTAMP || t.getColumnType(i) == VoltType.VARBINARY) ? "-" : "#") + padding[i] + "s";
                }
                if (OutputShowMetadata)
                {
                    for (int i = 0; i < columnCount; i++)
                    {
                        System.out.printf("%1$-" + padding[i] + "s", t.getColumnName(i));
                        if (i < columnCount - 1)
                            System.out.print(" ");
                    }
                    System.out.print("\n");
                    for (int i = 0; i < columnCount; i++)
                    {
                        System.out.print(paddingString("", padding[i], '-', false));
                        if (i < columnCount - 1)
                            System.out.print(" ");
                    }
                    System.out.print("\n");
                }
                t.resetRowPosition();
                while(t.advanceRow())
                {
                    for (int i = 0; i < columnCount; i++)
                    {
                        Object v = t.get(i, t.getColumnType(i));
                        if (v == null)
                            v = "null";
                        else if (t.getColumnType(i) == VoltType.VARBINARY)
                            v = byteArrayToHexString((byte[])v);
                        else
                            v = v.toString();
                        System.out.printf(fmt[i], v);
                        if (i < columnCount - 1)
                            System.out.print(" ");
                    }
                    System.out.print("\n");
                }
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
                        if (v == null)
                            v = "null";
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
    private static final List<String> Types = Arrays.asList("tinyint","smallint","integer","bigint","float","decimal","varchar","timestamp","varbinary");
    private static final List<String> StatisticsComponents = Arrays.asList("INDEX","INITIATOR","IOSTATS","MANAGEMENT","MEMORY","PROCEDURE","TABLE","PARTITIONCOUNT","STARVATION","LIVECLIENTS");
    private static final List<String> SysInfoSelectors = Arrays.asList("OVERVIEW","DEPLOYMENT");
    private static final List<String> MetaDataSelectors =
        Arrays.asList("TABLES", "COLUMNS", "INDEXINFO", "PRIMARYKEYS",
                      "PROCEDURES", "PROCEDURECOLUMNS");
    private static Map<String,List<String>> Procedures = Collections.synchronizedMap(new HashMap<String,List<String>>());
    private static void loadSystemProcedures()
    {
        Procedures.put("@Pause", new ArrayList<String>());
        Procedures.put("@Quiesce", new ArrayList<String>());
        Procedures.put("@Resume", new ArrayList<String>());
        Procedures.put("@Shutdown", new ArrayList<String>());
        Procedures.put("@SnapshotDelete", Arrays.asList("varchar", "varchar"));
        Procedures.put("@SnapshotRestore", Arrays.asList("varchar", "varchar"));
        Procedures.put("@SnapshotSave", Arrays.asList("varchar", "varchar", "bit"));
        Procedures.put("@SnapshotScan", Arrays.asList("varchar"));
        Procedures.put("@Statistics", Arrays.asList("statisticscomponent", "bit"));
        Procedures.put("@SystemCatalog", Arrays.asList("metadataselector"));
        Procedures.put("@SystemInformation", Arrays.asList("sysinfoselector"));
        Procedures.put("@UpdateApplicationCatalog", Arrays.asList("varchar", "varchar"));
        Procedures.put("@UpdateLogging", Arrays.asList("varchar"));
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
        "Usage: SQLCommand --help\n"
        + "   or  SQLCommand [--servers=comma_separated_server_list]\n"
        + "                  [--port=port_number]\n"
        + "                  [--user=user]\n"
        + "                  [--password=password]\n"
        + "                  [--output-format=(fixed|csv|tab)]\n"
        + "                  [--output-skip-metadata]\n"
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
    public static void printHelp()
    {
        try
        {
            byte[] bytes = new byte[1024 * 4];
            InputStream is = SQLCommand.class.getResourceAsStream("SQLCommandReadme.txt");
            while (is.available() > 0) {
                is.read(bytes, 0, bytes.length);
                System.out.write(bytes);
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

    private static void loadStoredProcedures(Map<String,List<String>> procedures)
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
            Integer param_count = proc_param_counts.get(proc_name);
            ArrayList<String> this_params = new ArrayList<String>();
            // prepopulate it to make sure the size is right
            if (proc_param_counts.get(proc_name) != null)
            {
                for (int i = 0; i < proc_param_counts.get(procs.getString("PROCEDURE_NAME")); i++) {
                    this_params.add(null);
                }
            }
            procedures.put(procs.getString("PROCEDURE_NAME"), this_params);
        }
        while (params.advanceRow())
        {
            List<String> this_params = procedures.get(params.getString("PROCEDURE_NAME"));
            this_params.set((int)params.getLong("ORDINAL_POSITION") - 1,
                            params.getString("TYPE_NAME").toLowerCase());
        }
    }

    private static InputStream in = null;
    private static Writer out = null;
    // Application entry point
    public static void main(String args[])
    {
        boolean debug = false;
        try
        {
            // Initialize parameter defaults
            String serverList = "localhost";
            int port = 21212;
            String user = "";
            String password = "";

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
                    printHelp();
                    printUsage(0);
                }
                else if (arg.equals("--usage"))
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
            VoltDB = getClient(new ClientConfig(user, password), servers,port);

            // Load user stored procs
            loadStoredProcedures(Procedures);

            List<String> queries = null;

            in = new FileInputStream(FileDescriptor.in);
            out = new PrintWriter(new OutputStreamWriter(System.out, System.getProperty("jline.WindowsTerminal.output.encoding", System.getProperty("file.encoding"))));
            Input = new ConsoleReader(in, out);

            Input.setBellEnabled(false);
            Input.addCompletor(new SimpleCompletor(new String[] {"select", "update", "insert", "delete", "exec", "file", "recall", "SELECT", "UPDATE", "INSERT", "DELETE", "EXEC", "FILE", "RECALL" }));

            // If Standard input comes loaded with data, run in non-interactive mode
            if (System.in.available() > 0)
            {
                queries = getQuery(false);
                if (queries == null)
                    System.exit(0);
                else
                    for(int i = 0;i<queries.size();i++)
                        executeQuery(queries.get(i));
            }
            else
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
