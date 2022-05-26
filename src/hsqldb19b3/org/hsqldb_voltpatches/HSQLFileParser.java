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

package org.hsqldb_voltpatches;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class HSQLFileParser {
    public static class Statement {
        public String statement;
        public int lineNo;
    }

    public static Statement[] getStatements(String path)
    throws HSQLInterface.HSQLParseException {
        ArrayList<Statement> statements = new ArrayList<Statement>();

        File inputFile = new File(path);
        FileReader fr = null;
        LineNumberReader reader = null;
        try {
            fr = new FileReader(inputFile);
            reader = new LineNumberReader(fr);
        } catch (FileNotFoundException e) {
            String msg = "Unable to open " + path + " for reading";
            throw new HSQLInterface.HSQLParseException(msg);
        }

        Statement stmt = getNextStatement(reader);
        while (stmt != null) {
            statements.add(stmt);
            stmt = getNextStatement(reader);
        }

        try {
            reader.close();
            fr.close();
        } catch (IOException e) {
            throw new HSQLInterface.HSQLParseException("Error closing file");
        }

        Statement[] retval = new Statement[statements.size()];
        for (int i = 0; i < statements.size(); i++)
            retval[i] = statements.get(i);
        return retval;
    }

    static String cleanupString(String line) {
        if (line == null)
            return null;
        // match all content preceding a comment.
        Pattern re = Pattern.compile("^(.*?)\\-\\-(.*)$");
        Matcher matcher = re.matcher(line);
        if (matcher.matches()) {
            return matcher.group(1).trim();
        }
        return line.trim();
    }

    static Statement getNextStatement(LineNumberReader reader)
    throws HSQLInterface.HSQLParseException {
        Statement retval = new Statement();
        try {
            String stmt = "";

            // skip empty lines.
            while (stmt.equals("")) {
                stmt = reader.readLine();
                stmt = cleanupString(stmt);
                if (stmt == null) return null;
            }

            // found interesting content. record the line number
            retval.lineNo = reader.getLineNumber();

            // add all lines until one ends with a semicolon
            while (!stmt.endsWith(";\n") && !stmt.endsWith(";")) {
                String newline = reader.readLine();
                newline = cleanupString(newline);
                if (newline == null) {
                    String msg = "Schema file ended mid statment (no semicolon found)";
                    throw new HSQLInterface.HSQLParseException(msg, retval.lineNo);
                }
                if (newline.equals(""))
                    continue;
                stmt += " " + newline;
            }
            retval.statement = stmt + "\n";

        } catch (IOException e) {
            throw new HSQLInterface.HSQLParseException("Unable to read from file");
        }

        return retval;
    }

    /**
     * Run the parser as a stand-alone tool sending output to standard out.
     * @param arg[0] is the file of sql commands to be processed.
     */
    public static void main(String args[]) {
        Statement stmts[] = null;
        try {
            stmts = getStatements(args[0]);
        } catch (Throwable e) {
            System.out.println(e.getMessage());
            return;
        }
        for (Statement s : stmts) {
            System.out.print(s.statement);
        }
    }

}
