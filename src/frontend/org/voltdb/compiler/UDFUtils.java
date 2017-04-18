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

package org.voltdb.compiler;

import java.util.regex.Matcher;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Function;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;

public class UDFUtils {

    public static boolean processCreateFunctionStatement(
            Matcher statementMatcher, Database db, VoltCompiler compiler) throws VoltCompilerException {
        String functionName = statementMatcher.group(1);
        String classDotMethodPath = statementMatcher.group(2);
        int classMethodDelimiterIndex = classDotMethodPath.lastIndexOf(".");
        String className = classDotMethodPath.substring(0, classMethodDelimiterIndex);
        String methodName = classDotMethodPath.substring(classMethodDelimiterIndex + 1);
        Function func = db.getFunctions().add(functionName);
        func.setFunctionname(functionName);
        func.setClassname(className);
        func.setMethodname(methodName);
        return true;
    }

}
