/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.compiler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.hsqldb.HSQLInterface;
import org.voltdb.ProcInfo;
import org.voltdb.ProcInfoData;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.User;
import org.voltdb.catalog.UserRef;
import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.utils.CatalogUtil;

/**
 * Compiles stored procedures into a given catalog,
 * invoking the StatementCompiler as needed.
 */
public abstract class ProcedureCompiler {

    static void compile(VoltCompiler compiler, HSQLInterface hsql,
            DatabaseEstimates estimates, Catalog catalog, Database db,
            ProcedureDescriptor procedureDescriptor)
    throws VoltCompiler.VoltCompilerException {

        assert(compiler != null);
        assert(hsql != null);
        assert(estimates != null);

        if (procedureDescriptor.m_singleStmt == null)
            compileJavaProcedure(compiler, hsql, estimates, catalog, db, procedureDescriptor);
        else
            compileSingleStmtProcedure(compiler, hsql, estimates, catalog, db, procedureDescriptor);
    }


    static void compileJavaProcedure(VoltCompiler compiler, HSQLInterface hsql,
            DatabaseEstimates estimates, Catalog catalog, Database db,
            ProcedureDescriptor procedureDescriptor)
    throws VoltCompiler.VoltCompilerException {

        final String className = procedureDescriptor.m_className;

        // Load the class given the class name
        Class<?> procClass = null;
        try {
            procClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            String msg = "Cannot load class for procedure: " + className;
            throw compiler.new VoltCompilerException(msg);
        }

        // get the short name of the class (no package)
        String[] parts = className.split("\\.");
        String shortName = parts[parts.length - 1];

        // add an entry to the catalog
        final Procedure procedure = db.getProcedures().add(shortName);
        for (String userName : procedureDescriptor.m_authUsers) {
            final User user = db.getUsers().get(userName);
            if (user == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " has a user " + userName + " that does not exist");
            }
            final UserRef userRef = procedure.getAuthusers().add(userName);
            userRef.setUser(user);
        }
        for (String groupName : procedureDescriptor.m_authGroups) {
            final Group group = db.getGroups().get(groupName);
            if (group == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " has a group " + groupName + " that does not exist");
            }
            final GroupRef groupRef = procedure.getAuthgroups().add(groupName);
            groupRef.setGroup(group);
        }
        procedure.setClassname(className);
        // sysprocs don't use the procedure compiler
        procedure.setSystemproc(false);
        procedure.setHasjava(true);

        // get the annotation
        // first try to get one that has been passed from the compiler
        ProcInfoData info = compiler.getProcInfoOverride(shortName);
        // then check for the usual one in the class itself
        // and create a ProcInfo.Data instance for it
        if (info == null) {
            info = new ProcInfoData();
            ProcInfo annotationInfo = procClass.getAnnotation(ProcInfo.class);
            if (annotationInfo != null) {
                info.partitionInfo = annotationInfo.partitionInfo();
                info.singlePartition = annotationInfo.singlePartition();
            }
        }
        assert(info != null);

        // make sure multi-partition implies no partitoning info
        if (info.singlePartition == false) {
            if ((info.partitionInfo != null) && (info.partitionInfo.length() > 0)) {
                String msg = "Procedure: " + shortName + " is annotated as multi-partition";
                msg += " but partitionInfo has non-empty value: \"" + info.partitionInfo + "\"";
                throw compiler.new VoltCompilerException(msg);
            }
        }

        VoltProcedure procInstance = null;
        try {
            procInstance = (VoltProcedure) procClass.newInstance();
        } catch (InstantiationException e1) {
            e1.printStackTrace();
        } catch (IllegalAccessException e1) {
            e1.printStackTrace();
        }

        // track if there are any writer stmts
        boolean procHasWriteStmts = false;

        // iterate through the fields and deal with
        Field[] fields = procClass.getFields();
        for (Field f : fields) {
            if (f.getType() == SQLStmt.class) {
                //String fieldName = f.getName();
                SQLStmt stmt = null;

                try {
                    stmt = (SQLStmt) f.get(procInstance);
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }

                // add the statement to the catalog
                Statement catalogStmt = procedure.getStatements().add(f.getName());

                // compile the statement
                StatementCompiler.compile(compiler, hsql, catalog, db,
                        estimates, catalogStmt, stmt.getText(),
                        info.singlePartition);

                // if a single stmt is not read only, then the proc is not read only
                if (catalogStmt.getReadonly() == false)
                    procHasWriteStmts = true;
            }
        }

        // set the read onlyness of a proc
        procedure.setReadonly(procHasWriteStmts == false);

        // find the run() method and get the params
        Method procMethod = null;
        Method[] methods = procClass.getMethods();
        for (final Method m : methods) {
            String name = m.getName();
            if (name.equals("run")) {
                // if not null, then we've got more than one run method
                if (procMethod != null) {
                    String msg = "Procedure: " + shortName + " has multiple run(...) methods. ";
                    msg += "Only a single run(...) method is supported.";
                    throw compiler.new VoltCompilerException(msg);
                }
                // found it!
                procMethod = m;
            }
        }
        if (procMethod == null) {
            String msg = "Procedure: " + shortName + " has no run(...) method.";
            throw compiler.new VoltCompilerException(msg);
        }
        // check the return type of the run method
        if ((procMethod.getReturnType() != VoltTable[].class) &&
           (procMethod.getReturnType() != VoltTable.class) &&
           (procMethod.getReturnType() != long.class) &&
           (procMethod.getReturnType() != Long.class)) {

            String msg = "Procedure: " + shortName + " has run(...) method that doesn't return long, Long, VoltTable or VoltTable[].";
            throw compiler.new VoltCompilerException(msg);
        }

        // set procedure parameter types
        CatalogMap<ProcParameter> params = procedure.getParameters();
        Class<?>[] paramTypes = procMethod.getParameterTypes();
        for (int i = 0; i < paramTypes.length; i++) {
            Class<?> cls = paramTypes[i];
            ProcParameter param = params.add(String.valueOf(i));
            param.setIndex(i);

            // handle the case where the param is an array
            if (cls.isArray()) {
                param.setIsarray(true);
                cls = cls.getComponentType();
            }
            else
                param.setIsarray(false);

            // boxed types are not supported parameters at this time
            if ((cls == Long.class) || (cls == Integer.class) || (cls == Short.class) ||
                (cls == Byte.class) || (cls == Double.class) || (cls == Float.class) ||
                (cls == Character.class) || (cls == Boolean.class))
            {
                String msg = "Procedure: " + shortName + " has a parameter with a boxed type: ";
                msg += cls.getSimpleName();
                msg += ". Replace this parameter with the corresponding primitive type and the procedure may compile.";
                throw compiler.new VoltCompilerException(msg);
            }

            VoltType type;
            try {
                type = VoltType.typeFromClass(cls);
            }
            catch (RuntimeException e) {
                // handle the case where the type is invalid
                String msg = "Procedure: " + shortName + " has a parameter with invalid type: ";
                msg += cls.getSimpleName();
                throw compiler.new VoltCompilerException(msg);
            }

            param.setType(type.getValue());
        }

        // parse the procinfo
        procedure.setSinglepartition(info.singlePartition);
        if (info.singlePartition) {
            parsePartitionInfo(compiler, db, procedure, info.partitionInfo);
            if (procedure.getPartitionparameter() >= paramTypes.length) {
                String msg = "PartitionInfo parameter not a valid parameter for procedure: " + procedure.getClassname();
                throw compiler.new VoltCompilerException(msg);
            }

            // check the type of partition parameter meets our high standards
            Class<?> partitionType = paramTypes[procedure.getPartitionparameter()];
            Class<?>[] validPartitionClzzes = {
                    Long.class, Integer.class, Short.class, Byte.class,
                    long.class, int.class, short.class, byte.class,
                    String.class
            };
            boolean found = false;
            for (Class<?> candidate : validPartitionClzzes) {
                if (partitionType == candidate)
                    found = true;
            }
            // assume on of the two tests above passes and one fails
            if (!found) {
                String msg = "PartitionInfo parameter must be a String or Number for procedure: " + procedure.getClassname();
                throw compiler.new VoltCompilerException(msg);
            }
        }

        // put the compiled code for this procedure into the jarfile
        VoltCompiler.addClassToJar(procClass, compiler);
    }

    static void compileSingleStmtProcedure(VoltCompiler compiler, HSQLInterface hsql,
            DatabaseEstimates estimates, Catalog catalog, Database db,
            ProcedureDescriptor procedureDescriptor)
    throws VoltCompiler.VoltCompilerException {

        final String className = procedureDescriptor.m_className;
        if (className.indexOf('@') != -1) {
            throw compiler.new VoltCompilerException("User procedure names can't contain \"@\".");
        }

        // get the short name of the class (no package)
        String[] parts = className.split("\\.");
        String shortName = parts[parts.length - 1];

        // add an entry to the catalog
        final Procedure procedure = db.getProcedures().add(shortName);
        for (String userName : procedureDescriptor.m_authUsers) {
            final User user = db.getUsers().get(userName);
            if (user == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " has a user " + userName + " that does not exist");
            }
            final UserRef userRef = procedure.getAuthusers().add(userName);
            userRef.setUser(user);
        }
        for (String groupName : procedureDescriptor.m_authGroups) {
            final Group group = db.getGroups().get(groupName);
            if (group == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " has a group " + groupName + " that does not exist");
            }
            final GroupRef groupRef = procedure.getAuthgroups().add(groupName);
            groupRef.setGroup(group);
        }
        procedure.setClassname(className);
        // sysprocs don't use the procedure compiler
        procedure.setSystemproc(false);
        procedure.setHasjava(false);

        // get the annotation
        // first try to get one that has been passed from the compiler
        ProcInfoData info = compiler.getProcInfoOverride(shortName);
        // then check for the usual one in the class itself
        // and create a ProcInfo.Data instance for it
        if (info == null) {
            info = new ProcInfoData();
            if (procedureDescriptor.m_partitionString != null) {
                info.partitionInfo = procedureDescriptor.m_partitionString;
                info.singlePartition = true;
            }
        }
        assert(info != null);

        // ADD THE STATEMENT

        // add the statement to the catalog
        Statement catalogStmt = procedure.getStatements().add(VoltProcedure.ANON_STMT_NAME);

        // compile the statement
        StatementCompiler.compile(compiler, hsql, catalog, db,
                estimates, catalogStmt, procedureDescriptor.m_singleStmt,
                info.singlePartition);

        // if the single stmt is not read only, then the proc is not read only
        boolean procHasWriteStmts = (catalogStmt.getReadonly() == false);

        // set the read onlyness of a proc
        procedure.setReadonly(procHasWriteStmts == false);

        // set procedure parameter types
        CatalogMap<ProcParameter> params = procedure.getParameters();
        CatalogMap<StmtParameter> stmtParams = catalogStmt.getParameters();

        // set the procedure parameter types from the statement parameter types
        int i = 0;
        for (StmtParameter stmtParam : CatalogUtil.getSortedCatalogItems(stmtParams, "index")) {
            // name each parameter "param1", "param2", etc...
            ProcParameter procParam = params.add("param" + String.valueOf(i));
            procParam.setIndex(stmtParam.getIndex());
            procParam.setIsarray(false);
            procParam.setType(stmtParam.getJavatype());
            i++;
        }

        // parse the procinfo
        procedure.setSinglepartition(info.singlePartition);
        if (info.singlePartition) {
            parsePartitionInfo(compiler, db, procedure, info.partitionInfo);
            if (procedure.getPartitionparameter() >= params.size()) {
                String msg = "PartitionInfo parameter not a valid parameter for procedure: " + procedure.getClassname();
                throw compiler.new VoltCompilerException(msg);
            }
        }
    }

    /**
     * Determine which parameter is the partition indicator
     */
    static void parsePartitionInfo(VoltCompiler compiler, Database db,
            Procedure procedure, String info) throws VoltCompilerException {

        assert(procedure.getSinglepartition() == true);

        // check this isn't empty
        if (info.length() == 0) {
            String msg = "Missing or Truncated PartitionInfo in attribute for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // split on the colon
        String[] parts = info.split(":");

        // if the colon doesn't split well, we have a problem
        if (parts.length != 2) {
            String msg = "Possibly invalid PartitionInfo in attribute for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // relabel the parts for code readability
        String columnInfo = parts[0].trim();
        int paramIndex = Integer.parseInt(parts[1].trim());

        int paramCount = procedure.getParameters().size();
        if ((paramIndex < 0) || (paramIndex >= paramCount)) {
            String msg = "PartitionInfo specifies invalid column for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // locate the parameter
        procedure.setPartitionparameter(paramIndex);

        // split the columninfo
        parts = columnInfo.split("\\.");
        if (parts.length != 2) {
            String msg = "Possibly invalid PartitionInfo in attribute for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // relabel the parts for code readability
        String tableName = parts[0].trim();
        String columnName = parts[1].trim();

        // locate the partition column
        CatalogMap<Table> tables = db.getTables();
        for (Table table : tables) {
            if (table.getTypeName().equalsIgnoreCase(tableName)) {
                CatalogMap<Column> columns = table.getColumns();
                for (Column column : columns) {
                    if (column.getTypeName().equalsIgnoreCase(columnName)) {
                        procedure.setPartitioncolumn(column);
                        return;
                    }
                }
            }
        }

        String msg = "Unable to locate partition column in PartitionInfo for procedure: " + procedure.getClassname();
        throw compiler.new VoltCompilerException(msg);
    }
}
