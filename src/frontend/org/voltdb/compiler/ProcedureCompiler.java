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

package org.voltdb.compiler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.hsqldb_voltpatches.HSQLInterface;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ProcInfo;
import org.voltdb.ProcInfoData;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
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
import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.planner.PartitioningForStatement;
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

    public static Map<String, Field> getValidSQLStmts(VoltCompiler compiler, String procName, Class<?> procClass, boolean withPrivate)
            throws VoltCompilerException {

        Map<String, Field> retval = new HashMap<String, Field>();

        Field[] fields = procClass.getDeclaredFields();
        for (Field f : fields) {
            // skip non SQL fields
            if (f.getType() != SQLStmt.class)
                continue;

            int modifiers = f.getModifiers();

            // skip private fields if asked (usually a superclass)
            if (Modifier.isPrivate(modifiers) && (!withPrivate))
                continue;

            // don't allow non-final SQLStmts
            if (Modifier.isFinal(modifiers) == false) {
                String msg = "Procedure " + procName + " contains a non-final SQLStmt field.";
                if (procClass.getSimpleName().equals(procName) == false) {
                    msg = "Superclass " + procClass.getSimpleName() + " of procedure " +
                          procName + " contains a non-final SQLStmt field.";
                }
                if (compiler != null)
                    throw compiler.new VoltCompilerException(msg);
                else
                    new VoltLogger("HOST").warn(msg);
            }

            f.setAccessible(true);
            retval.put(f.getName(), f);
        }

        Class<?> superClass = procClass.getSuperclass();
        if (superClass != null) {
            Map<String, Field> superStmts = getValidSQLStmts(compiler, procName, superClass, false);
            for (Entry<String, Field> e : superStmts.entrySet()) {
                if (retval.containsKey(e.getKey()) == false)
                    retval.put(e.getKey(), e.getValue());
            }
        }

        return retval;
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

        // track if there are any writer statements and/or sequential scans and/or an overlooked common partitioning parameter
        boolean procHasWriteStmts = false;
        boolean procHasSeqScans = false;
        boolean procHasCommonPartitioning = true; // true but procPartitionExpression == null means a correctly MP proc
        AbstractExpression commonPartitionExpression = null;
        String exampleSPstatement = null;
        Object exampleSPvalue = null;

        // iterate through the fields and deal with
        Map<String, Field> stmtMap = getValidSQLStmts(compiler, procClass.getSimpleName(), procClass, true);
        for (Field f : stmtMap.values()) {
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
            Object partitionParameter = null;
            if (info.singlePartition) {
                // Dummy up a partitioning value to indicate the intent and prevent the planner
                // from trying to infer a constant partitioning value from the statement.
                partitionParameter = "StatementCompiler dummied up single partitioning for QueryPlanner";
            }
            PartitioningForStatement partitioning = new PartitioningForStatement(partitionParameter, false);
            StatementCompiler.compile(compiler, hsql, catalog, db,
                    estimates, catalogStmt, stmt.getText(), stmt.getJoinOrder(), partitioning);

            if (partitioning.wasSpecifiedAsSingle()) {
                procHasCommonPartitioning = false; // Don't try to infer what's already been asserted.
                // The planner does not currently attempt to second-guess a plan declared as single-partition, maybe some day.
                // In theory, the PartitioningForStatement would confirm the use of (only) a parameter as a partition key --
                // or if the partition key was determined to be some other constant (expression?) it might display an informational
                // message that the passed parameter is assumed to be equal to the hard-coded partition key constant (expression).

                // Validate any inferred statement partitioning given the statement's possible usage, until a contradiction is found.
            } else if (procHasCommonPartitioning) {
                // Only consider statements that are capable of running SP with a partitioning parameter that does not seem to
                // conflict with the partitioning of prior statements.

                // Usually, replicated-only statements in a mix with others have no effect on the MP/SP decision
                if (partitioning.getCountOfPartitionedTables() == 0) {
                    // but SP is strictly forbidden in this case, for consistency of the replicated data.
                    if (partitioning.getIsReplicatedTableDML()) {
                        procHasCommonPartitioning = false;
                    }

                } else if (partitioning.getCountOfIndependentlyPartitionedTables() == 0) {
                    AbstractExpression statementPartitionExpression = partitioning.singlePartitioningExpression();
                    if (statementPartitionExpression != null) {
                        if (commonPartitionExpression == null) {
                            commonPartitionExpression = statementPartitionExpression;
                            exampleSPstatement = stmt.getText();
                            exampleSPvalue = partitioning.effectivePartitioningValue();
                        } else if (commonPartitionExpression.equals(statementPartitionExpression) ||
                                   (statementPartitionExpression instanceof ParameterValueExpression &&
                                    commonPartitionExpression instanceof ParameterValueExpression)) {
                            // Any constant used for partitioning would have to be the same for all statements, but
                            // any statement parameter used for partitioning MIGHT come from the same proc parameter as
                            // any other statement's parameter used for partitioning.
                        } else {
                            procHasCommonPartitioning = false; // appears to be different partitioning for different statements
                        }
                    }
                } else {
                    // There is a statement with a partitioned table whose partitioning column is
                    // not equality filtered with a constant or param. Abandon all hope.
                    procHasCommonPartitioning = false;
                }
            }

            // if a single stmt is not read only, then the proc is not read only
            if (catalogStmt.getReadonly() == false)
                procHasWriteStmts = true;

            if (catalogStmt.getSeqscancount() > 0) {
                procHasSeqScans = true;
            }
        }

        // MIGHT the planner have uncovered an overlooked opportunity to run all statements SP?
        if (procHasCommonPartitioning && (commonPartitionExpression != null)) {
            String msg = null;
            if (commonPartitionExpression instanceof ParameterValueExpression) {
                msg = "This procedure might benefit from an @ProcInfo annotation designating parameter " +
                        ((ParameterValueExpression) commonPartitionExpression).getParameterIndex() +
                        " of statement '" + exampleSPstatement + "'";
            } else {
                String valueDescription = null;
                if (exampleSPvalue == null) {
                    // Statements partitioned on a runtime constant. This is likely to be cryptic, but hopefully gets the idea across.
                    valueDescription = "of " + commonPartitionExpression.toString();
                } else {
                    valueDescription = exampleSPvalue.toString(); // A simple constant value COULD have been a parameter.
                }
                msg = "This procedure might benefit from an @ProcInfo annotation referencing an added parameter passed the value " +
                        valueDescription;
            }
            compiler.addInfo(msg);
        }

        // set the read onlyness of a proc
        procedure.setReadonly(procHasWriteStmts == false);

        procedure.setHasseqscans(procHasSeqScans);

        for (Statement catalogStmt : procedure.getStatements()) {
            if (catalogStmt.getIscontentdeterministic() == false) {
                String potentialErrMsg =
                    "Procedure " + shortName + " has a statement with a non-deterministic result - statement: \"" +
                    catalogStmt.getSqltext() + "\" , reason: " + catalogStmt.getNondeterminismdetail();
                // throw compiler.new VoltCompilerException(potentialErrMsg);
                compiler.addWarn(potentialErrMsg);
            }
            else if (catalogStmt.getIsorderdeterministic() == false) {
                String warnMsg;
                if (procHasWriteStmts) {
                    String rwPotentialErrMsg = "Procedure " + shortName +
                            " is RW and has a statement whose result has a non-deterministic ordering - statement: \"" +
                            catalogStmt.getSqltext() + "\", reason: " + catalogStmt.getNondeterminismdetail();
                    // throw compiler.new VoltCompilerException(rwPotentialErrMsg);
                    warnMsg = rwPotentialErrMsg;
                }
                else {
                    warnMsg = "Procedure " + shortName +
                        " has a statement with a non-deterministic result - statement: \"" +
                        catalogStmt.getSqltext() + "\", reason: " + catalogStmt.getNondeterminismdetail();
                }
                compiler.addWarn(warnMsg);
            }
        }

        // find the run() method and get the params
        Method procMethod = null;
        Method[] methods = procClass.getDeclaredMethods();
        for (final Method m : methods) {
            String name = m.getName();
            if (name.equals("run")) {
                assert (m.getDeclaringClass() == procClass);

                // if not null, then we've got more than one run method
                if (procMethod != null) {
                    String msg = "Procedure: " + shortName + " has multiple public run(...) methods. ";
                    msg += "Only a single run(...) method is supported.";
                    throw compiler.new VoltCompilerException(msg);
                }

                if (Modifier.isPublic(m.getModifiers())) {
                    // found it!
                    procMethod = m;
                }
                else {
                    compiler.addWarn("Procedure: " + shortName + " has non-public run(...) method.");
                }
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
            catch (VoltTypeException e) {
                // handle the case where the type is invalid
                String msg = "Procedure: " + shortName + " has a parameter with invalid type: ";
                msg += cls.getSimpleName();
                throw compiler.new VoltCompilerException(msg);
            }
            catch (RuntimeException e) {
                String msg = "Procedure: " + shortName + " unexpectedly failed a check on a parameter of type: ";
                msg += cls.getSimpleName();
                msg += " with error: ";
                msg += e.toString();
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

            VoltType columnType = VoltType.get((byte)procedure.getPartitioncolumn().getType());
            VoltType paramType = VoltType.typeFromClass(partitionType);
            if (columnType != paramType) {
                String msg = "Mismatch between partition column and partition parameter for procedure " +
                    procedure.getClassname() + "\nPartition column is type " + columnType +
                    " and partition parameter is type " + paramType;
                throw compiler.new VoltCompilerException(msg);
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

        // get the short name of the class (no package if a user procedure)
        // use the Table.<builtin> name (allowing the period) if builtin.
        String shortName = className;
        if (procedureDescriptor.m_builtInStmt == false) {
            String[] parts = className.split("\\.");
            shortName = parts[parts.length - 1];
        }


        // add an entry to the catalog (using the full className)
        final Procedure procedure = db.getProcedures().add(shortName);
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
        Statement catalogStmt = procedure.getStatements().add(VoltDB.ANON_STMT_NAME);

        // compile the statement
        Object partitionParameter = null;
        if (info.singlePartition) {
            // Dummy up a partitioning value to indicate the intent and prevent the planner
            // from trying to infer a constant partitioning value from the statement.
            partitionParameter = "StatementCompiler dummied up single partitioning for QueryPlanner";
        }
        PartitioningForStatement partitioning = new PartitioningForStatement(partitionParameter, false);
        StatementCompiler.compile(compiler, hsql, catalog, db,
                estimates, catalogStmt, procedureDescriptor.m_singleStmt,
                procedureDescriptor.m_joinOrder, partitioning);

        // if the single stmt is not read only, then the proc is not read only
        boolean procHasWriteStmts = (catalogStmt.getReadonly() == false);

        // set the read onlyness of a proc
        procedure.setReadonly(procHasWriteStmts == false);

        int seqs = catalogStmt.getSeqscancount();
        procedure.setHasseqscans(seqs > 0);

        // set procedure parameter types
        CatalogMap<ProcParameter> params = procedure.getParameters();
        CatalogMap<StmtParameter> stmtParams = catalogStmt.getParameters();

        // set the procedure parameter types from the statement parameter types
        int paramCount = 0;
        for (StmtParameter stmtParam : CatalogUtil.getSortedCatalogItems(stmtParams, "index")) {
            // name each parameter "param1", "param2", etc...
            ProcParameter procParam = params.add("param" + String.valueOf(paramCount));
            procParam.setIndex(stmtParam.getIndex());
            procParam.setIsarray(false);
            procParam.setType(stmtParam.getJavatype());
            paramCount++;
        }

        // parse the procinfo
        procedure.setSinglepartition(info.singlePartition);
        if (info.singlePartition) {
            parsePartitionInfo(compiler, db, procedure, info.partitionInfo);
            if (procedure.getPartitionparameter() >= params.size()) {
                String msg = "PartitionInfo parameter not a valid parameter for procedure: " + procedure.getClassname();
                throw compiler.new VoltCompilerException(msg);
            }
            // TODO: The planner does not currently validate that a single-statement plan declared as single-partition correctly uses
            // the designated parameter as a partitioning filter, maybe some day.
            // In theory, the PartitioningForStatement would confirm the use of (only) a parameter as a partition key --
            // or if the partition key was determined to be some other hard-coded constant (expression?) it might display a warning
            // message that the passed parameter is assumed to be equal to that constant (expression).
        } else {
            if (partitioning.getCountOfPartitionedTables() > 0 &&
                    partitioning.getCountOfIndependentlyPartitionedTables() == 0) {
                AbstractExpression statementPartitionExpression = partitioning.singlePartitioningExpression();
                if (statementPartitionExpression != null) {
                    // The planner has uncovered an overlooked opportunity to run the statement SP.
                    String msg = null;
                    if (statementPartitionExpression instanceof ParameterValueExpression) {
                        msg = "This procedure would benefit from setting the attribute 'partitioninfo=" + partitioning.getFullColumnName() +
                                ":" + ((ParameterValueExpression) statementPartitionExpression).getParameterIndex() + "'";
                    } else {
                        String valueDescription = null;
                        Object partitionValue = partitioning.effectivePartitioningValue();
                        if (partitionValue == null) {
                            // Statement partitioned on a runtime constant. This is likely to be cryptic, but hopefully gets the idea across.
                            valueDescription = "of " + statementPartitionExpression.toString();
                        } else {
                            valueDescription = partitionValue.toString(); // A simple constant value COULD have been a parameter.
                        }
                        msg = "This procedure would benefit from adding a parameter to be passed the value " + valueDescription +
                                " and setting the attribute 'partitioninfo=" + partitioning.getFullColumnName() +
                                ":" + paramCount  + "'";
                    }
                    compiler.addWarn(msg);
                }
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
            String msg = "PartitionInfo specifies invalid parameter index for procedure: " + procedure.getClassname();
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
                Column partitionColumn = table.getPartitioncolumn();
                if (partitionColumn == null) {
                    String msg = String.format("PartitionInfo for procedure %s references table %s which has no partition column (may be replicated).",
                            procedure.getClassname(), table.getTypeName());
                    throw compiler.new VoltCompilerException(msg);
                }

                for (Column column : columns) {
                    if (column.getTypeName().equalsIgnoreCase(columnName)) {
                        if (partitionColumn.getTypeName().equals(column.getTypeName())) {
                            procedure.setPartitioncolumn(column);
                            return;
                        }
                        else {
                            String msg = "PartitionInfo for procedure " + procedure.getClassname() + " refers to a column in schema which is not a partition key.";
                            throw compiler.new VoltCompilerException(msg);
                        }
                    }
                }
            }
        }

        String msg = "PartitionInfo for procedure " + procedure.getClassname() + " refers to a column in schema which can't be found.";
        throw compiler.new VoltCompilerException(msg);
    }
}
