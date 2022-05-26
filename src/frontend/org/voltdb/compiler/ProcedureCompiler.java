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

package org.voltdb.compiler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import org.hsqldb_voltpatches.HSQLInterface;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltNonTransactionalProcedure;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
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
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.compilereport.ProcedureAnnotation;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.parser.SQLLexer;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.types.QueryType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Compiles stored procedures into a given catalog,
 * invoking the StatementCompiler as needed.
 *
 * All member functions are static.
 */
public class ProcedureCompiler {

    static void compile(VoltCompiler compiler,
                        HSQLInterface hsql,
                        DatabaseEstimates estimates,
                        Database db,
                        ProcedureDescriptor procedureDescriptor,
                        InMemoryJarfile jarOutput) throws VoltCompiler.VoltCompilerException {

        assert(compiler != null);
        assert(hsql != null);
        assert(estimates != null);

        if (procedureDescriptor.m_stmtLiterals == null) {
            compileJavaProcedure(compiler, hsql, estimates, db, procedureDescriptor, jarOutput);
        }
        else {
            compileDDLProcedure(compiler, hsql, estimates, db, procedureDescriptor);
        }
    }

    public static Map<String, SQLStmt> getValidSQLStmts(VoltCompiler compiler,
                                                        String procName,
                                                        Class<?> procClass,
                                                        Object procInstance,
                                                        boolean withPrivate)
                                                                throws VoltCompilerException
    {
        Map<String, SQLStmt> retval = new HashMap<>();

        Field[] fields = procClass.getDeclaredFields();
        for (Field f : fields) {
            // skip non SQL fields
            if (f.getType() != SQLStmt.class) {
                continue;
            }

            int modifiers = f.getModifiers();

            // skip private fields if asked (usually a superclass)
            if (Modifier.isPrivate(modifiers) && (!withPrivate)) {
                continue;
            }

            // don't allow non-final SQLStmts
            if (Modifier.isFinal(modifiers) == false) {
                String msg = "Procedure " + procName + " contains a non-final SQLStmt field.";
                if (procClass.getSimpleName().equals(procName) == false) {
                    msg = "Superclass " + procClass.getSimpleName() + " of procedure " +
                          procName + " contains a non-final SQLStmt field.";
                }
                if (compiler != null) {
                    throw compiler.new VoltCompilerException(msg);
                } else {
                    new VoltLogger("HOST").warn(msg);
                }
            }

            f.setAccessible(true);

            SQLStmt stmt = null;

            try {
                stmt = (SQLStmt) f.get(procInstance);
            }
            // this exception handling here comes from other parts of the code
            // it's weird, but seems rather hard to hit
            catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            retval.put(f.getName(), stmt);
        }

        Class<?> superClass = procClass.getSuperclass();
        if (superClass != null) {
            Map<String, SQLStmt> superStmts = getValidSQLStmts(compiler, procName, superClass, procInstance, false);
            for (Entry<String, SQLStmt> e : superStmts.entrySet()) {
                if (retval.containsKey(e.getKey()) == false) {
                    retval.put(e.getKey(), e.getValue());
                }
            }
        }

        return retval;
    }

    public static Map<String, SQLStmt> getSQLStmtMap(VoltCompiler compiler, Class<?> procClass) throws VoltCompilerException {
        VoltProcedure procInstance;
        try {
            procInstance = (VoltProcedure) procClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Error instantiating procedure " + procClass.getName(), e);
        }
        Map<String, SQLStmt> stmtMap = getValidSQLStmts(compiler, procClass.getSimpleName(), procClass, procInstance, true);
        return stmtMap;
    }

    /**
     * get the short name of the class (no package)
     * @param className fully qualified (or not) class name
     * @return short name of the class (no package)
     */
    public static String deriveShortProcedureName( String className) {
        if( className == null || className.trim().isEmpty()) {
            return null;
        }
        String[] parts = className.split("\\.");
        String shortName = parts[parts.length - 1];

        return shortName;
    }

    public static Map<String, Object> getFiledsMap(VoltCompiler compiler, Map<String, SQLStmt> stmtMap,
            Class<?> procClass, String shortName) throws VoltCompilerException {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.putAll(stmtMap);

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
        if (!procMethod.getReturnType().getCanonicalName().equals(VoltTable[].class.getCanonicalName()) &&
            !procMethod.getReturnType().getCanonicalName().equals(VoltTable.class.getCanonicalName())   &&
            !procMethod.getReturnType().getCanonicalName().equals(long.class.getCanonicalName())        &&
            !procMethod.getReturnType().getCanonicalName().equals(Long.class.getCanonicalName())           ) {

            String msg = "Procedure: " + shortName + " has run(...) method that doesn't return long, Long, VoltTable or VoltTable[].";
            throw compiler.new VoltCompilerException(msg);
        }

        builder.put("@run",procMethod);

        Map<String, Object> fields = builder.build();
        return fields;
    }

    public static void compileSQLStmtUpdatingProcedureInfomation(VoltCompiler compiler,
            HSQLInterface hsql, DatabaseEstimates estimates, Database db,
            Procedure procedure, boolean isSinglePartition, Map<String, Object> fields)
                    throws VoltCompilerException {
        // track if there are any writer statements and/or sequential scans and/or an overlooked common partitioning parameter
        boolean procHasWriteStmts = false;
        boolean procHasSeqScans = false;
        // procWantsCommonPartitioning == true but commonPartitionExpression == null signifies a proc
        // for which the planner was requested to attempt to find an SP plan, but that was not possible
        // -- it had a replicated write or it had one or more partitioned reads that were not all
        // filtered by the same partition key value -- so it was planned as an MP proc.
        boolean procWantsCommonPartitioning = true;
        AbstractExpression commonPartitionExpression = null;
        String exampleSPstatement = null;
        Object exampleSPvalue = null;

        // Determine if the procedure is read-only or read-write by checking if the procedure contains any write SQL statements.
        boolean readWrite = false;
        for (Object field : fields.values()) {
            if (!(field instanceof SQLStmt)) {
                continue;
            }
            SQLStmt stmt = (SQLStmt)field;
            QueryType qtype = QueryType.getFromSQL(stmt.getText());
            if (!qtype.isReadOnly()) {
                readWrite = true;
                break;
            }
        }

        // default to FASTER determinism mode, which may favor non-deterministic plans
        // but if it's a read-write proc, use a SAFER planning mode wrt determinism.
        final DeterminismMode detMode = readWrite ? DeterminismMode.SAFER : DeterminismMode.FASTER;

        for (Entry<String, Object> entry : fields.entrySet()) {
            if (!(entry.getValue() instanceof SQLStmt)) {
                continue;
            }

            String stmtName = entry.getKey();
            SQLStmt stmt = (SQLStmt)entry.getValue();

            // add the statement to the catalog
            Statement catalogStmt = procedure.getStatements().add(stmtName);

            // compile the statement
            StatementPartitioning partitioning =
                    isSinglePartition ? StatementPartitioning.forceSP() :
                                       StatementPartitioning.forceMP();
            boolean cacheHit = StatementCompiler.compileFromSqlTextAndUpdateCatalog(compiler, hsql, db,
                    estimates, catalogStmt, stmt.getText(), stmt.getJoinOrder(),
                    detMode, partitioning);

            // ENG-14487 truncate statement is not allowed for single partitioned procedures.
            if (isSinglePartition && stmt.getText().toUpperCase().startsWith("TRUNCATE")) {
                throw compiler.new VoltCompilerException("Single partitioned procedure: " +
                        procedure.getClassname() + " has TRUNCATE statement: \"" + stmt.getText() + "\".");
            }

            // if this was a cache hit or specified single, don't worry about figuring out more partitioning
            if (partitioning.wasSpecifiedAsSingle() || cacheHit) {
                procWantsCommonPartitioning = false; // Don't try to infer what's already been asserted.
                // The planner does not currently attempt to second-guess a plan declared as single-partition, maybe some day.
                // In theory, the PartitioningForStatement would confirm the use of (only) a parameter as a partition key --
                // or if the partition key was determined to be some other constant (expression?) it might display an informational
                // message that the passed parameter is assumed to be equal to the hard-coded partition key constant (expression).

                // Validate any inferred statement partitioning given the statement's possible usage, until a contradiction is found.
            }
            else if (procWantsCommonPartitioning) {
                // Only consider statements that are capable of running SP with a partitioning parameter that does not seem to
                // conflict with the partitioning of prior statements.
                if (partitioning.getCountOfIndependentlyPartitionedTables() == 1) {
                    AbstractExpression statementPartitionExpression = partitioning.singlePartitioningExpressionForReport();
                    if (statementPartitionExpression != null) {
                        if (commonPartitionExpression == null) {
                            commonPartitionExpression = statementPartitionExpression;
                            exampleSPstatement = stmt.getText();
                            exampleSPvalue = partitioning.getInferredPartitioningValue();
                        }
                        else if (commonPartitionExpression.equals(statementPartitionExpression) ||
                                   (statementPartitionExpression instanceof ParameterValueExpression &&
                                    commonPartitionExpression instanceof ParameterValueExpression)) {
                            // Any constant used for partitioning would have to be the same for all statements, but
                            // any statement parameter used for partitioning MIGHT come from the same proc parameter as
                            // any other statement's parameter used for partitioning.
                        }
                        else {
                            procWantsCommonPartitioning = false; // appears to be different partitioning for different statements
                        }
                    }
                    else {
                        // There is a statement with a partitioned table whose partitioning column is
                        // not equality filtered with a constant or param. Abandon all hope.
                        procWantsCommonPartitioning = false;
                    }

                // Usually, replicated-only statements in a mix with others have no effect on the MP/SP decision
                }
                else if (partitioning.getCountOfPartitionedTables() == 0) {
                    // but SP is strictly forbidden for DML, to maintain the consistency of the replicated data.
                    if (partitioning.getIsReplicatedTableDML()) {
                        procWantsCommonPartitioning = false;
                    }

                }
                else {
                    // There is a statement with a partitioned table whose partitioning column is
                    // not equality filtered with a constant or param. Abandon all hope.
                    procWantsCommonPartitioning = false;
                }
            }

            // if a single stmt is not read only, then the proc is not read only
            if (catalogStmt.getReadonly() == false) {
                procHasWriteStmts = true;
            }

            if (catalogStmt.getSeqscancount() > 0) {
                procHasSeqScans = true;
            }
        }

        // MIGHT the planner have uncovered an overlooked opportunity to run all statements SP?
        if (procWantsCommonPartitioning && (commonPartitionExpression != null)) {
            String msg = null;
            if (commonPartitionExpression instanceof ParameterValueExpression) {
                msg = "This procedure might benefit from partitioning designating parameter " +
                        ((ParameterValueExpression) commonPartitionExpression).getParameterIndex() +
                        " of statement '" + exampleSPstatement + "'";
            } else {
                String valueDescription = null;
                if (exampleSPvalue == null) {
                    // Statements partitioned on a runtime constant. This is likely to be cryptic, but hopefully gets the idea across.
                    valueDescription = "of " + commonPartitionExpression.explain("");
                } else {
                    valueDescription = exampleSPvalue.toString(); // A simple constant value COULD have been a parameter.
                }
                msg = "This procedure might benefit from partitioning referencing an added parameter passed the value " +
                        valueDescription;
            }
            compiler.addInfo(msg);
        }

        // set the read onlyness of a proc
        procedure.setReadonly(procHasWriteStmts == false);

        procedure.setHasseqscans(procHasSeqScans);

        String shortName = deriveShortProcedureName(procedure.getClassname());
        checkForDeterminismWarnings(compiler, shortName, procedure, procHasWriteStmts);
    }

    public static Class<?>[] setParameterTypes(VoltCompiler compiler, Procedure procedure, String shortName, Method procMethod)
            throws VoltCompilerException {
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
            } else {
                param.setIsarray(false);
            }

            if ((cls == Float.class) || (cls == float.class)) {
                String msg = "Procedure: " + shortName + " has a parameter with type: ";
                msg += cls.getSimpleName();
                msg += ". Replace this parameter type with double and the procedure may compile.";
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
        return paramTypes;
    }

    public static void addPartitioningInfo(VoltCompiler compiler, Procedure procedure,
            Database db, Class<?>[] paramTypes, ProcedurePartitionData partitionData)
                    throws VoltCompilerException {

        if (partitionData.isMultiPartitionProcedure()) {
            procedure.setSinglepartition(false);
            return;
        }

        procedure.setSinglepartition(partitionData.isSinglePartition());
        setCatalogProcedurePartitionInfo(compiler, db, procedure, partitionData);
        if (procedure.getPartitionparameter() == -1) {
            return;
        }

        if (procedure.getPartitionparameter() >= paramTypes.length) {
            String msg = "Partition parameter is not a valid parameter for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // check the type of partition parameter meets our high standards
        Class<?> partitionType = paramTypes[procedure.getPartitionparameter()];
        Class<?>[] validPartitionClzzes = {
                Long.class, Integer.class, Short.class, Byte.class,
                long.class, int.class, short.class, byte.class,
                String.class, byte[].class
        };
        boolean found = false;
        for (Class<?> candidate : validPartitionClzzes) {
            if (partitionType == candidate) {
                found = true;
            }
        }
        if (!found) {
            String msg = "Partition parameter must be a String or Number for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        VoltType columnType = VoltType.get((byte)procedure.getPartitioncolumn().getType());
        VoltType paramType = VoltType.typeFromClass(partitionType);
        if ( ! columnType.canExactlyRepresentAnyValueOf(paramType)) {
            String msg = "Type mismatch between partition column and partition parameter for procedure " +
                    procedure.getClassname() + " may cause overflow or loss of precision.\nPartition column is type " + columnType +
                    " and partition parameter is type " + paramType;
            throw compiler.new VoltCompilerException(msg);
        } else if ( ! paramType.canExactlyRepresentAnyValueOf(columnType)) {
            String msg = "Type mismatch between partition column and partition parameter for procedure " +
                    procedure.getClassname() + " does not allow the full range of partition key values.\nPartition column is type " + columnType +
                    " and partition parameter is type " + paramType;
            compiler.addWarn(msg);
        }
    }

    static void compileJavaProcedure(VoltCompiler compiler,
                                     HSQLInterface hsql,
                                     DatabaseEstimates estimates,
                                     Database db,
                                     ProcedureDescriptor procedureDescriptor,
                                     InMemoryJarfile jarOutput) throws VoltCompiler.VoltCompilerException {
        final String className = procedureDescriptor.m_className;

        // Load the class given the class name
        Class<?> procClass = procedureDescriptor.m_class;

        // Get the short name of the class (no package)
        String shortName = deriveShortProcedureName(className);

        // Add an entry to the catalog
        final Procedure procedure = db.getProcedures().add(shortName);
        for (String groupName : procedureDescriptor.m_authGroups) {
            final Group group = db.getGroups().get(groupName);
            if (group == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " allows access by a role " + groupName + " that does not exist");
            }
            final GroupRef groupRef = procedure.getAuthgroups().add(groupName);
            groupRef.setGroup(group);
        }
        procedure.setClassname(className);
        // System procedures don't use the procedure compiler
        procedure.setSystemproc(false);
        procedure.setDefaultproc(procedureDescriptor.m_builtInStmt);
        procedure.setHasjava(true);
        ProcedureAnnotation pa = (ProcedureAnnotation) procedure.getAnnotation();
        if (pa == null) {
            pa = new ProcedureAnnotation();
            procedure.setAnnotation(pa);
        }

        // check if partition info was set in ddl.
        // if not, it's multi-partition; other non-partitioned cases (directed
        // and compound) have info containg a null partition table name
        ProcedurePartitionData info = procedureDescriptor.m_partitionData;
        if (info == null) {
            info = new ProcedurePartitionData(ProcedurePartitionData.Type.MULTI);
        }

        // if the procedure is non-transactional, then take this special path here
        if (VoltNonTransactionalProcedure.class.isAssignableFrom(procClass)) {
            compileNTProcedure(compiler, procClass, procedure, jarOutput);
            return;
        }

        // if still here, that means the procedure is transactional
        procedure.setTransactional(true);

        // iterate through the fields and get valid sql statements
        Map<String, SQLStmt> stmtMap = getSQLStmtMap(compiler, procClass);
        Map<String, Object> fields = getFiledsMap(compiler, stmtMap, procClass, shortName);
        Method procMethod = (Method) fields.get("@run");
        assert(procMethod != null);

        compileSQLStmtUpdatingProcedureInfomation(compiler, hsql, estimates, db, procedure,
                info.isSinglePartition(), fields);
        if( VoltDB.instance().getKFactor() > 0 && info.isSinglePartition() ) {
            checkForMutableParamsWarning(compiler,shortName,procMethod);
        }
        // set procedure parameter types
        Class<?>[] paramTypes = setParameterTypes(compiler, procedure, shortName, procMethod);

        addPartitioningInfo(compiler, procedure, db, paramTypes, info);

        // put the compiled code for this procedure into the jarfile
        // need to find the outermost ancestor class for the procedure in the event
        // that it's actually an inner (or inner inner...) class.
        // addClassToJar recursively adds all the children, which should include this
        // class
        Class<?> ancestor = procClass;
        while (ancestor.getEnclosingClass() != null) {
            ancestor = ancestor.getEnclosingClass();
        }
        compiler.addClassToJar(jarOutput, ancestor);
    }

    public static void compileNTProcedure(VoltCompiler compiler,
                                           Class<?> procClass,
                                           Procedure procedure,
                                           InMemoryJarfile jarOutput)
                                                   throws VoltCompilerException
    {
         // get the short name of the class (no package)
        String shortName = deriveShortProcedureName(procClass.getName());

        try {
            procClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(String.format("Error instantiating procedure \"%s\"", procClass.getName()), e);
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
        if ((procMethod.getReturnType() != CompletableFuture.class) &&
           (procMethod.getReturnType() != VoltTable[].class) &&
           (procMethod.getReturnType() != VoltTable.class) &&
           (procMethod.getReturnType() != long.class) &&
           (procMethod.getReturnType() != Long.class)) {

            String msg = "Procedure: " + shortName + " has run(...) method that doesn't return long, Long, VoltTable, VoltTable[] or CompleteableFuture.";
            throw compiler.new VoltCompilerException(msg);
        }

        // set procedure parameter types
        CatalogMap<ProcParameter> params = procedure.getParameters();
        Class<?>[] paramTypes = procMethod.getParameterTypes();
        setCatalogProcedureParameterTypes(compiler, shortName, params, paramTypes);

        // actually make sure the catalog records this is a different kind of procedure
        procedure.setTransactional(false);

        // put the compiled code for this procedure into the jarfile
        // need to find the outermost ancestor class for the procedure in the event
        // that it's actually an inner (or inner inner...) class.
        // addClassToJar recursively adds all the children, which should include this
        // class
        Class<?> ancestor = procClass;
        while (ancestor.getEnclosingClass() != null) {
            ancestor = ancestor.getEnclosingClass();
        }
        compiler.addClassToJar(jarOutput, ancestor);
    }

    private static void setCatalogProcedureParameterTypes(VoltCompiler compiler,
                                                   String shortName,
                                                   CatalogMap<ProcParameter> params,
                                                   Class<?>[] paramTypes)
                                                           throws VoltCompilerException
    {
        for (int i = 0; i < paramTypes.length; i++) {
            Class<?> cls = paramTypes[i];
            ProcParameter param = params.add(String.valueOf(i));
            param.setIndex(i);

            // handle the case where the param is an array
            if (cls.isArray()) {
                param.setIsarray(true);
                cls = cls.getComponentType();
            } else {
                param.setIsarray(false);
            }

            // boxed types are not supported parameters at this time
            if ((cls == Long.class) || (cls == Integer.class) || (cls == Short.class) ||
                (cls == Byte.class) || (cls == Double.class) ||
                (cls == Character.class) || (cls == Boolean.class))
            {
                String msg = "Procedure: " + shortName + " has a parameter with a boxed type: ";
                msg += cls.getSimpleName();
                msg += ". Replace this parameter with the corresponding primitive type and the procedure may compile.";
                throw compiler.new VoltCompilerException(msg);
            } else if ((cls == Float.class) || (cls == float.class)) {
                String msg = "Procedure: " + shortName + " has a parameter with type: ";
                msg += cls.getSimpleName();
                msg += ". Replace this parameter type with double and the procedure may compile.";
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
    }

    public static void checkForDeterminismWarnings(VoltCompiler compiler, String shortName, final Procedure procedure,
                                         boolean procHasWriteStmts) {
        for (Statement catalogStmt : procedure.getStatements()) {
            if (catalogStmt.getIscontentdeterministic() == false) {
                String potentialErrMsg =
                    "Procedure " + shortName + " has a statement with a non-deterministic result - statement: \"" +
                    catalogStmt.getSqltext() + "\" , reason: " + catalogStmt.getNondeterminismdetail();
                compiler.addWarn(potentialErrMsg);
            }
            else if (catalogStmt.getIsorderdeterministic() == false) {
                String warnMsg;
                if (procHasWriteStmts) {
                    String rwPotentialErrMsg = "Procedure " + shortName +
                            " is RW and has a statement whose result has a non-deterministic ordering - statement: \"" +
                            catalogStmt.getSqltext() + "\", reason: " + catalogStmt.getNondeterminismdetail();
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
    }

    public static void checkForMutableParamsWarning(VoltCompiler compiler, String shortName, Method procMethod) {
        Class<?>[] paramTypes = procMethod.getParameterTypes();
        boolean hasArr = false;
        for(Class<?> param : paramTypes){
            if( param.isArray() ) {
                hasArr = true;
                break;
            }
        }
        if( hasArr ) {
            SystemSettingsType.Procedure procedureSetting = VoltDB.instance().getCatalogContext().getDeployment().getSystemsettings().getProcedure();
            if ( procedureSetting == null || procedureSetting.isCopyparameters() ) {
                String infoMsg = "Procedure "+ shortName + " contains a mutable array parameter." +
                        " VoltDb can be optimized by disabling copyparameters configuration option." +
                        " In that case, all parameters including arrays must remain immutable within the scope of Stored Procedures.";
                compiler.addInfo(infoMsg);
            }
            else {
                String warnMsg = "Procedure " + shortName +
                        " contains a mutable array parameter but the database is configured not to copy parameters before execution." +
                        " This can result in unpredictable behavior, crashes or data corruption if stored procedure modifies the content of the parameters." +
                        " Set the copyparameters configuration option to true to avoid this danger if the stored procedures might modify parameter content.";
                compiler.addWarn(warnMsg);
            }
        }
    }


    static void compileDDLProcedure(VoltCompiler compiler,
                                    HSQLInterface hsql,
                                    DatabaseEstimates estimates,
                                    Database db,
                                    ProcedureDescriptor procedureDescriptor) throws VoltCompiler.VoltCompilerException {
        final String className = procedureDescriptor.m_className;

        if (className.indexOf('@') != -1) {
            throw compiler.new VoltCompilerException("User procedure names can't contain \"@\".");
        }

        // If there are multiple statements, all the statements are concatenated and stored in m_stmtLiterals.
        String stmtsStr = procedureDescriptor.m_stmtLiterals;

        // Get the short name of the class (no package if a user procedure)
        // use the Table.<built-in name> (allowing the period) if it is built-in.
        String shortName = className;
        if (procedureDescriptor.m_builtInStmt == false) {
            String[] parts = className.split("\\.");
            shortName = parts[parts.length - 1];
        }

        // Add an entry to the catalog (using the full className)
        final Procedure procedure = db.getProcedures().add(shortName);
        for (String groupName : procedureDescriptor.m_authGroups) {
            final Group group = db.getGroups().get(groupName);
            if (group == null) {
                throw compiler.new VoltCompilerException("Procedure " + className + " allows access by a role " + groupName + " that does not exist");
            }
            final GroupRef groupRef = procedure.getAuthgroups().add(groupName);
            groupRef.setGroup(group);
        }
        procedure.setClassname(className);
        // System procedures don't use the procedure compiler
        procedure.setSystemproc(false);
        procedure.setDefaultproc(procedureDescriptor.m_builtInStmt);
        procedure.setHasjava(false);
        procedure.setTransactional(true);

        ProcedurePartitionData info = procedureDescriptor.m_partitionData;
        if (info == null) {
            info = new ProcedurePartitionData(ProcedurePartitionData.Type.MULTI);
        }

        String[] stmts = SQLLexer.splitStatements(stmtsStr).getCompletelyParsedStmts().toArray(new String[0]);

        int stmtNum = 0;
        // Track if there are any writer statements and/or sequential scans and/or an overlooked common partitioning parameter
        boolean procHasWriteStmts = false;
        boolean procHasSeqScans = false;

        StatementPartitioning partitioning = info.isSinglePartition() ?
                StatementPartitioning.forceSP() : StatementPartitioning.forceMP();

        for (String curStmt : stmts) {
            // Skip processing 'END' statement in multi-statement procedures
            if (curStmt.equalsIgnoreCase("end")) {
                continue;
            }

            // ENG-14487 truncate statement is not allowed for single partitioned procedures.
            if (info.isSinglePartition() && curStmt.toUpperCase().startsWith("TRUNCATE")) {
                throw compiler.new VoltCompilerException("Single partitioned procedure: " +
                        shortName + " has TRUNCATE statement: \"" + curStmt + "\".");
            }

            // Add the statement to the catalog
            Statement catalogStmt = procedure.getStatements().add(VoltDB.ANON_STMT_NAME + String.valueOf(stmtNum));
            stmtNum++;

            // Compile the statement
            // Default to FASTER determinism mode because statement procedures can't feed read output into writes.
            StatementCompiler.compileFromSqlTextAndUpdateCatalog(compiler, hsql, db, estimates,
                    catalogStmt, curStmt, procedureDescriptor.m_joinOrder, DeterminismMode.FASTER, partitioning);

            // If any statement is not read-only, then the procedure is not read-only.
            if (catalogStmt.getReadonly() == false) {
                procHasWriteStmts = true;
            }

            if (catalogStmt.getSeqscancount() > 0) {
                procHasSeqScans = true;
            }

            // Set procedure parameter types
            CatalogMap<ProcParameter> params = procedure.getParameters();
            CatalogMap<StmtParameter> stmtParams = catalogStmt.getParameters();

            // Set the procedure parameter types from the statement parameter types
            int paramCount = params.size();
            for (StmtParameter stmtParam : CatalogUtil.getSortedCatalogItems(stmtParams, "index")) {
                // name each parameter "param1", "param2", etc...
                ProcParameter procParam = params.add("param" + String.valueOf(paramCount));
                procParam.setIndex(paramCount);
                procParam.setIsarray(stmtParam.getIsarray());
                procParam.setType(stmtParam.getJavatype());
                paramCount++;
            }
        }

        if (stmtNum == 0) {
            throw compiler.new VoltCompilerException("Cannot create a stored procedure with no statements "
                    + "for procedure: " + procedure.getClassname());
        }

        boolean twoPartitionTxn = info.isTwoPartitionProcedure();
        procedure.setSinglepartition(info.isSinglePartition());

        if (info.isSinglePartition() || twoPartitionTxn) {
            setCatalogProcedurePartitionInfo(compiler, db, procedure, info);
            // TODO: The planner does not currently validate that a single-statement plan declared as single-partition correctly uses
            // the designated parameter as a partitioning filter, maybe some day.
            // In theory, the PartitioningForStatement would confirm the use of (only) a parameter as a partition key --
            // or if the partition key was determined to be some other hard-coded constant (expression?) it might display a warning
            // message that the passed parameter is assumed to be equal to that constant (expression).
        } else if (partitioning.getCountOfIndependentlyPartitionedTables() == 1) {
            AbstractExpression statementPartitionExpression = partitioning.singlePartitioningExpressionForReport();
            if (statementPartitionExpression != null) {
                // The planner has uncovered an overlooked opportunity to run the statement SP.
                String msg = "This procedure " + shortName + " would benefit from being partitioned, by ";
                String tableName = "tableName", partitionColumnName = "partitionColumnName";
                try {
                    assert(partitioning.getFullColumnName() != null);
                    String array[] = partitioning.getFullColumnName().split("\\.");
                    tableName = array[0];
                    partitionColumnName = array[1];
                } catch(Exception ex) {
                }

                int paramCount = procedure.getParameters().size();
                if (statementPartitionExpression instanceof ParameterValueExpression) {
                    paramCount = ((ParameterValueExpression) statementPartitionExpression).getParameterIndex();
                } else {
                    String valueDescription = null;
                    Object partitionValue = partitioning.getInferredPartitioningValue();
                    if (partitionValue == null) {
                        // Statement partitioned on a runtime constant. This is likely to be cryptic, but hopefully gets the idea across.
                        valueDescription = "of " + statementPartitionExpression.explain("");
                    } else {
                        valueDescription = partitionValue.toString(); // A simple constant value COULD have been a parameter.
                    }
                    msg += "adding a parameter to be passed the value " + valueDescription + " and ";
                }
                msg += "adding a 'PARTITION ON TABLE " + tableName + " COLUMN " +
                        partitionColumnName + " PARAMETER " + paramCount + "' clause to the " +
                        "CREATE PROCEDURE statement. or using a separate PARTITION PROCEDURE statement";
                compiler.addWarn(msg);
            }
        }

        // set the read-only property of a procedure.
        procedure.setReadonly(procHasWriteStmts == false);

        procedure.setHasseqscans(procHasSeqScans);
    }

    static class ParititonDataReturnType {
        final Table partitionTable;
        final Column partitionColumn;
        final int partitionParamIndex;

        public ParititonDataReturnType(Table table, Column col, int paramIndex) {
            partitionTable = table;
            partitionColumn = col;
            partitionParamIndex = paramIndex;
        }

        public ParititonDataReturnType() {
            partitionTable = null;
            partitionColumn = null;
            partitionParamIndex = -1;
        }
    }

    /**
     * Set partition table, column, and parameter index for catalog procedure
     */
    public static void setCatalogProcedurePartitionInfo(VoltCompiler compiler, Database db,
            Procedure procedure, ProcedurePartitionData partitionData) throws VoltCompilerException {
        ParititonDataReturnType partitionClauseData = resolvePartitionData(compiler, db, procedure,
                partitionData.m_tableName, partitionData.m_columnName, partitionData.m_paramIndex);
        procedure.setPartitionparameter(partitionClauseData.partitionParamIndex);
        procedure.setPartitioncolumn(partitionClauseData.partitionColumn);
        procedure.setPartitiontable(partitionClauseData.partitionTable);
        procedure.setSinglepartition(true);

        // handle a two partition procedure
        if (partitionData.isTwoPartitionProcedure()) {
            partitionClauseData = resolvePartitionData(compiler, db, procedure,
                    partitionData.m_tableName2, partitionData.m_columnName2, partitionData.m_paramIndex2);
            procedure.setPartitionparameter2(partitionClauseData.partitionParamIndex);
            procedure.setPartitioncolumn2(partitionClauseData.partitionColumn);
            procedure.setPartitiontable2(partitionClauseData.partitionTable);
            procedure.setSinglepartition(false);
        }
    }

    static public ParititonDataReturnType resolvePartitionData(VoltCompiler compiler, Database db, Procedure procedure,
            String tableName, String columnName, String paramIndexString) throws VoltCompilerException {
        if (tableName == null) {
            // Partitioned procedure which does not use a parameter
            return new ParititonDataReturnType();
        }

        // check parameter index range
        int paramIndex = Integer.parseInt(paramIndexString);
        int paramCount = procedure.getParameters().size();
        if ((paramIndex < 0) || (paramIndex >= paramCount)) {
            String msg = "Invalid parameter index value " + paramIndex + " for procedure: " + procedure.getClassname();
            throw compiler.new VoltCompilerException(msg);
        }

        // locate the catalog table and partition column
        CatalogMap<Table> tables = db.getTables();
        for (Table table : tables) {
            if (table.getTypeName().equalsIgnoreCase(tableName)) {
                CatalogMap<Column> columns = table.getColumns();
                Column partitionColumn = table.getPartitioncolumn();
                if (partitionColumn == null) {
                    String msg = String.format("Procedure %s references table %s which has no partition column (may be replicated).",
                            procedure.getClassname(), table.getTypeName());
                    throw compiler.new VoltCompilerException(msg);
                }

                for (Column column : columns) {
                    if (column.getTypeName().equalsIgnoreCase(columnName)) {
                        if (partitionColumn.getTypeName().equals(column.getTypeName())) {
                            return new ParititonDataReturnType(table, column, paramIndex);
                        }
                        String msg = "Procedure " + procedure.getClassname() +
                                " refers to a column in schema which is not a partition key.";
                        throw compiler.new VoltCompilerException(msg);
                    }
                }
            }
        }

        String msg = "Procedure " + procedure.getClassname() + " is partitioned on a column "
                + columnName + " which can't be found in table " + tableName + ".";
        throw compiler.new VoltCompilerException(msg);
    }
}
