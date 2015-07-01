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

package org.voltdb.compiler;

import groovy.lang.Closure;

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
import org.voltdb.compilereport.ProcedureAnnotation;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.groovy.GroovyCodeBlockConstants;
import org.voltdb.groovy.GroovyScriptProcedureDelegate;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.types.QueryType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Compiles stored procedures into a given catalog,
 * invoking the StatementCompiler as needed.
 */
public abstract class ProcedureCompiler implements GroovyCodeBlockConstants {

    static void compile(VoltCompiler compiler,
                        HSQLInterface hsql,
                        DatabaseEstimates estimates,
                        Catalog catalog,
                        Database db,
                        ProcedureDescriptor procedureDescriptor,
                        InMemoryJarfile jarOutput)
                                throws VoltCompiler.VoltCompilerException
    {

        assert(compiler != null);
        assert(hsql != null);
        assert(estimates != null);

        if (procedureDescriptor.m_singleStmt == null) {
            compileJavaProcedure(compiler, hsql, estimates, catalog, db, procedureDescriptor, jarOutput);
        }
        else {
            compileSingleStmtProcedure(compiler, hsql, estimates, catalog, db, procedureDescriptor);
        }
    }

    public static Map<String, SQLStmt> getValidSQLStmts(VoltCompiler compiler,
                                                        String procName,
                                                        Class<?> procClass,
                                                        Object procInstance,
                                                        boolean withPrivate)
                                                                throws VoltCompilerException
    {
        Map<String, SQLStmt> retval = new HashMap<String, SQLStmt>();

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
                if (retval.containsKey(e.getKey()) == false)
                    retval.put(e.getKey(), e.getValue());
            }
        }

        return retval;
    }

    /**
     * Return a language visitor that, when run, it returns a map consisting of field names and their
     * assigned objects
     *
     * @param compiler volt compiler instance
     * @return a {@link Language.Visitor}
     */
    static Language.CheckedExceptionVisitor<Map<String,Object>, Class<?>, VoltCompilerException> procedureIntrospector(final VoltCompiler compiler) {
            return new Language.CheckedExceptionVisitor<Map<String,Object>, Class<?>, VoltCompilerException>() {

                @Override
                public Map<String, Object> visitJava(Class<?> p) throws VoltCompilerException {
                    // get the short name of the class (no package)
                    String shortName = deriveShortProcedureName(p.getName());

                    VoltProcedure procInstance;
                    try {
                        procInstance = (VoltProcedure)p.newInstance();
                    } catch (InstantiationException e) {
                        throw new RuntimeException("Error instantiating procedure \"%s\"" + p.getName(), e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Error instantiating procedure \"%s\"" + p.getName(), e);
                    }
                    Map<String, SQLStmt> stmtMap = getValidSQLStmts(compiler, p.getSimpleName(), p, procInstance, true);

                    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                    builder.putAll(stmtMap);

                    // find the run() method and get the params
                    Method procMethod = null;
                    Method[] methods = p.getDeclaredMethods();
                    for (final Method m : methods) {
                        String name = m.getName();
                        if (name.equals("run")) {
                            assert (m.getDeclaringClass() == p);

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

                    builder.put("@run",procMethod);

                    return builder.build();
                }

                @Override
                public Map<String,Object> visitGroovy(Class<?> p) throws VoltCompilerException {
                    GroovyScriptProcedureDelegate scripDelegate;
                    try {
                        scripDelegate = new GroovyScriptProcedureDelegate(p);
                    } catch (GroovyScriptProcedureDelegate.SetupException tupex) {
                        throw compiler.new VoltCompilerException(tupex.getMessage());
                    }
                    return scripDelegate.getIntrospectedFields();
                }
            };
    };

    final static Language.Visitor<Class<?>[], Map<String,Object>> procedureEntryPointParametersTypeExtractor =
            new Language.SimpleVisitor<Class<?>[], Map<String,Object>>() {

                @Override
                public Class<?>[] visitJava(Map<String, Object> p) {
                    Method procMethod = (Method)p.get("@run");
                    return procMethod.getParameterTypes();
                }

                @Override
                public Class<?>[] visitGroovy(Map<String, Object> p) {
                    @SuppressWarnings("unchecked")
                    Closure<Object> transactOn = (Closure<Object>)p.get(GVY_PROCEDURE_ENTRY_CLOSURE);

                    // closure with no parameters has an object as the default parameter
                    Class<?> [] parameterTypes = transactOn.getParameterTypes();
                    if ( parameterTypes.length == 1 && parameterTypes[0] == Object.class) {
                        return new Class<?>[0];
                    }
                    return transactOn.getParameterTypes();
                }
    };

    /**
     * get the short name of the class (no package)
     * @param className fully qualified (or not) class name
     * @return short name of the class (no package)
     */
    static String deriveShortProcedureName( String className) {
        if( className == null || className.trim().isEmpty()) {
            return null;
        }
        String[] parts = className.split("\\.");
        String shortName = parts[parts.length - 1];

        return shortName;
    }


    static void compileJavaProcedure(VoltCompiler compiler,
                                     HSQLInterface hsql,
                                     DatabaseEstimates estimates,
                                     Catalog catalog,
                                     Database db,
                                     ProcedureDescriptor procedureDescriptor,
                                     InMemoryJarfile jarOutput)
                                             throws VoltCompiler.VoltCompilerException
    {

        final String className = procedureDescriptor.m_className;
        final Language lang = procedureDescriptor.m_language;

        // Load the class given the class name
        Class<?> procClass = procedureDescriptor.m_class;

        // get the short name of the class (no package)
        String shortName = deriveShortProcedureName(className);

        // add an entry to the catalog
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
        // sysprocs don't use the procedure compiler
        procedure.setSystemproc(false);
        procedure.setDefaultproc(procedureDescriptor.m_builtInStmt);
        procedure.setHasjava(true);
        procedure.setLanguage(lang.name());
        ProcedureAnnotation pa = (ProcedureAnnotation) procedure.getAnnotation();
        if (pa == null) {
            pa = new ProcedureAnnotation();
            procedure.setAnnotation(pa);
        }
        if (procedureDescriptor.m_scriptImpl != null) {
            // This is a Groovy or other Java derived procedure and we need to add an annotation with
            // the script to the Procedure element in the Catalog
            pa.scriptImpl = procedureDescriptor.m_scriptImpl;
        }

        // get the annotation
        // first try to get one that has been passed from the compiler
        ProcInfoData info = compiler.getProcInfoOverride(shortName);
        // check if partition info was set in ddl
        ProcInfoData ddlInfo = null;
        if (procedureDescriptor.m_partitionString != null && ! procedureDescriptor.m_partitionString.trim().isEmpty()) {
            ddlInfo = new ProcInfoData();
            ddlInfo.partitionInfo = procedureDescriptor.m_partitionString;
            ddlInfo.singlePartition = true;
        }
        // then check for the usual one in the class itself
        // and create a ProcInfo.Data instance for it
        if (info == null) {
            info = new ProcInfoData();
            ProcInfo annotationInfo = procClass.getAnnotation(ProcInfo.class);
            // error out if partition info is present in both ddl and annotation
            if (annotationInfo != null) {
                if (ddlInfo != null) {
                    String msg = "Procedure: " + shortName + " has partition properties defined both in ";
                    msg += "class \"" + className + "\" and in the schema defintion file(s)";
                    throw compiler.new VoltCompilerException(msg);
                }
                // Prevent AutoGenerated DDL from including PARTITION PROCEDURE for this procedure.
                pa.classAnnotated = true;
                info.partitionInfo = annotationInfo.partitionInfo();
                info.singlePartition = annotationInfo.singlePartition();
            }
            else if (ddlInfo != null) {
                info = ddlInfo;
            }
        }
        else {
            pa.classAnnotated = true;
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

        // iterate through the fields and get valid sql statements
        Map<String, Object> fields = lang.accept(procedureIntrospector(compiler), procClass);

        // determine if proc is read or read-write by checking if the proc contains any write sql stmts
        boolean readWrite = false;
        for (Object field : fields.values()) {
            if (!(field instanceof SQLStmt)) continue;
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
            if (!(entry.getValue() instanceof SQLStmt)) continue;

            String stmtName = entry.getKey();
            SQLStmt stmt = (SQLStmt)entry.getValue();

            // add the statement to the catalog
            Statement catalogStmt = procedure.getStatements().add(stmtName);

            // compile the statement
            StatementPartitioning partitioning =
                info.singlePartition ? StatementPartitioning.forceSP() :
                                       StatementPartitioning.forceMP();
            boolean cacheHit = StatementCompiler.compileFromSqlTextAndUpdateCatalog(compiler, hsql, catalog, db,
                    estimates, catalogStmt, stmt.getText(), stmt.getJoinOrder(),
                    detMode, partitioning);

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
                msg = "This procedure might benefit from an @ProcInfo annotation designating parameter " +
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

        // set procedure parameter types
        CatalogMap<ProcParameter> params = procedure.getParameters();
        Class<?>[] paramTypes = lang.accept(procedureEntryPointParametersTypeExtractor, fields);
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
                    String.class, byte[].class
            };
            boolean found = false;
            for (Class<?> candidate : validPartitionClzzes) {
                if (partitionType == candidate)
                    found = true;
            }
            if (!found) {
                String msg = "PartitionInfo parameter must be a String or Number for procedure: " + procedure.getClassname();
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

    static void compileSingleStmtProcedure(VoltCompiler compiler,
                                           HSQLInterface hsql,
                                           DatabaseEstimates estimates,
                                           Catalog catalog,
                                           Database db,
                                           ProcedureDescriptor procedureDescriptor)
                                                   throws VoltCompiler.VoltCompilerException
    {
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
                throw compiler.new VoltCompilerException("Procedure " + className + " allows access by a role " + groupName + " that does not exist");
            }
            final GroupRef groupRef = procedure.getAuthgroups().add(groupName);
            groupRef.setGroup(group);
        }
        procedure.setClassname(className);
        // sysprocs don't use the procedure compiler
        procedure.setSystemproc(false);
        procedure.setDefaultproc(procedureDescriptor.m_builtInStmt);
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
        StatementPartitioning partitioning =
            info.singlePartition ? StatementPartitioning.forceSP() :
                                   StatementPartitioning.forceMP();
        // default to FASTER detmode because stmt procs can't feed read output into writes
        StatementCompiler.compileFromSqlTextAndUpdateCatalog(compiler, hsql, catalog, db,
                estimates, catalogStmt, procedureDescriptor.m_singleStmt,
                procedureDescriptor.m_joinOrder, DeterminismMode.FASTER, partitioning);

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
            procParam.setIsarray(stmtParam.getIsarray());
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
            if (partitioning.getCountOfIndependentlyPartitionedTables() == 1) {
                AbstractExpression statementPartitionExpression = partitioning.singlePartitioningExpressionForReport();
                if (statementPartitionExpression != null) {
                    // The planner has uncovered an overlooked opportunity to run the statement SP.
                    String msg = null;
                    if (statementPartitionExpression instanceof ParameterValueExpression) {
                        msg = "This procedure would benefit from setting the attribute 'partitioninfo=" + partitioning.getFullColumnName() +
                                ":" + ((ParameterValueExpression) statementPartitionExpression).getParameterIndex() + "'";
                    } else {
                        String valueDescription = null;
                        Object partitionValue = partitioning.getInferredPartitioningValue();
                        if (partitionValue == null) {
                            // Statement partitioned on a runtime constant. This is likely to be cryptic, but hopefully gets the idea across.
                            valueDescription = "of " + statementPartitionExpression.explain("");
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
                            procedure.setPartitiontable(table);
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
