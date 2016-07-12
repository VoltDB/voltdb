/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.calciteadapter;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Set;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.BuildDirectoryUtils;

import junit.framework.TestCase;

public class TestCalcite extends TestCase {

    // Stolen from TestVoltCompiler
    String nothing_jar;
    String testout_jar;

    @Override
    public void setUp() {
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "testout.jar";
    }

    @Override
    public void tearDown() {
        File tjar = new File(testout_jar);
        tjar.delete();
    }

    private boolean compileDDL(String ddl, VoltCompiler compiler) {
        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas>" +
            "<schema path='" + schemaPath + "' />" +
            "</schemas>" +
            "<procedures/>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        return compiler.compileWithProjectXML(projectPath, testout_jar);
    }

    private static class TableAdapter implements org.apache.calcite.schema.Table {

        org.voltdb.catalog.Table m_catTable;

        public TableAdapter(org.voltdb.catalog.Table table) {
            m_catTable = table;
        }

        static public TableAdapter fromTableCatalog(org.voltdb.catalog.Table table) {
            return new TableAdapter(table);
        }

        public static RelDataType toRelDataType(RelDataTypeFactory typeFactory, VoltType vt, int prec) {
            SqlTypeName sqlTypeName = SqlTypeName.get(vt.toSQLString().toUpperCase());
            RelDataType rdt;
            switch (vt) {
            case STRING:
                // This doesn't seem quite right...
                rdt = typeFactory.createSqlType(sqlTypeName, prec);
                rdt = typeFactory.createTypeWithCharsetAndCollation(rdt, Charset.forName("UTF-8"), SqlCollation.IMPLICIT);
                break;
                default:
                    rdt = typeFactory.createSqlType(sqlTypeName);
            }
            return rdt;
        }

        @Override
        public TableType getJdbcTableType() {
            // TODO Auto-generated method stub
            return Schema.TableType.TABLE;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            FieldInfoBuilder builder = typeFactory.builder();

            for (Column catColumn : m_catTable.getColumns()) {
                VoltType vt = VoltType.get((byte)catColumn.getType());
                RelDataType rdt = toRelDataType(typeFactory, vt, catColumn.getSize());
                rdt = typeFactory.createTypeWithNullability(rdt, catColumn.getNullable());
                builder.add(catColumn.getName(), rdt);
            }
            return builder.build();
        }

        @Override
        public Statistic getStatistic() {
            // TODO Auto-generated method stub
            return null;
        }

    }

    private static class SchemaAdapter implements Schema {

        final Catalog m_catalog;

        public SchemaAdapter(Catalog catalog) {
            m_catalog = catalog;
        }


        @Override
        public Expression getExpression(SchemaPlus sp, String str) {
            return null;
        }

        @Override
        public boolean contentsHaveChangedSince(long arg0, long arg1) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public Set<String> getFunctionNames() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Collection<Function> getFunctions(String arg0) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Schema getSubSchema(String arg0) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Set<String> getSubSchemaNames() {
            // TODO Auto-generated method stub
            return null;
        }

        private CatalogMap<Table> getCatalogTables() {
            return m_catalog.getClusters().get("cluster").getDatabases().get("database").getTables();
        }

        @Override
        public org.apache.calcite.schema.Table getTable(String arg0) {
            return TableAdapter.fromTableCatalog(getCatalogTables().get(arg0));
        }

        @Override
        public Set<String> getTableNames() {
            return getCatalogTables().keySet();
        }

        @Override
        public boolean isMutable() {
            // TODO Auto-generated method stub
            return false;
        }

    }

    private SchemaPlus schemaPlusFromDDL(String ddl) {
        VoltCompiler compiler = new VoltCompiler();
        boolean success = compileDDL(ddl, compiler);

        assertTrue(success);

        Catalog cat = compiler.getCatalog();
        SchemaAdapter schema = new SchemaAdapter(cat);
        assert(schema != null);

        SchemaPlus rootSchema = Frameworks.createRootSchema(false);
        rootSchema.add("voltdb", schema);

        return rootSchema;
    }

    private void dumpRootSchema(SchemaPlus rootSchema) {
        final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        Set<String> subSchemaNames = rootSchema.getSubSchemaNames();
        for (String subSchemaName : subSchemaNames) {
            System.out.println(subSchemaName);
            Schema subSchema = rootSchema.getSubSchema(subSchemaName);
            for (String tableName : subSchema.getTableNames()) {
                System.out.println("  " + tableName);
                org.apache.calcite.schema.Table table = subSchema.getTable(tableName);
                RelDataType rowType = table.getRowType(typeFactory);
                System.out.println("    " + rowType.getFullTypeString());
            }
        }
    }

    public void testSchema() {
        String ddl = "create table test_calcite ("
                + "i integer primary key, "
                + "si smallint, "
                + "ti tinyint,"
                + "bi bigint,"
                + "f float not null, "
                + "v varchar(32));";
        SchemaPlus rootSchema = schemaPlusFromDDL(ddl);
        assertTrue(rootSchema != null);
        dumpRootSchema(rootSchema);
    }
}
