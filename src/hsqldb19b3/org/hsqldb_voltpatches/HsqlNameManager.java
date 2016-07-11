/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.lib.StringConverter;
import org.hsqldb_voltpatches.rights.Grantee;

/**
 * Provides Name Management for SQL objects. <p>
 *
 * This class now includes the HsqlName class introduced in 1.7.1 and improves
 * auto-naming with multiple databases in the engine.<p>
 *
 * Methods check user defined names and issue system generated names
 * for SQL objects.<p>
 *
 * This class does not deal with the type of the SQL object for which it
 * is used.<p>
 *
 * Some names beginning with SYS_ are reserved for system generated names.
 * These are defined in isReserveName(String name) and created by the
 * makeAutoName(String type) factory method<p>
 *
 * sysNumber is used to generate system-generated names. It is
 * set to the largest integer encountered in names that use the
 * SYS_xxxxxxx_INTEGER format. As the DDL is processed before any ALTER
 * command, any new system generated name will have a larger integer suffix
 * than all the existing names.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public final class HsqlNameManager {

    public static final String DEFAULT_CATALOG_NAME = "PUBLIC";
    private static final HsqlNameManager staticManager =
        new HsqlNameManager(null);

    static {
        staticManager.serialNumber = Integer.MIN_VALUE;
    }

    private static final HsqlName[] autoColumnNames       = new HsqlName[32];
    private static final String[]   autoNoNameColumnNames = new String[32];

    static {
        for (int i = 0; i < autoColumnNames.length; i++) {
            autoColumnNames[i] = new HsqlName(staticManager, makeAutoColumnName("C", i), 0,
                                              false);
            autoNoNameColumnNames[i] = String.valueOf(i);
        }
    }

    private int      serialNumber = 1;        // 0 is reserved in lookups
    private int      sysNumber    = 10000;    // avoid name clash in older scripts
    private HsqlName catalogName;

    public HsqlNameManager(Database database) {
        catalogName = new HsqlName(this, DEFAULT_CATALOG_NAME,
                                   SchemaObject.CATALOG, false);
    }

    public HsqlName getCatalogName() {
        return catalogName;
    }

    public static HsqlName newSystemObjectName(String name, int type) {
        return new HsqlName(staticManager, name, type, false);
    }

    public static HsqlName newInfoSchemaColumnName(String name,
            HsqlName table) {

        HsqlName hsqlName = new HsqlName(staticManager, name, false,
                                         SchemaObject.COLUMN);

        hsqlName.schema = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
        hsqlName.parent = table;

        return hsqlName;
    }

    public static HsqlName newInfoSchemaTableName(String name) {

        HsqlName hsqlName = new HsqlName(staticManager, name,
                                         SchemaObject.TABLE, false);

        hsqlName.schema = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;

        return hsqlName;
    }

    public static HsqlName newInfoSchemaObjectName(String name,
            boolean isQuoted, int type) {

        HsqlName hsqlName = new HsqlName(staticManager, name, type, isQuoted);

        hsqlName.schema = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;

        return hsqlName;
    }

    public HsqlName newHsqlName(HsqlName schema, String name, int type) {

        HsqlName hsqlName = new HsqlName(this, name, type, false);

        hsqlName.schema = schema;

        return hsqlName;
    }

    //
    public HsqlName newHsqlName(String name, boolean isquoted, int type) {
        return new HsqlName(this, name, isquoted, type);
    }

    public HsqlName newHsqlName(HsqlName schema, String name,
                                boolean isquoted, int type) {

        HsqlName hsqlName = new HsqlName(this, name, isquoted, type);

        hsqlName.schema = schema;

        return hsqlName;
    }

    public HsqlName newHsqlName(HsqlName schema, String name,
                                boolean isquoted, int type, HsqlName parent) {

        HsqlName hsqlName = new HsqlName(this, name, isquoted, type);

        hsqlName.schema = schema;
        hsqlName.parent = parent;

        return hsqlName;
    }

    public HsqlName newColumnSchemaHsqlName(HsqlName table, SimpleName name) {
        return newColumnHsqlName(table, name.name, name.isNameQuoted);
    }

    public HsqlName newColumnHsqlName(HsqlName table, String name,
                                      boolean isquoted) {

        HsqlName hsqlName = new HsqlName(this, name, isquoted,
                                         SchemaObject.COLUMN);

        hsqlName.schema = table.schema;
        hsqlName.parent = table;

        return hsqlName;
    }

    /**
     * Same name string but different objects and serial number
     */
    public HsqlName getSubqueryTableName() {

        HsqlName hsqlName = new HsqlName(this, SqlInvariants.SYSTEM_SUBQUERY,
                                         false, SchemaObject.TABLE);

        hsqlName.schema = SqlInvariants.SYSTEM_SCHEMA_HSQLNAME;

        return hsqlName;
    }

    /**
     * Auto names are used for autogenerated indexes or anonymous constraints.
     */
    public HsqlName newAutoName(String prefix, HsqlName schema,
                                HsqlName parent, int type) {

        HsqlName name = newAutoName(prefix, (String) null, schema, parent,
                                    type);

        return name;
    }

    /**
     * Column index i is 0 based, returns 1 based numbered column.
     */
    static public HsqlName getAutoColumnName(int i) {

        if (i < autoColumnNames.length) {
            return autoColumnNames[i];
        }

        return new HsqlName(staticManager, makeAutoColumnName("C_", i), 0, false);
    }

    static public String getAutoColumnNameString(int i) {

        if (i < autoColumnNames.length) {
            return autoColumnNames[i].name;
        }

        return makeAutoColumnName("C", i);
    }

    /**
     * Column index i is 0 based, returns 1 based numbered column.
     */
    private static String makeAutoColumnName(String prefix, int i) {
        return prefix + (i + 1);
    }

    static public String getAutoNoNameColumnString(int i) {

        if (i < autoColumnNames.length) {
            return autoNoNameColumnNames[i];
        }

        return String.valueOf(i);
    }

    static public String getAutoSavepointNameString(long i, int j) {

        StringBuffer sb = new StringBuffer("S");

        sb.append(i).append('_').append(j);

        return sb.toString();
    }

    /**
     * Auto names are used for autogenerated indexes or anonymous constraints.
     */
    HsqlName newAutoName(String prefix, String namepart, HsqlName schema,
                         HsqlName parent, int type) {

        StringBuffer sb = new StringBuffer();

        if (prefix != null) {
            if (prefix.length() != 0) {
                sb.append("SYS_");
                sb.append(prefix);
                sb.append('_');

                if (namepart != null) {
                    sb.append(namepart);
                    sb.append('_');
                }

                sb.append(++sysNumber);
            }
        } else {
            sb.append(namepart);
        }

        HsqlName name = new HsqlName(this, sb.toString(), type, false);

        name.schema = schema;
        name.parent = parent;

        return name;
    }

    void resetNumbering() {
        sysNumber    = 0;
        serialNumber = 0;
    }

    public static SimpleName getSimpleName(String name, boolean isNameQuoted) {
        return new SimpleName(name, isNameQuoted);
    }

    public static class SimpleName {

        public String  name;
        public boolean isNameQuoted;

        private SimpleName() {}

        private SimpleName(String name, boolean isNameQuoted) {
            this.name         = name;
            this.isNameQuoted = isNameQuoted;
        }

        @Override
		public int hashCode() {
            return name.hashCode();
        }

        @Override
		public boolean equals(Object other) {

            if (other instanceof SimpleName) {
                return ((SimpleName) other).isNameQuoted == isNameQuoted
                       && ((SimpleName) other).name.equals(name);
            }

            return false;
        }

        public String getStatementName() {

            return isNameQuoted
                   ? StringConverter.toQuotedString(name, '"', true)
                   : name;
        }
    }

    public static final class HsqlName extends SimpleName {

        static HsqlName[] emptyArray = new HsqlName[]{};

        //
        HsqlNameManager   manager;
        public String     statementName;
        public HsqlName   schema;
        public HsqlName   parent;
        public Grantee    owner;
        public final int  type;
        private final int hashCode;

        private HsqlName(HsqlNameManager man, int type) {

            manager   = man;
            this.type = type;
            hashCode  = manager.serialNumber++;
        }

        private HsqlName(HsqlNameManager man, String name, boolean isquoted,
                         int type) {

            this(man, type);

            rename(name, isquoted);
        }

        /** for auto names and system-defined names */
        private HsqlName(HsqlNameManager man, String name, int type,
                         boolean isQuoted) {

            this(man, type);

            this.name         = this.statementName = name;
            this.isNameQuoted = isQuoted;

            if (isNameQuoted) {
                statementName = StringConverter.toQuotedString(name, '"',
                        true);
            }
        }

        @Override
		public String getStatementName() {
            return statementName;
        }

        public String getSchemaQualifiedStatementName() {

            if (type == SchemaObject.COLUMN) {
                if (parent == null
                        || SqlInvariants.SYSTEM_SUBQUERY.equals(parent.name)) {
                    return statementName;
                }

                StringBuffer sb = new StringBuffer();

                if (schema != null) {
                    sb.append(schema.getStatementName());
                    sb.append('.');
                }

                sb.append(parent.getStatementName());
                sb.append('.');
                sb.append(statementName);

                return sb.toString();
            }

            if (schema == null) {
                return statementName;
            }

            StringBuffer sb = new StringBuffer();

            if (schema != null) {
                sb.append(schema.getStatementName());
                sb.append('.');
            }

            sb.append(statementName);

            return sb.toString();
        }

        public void rename(HsqlName name) {
            rename(name.name, name.isNameQuoted);
        }

        public void rename(String name, boolean isquoted) {

            this.name          = name;
            this.statementName = name;
            this.isNameQuoted  = isquoted;

            if (name.length() > 128) {}

            if (isNameQuoted) {
                statementName = StringConverter.toQuotedString(name, '"',
                        true);
            }

            if (name.startsWith("SYS_")) {
                int length = sysPrefixLength(name);

                if (length > 0) {
                    try {
                        int temp = Integer.parseInt(name.substring(length));

                        if (temp > manager.sysNumber) {
                            manager.sysNumber = temp;
                        }
                    } catch (NumberFormatException e) {}
                }
            }
        }

        void rename(String prefix, String name, boolean isquoted) {

            StringBuffer sbname = new StringBuffer(prefix);

            sbname.append('_');
            sbname.append(name);
            rename(sbname.toString(), isquoted);
        }

        public void setSchemaIfNull(HsqlName schema) {

            if (this.schema == null) {
                this.schema = schema;
            }
        }

        @Override
		public boolean equals(Object other) {

            if (other instanceof HsqlName) {
                return hashCode == ((HsqlName) other).hashCode;
            }

            return false;
        }

        /**
         * hash code for this object is its unique serial number.
         */
        @Override
		public int hashCode() {
            return hashCode;
        }

        /**
         * "SYS_IDX_" is used for auto-indexes on referring FK columns or
         * unique constraints.
         * "SYS_PK_" is for the primary key constraints
         * "SYS_CT_" is for unique and check constraints
         * "SYS_REF_" is for FK constraints in referenced tables
         * "SYS_FK_" is for FK constraints in referencing tables
         *
         */
        static final String[] sysPrefixes = new String[] {
            "SYS_IDX_", "SYS_PK_", "SYS_REF_", "SYS_CT_", "SYS_FK_",
        };

        static int sysPrefixLength(String name) {

            for (int i = 0; i < sysPrefixes.length; i++) {
                if (name.startsWith(sysPrefixes[i])) {
                    return sysPrefixes[i].length();
                }
            }

            return 0;
        }

        static boolean isReservedName(String name) {
            return sysPrefixLength(name) > 0;
        }

        boolean isReservedName() {
            return isReservedName(name);
        }

        @Override
		public String toString() {

            return getClass().getName() + super.hashCode()
                   + "[this.hashCode()=" + this.hashCode + ", name=" + name
                   + ", name.hashCode()=" + name.hashCode()
                   + ", isNameQuoted=" + isNameQuoted + "]";
        }

        public int compareTo(Object o) {
            return hashCode - o.hashCode();
        }

        /**
         * Returns true if the identifier consists of all uppercase letters
         * digits and underscore, beginning with a letter and is not in the
         * keyword list.
         */
        static boolean isRegularIdentifier(String name) {

            for (int i = 0, length = name.length(); i < length; i++) {
                int c = name.charAt(i);

                if (c >= 'A' && c <= 'Z') {
                    continue;
                } else if (c == '_' && i > 0) {
                    continue;
                } else if (c >= '0' && c <= '9') {
                    continue;
                }

                return false;
            }

            return !Tokens.isKeyword(name);
        }
    }

    public static SimpleName getAutoColumnName(String realAlias) {
        return new HsqlName(staticManager, realAlias, 0, false);
    }
}
