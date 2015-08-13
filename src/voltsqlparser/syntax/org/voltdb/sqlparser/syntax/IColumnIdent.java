package org.voltdb.sqlparser.syntax;

/**
 * An IColumnIdent is a reference to a column in
 * a table definition.  It's very much like an IProjection,
 * but slightly more specialized, since there is no alias
 * and the table is implicit.  Maybe they should be be
 * merged.
 *
 * @author bwhite
 */
public interface IColumnIdent {

    public abstract String getColumnName();

    public abstract int getColLineNo();

    public abstract int getColColNo();

}