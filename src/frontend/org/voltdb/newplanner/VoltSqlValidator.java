package org.voltdb.newplanner;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

/**
 * VoltDB SQL validator.
 * @author Yiqun Zhang
 * @since 8.4
 */
public class VoltSqlValidator extends SqlValidatorImpl {

    /**
     * Build a VoltDB SQL validator.
     * @param opTab
     * @param catalogReader
     * @param typeFactory
     * @param conformance
     */
    public VoltSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory, SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, conformance);
    }
}
