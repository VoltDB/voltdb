/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.expressions;

import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents a vector of expression trees.
 * Currently used for SQL IN lists (of values), and (column list) IN (SELECT ...)
 */
public class VectorValueExpression extends AbstractExpression {

    public VectorValueExpression() {
        super(ExpressionType.VALUE_VECTOR);
    }
    public VectorValueExpression(List<AbstractExpression> args) {
        this();
        setArgs(args);
    }

    public VectorValueExpression(List<AbstractExpression> args, VoltType type) {
        this(args);
        setValueType(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof VectorValueExpression)) {
            return false;
        }

        VectorValueExpression other = (VectorValueExpression) obj;

        if (other.m_args.size() != m_args.size()) {
            return false;
        }

        for (int i = 0; i < m_args.size(); i++) {
            if (!other.m_args.get(i).equals(m_args.get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void finalizeValueTypes() {
        // just make sure the children have valid types.
        finalizeChildValueTypes();
    }

    @Override
    public boolean equivalent(AbstractExpression other) {
        if (other instanceof VectorValueExpression) {
            return getArgs() == null || other.getArgs() == null ? getArgs() == other.getArgs() :
                    // convert all CVE to PVE then compare set equivalence
                    getArgs().stream().map(e -> {
                        if (e instanceof ConstantValueExpression) {
                            return new ParameterValueExpression((ConstantValueExpression) e);
                        } else {
                            return e;
                        }
                    }).collect(Collectors.toSet()).equals(other.getArgs().stream().map(e -> {
                        if (e instanceof ConstantValueExpression) {
                            return new ParameterValueExpression((ConstantValueExpression) e);
                        } else {
                            return e;
                        }
                    }).collect(Collectors.toSet()));
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(AbstractExpression other) {
        return equivalent(other) ? 0 : super.compareTo(other);
    }

    @Override
    public String explain(String impliedTableName) {
        String result = "(";
        String connector = "";
        for (AbstractExpression arg : m_args) {
            result += connector + arg.explain(impliedTableName);
            connector = ", ";
        }
        result += ")";
        return result;
    }
}
