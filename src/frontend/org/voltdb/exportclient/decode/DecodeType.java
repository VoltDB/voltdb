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

package org.voltdb.exportclient.decode;

import java.util.EnumMap;
import java.util.Map;

import org.voltdb.VoltType;

/**
 * A data type mirror for {@link VoltType} that supports the visitor pattern
 * @author stefano
 *
 */
public enum DecodeType {

    TINYINT(VoltType.TINYINT) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitTinyInt(p,v);
        }
    },
    SMALLINT(VoltType.SMALLINT) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitSmallInt(p,v);
        }
    },
    INTEGER(VoltType.INTEGER) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitInteger(p,v);
        }
    },
    BIGINT(VoltType.BIGINT) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitBigInt(p,v);
        }
    },
    FLOAT(VoltType.FLOAT) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitFloat(p,v);
        }
    },
    TIMESTAMP(VoltType.TIMESTAMP) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitTimestamp(p,v);
        }
    },
    STRING(VoltType.STRING) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitString(p,v);
        }
    },
    VARBINARY(VoltType.VARBINARY) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitVarBinary(p,v);
        }
    },
    DECIMAL(VoltType.DECIMAL) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitDecimal(p,v);
        }
    },
    GEOGRAPHY_POINT(VoltType.GEOGRAPHY_POINT) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitGeographyPoint(p,v);
        }
    },
    GEOGRAPHY(VoltType.GEOGRAPHY) {
        @Override
        public <R, P, E extends Exception> R accept(Visitor<R, P, E> vtor, P p, Object v)
                throws E {
            return vtor.visitGeography(p,v);
        }
    };

    private static final Map<VoltType, DecodeType> m_typeMap =
            new EnumMap<VoltType, DecodeType>(VoltType.class);

    static {
        for (DecodeType dt: values()) {
            m_typeMap.put(dt.voltType(), dt);
        }
    }

    /**
     * The associated {@link VoltType}
     */
    private final VoltType m_type;

    DecodeType(VoltType type) {
        m_type = type;
    }

    public VoltType voltType() {
        return m_type;
    }

    public interface Visitor<R,P,E extends Exception> {
        R visitTinyInt(P p, Object v) throws E;
        R visitSmallInt(P p, Object v) throws E;
        R visitInteger(P p, Object v) throws E;
        R visitBigInt(P p, Object v) throws E;
        R visitFloat(P p, Object v) throws E;
        R visitTimestamp(P p, Object v) throws E;
        R visitString(P p, Object v) throws E;
        R visitVarBinary(P p, Object v) throws E;
        R visitDecimal(P p, Object v) throws E;
        R visitGeographyPoint(P p, Object v) throws E;
        R visitGeography(P p, Object v) throws E;
    }

    public static class SimpleVisitor<R, P> implements Visitor<R,P,RuntimeException> {
        final R DEFAULT_RETURN;

        public SimpleVisitor(R defaultReturnValue) {
            DEFAULT_RETURN = defaultReturnValue;
        }

        public SimpleVisitor() {
            this(null);
        }

        @Override
        public R visitTinyInt(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitSmallInt(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitInteger(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitBigInt(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitFloat(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitTimestamp(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitString(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitVarBinary(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitDecimal(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitGeographyPoint(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }
        @Override
        public R visitGeography(P p, Object v) throws RuntimeException {
            return DEFAULT_RETURN;
        }
    }

    public static DecodeType forType(VoltType type) {
        DecodeType dt = m_typeMap.get(type);
        if (dt == null) {
            throw new IllegalArgumentException("no mapping found for given volt type");
        }
        return dt;
    }

    public abstract <R,P,E extends Exception> R accept(Visitor<R,P,E> vtor, P p, Object v) throws E;
}
