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

public enum Language {
    JAVA() {
        @Override
        public <R, P> R accept(Visitor<R, P> vtor, P p) {
            return vtor.visitJava(p);
        }

        @Override
        public <R, P, E extends Exception> R accept(
                CheckedExceptionVisitor<R, P, E> vtor, P p) throws E {
            return vtor.visitJava(p);
        }
    },
    GROOVY() {
        @Override
        public <R, P> R accept(Visitor<R, P> vtor, P p) {
            return vtor.visitGroovy(p);
        }

        @Override
        public <R, P, E extends Exception> R accept(
                CheckedExceptionVisitor<R, P, E> vtor, P p) throws E {
            return vtor.visitGroovy(p);
        }
    };

    public interface Visitor<R,P> {
        R visitJava(P p);
        R visitGroovy(P p);
    }

    public interface CheckedExceptionVisitor<R,P,E extends Exception> {
        R visitJava(P p) throws E;
        R visitGroovy(P p) throws E;
    }

    public static class SimpleVisitor<R,P> implements Visitor<R,P> {
        final R DEFAULT_RETURN;

        public SimpleVisitor(R defaultReturnValue) {
            DEFAULT_RETURN = defaultReturnValue;
        }

        public SimpleVisitor() {
            this(null);
        }

        @Override
        public R visitJava(P p) {
            return DEFAULT_RETURN;
        }

        @Override
        public R visitGroovy(P p) {
            return DEFAULT_RETURN;
        }
    }

    public abstract <R,P> R accept( Visitor<R,P> vtor, P p);
    public abstract <R,P,E extends Exception> R accept(
            CheckedExceptionVisitor<R, P, E> vtor, P p) throws E;

}
