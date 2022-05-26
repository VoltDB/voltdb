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

package org.voltdb.parser;

/**
 * Inherit from this class in order to use the static methods without the class name
 * to build patterns.
 */
public class SQLPatternFactory
{
    //===== Flags used internally and from SQLPatternPart's to modify the resulting pattern

    // Non-capturing group
    static int GROUP = 0x0001;
    // Capturing group
    static int CAPTURE = 0x0002;
    // Optional, implies at least a non-capturing group
    static int OPTIONAL = 0x0004;
    // Case-sensitive
    static int CASE_SENSITIVE = 0x0008;
    // Ignore new lines
    static int IGNORE_NEW_LINE = 0x0010;
    // Single line only
    static int SINGLE_LINE = 0x0020;
    // Insert leading whitespace
    static int LEADING_SPACE = 0x0040;
    // Insert leading whitespace before child parts
    static int CHILD_SPACE_SEPARATOR = 0x0080;
    // Set when it's a pure container, so that leading space is added to the child.
    static int ADD_LEADING_SPACE_TO_CHILD = 0x0100;

    //===== Public

    // Use the SPF wrapper class as a abbreviated namespace to keep pattern construction
    // as concise as possible while not polluting the derived class' namespace.

    public static class SPF
    {
        public static SQLPatternPart statement(SQLPatternPart... parts)
        {
            return makeStatementPart(true, true, true, true, parts);
        }

        public static SQLPatternPart statementLeader(SQLPatternPart... parts)
        {
            return makeStatementPart(true, false, false, false, parts);
        }

        public static SQLPatternPart statementTrailer(SQLPatternPart... parts)
        {
            return makeStatementPart(false, true, true, false, parts);
        }

        public static SQLPatternPart clause(SQLPatternPart... parts)
        {
            SQLPatternPartElement retElem = new SQLPatternPartElement(parts);
            retElem.m_flags |= SQLPatternFactory.GROUP;
            retElem.m_flags |= SQLPatternFactory.CHILD_SPACE_SEPARATOR;
            return retElem;
        }

        public static SQLPatternPart anyClause()
        {
            return new SQLPatternPartElement(".+");
        }

        /**
         * Non-capturing group.
         */
        public static SQLPatternPart group(SQLPatternPart part)
        {
            return makeGroup(false, null, part);
        }

        /**
         * Capturing or non-capturing group
         */
        public static SQLPatternPart group(boolean capture, SQLPatternPart part)
        {
            return makeGroup(capture, null, part);
        }

        /**
         * Capturing or non-capturing group with capture label.
         */
        public static SQLPatternPart group(boolean capture, String captureLabel, SQLPatternPart part)
        {
            return makeGroup(capture, captureLabel, part);
        }

        /**
         * Capturing group.
         */
        public static SQLPatternPart capture(SQLPatternPart part)
        {
            return makeGroup(true, null, part);
        }

        /**
         * Capturing group with label.
         */
        public static SQLPatternPart capture(String captureLabel, SQLPatternPart part)
        {
            return makeGroup(true, captureLabel, part);
        }

        /**
         * Create a new optional {@link SQLPatternPart} out of {@code parts}
         *
         * @param parts to construct the pattern out of
         * @return Constructed optional {@link SQLPatternPart}
         */
        public static SQLPatternPart optional(SQLPatternPart... parts) {
            return optional(clause(parts));
        }

        public static SQLPatternPart optional(SQLPatternPart part)
        {
            part.m_flags |= SQLPatternFactory.OPTIONAL;
            return part;
        }

        /**
         * Create a new {@link SQLPatternPart} for an optional group which is captured with {@code label}
         *
         * @param label of capture
         * @param parts sql pattern parts to capture
         * @return Constructed optional capture {@link SQLPatternPart}
         */
        public static SQLPatternPart optionalCapture(String label, SQLPatternPart... parts) {
            return optional(capture(label, parts.length == 1 ? parts[0] : clause(parts)));
        }

        /// Unused/untested "one of" to support future OR-ing of multiple alternative clauses.
        /// Please add unit tests, etc. when it is actually in use.
        public static SQLPatternPart oneOf(SQLPatternPart... parts)
        {
            // Default to outer and inner non-capturing groups.
            SQLPatternPartElement retElem = new SQLPatternPartElement(parts);
            retElem.m_flags |= SQLPatternFactory.GROUP;
            retElem.m_separator = "|";
            return retElem;
        }

        public static SQLPatternPart oneOf(String... strs)
        {
            // Default to outer and inner non-capturing groups.
            SQLPatternPartElement retElem = new SQLPatternPartElement(strs);
            retElem.m_flags |= SQLPatternFactory.GROUP;
            retElem.m_separator = "|";
            return retElem;
        }

        public static SQLPatternPart token(String str)
        {
            return new SQLPatternPartElement(str);
        }

        public static SQLPatternPart dot()
        {
            return new SQLPatternPartElement("\\.");
        }

        public static SQLPatternPart tokenAlternatives(String... strs)
        {
            return oneOf(strs);
        }

        /*
         * For table, column, index, view, etc. names.
         */
        public static SQLPatternPart databaseObjectName()
        {
            //TODO: Does not recognize quoted identifiers.
            return new SQLPatternPartElement("[\\w$]+");
        }

        public static SQLPatternPart databaseObjectTypeName()
        {
            return new SQLPatternPartElement("[a-z][a-z]*");
        }

        public static SQLPatternPart procedureName()
        {
            //TODO: Does not recognize quoted identifiers.
            // Accepts '.', but they get rejected in the code with a clear error message.
            return new SQLPatternPartElement("[\\w.$]+");
        }

        public static SQLPatternPart functionName()
        {
            return new SQLPatternPartElement("[\\w$]+");
        }

        public static SQLPatternPart classPath()
        {
            return new SQLPatternPartElement("(?:\\w+\\.)*\\w+");
        }

        public static SQLPatternPart languageName()
        {
            //TODO: Does not recognize quoted identifiers.
            return new SQLPatternPartElement("[\\w.$]+");
        }

        public static SQLPatternPart userName()
        {
            return new SQLPatternPartElement("[\\w.$]+");
        }

        public static SQLPatternPart className()
        {
            return new SQLPatternPartElement("[\\w.$]+");
        }

        public static SQLPatternPart databaseTrigger()
        {
            return new SQLPatternPartElement("[\\w.$]+");
        }

        public static SQLPatternPart anythingOrNothing()
        {
            return new SQLPatternPartElement(".*");
        }

        public static SQLPatternPart integer()
        {
            return new SQLPatternPartElement("\\d+");
        }

        public static SQLPatternPart ifExists() {
            return SPF.optional(SPF.capture("ifExists", SPF.clause(SPF.token("if"), SPF.token("exists"))));
        }

        /**
         * One or more repetitions of a pattern separated by a comma.
         */
        public static SQLPatternPart commaList(SQLPatternPart part)
        {
            String itemExpr = part.generateExpression(0);
            String listExpr = String.format("%s(?:\\s*,\\s*%s)*", itemExpr, itemExpr);
            return new SQLPatternPartElement(listExpr);
        }

        public static SQLPatternPart delimitedCaptureBlock(String delimiter, String captureLabel)
        {
            return new SQLPatternPartElement(
                new SQLPatternPartElement(delimiter),
                makeGroup(true, captureLabel, anyClause()),
                new SQLPatternPartElement(delimiter)
            );
        }

        /**
         * Repetition modifier without min/max.
         *
         * @param part  Part that can repeat
         * @return      Part wrapper with added repetition
         */
        public static SQLPatternPart repeat(SQLPatternPart part) {
            return repeat(0, null, part);
        }

        /**
         * Repetition modifier with min and optional max. Null max is infinity.
         *
         * Choosest cleanest notation based on counts. Does worry about combinations
         * like (0,1) that could be handled another way, e.g. using optional().
         * Asserts on negative numbers, max == 0, or max < min.
         *
         * @param minCount  Min count (>=0)
         * @param maxCount  Max count (>0 and >= min count) if not null or infinity if null
         * @param part      Part that can repeat
         * @return          Part wrapper with added repetition
         */
        public static SQLPatternPart repeat(int minCount, Integer maxCount, SQLPatternPart part)
        {
            assert minCount >= 0;
            assert maxCount == null || (maxCount > 0 && maxCount >= minCount);
            SQLPatternPartElement retElem = new SQLPatternPartElement(part);
            SQLPatternPart retPart = retElem;
            retElem.m_flags |= SQLPatternFactory.GROUP;
            if (maxCount != null) {
                // At least min count, but not more than max count.
                retElem.m_trailer = String.format("{%d,%d}", minCount, maxCount);
            }
            else {
                // No max - choose cleanest notation based on the min count.
                if (minCount <= 0) {
                    retElem.m_trailer = String.format("*", minCount);
                }
                else if (minCount == 1) {
                    retElem.m_trailer = String.format("+", minCount);
                }
                else {
                    retElem.m_trailer = String.format("{%d,}", minCount);
                }
            }
            return retPart;
        }

        public static SQLPatternPart anyColumnFields() {
            return new SQLPatternPartElement("\\((?:.+?)\\)");
        }
    }

    //===== Private methods

    /**
     * Make a capturing or non-capturing group
     */
    private static SQLPatternPart makeGroup(boolean capture, String captureLabel, SQLPatternPart part)
    {
        // Need an outer part if capturing something that's already a group (capturing or not)
        boolean alreadyGroup = (part.m_flags & (SQLPatternFactory.GROUP | SQLPatternFactory.CAPTURE)) != 0;
        SQLPatternPart retPart = alreadyGroup ? new SQLPatternPartElement(part) : part;
        if (capture) {
            retPart.m_flags |= SQLPatternFactory.CAPTURE;
            retPart.setCaptureLabel(captureLabel);
        }
        else {
            retPart.m_flags |= SQLPatternFactory.GROUP;
        }
        return retPart;
    }

    private static SQLPatternPartElement makeStatementPart(
            boolean beginLine,
            boolean endLine,
            boolean terminated,
            boolean terminatorRequired,
            SQLPatternPart... parts)
    {
        SQLPatternPartElement retElem = new SQLPatternPartElement(parts);
        if (beginLine) {
            retElem.m_leader = "\\A\\s*";
        }
        else {
            retElem.m_leader = "\\A.*?";
        }
        if (endLine) {
            if (terminated) {
                if (terminatorRequired) {
                    retElem.m_trailer = "\\s*;\\s*\\z";
                }
                else {
                    retElem.m_trailer = "\\s*;?\\s*\\z";
                }
            }
            else {
                retElem.m_trailer = "\\s*\\z";
            }
        }
        else {
            retElem.m_trailer = ".*\\z";
        }
        retElem.m_flags |= SQLPatternFactory.CHILD_SPACE_SEPARATOR;
        return retElem;
    }
}
