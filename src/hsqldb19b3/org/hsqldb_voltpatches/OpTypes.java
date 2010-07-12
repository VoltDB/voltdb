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

public interface OpTypes {

    int VALUE                = 1,     // constant value
        COLUMN               = 2,     // references
        COALESCE             = 3,
        DEFAULT              = 4,
        SIMPLE_COLUMN        = 5,
        VARIABLE             = 6,
        PARAMETER            = 7,
        DYNAMIC_PARAM        = 8,
        ASTERISK             = 9,
        SEQUENCE             = 10,
        SCALAR_SUBQUERY      = 21,    // query based row or table
        ROW_SUBQUERY         = 22,
        TABLE_SUBQUERY       = 23,
        ROW                  = 25,    // rows
        TABLE                = 26,
        FUNCTION             = 27,
        SQL_FUNCTION         = 28,
        ROUTINE_FUNCTION     = 29,
        NEGATE               = 31,    // arithmetic operations
        ADD                  = 32,
        SUBTRACT             = 33,
        MULTIPLY             = 34,
        DIVIDE               = 35,
        CONCAT               = 36,    // concatenation
        EQUAL                = 41,    // logical - comparison
        GREATER_EQUAL        = 42,
        GREATER              = 43,
        SMALLER              = 44,
        SMALLER_EQUAL        = 45,
        NOT_EQUAL            = 46,
        IS_NULL              = 47,
        NOT                  = 48,    // logical operations
        AND                  = 49,
        OR                   = 50,
        ALL_QUANTIFIED       = 51,    // logical - quantified comparison
        ANY_QUANTIFIED       = 52,
        LIKE                 = 53,    // logical - predicates
        IN                   = 54,
        EXISTS               = 55,
        OVERLAPS             = 56,
        UNIQUE               = 57,
        NOT_DISTINCT         = 58,
        MATCH_SIMPLE         = 59,
        MATCH_PARTIAL        = 60,
        MATCH_FULL           = 61,
        MATCH_UNIQUE_SIMPLE  = 62,
        MATCH_UNIQUE_PARTIAL = 63,
        MATCH_UNIQUE_FULL    = 64,
        COUNT                = 71,    // aggregate functions
        SUM                  = 72,
        MIN                  = 73,
        MAX                  = 74,
        AVG                  = 75,
        EVERY                = 76,
        SOME                 = 77,
        STDDEV_POP           = 78,
        STDDEV_SAMP          = 79,
        VAR_POP              = 80,
        VAR_SAMP             = 81,
        CAST                 = 91,    // other operations
        ZONE_MODIFIER        = 92,
        CASEWHEN             = 93,
        ORDER_BY             = 94,
        LIMIT                = 95,
        ALTERNATIVE          = 96,
        MULTICOLUMN          = 97
    ;
}
