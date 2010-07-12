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


package org.hsqldb_voltpatches.rights;

/**
 * The constants for grants.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @version 1.7.2
 * @since 1.7.2
 */
public interface GrantConstants {

    /** Flag required to SELECT from a table. */
    int SELECT = 1 << 0;

    /** Flag required to DELETE from a table. */
    int DELETE = 1 << 1;

    /** Flag required to INSERT into a table. */
    int INSERT = 1 << 2;

    /** Flag required to UPDATE a table. */
    int UPDATE = 1 << 3;

    /** Flag required to use a sequence. */
    int USAGE = 1 << 4;

    /** Flag required to execute a routine. */
    int EXECUTE = 1 << 5;

    /** Flag required for check or assertion constraint referencing a column. */
    int REFERENCES = 1 << 6;

    /** Flag required to use of a trigger. */
    int TRIGGER = 1 << 7;

    /** Combined flag permitting any action. */
    int ALL = SELECT | DELETE | INSERT | UPDATE | USAGE | EXECUTE;
}
