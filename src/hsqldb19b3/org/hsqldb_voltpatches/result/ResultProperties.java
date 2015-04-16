/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb_voltpatches.result;

/*
 * Execute properties for SELECT statements.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @since 1.9.0
 */
public class ResultProperties {

    //
    static final int idx_returnable = 0;
    static final int idx_holdable   = 1;
    static final int idx_scrollable = 2;
    static final int idx_updatable  = 3;
    static final int idx_sensitive  = 4;
    static final int idx_isheld     = 5;

    //
    public static final int defaultPropsValue   = 0;
    public static final int updatablePropsValue = 1 << idx_updatable;

    // uses SQL constants - no JDBC
    public static int getProperties(int sensitive, int updatable,
                                    int scrollable, int holdable,
                                    int returnable) {

        int combined = (sensitive << idx_sensitive)
                       | (updatable << idx_updatable)
                       | (scrollable << idx_scrollable)
                       | (holdable << idx_holdable)
                       | (returnable << idx_returnable);

        return combined;
    }

    public static int getJDBCHoldability(int props) {
        return isHoldable(props) ? ResultConstants.HOLD_CURSORS_OVER_COMMIT
                                 : ResultConstants.CLOSE_CURSORS_AT_COMMIT;
    }

    public static int getJDBCConcurrency(int props) {
        return isReadOnly(props) ? ResultConstants.CONCUR_READ_ONLY
                                 : ResultConstants.CONCUR_UPDATABLE;
    }

    public static int getJDBCScrollability(int props) {
        return isScrollable(props) ? ResultConstants.TYPE_SCROLL_INSENSITIVE
                                   : ResultConstants.TYPE_FORWARD_ONLY;
    }

    public static int getValueForJDBC(int type, int concurrency,
                                      int holdability) {

        int scrollable = type == ResultConstants.TYPE_FORWARD_ONLY ? 0
                                                                   : 1;
        int updatable  = concurrency == ResultConstants.CONCUR_UPDATABLE ? 1
                                                                         : 0;
        int holdable = holdability == ResultConstants.HOLD_CURSORS_OVER_COMMIT
                       ? 1
                       : 0;
        int prop = (updatable << idx_updatable)
                   | (scrollable << idx_scrollable)
                   | (holdable << idx_holdable);

        return prop;
    }

    public static boolean isUpdatable(int props) {
        return (props & (1 << idx_updatable)) == 0 ? false
                                                   : true;
    }

    public static boolean isScrollable(int props) {
        return (props & (1 << idx_scrollable)) == 0 ? false
                                                    : true;
    }

    public static boolean isHoldable(int props) {
        return (props & (1 << idx_holdable)) == 0 ? false
                                                  : true;
    }

    public static boolean isSensitive(int props) {
        return (props & (1 << idx_sensitive)) == 0 ? false
                                                   : true;
    }

    public static boolean isReadOnly(int props) {
        return (props & (1 << idx_updatable)) == 0 ? true
                                                   : false;
    }

    public static boolean isHeld(int props) {
        return (props & (1 << idx_isheld)) == 0 ? false
                                                : true;
    }

    public static int addUpdatable(int props, boolean flag) {
        return flag ? props | ((1) << idx_updatable)
                    : props & (~(1 << idx_updatable));
    }

    public static int addHoldable(int props, boolean flag) {
        return flag ? props | ((1) << idx_holdable)
                    : props & (~(1 << idx_holdable));
    }

    public static int addScrollable(int props, boolean flag) {
        return flag ? props | ((1) << idx_scrollable)
                    : props & (~(1 << idx_scrollable));
    }

    public static int addIsHeld(int props, boolean flag) {
        return flag ? props | ((1) << idx_isheld)
                    : props & (~(1 << idx_isheld));
    }
}
