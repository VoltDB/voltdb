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


package org.hsqldb_voltpatches.index;

import org.hsqldb_voltpatches.RowAVLDiskData;
import org.hsqldb_voltpatches.RowAVLDisk;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.persist.PersistentStore;

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020920 - path 1.7.1 - refactoring to cut mamory footprint
// fredt@users 20021205 - path 1.7.2 - enhancements
// fredt@users 20021215 - doc 1.7.2 - javadoc comments

/**
 *  Text table node implementation.<p>
 *  Nodes for the AVL tree are all built and kept in memory while the actual
 *  row data is accessed via TextCache from disk.<p>
 *  This differs from MemoryNode by maintaining an integral pointer for the
 *  Row data instead of a Java reference.
 *
 *  New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.1
 */
public class NodeAVLMemoryPointer extends NodeAVLBaseMemory {

    public int      iData = NO_POS;
    private NodeAVL nPrimary;    // node of key / primary index for this row

    public NodeAVLMemoryPointer(RowAVLDisk r) {

        iData    = r.getPos();
        nPrimary = r.nPrimaryNode == null ? this
                                          : r.nPrimaryNode;
    }

    public void delete() {
        super.delete();
    }

    public int getPos() {
        return iData;
    }

    Row getRow(PersistentStore store) {

        if (iData == NO_POS) {
            return null;
        }

        RowAVLDiskData row = (RowAVLDiskData) store.get(iData, false);

        row.nPrimaryNode = nPrimary;

        return row;
    }
}
