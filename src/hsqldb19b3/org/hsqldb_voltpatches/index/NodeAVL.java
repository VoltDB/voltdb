/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import org.hsqldb_voltpatches.RowAVLDisk;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.persist.CachedObject;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;
import org.hsqldb_voltpatches.lib.IntLookup;

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020920 - path 1.7.1 - refactoring to cut mamory footprint
// fredt@users 20021205 - path 1.7.2 - enhancements
// fredt@users 20021215 - doc 1.7.2 - javadoc comments

/**
 *  The parent for all AVL node implementations. Subclasses of Node vary
 *  in the way they hold
 *  references to other Nodes in the AVL tree, or to their Row data.<br>
 *
 *  nNext links the Node objects belonging to different indexes for each
 *  table row. It is used solely by Row to locate the node belonging to a
 *  particular index.<br>
 *
 *  New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public abstract class NodeAVL implements CachedObject {

    static final int NO_POS = RowAVLDisk.NO_POS;
    public int       iBalance;    // currently, -2 means 'deleted'
    public NodeAVL   nNext;       // node of next index (nNext==null || nNext.iId=iId+1)

    /**
     *  This method unlinks the Node from the other Nodes in the same Index
     *  and from the Row.
     *
     *  It must keep the links between the Nodes in different Indexes.
     */
    abstract public void delete();

    public boolean isMemory() {
        return true;
    }

    /**
     *  File offset of Node. Used with CachedRow objects only
     */
    abstract public int getPos();

    /**
     *  Return the Row Object that is linked to this Node.
     */
    abstract Row getRow(PersistentStore store);

    /**
     *  Getters and setters for AVL index operations.
     */
    abstract boolean isLeft(NodeAVL node);

    abstract boolean isRight(NodeAVL node);

    abstract NodeAVL getLeft(PersistentStore store);

    abstract NodeAVL setLeft(PersistentStore store, NodeAVL n);

    abstract NodeAVL getRight(PersistentStore store);

    abstract NodeAVL setRight(PersistentStore store, NodeAVL n);

    abstract NodeAVL getParent(PersistentStore store);

    abstract NodeAVL setParent(PersistentStore store, NodeAVL n);

    abstract int getBalance();

    abstract public NodeAVL setBalance(PersistentStore store, int b);

    abstract boolean isRoot();

    abstract boolean isFromLeft(PersistentStore store);

    abstract boolean equals(NodeAVL n);

    public void updateAccessCount(int count) {}

    public int getAccessCount() {
        return 0;
    }

    public void setStorageSize(int size) {}

    public int getStorageSize() {
        return 0;
    }

    public void setPos(int pos) {}

    public boolean hasChanged() {
        return false;
    }

    public boolean isKeepInMemory() {
        return false;
    }
    ;

    public boolean keepInMemory(boolean keep) {
        return true;
    }

    public boolean isInMemory() {
        return false;
    }

    abstract public void setInMemory(boolean in);

    public void restore() {}

    public void destroy() {}

    public int getRealSize(RowOutputInterface out) {
        return 0;
    }

    abstract public void write(RowOutputInterface out);

    public void write(RowOutputInterface out, IntLookup lookup) {}
}
