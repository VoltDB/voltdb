/*
 * For work developed by the HSQL Development Group:
 *
 * Copyright (c) 2001-2011, The HSQL Development Group
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
 *
 *
 *
 * For work originally developed by the Hypersonic SQL Group:
 *
 * Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 */


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.index.NodeAVL;
import org.hsqldb_voltpatches.lib.java.JavaSystem;
import org.hsqldb_voltpatches.persist.PersistentStore;

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020920 - patch 1.7.1 - refactoring to cut mamory footprint
// fredt@users 20021215 - doc 1.7.2 - javadoc comments

/**
 * Base class for a database row object implementing rows for
 * memory resident tables.<p>
 *
 * Subclass RowAVLDisk implements rows for CACHED and TEXT tables.<p>
 * New class derived from Hypersonic SQL code and enhanced in HSQLDB.<p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 2.0.1
 * @since Hypersonic SQL
 */
public class RowAVL extends Row {

    public NodeAVL nPrimaryNode;

    /**
     *  Default constructor used only in subclasses.
     */
    protected RowAVL(TableBase table, Object[] data) {
        super(table, data);
    }

    /**
     *  Constructor for MEMORY table Row. The result is a Row with Nodes that
     *  are not yet linked with other Nodes in the AVL indexes.
     */
    public RowAVL(TableBase table, Object[] data, int position,
                  PersistentStore store) {

        super(table, data);

        this.position = position;

        setNewNodes(store);
    }

    public void setNewNodes(PersistentStore store) {

        int indexCount = store.getAccessorKeys().length;

        nPrimaryNode = new NodeAVL(this);

        NodeAVL n = nPrimaryNode;

        for (int i = 1; i < indexCount; i++) {
            n.nNext = new NodeAVL(this);
            n       = n.nNext;
        }
    }

    /**
     * Returns the Node for a given Index, using the ordinal position of the
     * Index within the Table Object.
     */
    public NodeAVL getNode(int index) {

        NodeAVL n = nPrimaryNode;

        while (index-- > 0) {
            n = n.nNext;
        }

        return n;
    }

    /**
     *  Returns the Node for the next Index on this database row, given the
     *  Node for any Index.
     */
    NodeAVL getNextNode(NodeAVL n) {

        if (n == null) {
            n = nPrimaryNode;
        } else {
            n = n.nNext;
        }

        return n;
    }

    public NodeAVL insertNode(int index) {

        NodeAVL backnode = getNode(index - 1);
        NodeAVL newnode  = new NodeAVL(this);

        newnode.nNext  = backnode.nNext;
        backnode.nNext = newnode;

        return newnode;
    }

    public void clearNonPrimaryNodes() {

        NodeAVL n = nPrimaryNode.nNext;

        while (n != null) {
            n.delete();

            n = n.nNext;
        }
    }

    public void delete(PersistentStore store) {

        NodeAVL n = nPrimaryNode;

        while (n != null) {
            n.delete();

            n = n.nNext;
        }
    }

    public void restore() {}

    /**
     * Helps GC, removing secondary node links. Keeps primary index links to
     * allow iteration.
     */
    public void destroy() {

        JavaSystem.memoryRecords++;

        clearNonPrimaryNodes();

        NodeAVL n = nPrimaryNode;

        while (n != null) {
            NodeAVL last = n;

            n          = n.nNext;
            last.nNext = null;
        }
    }
}
