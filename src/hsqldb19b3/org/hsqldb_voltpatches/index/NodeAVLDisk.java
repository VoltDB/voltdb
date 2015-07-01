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

import java.io.IOException;

import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.RowAVLDisk;
import org.hsqldb_voltpatches.Row;
import org.hsqldb_voltpatches.lib.IntLookup;
import org.hsqldb_voltpatches.persist.PersistentStore;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;
import org.hsqldb_voltpatches.RowAVL;

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020920 - path 1.7.1 - refactoring to cut mamory footprint
// fredt@users 20021205 - path 1.7.2 - enhancements

/**
 *  Cached table Node implementation.<p>
 *  Only integral references to left, right and parent nodes in the AVL tree
 *  are held and used as pointers data.<p>
 *
 *  iId is a reference to the Index object that contains this node.<br>
 *  This fields can be eliminated in the future, by changing the
 *  method signatures to take a Index parameter from Index.java (fredt@users)
 *
 *  New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public class NodeAVLDisk extends NodeAVL {

    final RowAVLDisk row;

    //
    protected NodeAVLDisk nLeft;
    protected NodeAVLDisk nRight;
    protected NodeAVLDisk nParent;

    //
    public int              iData;
    private int             iLeft   = NO_POS;
    private int             iRight  = NO_POS;
    private int             iParent = NO_POS;
    private int             iId;    // id of Index object for this Node
    public static final int SIZE_IN_BYTE = 4 * 4;

    public NodeAVLDisk(RowAVLDisk r, RowInputInterface in,
                       int id) throws IOException {

        row      = r;
        iId      = id;
        iData    = r.getPos();
        iBalance = in.readInt();
        iLeft    = in.readInt();

        if (iLeft <= 0) {
            iLeft = NO_POS;
        }

        iRight = in.readInt();

        if (iRight <= 0) {
            iRight = NO_POS;
        }

        iParent = in.readInt();

        if (iParent <= 0) {
            iParent = NO_POS;
        }
    }

    public NodeAVLDisk(RowAVLDisk r, int id) {

        row   = r;
        iId   = id;
        iData = r.getPos();
    }

    public void delete() {

        iLeft    = NO_POS;
        iRight   = NO_POS;
        iParent  = NO_POS;
        iBalance = -2;
    }

    public boolean isInMemory() {
        return row.isInMemory();
    }

    public boolean isMemory() {
        return false;
    }

    public int getPos() {
        return iData;
    }

    Row getRow(PersistentStore store) {

        if (!row.isInMemory()) {
            return (RowAVLDisk) store.get(this.row, false);
        } else {
            row.updateAccessCount(store.getAccessCount());
        }

        return row;
    }

    private NodeAVLDisk findNode(PersistentStore store, int pos) {

        NodeAVLDisk ret = null;
        RowAVLDisk  r   = (RowAVLDisk) store.get(pos, false);

        if (r != null) {
            ret = (NodeAVLDisk) r.getNode(iId);
        }

        return ret;
    }

    boolean isLeft(NodeAVL n) {

        if (n == null) {
            return iLeft == NO_POS;
        }

        return iLeft == ((NodeAVLDisk) n).getPos();
    }

    boolean isRight(NodeAVL n) {

        if (n == null) {
            return iRight == NO_POS;
        }

        return iRight == ((NodeAVLDisk) n).getPos();
    }

    NodeAVL getLeft(PersistentStore store) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (node.iLeft == NO_POS) {
            return null;
        }

        if (node.nLeft == null || !node.nLeft.isInMemory()) {
            node.nLeft         = findNode(store, node.iLeft);
            node.nLeft.nParent = node;
        }

        return node.nLeft;
    }

    NodeAVL getRight(PersistentStore store) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (node.iRight == NO_POS) {
            return null;
        }

        if (node.nRight == null || !node.nRight.isInMemory()) {
            node.nRight         = findNode(store, node.iRight);
            node.nRight.nParent = node;
        }

        return node.nRight;
    }

    NodeAVL getParent(PersistentStore store) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            return null;
        }

        if (node.nParent == null || !node.nParent.isInMemory()) {
            node.nParent = findNode(store, iParent);
        }

        return node.nParent;
    }

    int getBalance() {
        return iBalance;
    }

    boolean isRoot() {
        return iParent == NodeAVL.NO_POS;
    }

    boolean isFromLeft(PersistentStore store) {

        if (this.isRoot()) {
            return true;
        }

        NodeAVLDisk parent = (NodeAVLDisk) getParent(store);

        return getPos() == parent.iLeft;
    }

    NodeAVL setParent(PersistentStore store, NodeAVL n) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NAVLD");
        }

        row.setNodesChanged();

        node.iParent = n == null ? NO_POS
                                 : n.getPos();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nParent = (NodeAVLDisk) n;

        row.keepInMemory(false);

        return node;
    }

    public NodeAVL setBalance(PersistentStore store, int b) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NAVLD");
        }

        row.setNodesChanged();

        node.iBalance = b;

        row.keepInMemory(false);

        return node;
    }

    NodeAVL setLeft(PersistentStore store, NodeAVL n) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NAVLD");
        }

        row.setNodesChanged();

        node.iLeft = n == null ? NO_POS
                               : n.getPos();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nLeft = (NodeAVLDisk) n;

        row.keepInMemory(false);

        return node;
    }

    NodeAVL setRight(PersistentStore store, NodeAVL n) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NAVLD");
        }

        row.setNodesChanged();

        node.iRight = n == null ? NO_POS
                                : n.getPos();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nRight = (NodeAVLDisk) n;

        row.keepInMemory(false);

        return node;
    }

    boolean equals(NodeAVL n) {
        return this == n
               || (n != null && getPos() == ((NodeAVLDisk) n).getPos());
    }

    public int getRealSize(RowOutputInterface out) {
        return NodeAVLDisk.SIZE_IN_BYTE;
    }

    public void setInMemory(boolean in) {

        if (!in) {
            if (nLeft != null) {
                nLeft.nParent = null;
            }

            if (nRight != null) {
                nRight.nParent = null;
            }

            if (nParent != null) {
                if (iData == nParent.iLeft) {
                    nParent.nLeft = null;
                } else {
                    nParent.nRight = null;
                }
            }

            nLeft = nRight = nParent = null;
        }
    }

    public void write(RowOutputInterface out) {

        out.writeInt(iBalance);
        out.writeInt((iLeft == NO_POS) ? 0
                                       : iLeft);
        out.writeInt((iRight == NO_POS) ? 0
                                        : iRight);
        out.writeInt((iParent == NO_POS) ? 0
                                         : iParent);
    }

    public void writeTranslate(RowOutputInterface out, IntLookup lookup) {

        out.writeInt(iBalance);
        writeTranslatePointer(iLeft, out, lookup);
        writeTranslatePointer(iRight, out, lookup);
        writeTranslatePointer(iParent, out, lookup);
    }

    private static void writeTranslatePointer(int pointer,
            RowOutputInterface out, IntLookup lookup) {

        int newPointer = 0;

        if (pointer != NodeAVL.NO_POS) {
            newPointer = lookup.lookupFirstEqual(pointer);
        }

        out.writeInt(newPointer);
    }
}
