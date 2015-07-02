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


package org.hsqldb_voltpatches.persist;

import java.io.IOException;

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.LongLookup;
import org.hsqldb_voltpatches.map.BitMap;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.3.0
 */
public class BitMapCachedObject extends CachedObjectBase {

    public final static int fileSizeFactor = 4;

    //
    BitMap bitMap;

    public BitMapCachedObject(int capacity) {
        bitMap     = new BitMap(new int[capacity]);
        hasChanged = true;
    }

    public CachedObject newInstance(int size) {
        return new BitMapCachedObject(size);
    }

    public void read(RowInputInterface in) {

        this.position = in.getPos();

        int[] array    = bitMap.getIntArray();
        int   capacity = array.length;

        try {
            for (int i = 0; i < capacity; i++) {
                array[i] = in.readInt();
            }
        } catch (IOException e) {
            throw Error.error(ErrorCode.GENERAL_IO_ERROR, e);
        }

        hasChanged = false;
    }

    public int getDefaultCapacity() {
        return bitMap.getIntArray().length;
    }

    public int getRealSize(RowOutputInterface out) {
        return bitMap.getIntArray().length * PersistentStore.INT_STORE_SIZE;
    }

    public void write(RowOutputInterface out) {

        int[] array    = bitMap.getIntArray();
        int   capacity = array.length;

        for (int i = 0; i < capacity; i++) {
            out.writeInt(array[i]);
        }

        out.writeEnd();

        hasChanged = false;
    }

    public void write(RowOutputInterface out, LongLookup lookup) {
        write(out);
    }

    public BitMap getBitMap() {
        return bitMap;
    }
}
