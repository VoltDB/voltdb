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


package org.hsqldb_voltpatches.store;

import java.util.NoSuchElementException;

import org.hsqldb_voltpatches.lib.ArrayCounter;
import org.hsqldb_voltpatches.lib.Iterator;

/**
 * Base class for hash tables or sets. The exact type of the structure is
 * defined by the constructor. Each instance has at least a keyTable array
 * and a HashIndex instance for looking up the keys into this table. Instances
 * that are maps also have a valueTable the same size as the keyTable.
 *
 * Special getOrAddXXX() methods are used for object maps in some subclasses.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public class BaseHashMap {

/*

    data store:
    keys: {array of primitive | array of object}
    values: {none | array of primitive | array of object} same size as keys
    objects support : hashCode(), equals()

    implemented types of keyTable:
    {objectKeyTable: variable size Object[] array for keys |
    intKeyTable: variable size int[] for keys |
    longKeyTable: variable size long[] for keys }

    implemented types of valueTable:
    {objectValueTable: variable size Object[] array for values |
    intValueTable: variable size int[] for values |
    longValueTable: variable size long[] for values}

    valueTable does not exist for sets or for object pools

    hash index:
    hashTable: fixed size int[] array for hash lookup into keyTable
    linkTable: pointer to the next key ; size equal or larger than hashTable
    but equal to the valueTable

    access count table:
    {none |
    variable size int[] array for access count} same size as xxxKeyTable
*/

    //
    boolean           isIntKey;
    boolean           isLongKey;
    boolean           isObjectKey;
    boolean           isNoValue;
    boolean           isIntValue;
    boolean           isLongValue;
    boolean           isObjectValue;
    protected boolean isList;

    //
    private ValuesIterator valuesIterator;

    //
    protected HashIndex hashIndex;

    //
    protected int[]    intKeyTable;
    protected Object[] objectKeyTable;
    protected long[]   longKeyTable;

    //
    protected int[]    intValueTable;
    protected Object[] objectValueTable;
    protected long[]   longValueTable;

    //
    int                 accessMin;
    protected int       accessCount;
    protected int[]     accessTable;
    protected boolean[] multiValueTable;

    //
    final float       loadFactor;
    final int         initialCapacity;
    int               threshold;
    int               maxCapacity;
    protected int     purgePolicy = NO_PURGE;
    protected boolean minimizeOnEmpty;

    //
    boolean hasZeroKey;
    int     zeroKeyIndex = -1;

    // keyOrValueTypes
    protected static final int noKeyOrValue     = 0;
    protected static final int intKeyOrValue    = 1;
    protected static final int longKeyOrValue   = 2;
    protected static final int objectKeyOrValue = 3;

    // purgePolicy
    protected static final int NO_PURGE      = 0;
    protected static final int PURGE_ALL     = 1;
    protected static final int PURGE_HALF    = 2;
    protected static final int PURGE_QUARTER = 3;

    protected BaseHashMap(int initialCapacity, int keyType, int valueType,
                          boolean hasAccessCount)
                          throws IllegalArgumentException {

        if (initialCapacity <= 0) {
            throw new IllegalArgumentException();
        }

        // CHERRY PICK to prevent a flaky crash?
        if (initialCapacity < 3) {
            initialCapacity = 3;
        }
        // End of CHERRY PICK to prevent a flaky crash?
        this.loadFactor      = 1;    // can use any value if necessary
        this.initialCapacity = initialCapacity;
        threshold            = initialCapacity;

        if (threshold < 3) {
            threshold = 3;
        }

        int hashtablesize = (int) (initialCapacity * loadFactor);

        if (hashtablesize < 3) {
            hashtablesize = 3;
        }

        hashIndex = new HashIndex(hashtablesize, initialCapacity, true);

        int arraySize = threshold;

        if (keyType == BaseHashMap.intKeyOrValue) {
            isIntKey    = true;
            intKeyTable = new int[arraySize];
        } else if (keyType == BaseHashMap.objectKeyOrValue) {
            isObjectKey    = true;
            objectKeyTable = new Object[arraySize];
        } else {
            isLongKey    = true;
            longKeyTable = new long[arraySize];
        }

        if (valueType == BaseHashMap.intKeyOrValue) {
            isIntValue    = true;
            intValueTable = new int[arraySize];
        } else if (valueType == BaseHashMap.objectKeyOrValue) {
            isObjectValue    = true;
            objectValueTable = new Object[arraySize];
        } else if (valueType == BaseHashMap.longKeyOrValue) {
            isLongValue    = true;
            longValueTable = new long[arraySize];
        } else {
            isNoValue = true;
        }

        if (hasAccessCount) {
            accessTable = new int[arraySize];
        }
    }

    protected int getLookup(Object key, int hash) {

        int    lookup = hashIndex.getLookup(hash);
        Object tempKey;

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            tempKey = objectKeyTable[lookup];

            if (key.equals(tempKey)) {
                return lookup;
            }
        }

        return lookup;
    }

    protected int getLookup(int key) {

        int lookup = hashIndex.getLookup(key);
        int tempKey;

        for (; lookup >= 0; lookup = hashIndex.linkTable[lookup]) {
            tempKey = intKeyTable[lookup];

            if (key == tempKey) {
                return lookup;
            }
        }

        return lookup;
    }

    protected int getLookup(long key) {

        int  lookup = hashIndex.getLookup((int) key);
        long tempKey;

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            tempKey = longKeyTable[lookup];

            if (key == tempKey) {
                return lookup;
            }
        }

        return lookup;
    }

    protected Iterator getValuesIterator(Object key, int hash) {

        int lookup = getLookup(key, hash);

        if (valuesIterator == null) {
            valuesIterator = new ValuesIterator();
        }

        valuesIterator.reset(key, lookup);

        return valuesIterator;
    }

    /**
     * generic method for adding or removing keys
     */
    protected Object addOrRemove(long longKey, long longValue,
                                 Object objectKey, Object objectValue,
                                 boolean remove) {

        int hash = (int) longKey;

        if (isObjectKey) {
            if (objectKey == null) {
                return null;
            }

            hash = objectKey.hashCode();
        }

        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.hashTable[index];
        int    lastLookup  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (isObjectKey) {
                // A VoltDB extension to prevent an intermittent NPE on catalogUpdate?
                if (objectKey.equals(objectKeyTable[lookup])) {
                /* disabled 1 line ...
                if (objectKeyTable[lookup].equals(objectKey)) {
                ... disabled 1 line */
                // End of VoltDB extension
                    break;
                }
            } else if (isIntKey) {
                if (longKey == intKeyTable[lookup]) {
                    break;
                }
            } else if (isLongKey) {
                if (longKey == longKeyTable[lookup]) {
                    break;
                }
            }
        }

        if (lookup >= 0) {
            if (remove) {
                if (isObjectKey) {
                    objectKeyTable[lookup] = null;
                } else {
                    if (longKey == 0) {
                        hasZeroKey   = false;
                        zeroKeyIndex = -1;
                    }

                    if (isIntKey) {
                        intKeyTable[lookup] = 0;
                    } else {
                        longKeyTable[lookup] = 0;
                    }
                }

                if (isObjectValue) {
                    returnValue              = objectValueTable[lookup];
                    objectValueTable[lookup] = null;
                } else if (isIntValue) {
                    intValueTable[lookup] = 0;
                } else if (isLongValue) {
                    longValueTable[lookup] = 0;
                }

                hashIndex.unlinkNode(index, lastLookup, lookup);

                if (accessTable != null) {
                    accessTable[lookup] = 0;
                }

                if (minimizeOnEmpty && hashIndex.elementCount == 0) {
                    rehash(initialCapacity);
                }

                return returnValue;
            }

            if (isObjectValue) {
                returnValue              = objectValueTable[lookup];
                objectValueTable[lookup] = objectValue;
            } else if (isIntValue) {
                intValueTable[lookup] = (int) longValue;
            } else if (isLongValue) {
                longValueTable[lookup] = longValue;
            }

            if (accessTable != null) {
                accessTable[lookup] = accessCount++;
            }

            return returnValue;
        }

        // not found
        if (remove) {
            return null;
        }

        if (hashIndex.elementCount >= threshold) {

            // should throw maybe, if reset returns false?
            if (reset()) {
                return addOrRemove(longKey, longValue, objectKey, objectValue,
                                   remove);
            } else {
                return null;
            }
        }

        lookup = hashIndex.linkNode(index, lastLookup);

        // type dependent block
        if (isObjectKey) {
            objectKeyTable[lookup] = objectKey;
        } else if (isIntKey) {
            intKeyTable[lookup] = (int) longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        } else if (isLongKey) {
            longKeyTable[lookup] = longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        }

        if (isObjectValue) {
            objectValueTable[lookup] = objectValue;
        } else if (isIntValue) {
            intValueTable[lookup] = (int) longValue;
        } else if (isLongValue) {
            longValueTable[lookup] = longValue;
        }

        //
        if (accessTable != null) {
            accessTable[lookup] = accessCount++;
        }

        return returnValue;
    }

    /**
     * generic method for adding or removing key / values in multi-value
     * maps
     */
    protected Object addOrRemoveMultiVal(long longKey, long longValue,
                                         Object objectKey, Object objectValue,
                                         boolean removeKey,
                                         boolean removeValue) {

        int hash = (int) longKey;

        if (isObjectKey) {
            if (objectKey == null) {
                return null;
            }

            hash = objectKey.hashCode();
        }

        int     index       = hashIndex.getHashIndex(hash);
        int     lookup      = hashIndex.hashTable[index];
        int     lastLookup  = -1;
        Object  returnValue = null;
        boolean multiValue  = false;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (isObjectKey) {
                if (objectKeyTable[lookup].equals(objectKey)) {
                    if (removeKey) {
                        while (true) {
                            objectKeyTable[lookup]   = null;
                            returnValue = objectValueTable[lookup];
                            objectValueTable[lookup] = null;

                            hashIndex.unlinkNode(index, lastLookup, lookup);

                            multiValueTable[lookup] = false;
                            lookup = hashIndex.hashTable[index];

                            if (lookup < 0
                                    || !objectKeyTable[lookup].equals(
                                        objectKey)) {
                                return returnValue;
                            }
                        }
                    } else {
                        if (objectValueTable[lookup].equals(objectValue)) {
                            if (removeValue) {
                                objectKeyTable[lookup]   = null;
                                returnValue = objectValueTable[lookup];
                                objectValueTable[lookup] = null;

                                hashIndex.unlinkNode(index, lastLookup,
                                                     lookup);

                                multiValueTable[lookup] = false;
                                lookup                  = lastLookup;

                                return returnValue;
                            } else {
                                return objectValueTable[lookup];
                            }
                        }
                    }

                    multiValue = true;
                }
            } else if (isIntKey) {
                if (longKey == intKeyTable[lookup]) {
                    if (removeKey) {
                        while (true) {
                            if (longKey == 0) {
                                hasZeroKey   = false;
                                zeroKeyIndex = -1;
                            }

                            intKeyTable[lookup]   = 0;
                            intValueTable[lookup] = 0;

                            hashIndex.unlinkNode(index, lastLookup, lookup);

                            multiValueTable[lookup] = false;
                            lookup = hashIndex.hashTable[index];

                            if (lookup < 0 || longKey != intKeyTable[lookup]) {
                                return null;
                            }
                        }
                    } else {
                        if (intValueTable[lookup] == longValue) {
                            return null;
                        }
                    }

                    multiValue = true;
                }
            } else if (isLongKey) {
                if (longKey == longKeyTable[lookup]) {
                    if (removeKey) {
                        while (true) {
                            if (longKey == 0) {
                                hasZeroKey   = false;
                                zeroKeyIndex = -1;
                            }

                            longKeyTable[lookup]   = 0;
                            longValueTable[lookup] = 0;

                            hashIndex.unlinkNode(index, lastLookup, lookup);

                            multiValueTable[lookup] = false;
                            lookup = hashIndex.hashTable[index];

                            if (lookup < 0
                                    || longKey != longKeyTable[lookup]) {
                                return null;
                            }
                        }
                    } else {
                        if (intValueTable[lookup] == longValue) {
                            return null;
                        }
                    }

                    multiValue = true;
                }
            }
        }

        if (removeKey || removeValue) {
            return returnValue;
        }

        if (hashIndex.elementCount >= threshold) {

            // should throw maybe, if reset returns false?
            if (reset()) {
                return addOrRemoveMultiVal(longKey, longValue, objectKey,
                                           objectValue, removeKey,
                                           removeValue);
            } else {
                return null;
            }
        }

        lookup = hashIndex.linkNode(index, lastLookup);

        // type dependent block
        if (isObjectKey) {
            objectKeyTable[lookup] = objectKey;
        } else if (isIntKey) {
            intKeyTable[lookup] = (int) longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        } else if (isLongKey) {
            longKeyTable[lookup] = longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        }

        if (isObjectValue) {
            objectValueTable[lookup] = objectValue;
        } else if (isIntValue) {
            intValueTable[lookup] = (int) longValue;
        } else if (isLongValue) {
            longValueTable[lookup] = longValue;
        }

        if (multiValue) {
            multiValueTable[lookup] = true;
        }

        //
        if (accessTable != null) {
            accessTable[lookup] = accessCount++;
        }

        return returnValue;
    }

    /**
     * type-specific method for adding or removing keys in int->Object maps
     */
    protected Object addOrRemove(int intKey, Object objectValue,
                                 boolean remove) {

        int    hash        = intKey;
        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.hashTable[index];
        int    lastLookup  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (intKey == intKeyTable[lookup]) {
                break;
            }
        }

        if (lookup >= 0) {
            if (remove) {
                if (intKey == 0) {
                    hasZeroKey   = false;
                    zeroKeyIndex = -1;
                }

                intKeyTable[lookup]      = 0;
                returnValue              = objectValueTable[lookup];
                objectValueTable[lookup] = null;

                hashIndex.unlinkNode(index, lastLookup, lookup);

                if (accessTable != null) {
                    accessTable[lookup] = 0;
                }

                return returnValue;
            }

            if (isObjectValue) {
                returnValue              = objectValueTable[lookup];
                objectValueTable[lookup] = objectValue;
            }

            if (accessTable != null) {
                accessTable[lookup] = accessCount++;
            }

            return returnValue;
        }

        // not found
        if (remove) {
            return returnValue;
        }

        if (hashIndex.elementCount >= threshold) {
            if (reset()) {
                return addOrRemove(intKey, objectValue, remove);
            } else {
                return null;
            }
        }

        lookup              = hashIndex.linkNode(index, lastLookup);
        intKeyTable[lookup] = intKey;

        if (intKey == 0) {
            hasZeroKey   = true;
            zeroKeyIndex = lookup;
        }

        objectValueTable[lookup] = objectValue;

        if (accessTable != null) {
            accessTable[lookup] = accessCount++;
        }

        return returnValue;
    }

    /**
     * type specific method for Object sets or Object->Object maps
     */
    protected Object removeObject(Object objectKey, boolean removeRow) {

        if (objectKey == null) {
            return null;
        }

        int    hash        = objectKey.hashCode();
        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.hashTable[index];
        int    lastLookup  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (objectKeyTable[lookup].equals(objectKey)) {
                objectKeyTable[lookup] = null;

                hashIndex.unlinkNode(index, lastLookup, lookup);

                if (isObjectValue) {
                    returnValue              = objectValueTable[lookup];
                    objectValueTable[lookup] = null;
                }

                if (removeRow) {
                    removeRow(lookup);
                }

                return returnValue;
            }
        }

        // not found
        return returnValue;
    }

    protected boolean reset() {

        if (maxCapacity == 0 || maxCapacity > threshold) {
            rehash(hashIndex.linkTable.length * 2);

            return true;
        } else if (purgePolicy == PURGE_ALL) {
            clear();

            return true;
        } else if (purgePolicy == PURGE_QUARTER) {
            clear(threshold / 4, threshold >> 8);

            return true;
        } else if (purgePolicy == PURGE_HALF) {
            clear(threshold / 2, threshold >> 8);

            return true;
        } else if (purgePolicy == NO_PURGE) {
            return false;
        }

        return false;
    }

    /**
     * rehash uses existing key and element arrays. key / value pairs are
     * put back into the arrays from the top, removing any gaps. any redundant
     * key / value pairs duplicated at the end of the array are then cleared.
     *
     * newCapacity must be larger or equal to existing number of elements.
     */
    protected void rehash(int newCapacity) {

        int     limitLookup     = hashIndex.newNodePointer;
        boolean oldZeroKey      = hasZeroKey;
        int     oldZeroKeyIndex = zeroKeyIndex;

        if (newCapacity < hashIndex.elementCount) {
            return;
        }

        hashIndex.reset((int) (newCapacity * loadFactor), newCapacity);

        if (multiValueTable != null) {
            int counter = multiValueTable.length;

            while (--counter >= 0) {
                multiValueTable[counter] = false;
            }
        }

        hasZeroKey   = false;
        zeroKeyIndex = -1;
        threshold    = newCapacity;

        for (int lookup = -1;
                (lookup = nextLookup(lookup, limitLookup, oldZeroKey, oldZeroKeyIndex))
                < limitLookup; ) {
            long   longKey     = 0;
            long   longValue   = 0;
            Object objectKey   = null;
            Object objectValue = null;

            if (isObjectKey) {
                objectKey = objectKeyTable[lookup];
            } else if (isIntKey) {
                longKey = intKeyTable[lookup];
            } else {
                longKey = longKeyTable[lookup];
            }

            if (isObjectValue) {
                objectValue = objectValueTable[lookup];
            } else if (isIntValue) {
                longValue = intValueTable[lookup];
            } else if (isLongValue) {
                longValue = longValueTable[lookup];
            }

            if (multiValueTable == null) {
                addOrRemove(longKey, longValue, objectKey, objectValue, false);
            } else {
                addOrRemoveMultiVal(longKey, longValue, objectKey,
                                    objectValue, false, false);
            }

            if (accessTable != null) {
                accessTable[hashIndex.elementCount - 1] = accessTable[lookup];
            }
        }

        resizeElementArrays(hashIndex.newNodePointer, newCapacity);
    }

    /**
     * resize the arrays contianing the key / value data
     */
    private void resizeElementArrays(int dataLength, int newLength) {

        Object temp;
        int    usedLength = newLength > dataLength ? dataLength
                                                   : newLength;

        if (isIntKey) {
            temp        = intKeyTable;
            intKeyTable = new int[newLength];

            System.arraycopy(temp, 0, intKeyTable, 0, usedLength);
        }

        if (isIntValue) {
            temp          = intValueTable;
            intValueTable = new int[newLength];

            System.arraycopy(temp, 0, intValueTable, 0, usedLength);
        }

        if (isLongKey) {
            temp         = longKeyTable;
            longKeyTable = new long[newLength];

            System.arraycopy(temp, 0, longKeyTable, 0, usedLength);
        }

        if (isLongValue) {
            temp           = longValueTable;
            longValueTable = new long[newLength];

            System.arraycopy(temp, 0, longValueTable, 0, usedLength);
        }

        if (isObjectKey) {
            temp           = objectKeyTable;
            objectKeyTable = new Object[newLength];

            System.arraycopy(temp, 0, objectKeyTable, 0, usedLength);
        }

        if (isObjectValue) {
            temp             = objectValueTable;
            objectValueTable = new Object[newLength];

            System.arraycopy(temp, 0, objectValueTable, 0, usedLength);
        }

        if (accessTable != null) {
            temp        = accessTable;
            accessTable = new int[newLength];

            System.arraycopy(temp, 0, accessTable, 0, usedLength);
        }

        if (multiValueTable != null) {
            temp            = multiValueTable;
            multiValueTable = new boolean[newLength];

            System.arraycopy(temp, 0, multiValueTable, 0, usedLength);
        }
    }

    /**
     * clear all the key / value data in a range.
     */
    private void clearElementArrays(final int from, final int to) {

        if (isIntKey) {
            int counter = to;

            while (--counter >= from) {
                intKeyTable[counter] = 0;
            }
        } else if (isLongKey) {
            int counter = to;

            while (--counter >= from) {
                longKeyTable[counter] = 0;
            }
        } else if (isObjectKey) {
            int counter = to;

            while (--counter >= from) {
                objectKeyTable[counter] = null;
            }
        }

        if (isIntValue) {
            int counter = to;

            while (--counter >= from) {
                intValueTable[counter] = 0;
            }
        } else if (isLongValue) {
            int counter = to;

            while (--counter >= from) {
                longValueTable[counter] = 0;
            }
        } else if (isObjectValue) {
            int counter = to;

            while (--counter >= from) {
                objectValueTable[counter] = null;
            }
        }

        if (accessTable != null) {
            int counter = to;

            while (--counter >= from) {
                accessTable[counter] = 0;
            }
        }

        if (multiValueTable != null) {
            int counter = to;

            while (--counter >= from) {
                multiValueTable[counter] = false;
            }
        }
    }

    /**
     * move the elements after a removed key / value pair to fill the gap
     */
    void removeFromElementArrays(int lookup) {

        int arrayLength = hashIndex.linkTable.length;

        if (isIntKey) {
            Object array = intKeyTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            intKeyTable[arrayLength - 1] = 0;
        }

        if (isLongKey) {
            Object array = longKeyTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            longKeyTable[arrayLength - 1] = 0;
        }

        if (isObjectKey) {
            Object array = objectKeyTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            objectKeyTable[arrayLength - 1] = null;
        }

        if (isIntValue) {
            Object array = intValueTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            intValueTable[arrayLength - 1] = 0;
        }

        if (isLongValue) {
            Object array = longValueTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            longValueTable[arrayLength - 1] = 0;
        }

        if (isObjectValue) {
            Object array = objectValueTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            objectValueTable[arrayLength - 1] = null;
        }
    }

    /**
     * find the next lookup in the key/value tables with an entry
     * allows the use of old limit and zero int key attributes
     */
    int nextLookup(int lookup, int limitLookup, boolean hasZeroKey,
                   int zeroKeyIndex) {

        for (++lookup; lookup < limitLookup; lookup++) {
            if (isObjectKey) {
                if (objectKeyTable[lookup] != null) {
                    return lookup;
                }
            } else if (isIntKey) {
                if (intKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            } else {
                if (longKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            }
        }

        return lookup;
    }

    /**
     * find the next lookup in the key/value tables with an entry
     * uses current limits and zero integer key state
     */
    protected int nextLookup(int lookup) {

        for (++lookup; lookup < hashIndex.newNodePointer; lookup++) {
            if (isObjectKey) {
                if (objectKeyTable[lookup] != null) {
                    return lookup;
                }
            } else if (isIntKey) {
                if (intKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            } else {
                if (longKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            }
        }

        return -1;
    }

    /**
     * row must already been freed of key / element
     */
    protected void removeRow(int lookup) {
        hashIndex.removeEmptyNode(lookup);
        removeFromElementArrays(lookup);
    }

    /**
     * Clear the map completely.
     */
    public void clear() {

        if (hashIndex.modified) {
            accessCount  = 0;
            accessMin    = accessCount;
            hasZeroKey   = false;
            zeroKeyIndex = -1;

            clearElementArrays(0, hashIndex.linkTable.length);
            hashIndex.clear();

            if (minimizeOnEmpty) {
                rehash(initialCapacity);
            }
        }
    }

    /**
     * Return the max accessCount value for count elements with the lowest
     * access count. Always return at least accessMin + 1
     */
    public int getAccessCountCeiling(int count, int margin) {
        return ArrayCounter.rank(accessTable, hashIndex.newNodePointer, count,
                                 accessMin + 1, accessCount, margin);
    }

    /**
     * This is called after all elements below count accessCount have been
     * removed
     */
    public void setAccessCountFloor(int count) {
        accessMin = count;
    }

    public int incrementAccessCount() {
        return ++accessCount;
    }

    /**
     * Clear approximately count elements from the map, starting with
     * those with low accessTable ranking.
     *
     * Only for maps with Object key table
     */
    protected void clear(int count, int margin) {

        if (margin < 64) {
            margin = 64;
        }

        int maxlookup  = hashIndex.newNodePointer;
        int accessBase = getAccessCountCeiling(count, margin);

        for (int lookup = 0; lookup < maxlookup; lookup++) {
            Object o = objectKeyTable[lookup];

            if (o != null && accessTable[lookup] < accessBase) {
                removeObject(o, false);
            }
        }

        accessMin = accessBase;
    }

    protected void resetAccessCount() {

        if (accessCount < Integer.MAX_VALUE) {
            return;
        }

        accessMin   >>= 2;
        accessCount >>= 2;

        int i = accessTable.length;

        while (--i >= 0) {
            accessTable[i] >>= 2;
        }
    }

    public int capacity() {
        return hashIndex.linkTable.length;
    }

    public int size() {
        return hashIndex.elementCount;
    }

    public boolean isEmpty() {
        return hashIndex.elementCount == 0;
    }

    protected boolean containsKey(Object key) {

        if (key == null) {
            return false;
        }

        // CHERRY PICK to prevent a flaky crash?
        if (hashIndex.elementCount == 0) {
            return false;
        }

        // End of CHERRY PICK

        int lookup = getLookup(key, key.hashCode());

        return lookup == -1 ? false
                            : true;
    }

    protected boolean containsKey(int key) {
        // CHERRY PICK to prevent a flaky crash?
        if (hashIndex.elementCount == 0) {
            return false;
        }

        // End of CHERRY PICK

        int lookup = getLookup(key);

        return lookup == -1 ? false
                            : true;
    }

    protected boolean containsKey(long key) {

        // CHERRY PICK to prevent a flaky crash?
        if (hashIndex.elementCount == 0) {
            return false;
        }

        // End of CHERRY PICK
        int lookup = getLookup(key);

        return lookup == -1 ? false
                            : true;
    }

    protected boolean containsValue(Object value) {

        int lookup = 0;

        // CHERRY PICK to prevent a flaky crash?
        if (hashIndex.elementCount == 0) {
            return false;
        }

        // End of CHERRY PICK
        if (value == null) {
            for (; lookup < hashIndex.newNodePointer; lookup++) {
                if (objectValueTable[lookup] == null) {
                    if (isObjectKey) {
                        if (objectKeyTable[lookup] != null) {
                            return true;
                        }
                    } else if (isIntKey) {
                        if (intKeyTable[lookup] != 0) {
                            return true;
                        } else if (hasZeroKey && lookup == zeroKeyIndex) {
                            return true;
                        }
                    } else {
                        if (longKeyTable[lookup] != 0) {
                            return true;
                        } else if (hasZeroKey && lookup == zeroKeyIndex) {
                            return true;
                        }
                    }
                }
            }
        } else {
            for (; lookup < hashIndex.newNodePointer; lookup++) {
                if (value.equals(objectValueTable[lookup])) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Currently only for object maps
     */
    protected class ValuesIterator implements org.hsqldb_voltpatches.lib.Iterator {

        int    lookup = -1;
        Object key;

        private void reset(Object key, int lookup) {
            this.key    = key;
            this.lookup = lookup;
        }

        public boolean hasNext() {
            return lookup != -1;
        }

        public Object next() throws NoSuchElementException {

            if (lookup == -1) {
                return null;
            }

            Object value = BaseHashMap.this.objectValueTable[lookup];

            while (true) {
                lookup = BaseHashMap.this.hashIndex.getNextLookup(lookup);

                if (lookup == -1
                        || BaseHashMap.this.objectKeyTable[lookup].equals(
                            key)) {
                    break;
                }
            }

            return value;
        }

        public int nextInt() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public long nextLong() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void remove() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void setValue(Object value) {
            throw new NoSuchElementException("Hash Iterator");
        }
    }

    protected class MultiValueKeyIterator implements Iterator {

        boolean keys;
        int     lookup = -1;
        int     counter;
        boolean removed;

        public MultiValueKeyIterator() {
            toNextLookup();
        }

        private void toNextLookup() {

            while (true) {
                lookup = nextLookup(lookup);

                if (lookup == -1 || !multiValueTable[lookup]) {
                    break;
                }
            }
        }

        public boolean hasNext() {
            return lookup != -1;
        }

        public Object next() throws NoSuchElementException {

            Object value = objectKeyTable[lookup];

            toNextLookup();

            return value;
        }

        public int nextInt() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public long nextLong() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void remove() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void setValue(Object value) {
            throw new NoSuchElementException("Hash Iterator");
        }
    }

    /**
     * Iterator returns Object, int or long and is used both for keys and
     * values
     */
    protected class BaseHashIterator implements Iterator {

        boolean keys;
        int     lookup = -1;
        int     counter;
        boolean removed;

        /**
         * default is iterator for values
         */
        public BaseHashIterator() {}

        public BaseHashIterator(boolean keys) {
            this.keys = keys;
        }

        public boolean hasNext() {
            return counter < hashIndex.elementCount;
        }

        public Object next() throws NoSuchElementException {

            if ((keys && !isObjectKey) || (!keys && !isObjectValue)) {
                throw new NoSuchElementException("Hash Iterator");
            }

            removed = false;

            if (hasNext()) {
                counter++;

                lookup = nextLookup(lookup);

                if (keys) {
                    return objectKeyTable[lookup];
                } else {
                    return objectValueTable[lookup];
                }
            }

            throw new NoSuchElementException("Hash Iterator");
        }

        public int nextInt() throws NoSuchElementException {

            if ((keys && !isIntKey) || (!keys && !isIntValue)) {
                throw new NoSuchElementException("Hash Iterator");
            }

            removed = false;

            if (hasNext()) {
                counter++;

                lookup = nextLookup(lookup);

                if (keys) {
                    return intKeyTable[lookup];
                } else {
                    return intValueTable[lookup];
                }
            }

            throw new NoSuchElementException("Hash Iterator");
        }

        public long nextLong() throws NoSuchElementException {

            if ((!isLongKey || !keys)) {
                throw new NoSuchElementException("Hash Iterator");
            }

            removed = false;

            if (hasNext()) {
                counter++;

                lookup = nextLookup(lookup);

                if (keys) {
                    return longKeyTable[lookup];
                } else {
                    return longValueTable[lookup];
                }
            }

            throw new NoSuchElementException("Hash Iterator");
        }

        public void remove() throws NoSuchElementException {

            if (removed) {
                throw new NoSuchElementException("Hash Iterator");
            }

            counter--;

            removed = true;

            if (BaseHashMap.this.isObjectKey) {
                if (multiValueTable == null) {
                    addOrRemove(0, 0, objectKeyTable[lookup], null, true);
                } else {
                    if (keys) {
                        addOrRemoveMultiVal(0, 0, objectKeyTable[lookup],
                                            null, true, false);
                    } else {
                        addOrRemoveMultiVal(0, 0, objectKeyTable[lookup],
                                            objectValueTable[lookup], false,
                                            true);
                    }
                }
            } else if (isIntKey) {
                addOrRemove(intKeyTable[lookup], 0, null, null, true);
            } else {
                addOrRemove(longKeyTable[lookup], 0, null, null, true);
            }

            if (isList) {
                // CHERRY PICK to prevent a flaky crash?
                removeRow(lookup);

                // End of CHERRY PICK
                lookup--;
            }
        }

        public void setValue(Object value) {

            if (keys) {
                throw new NoSuchElementException();
            }

            objectValueTable[lookup] = value;
        }

        public int getAccessCount() {

            if (removed || accessTable == null) {
                throw new NoSuchElementException();
            }

            return accessTable[lookup];
        }

        public int getLookup() {
            return lookup;
        }
    }
}
