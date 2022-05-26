/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 * Other contributors include Andrew Wright, Jeffrey Hayes,
 * Pat Fisher, Mike Judd.
 */
package org.voltcore.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestSuite;


public class TestCOWMap extends JSR166TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
    public static Test suite() {
        return new TestSuite(TestCOWMap.class);
    }

    /**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     */
    private static COWMap map5() {
        COWMap map = new COWMap();
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        assertFalse(map.isEmpty());
        assertEquals(5, map.size());
        return map;
    }

    /**
     * clear removes all pairs
     */
    public void testClear() {
        COWMap map = map5();
        map.clear();
        assertEquals(0, map.size());
    }

    /**
     * Maps with same contents are equal
     */
    public void testEquals() {
        COWMap map1 = map5();
        COWMap map2 = map5();
        assertEquals(map1, map2);
        assertEquals(map2, map1);
        map1.clear();
        assertFalse(map1.equals(map2));
        assertFalse(map2.equals(map1));
    }

//    /**
//     * contains returns true for contained value
//     */
//    public void testContains() {
//        COWMap map = map5();
//        assertTrue(map.contains("A"));
//        assertFalse(map.contains("Z"));
//    }

    /**
     * containsKey returns true for contained key
     */
    public void testContainsKey() {
        COWMap map = map5();
        assertTrue(map.containsKey(one));
        assertFalse(map.containsKey(zero));
    }

    /**
     * containsValue returns true for held values
     */
    public void testContainsValue() {
        COWMap map = map5();
        assertTrue(map.containsValue("A"));
        assertFalse(map.containsValue("Z"));
    }

//    /**
//     * enumeration returns an enumeration containing the correct
//     * elements
//     */
//    public void testEnumeration() {
//        COWMap map = map5();
//        Enumeration e = map.elements();
//        int count = 0;
//        while (e.hasMoreElements()) {
//            count++;
//            e.nextElement();
//        }
//        assertEquals(5, count);
//    }

    /**
     * get returns the correct element at the given key,
     * or null if not present
     */
    public void testGet() {
        COWMap map = map5();
        assertEquals("A", (String)map.get(one));
        COWMap empty = new COWMap();
        assertNull(map.get("anything"));
    }

    /**
     * isEmpty is true of empty map and false for non-empty
     */
    public void testIsEmpty() {
        COWMap empty = new COWMap();
        COWMap map = map5();
        assertTrue(empty.isEmpty());
        assertFalse(map.isEmpty());
    }

//    /**
//     * keys returns an enumeration containing all the keys from the map
//     */
//    public void testKeys() {
//        COWMap map = map5();
//        Enumeration e = map.keys();
//        int count = 0;
//        while (e.hasMoreElements()) {
//            count++;
//            e.nextElement();
//        }
//        assertEquals(5, count);
//    }

    /**
     * keySet returns a Set containing all the keys
     */
    public void testKeySet() {
        COWMap map = map5();
        Set s = map.keySet();
        assertEquals(5, s.size());
        assertTrue(s.contains(one));
        assertTrue(s.contains(two));
        assertTrue(s.contains(three));
        assertTrue(s.contains(four));
        assertTrue(s.contains(five));
    }

    /**
     * keySet.toArray returns contains all keys
     */
    public void testKeySetToArray() {
        COWMap map = map5();
        Set s = map.keySet();
        Object[] ar = s.toArray();
        assertTrue(s.containsAll(Arrays.asList(ar)));
        assertEquals(5, ar.length);
        ar[0] = m10;
        assertFalse(s.containsAll(Arrays.asList(ar)));
    }

    /**
     * Values.toArray contains all values
     */
    public void testValuesToArray() {
        COWMap map = map5();
        Collection v = map.values();
        Object[] ar = v.toArray();
        ArrayList s = new ArrayList(Arrays.asList(ar));
        assertEquals(5, ar.length);
        assertTrue(s.contains("A"));
        assertTrue(s.contains("B"));
        assertTrue(s.contains("C"));
        assertTrue(s.contains("D"));
        assertTrue(s.contains("E"));
    }

    /**
     * entrySet.toArray contains all entries
     */
    public void testEntrySetToArray() {
        COWMap map = map5();
        Set s = map.entrySet();
        Object[] ar = s.toArray();
        assertEquals(5, ar.length);
        for (int i = 0; i < 5; ++i) {
            assertTrue(map.containsKey(((Map.Entry)(ar[i])).getKey()));
            assertTrue(map.containsValue(((Map.Entry)(ar[i])).getValue()));
        }
    }

    /**
     * values collection contains all values
     */
    public void testValues() {
        COWMap map = map5();
        Collection s = map.values();
        assertEquals(5, s.size());
        assertTrue(s.contains("A"));
        assertTrue(s.contains("B"));
        assertTrue(s.contains("C"));
        assertTrue(s.contains("D"));
        assertTrue(s.contains("E"));
    }

    /**
     * entrySet contains all pairs
     */
    public void testEntrySet() {
        COWMap map = map5();
        Set s = map.entrySet();
        assertEquals(5, s.size());
        Iterator it = s.iterator();
        while (it.hasNext()) {
            Map.Entry e = (Map.Entry) it.next();
            assertTrue(
                       (e.getKey().equals(one) && e.getValue().equals("A")) ||
                       (e.getKey().equals(two) && e.getValue().equals("B")) ||
                       (e.getKey().equals(three) && e.getValue().equals("C")) ||
                       (e.getKey().equals(four) && e.getValue().equals("D")) ||
                       (e.getKey().equals(five) && e.getValue().equals("E")));
        }
    }

    /**
     * putAll adds all key-value pairs from the given map
     */
    public void testPutAll() {
        COWMap empty = new COWMap();
        COWMap map = map5();
        empty.putAll(map);
        assertEquals(5, empty.size());
        assertTrue(empty.containsKey(one));
        assertTrue(empty.containsKey(two));
        assertTrue(empty.containsKey(three));
        assertTrue(empty.containsKey(four));
        assertTrue(empty.containsKey(five));
    }

    /**
     * putIfAbsent works when the given key is not present
     */
    public void testPutIfAbsent() {
        COWMap map = map5();
        map.putIfAbsent(six, "Z");
        assertTrue(map.containsKey(six));
    }

    /**
     * putIfAbsent does not add the pair if the key is already present
     */
    public void testPutIfAbsent2() {
        COWMap map = map5();
        assertEquals("A", map.putIfAbsent(one, "Z"));
    }

    /**
     * replace fails when the given key is not present
     */
    public void testReplace() {
        COWMap map = map5();
        assertNull(map.replace(six, "Z"));
        assertFalse(map.containsKey(six));
    }

    /**
     * replace succeeds if the key is already present
     */
    public void testReplace2() {
        COWMap map = map5();
        assertNotNull(map.replace(one, "Z"));
        assertEquals("Z", map.get(one));
    }

    /**
     * replace value fails when the given key not mapped to expected value
     */
    public void testReplaceValue() {
        COWMap map = map5();
        assertEquals("A", map.get(one));
        assertFalse(map.replace(one, "Z", "Z"));
        assertEquals("A", map.get(one));
    }

    /**
     * replace value succeeds when the given key mapped to expected value
     */
    public void testReplaceValue2() {
        COWMap map = map5();
        assertEquals("A", map.get(one));
        assertTrue(map.replace(one, "A", "Z"));
        assertEquals("Z", map.get(one));
    }

    /**
     * remove removes the correct key-value pair from the map
     */
    public void testRemove() {
        COWMap map = map5();
        map.remove(five);
        assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
    }

    /**
     * remove(key,value) removes only if pair present
     */
    public void testRemove2() {
        COWMap map = map5();
        map.remove(five, "E");
        assertEquals(4, map.size());
        assertFalse(map.containsKey(five));
        map.remove(four, "A");
        assertEquals(4, map.size());
        assertTrue(map.containsKey(four));
    }

    /**
     * size returns the correct values
     */
    public void testSize() {
        COWMap map = map5();
        COWMap empty = new COWMap();
        assertEquals(0, empty.size());
        assertEquals(5, map.size());
    }

    /**
     * toString contains toString of elements
     */
    public void testToString() {
        COWMap map = map5();
        String s = map.toString();
        for (int i = 1; i <= 5; ++i) {
            assertTrue(s.contains(String.valueOf(i)));
        }
    }

    // Exception tests
    /**
     * get(null) throws NPE
     */
    public void testGet_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.get(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * containsKey(null) throws NPE
     */
    public void testContainsKey_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.containsKey(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * containsValue(null) throws NPE
     */
    public void testContainsValue_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.containsValue(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

//    /**
//     * contains(null) throws NPE
//     */
//    public void testContains_NullPointerException() {
//        try {
//            COWMap c = new COWMap();
//            c.contains(null);
//            shouldThrow();
//        } catch (NullPointerException success) {}
//    }

    /**
     * put(null,x) throws NPE
     */
    public void testPut1_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.put(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * put(x, null) throws NPE
     */
    public void testPut2_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.put("whatever", null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * putIfAbsent(null, x) throws NPE
     */
    public void testPutIfAbsent1_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.putIfAbsent(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(null, x) throws NPE
     */
    public void testReplace_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.replace(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(null, x, y) throws NPE
     */
    public void testReplaceValue_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.replace(null, one, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * putIfAbsent(x, null) throws NPE
     */
    public void testPutIfAbsent2_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.putIfAbsent("whatever", null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(x, null) throws NPE
     */
    public void testReplace2_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.replace("whatever", null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(x, null, y) throws NPE
     */
    public void testReplaceValue2_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.replace("whatever", null, "A");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * replace(x, y, null) throws NPE
     */
    public void testReplaceValue3_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.replace("whatever", one, null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * remove(null) throws NPE
     */
    public void testRemove1_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.put("sadsdf", "asdads");
            c.remove(null);
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * remove(null, x) throws NPE
     */
    public void testRemove2_NullPointerException() {
        try {
            COWMap c = new COWMap();
            c.put("sadsdf", "asdads");
            c.remove(null, "whatever");
            shouldThrow();
        } catch (NullPointerException success) {}
    }

    /**
     * remove(x, null) returns false
     */
    public void testRemove3() {
        COWMap c = new COWMap();
        c.put("sadsdf", "asdads");
        assertFalse(c.remove("sadsdf", null));
    }
//
//    /**
//     * A deserialized map equals original
//     */
//    public void testSerialization() throws Exception {
//        Map x = map5();
//        Map y = serialClone(x);
//
//        assertTrue(x != y);
//        assertEquals(x.size(), y.size());
//        assertEquals(x, y);
//        assertEquals(y, x);
//    }

//    /**
//     * SetValue of an EntrySet entry sets value in the map.
//     */
//    public void testSetValueWriteThrough() {
//        // Adapted from a bug report by Eric Zoerner
//        COWMap map = new COWMap();
//        assertTrue(map.isEmpty());
//        for (int i = 0; i < 20; i++)
//            map.put(new Integer(i), new Integer(i));
//        assertFalse(map.isEmpty());
//        Map.Entry entry1 = (Map.Entry)map.entrySet().iterator().next();
//        // Unless it happens to be first (in which case remainder of
//        // test is skipped), remove a possibly-colliding key from map
//        // which, under some implementations, may cause entry1 to be
//        // cloned in map
//        if (!entry1.getKey().equals(new Integer(16))) {
//            map.remove(new Integer(16));
//            entry1.setValue("XYZ");
//            assertTrue(map.containsValue("XYZ")); // fails if write-through broken
//        }
//    }

}
