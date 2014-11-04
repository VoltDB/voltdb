/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty_voltpatches;

import java.lang.reflect.Field;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.AbstractSet;
import java.util.Iterator;

/*
 * A set implementation optimized for use in an NIO selector
 * which doesn't actually need a set. Taken from netty
 * and munged for use in Volt where we don't need to flip.
 * Also has the boilerplate code for instrumenting a selector with the key set
 */
public final class NinjaKeySet extends AbstractSet<SelectionKey> {
    private SelectionKey[] keys;
    private int numKeys;

    public NinjaKeySet() {
        keys = new SelectionKey[1024];
    }

    @Override
    public boolean add(SelectionKey o) {
        if (o == null) {
            return false;
        }

        int size = numKeys;
        keys[size ++] = o;
        numKeys = size;
        if (size == keys.length) {
            doubleCapacity();
        }
        return true;
    }

    public SelectionKey[] keys() {
        return keys;
    }

    private void doubleCapacity() {
        SelectionKey[] newKeys = new SelectionKey[keys.length << 1];
        System.arraycopy(keys, 0, newKeys, 0, numKeys);
        keys = newKeys;
    }

    @Override
    public void clear() {
        //Don't packrat references to keys
        for (int ii = 0; ii < numKeys; ii++) {
            keys[ii] = null;
        }
        numKeys = 0;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<SelectionKey> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return numKeys;
    }

    public static NinjaKeySet instrumentSelector(Selector selector) {
        try {

            NinjaKeySet selectedKeySet = new NinjaKeySet();

            if (!supported) return selectedKeySet;

            Class<?> selectorImplClass =
                    Class.forName("sun.nio.ch.SelectorImpl", false, ClassLoader.getSystemClassLoader());

            // Ensure the current selector implementation is what we can instrument.
            if (!selectorImplClass.isAssignableFrom(selector.getClass())) {
                return selectedKeySet;
            }

            Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
            Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");

            selectedKeysField.setAccessible(true);
            publicSelectedKeysField.setAccessible(true);

            selectedKeysField.set(selector, selectedKeySet);
            publicSelectedKeysField.set(selector, selectedKeySet);

            return selectedKeySet;
        } catch (Throwable t) {
            return null;
        }
    }

    public static final boolean supported;
    static {
        boolean supportedTemp = false;
        try {
            Selector s = Selector.open();
            try {
                Class<?> selectorImplClass =
                        Class.forName("sun.nio.ch.SelectorImpl", false, ClassLoader.getSystemClassLoader());

                // Ensure the current selector implementation is what we can instrument.
                if (selectorImplClass.isAssignableFrom(s.getClass())) {
                    supportedTemp = true;
                }
            } finally {
                s.close();
            }
        } catch (Throwable t){}
        supported = supportedTemp;
    }
}
