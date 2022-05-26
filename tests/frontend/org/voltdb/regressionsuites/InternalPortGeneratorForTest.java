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

package org.voltdb.regressionsuites;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableSortedSet;

public class InternalPortGeneratorForTest implements Iterable<Integer>{
    final List<Integer> ports;
    final NavigableSet<String> coordinators;
    final PortGeneratorForTest generator;

    public InternalPortGeneratorForTest(PortGeneratorForTest generator, int seedCount) {
        checkArgument(seedCount >= 0, "clusterSize %s in less then zero", seedCount);
        ImmutableList.Builder<Integer> lb = ImmutableList.builder();
        ImmutableSortedSet.Builder<String> sb = ImmutableSortedSet.naturalOrder();
        for (int i = 0; i < seedCount; ++i) {
            int port = generator.nextInternalPort();
            lb.add(port);
            sb.add(":" + Integer.toString(port));
        }
        this.ports = lb.build();
        this.coordinators = sb.build();
        this.generator = generator;
    }

    @Override
    public Iterator<Integer> iterator() {
        return ports.iterator();
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public NavigableSet<String> getCoordinators() {
        return coordinators;
    }

    public int  nextInternalPort(int hostId) {
        checkArgument(hostId >= 0, "host id %s is less then 0",hostId);
        if (hostId >= ports.size()) {
            return generator.nextInternalPort();
        }
        return ports.get(hostId);
    }
}
