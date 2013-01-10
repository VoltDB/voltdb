/*
 * Copyright 2011 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vanilla.java.affinity.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;


/**
 * Implementation of IAffinity based on JNA call of
 * sched_setaffinity(3)/sched_getaffinity(3) from 'c' library. Applicable for most
 * linux/unix platforms
 * <p/>
 * TODO Support assignment to core 64 and above
 *
 * @author peter.lawrey
 * @author BegemoT
 */
public enum PosixJNAAffinity {
    INSTANCE;
    private static final Logger LOGGER = Logger.getLogger(PosixJNAAffinity.class.getName());

    public static final boolean LOADED;

    private static final String LIBRARY_NAME = Platform.isWindows() ? "msvcrt" : "c";

    /**
     * @author BegemoT
     */
    private interface CLibrary extends Library {
        public static final CLibrary INSTANCE = (CLibrary)
                Native.loadLibrary(LIBRARY_NAME, CLibrary.class);

        public int sched_setaffinity(final int pid,
                                     final int cpusetsize,
                                     final long[] cpuset) throws LastErrorException;

        public int sched_getaffinity(final int pid,
                                     final int cpusetsize,
                                     final long[] cpuset) throws LastErrorException;
    }

    static {
        boolean loaded = false;
        try {
            INSTANCE.getAffinity();
            loaded = true;
        } catch (UnsatisfiedLinkError e) {
            LOGGER.log(Level.WARNING, "Unable to load jna library " + e);
        }
        LOADED = loaded;
    }

    public long getAffinity() {
        final CLibrary lib = CLibrary.INSTANCE;
        // TODO where are systems with 64+ cores...
        final long cpuset[] = new long[16];
        try {
            final int ret = lib.sched_getaffinity(0, 16 * (Long.SIZE / 8), cpuset);
            if (ret < 0)
                throw new IllegalStateException("sched_getaffinity((" + Long.SIZE / 8 + ") , &(" + cpuset + ") ) return " + ret);
            return cpuset[0];
        } catch (LastErrorException e) {
            throw new IllegalStateException("sched_getaffinity((" + Long.SIZE / 8 + ") , &(" + cpuset + ") ) errorNo=" + e.getErrorCode(), e);
        }
    }

    public void setAffinity(final String affinityString) {
        Set<Integer> cores = new HashSet<Integer>();
        for (String affinity : affinityString.split(":")) {
            String affinityRange[] = affinity.split("-");
            if (affinityRange.length == 1) {
                cores.add(Integer.valueOf(affinityRange[0]));
            } else {
                /*
                 * For hyper threading, skip every other thread. God help you
                 * if you have more than two threads per core
                 */
                boolean skipEveryOther = false;
                if (affinityRange[0].startsWith("!")) {
                    affinityRange[0] = affinityRange[0].substring(1, affinityRange[0].length());
                    skipEveryOther = true;
                }
                int rangeStart = Integer.valueOf(affinityRange[0]);
                int rangeEnd = Integer.valueOf(affinityRange[1]);
                if (rangeEnd < rangeStart) {
                    throw new IllegalArgumentException();
                }
                for (int ii = rangeStart; ii <= rangeEnd; ii++) {
                    if (skipEveryOther) {
                        /*
                         * If the start of the range is even, skip the odd values
                         */
                        if (rangeStart % 2 == 0 && ii % 2 == 1) {
                            continue;
                        }
                        /*
                         * If the start was odd, skip the even values
                         */
                        if (rangeStart % 2 == 1 && ii % 2 == 0) {
                            continue;
                        }
                    }
                    cores.add(ii);
                }
            }
        }

        long mask = 0;
        for (Integer core : cores) {
            mask |= 1L << core;
        }
        System.out.println("Thread (" + Thread.currentThread().getName() + ") setting affinity to " + cores);
        setAffinity(mask);
    }
    public void setAffinity(final long affinity) {
        final CLibrary lib = CLibrary.INSTANCE;
        try {
            //fixme: where are systems with more then 64 cores...
            long affinityMask[] = new long[16];
            affinityMask[0] = affinity;
            final int ret = lib.sched_setaffinity(0, 16 * (Long.SIZE / 8), affinityMask);
            if (ret < 0) {
                throw new IllegalStateException("sched_setaffinity((" + Long.SIZE / 8 + ") , &(" + affinity + ") ) return " + ret);
            }
        } catch (LastErrorException e) {
            throw new IllegalStateException("sched_getaffinity((" + Long.SIZE / 8 + ") , &(" + affinity + ") ) errorNo=" + e.getErrorCode(), e);
        }
    }
}
