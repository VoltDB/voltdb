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


package org.hsqldb_voltpatches.lib;

/**
 * Provides the programatic analog of a physical stop watch. <p>
 *
 * The watch can be started, stopped and zeroed and can be queried for
 * elapsed running time.  The watch accumulates elapsed time over starts
 * and stops such that only the time actually spent running is recorded.
 * If the watch is zeroed, then the accumulated time is discarded and
 * the watch starts again with zero acumulated time. <p>
 *
 * @author boucherb@users
 * @version 1.7.2
 * @since 1.7.2
 */
public class StopWatch {

    /**
     * The last time this object made the transition
     * from stopped to running state, as reported
     * by System.currentTimeMillis().
     */
    private long startTime;
    private long lastStart;

    /**
     * The accumulated running time of this object since
     * it was last zeroed.
     */
    private long total;

    /** Flags if this object is started or stopped. */
    boolean running = false;

    /** Creates, zeros, and starts a new StopWatch */
    public StopWatch() {
        this(true);
    }

    /** Creates, zeros, and starts a new StopWatch */
    public StopWatch(boolean start) {

        if (start) {
            start();
        }
    }

    /**
     * Retrieves the accumulated time this object has spent running since
     * it was last zeroed.
     * @return the accumulated time this object has spent running since
     * it was last zeroed.
     */
    public long elapsedTime() {

        if (running) {
            return total + System.currentTimeMillis() - startTime;
        } else {
            return total;
        }
    }

    /**
     * Retrieves the accumulated time this object has spent running since
     * it was last started.
     * @return the accumulated time this object has spent running since
     * it was last started.
     */
    public long currentElapsedTime() {

        if (running) {
            return System.currentTimeMillis() - startTime;
        } else {
            return 0;
        }
    }

    /** Zeros accumulated running time and restarts this object. */
    public void zero() {

        total = 0;

        start();
    }

    /**
     * Ensures that this object is in the running state.  If this object is not
     * running, then the call has the effect of setting the <code>startTime</code>
     * attribute to the current value of System.currentTimeMillis() and setting
     * the <code>running</code> attribute to <code>true</code>.
     */
    public void start() {
        startTime = System.currentTimeMillis();
        running   = true;
    }

    /**
     * Ensures that this object is in the stopped state.  If this object is
     * in the running state, then this has the effect of adding to the
     * <code>total</code> attribute the elapsed time since the last transition
     * from stopped to running state and sets the <code>running</code> attribute
     * to false. If this object is not in the running state, this call has no
     * effect.
     */
    public void stop() {

        if (running) {
            total   += System.currentTimeMillis() - startTime;
            running = false;
        }
    }

    public void mark() {
        stop();
        start();
    }

    /**
     * Retrieves prefix + " in " + elapsedTime() + " ms."
     * @param prefix The string to use as a prefix
     * @return prefix + " in " + elapsedTime() + " ms."
     */
    public String elapsedTimeToMessage(String prefix) {
        return prefix + " in " + elapsedTime() + " ms.";
    }

    /**
     * Retrieves prefix + " in " + elapsedTime() + " ms."
     * @param prefix The string to use as a prefix
     * @return prefix + " in " + elapsedTime() + " ms."
     */
    public String currentElapsedTimeToMessage(String prefix) {
        return prefix + " in " + currentElapsedTime() + " ms.";
    }

    /**
     * Retrieves the internal state of this object, as a String.
     *
     * The retreived value is:
     *
     * <pre>
     *    super.toString() +
     *    "[running=" +
     *    running +
     *    ", startTime=" +
     *    startTime +
     *    ", total=" +
     *    total + "]";
     * </pre>
     * @return the state of this object, as a String
     */
    public String toString() {
        return super.toString() + "[running=" + running + ", startTime="
               + startTime + ", total=" + total + "]";
    }
}
