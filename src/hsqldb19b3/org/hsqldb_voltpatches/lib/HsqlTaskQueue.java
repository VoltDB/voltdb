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
 * Provides very simple queued execution of Runnable objects in a background
 * thread. The underlying queue is an HsqlDeque instance, an array-based
 * circular queue implementation with automatic capacity expansion.
 *
 * @author boucherb@users
 * @version 1.7.2
 * @since 1.7.2
 */
public class HsqlTaskQueue {

    /** The thread used to process commands */
    protected Thread taskRunnerThread;

    /** Special queue element to signal termination */
    protected static final Runnable SHUTDOWNTASK = new Runnable() {
        public void run() {}
    };

    /**
     * true if thread should shut down after processing current task.
     *
     * Once set true, stays true forever
     */
    protected volatile boolean isShutdown;

    public synchronized Thread getTaskRunnerThread() {
        return taskRunnerThread;
    }

    protected synchronized void clearThread() {
        taskRunnerThread = null;
    }

    protected final HsqlDeque queue = new HsqlDeque();

    protected class TaskRunner implements Runnable {

        public void run() {

            Runnable task;

            try {
                while (!isShutdown) {
                    synchronized (queue) {
                        task = (Runnable) queue.getFirst();
                    }

                    if (task == SHUTDOWNTASK) {
                        isShutdown = true;

                        synchronized (queue) {
                            queue.clear();
                        }

                        break;
                    } else if (task != null) {
                        task.run();

                        task = null;
                    } else {
                        break;
                    }
                }
            } finally {
                clearThread();
            }
        }
    }

    protected final TaskRunner taskRunner = new TaskRunner();

    public HsqlTaskQueue() {}

    public boolean isShutdown() {
        return isShutdown;
    }

    public synchronized void restart() {

        if (taskRunnerThread == null &&!isShutdown) {
            taskRunnerThread = new Thread(taskRunner);

            taskRunnerThread.start();
        }
    }

    public void execute(Runnable command) throws RuntimeException {

        if (!isShutdown) {
            synchronized (queue) {
                queue.addLast(command);
            }

            restart();
        }
    }

    public synchronized void shutdownAfterQueued() {

        if (!isShutdown) {
            synchronized (queue) {
                queue.addLast(SHUTDOWNTASK);
            }
        }
    }

    public synchronized void shutdownAfterCurrent() {

        isShutdown = true;

        synchronized (queue) {
            queue.clear();
            queue.addLast(SHUTDOWNTASK);
        }
    }

    public synchronized void shutdownImmediately() {

        isShutdown = true;

        if (taskRunnerThread != null) {
            taskRunnerThread.interrupt();
        }

        synchronized (queue) {
            queue.clear();
            queue.addLast(SHUTDOWNTASK);
        }
    }
}
