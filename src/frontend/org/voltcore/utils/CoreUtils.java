/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.utils;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.ReverseDNSCache;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListenableFutureTask;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

import jsr166y.LinkedTransferQueue;

public class CoreUtils {
    public static final int SMALL_STACK_SIZE = 1024 * 256;
    public static final int MEDIUM_STACK_SIZE = 1024 * 512;

    public static volatile Runnable m_threadLocalDeallocator = () -> {};

    public static final ExecutorService SAMETHREADEXECUTOR = new ExecutorService() {

        @Override
        public void execute(Runnable command) {
            if (command == null) throw new NullPointerException();
            command.run();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            Preconditions.checkNotNull(task);
            FutureTask<T> retval = new FutureTask<T>(task);
            retval.run();
            return retval;
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            Preconditions.checkNotNull(task);
            FutureTask<T> retval = new FutureTask<T>(task, result);
            retval.run();
            return retval;
        }

        @Override
        public Future<?> submit(Runnable task) {
            Preconditions.checkNotNull(task);
            FutureTask<Object> retval = new FutureTask<>(task, null);
            retval.run();
            return retval;
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
            Preconditions.checkNotNull(tasks);
            List<Future<T>> retval = new ArrayList<>(tasks.size());
            for (Callable<T> c : tasks) {
                FutureTask<T> ft = new FutureTask<>(c);
                retval.add(new FutureTask<>(c));
                ft.run();
            }
            return retval;
        }

        @Override
        public <T> List<Future<T>> invokeAll(
                Collection<? extends Callable<T>> tasks, long timeout,
                TimeUnit unit) {
            Preconditions.checkNotNull(tasks);
            Preconditions.checkNotNull(unit);

            final long end = System.nanoTime() + unit.toNanos(timeout);

            List<Future<T>> retval = new ArrayList<>(tasks.size());
            for (Callable<T> c : tasks) {
                retval.add(new FutureTask<T>(c));
            }

            int size = retval.size();
            int ii = 0;
            for (; ii < size; ii++) {
                @SuppressWarnings("rawtypes")
                FutureTask ft = (FutureTask)retval.get(ii);
                ft.run();
                if (System.nanoTime() > end) break;
            }

            for (; ii < size; ii++) {
                @SuppressWarnings("rawtypes")
                FutureTask ft = (FutureTask)retval.get(ii);
                ft.cancel(false);
            }

            return retval;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws ExecutionException {
            T retval = null;
            Throwable lastException = null;
            boolean haveRetval = false;
            for (Callable<T> c : tasks) {
                try {
                    retval = c.call();
                    haveRetval = true;
                    break;
                } catch (Throwable t) {
                    lastException = t;
                }
            }

            if (haveRetval) {
                return retval;
            } else {
                throw new ExecutionException(lastException);
            }
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
                long timeout, TimeUnit unit) throws ExecutionException, TimeoutException {
            final long end = System.nanoTime() + unit.toNanos(timeout);
            T retval = null;
            Throwable lastException = null;
            boolean haveRetval = false;
            for (Callable<T> c : tasks) {
                if (System.nanoTime() > end) {
                    throw new TimeoutException("CoreUtils.ExecutorService.invokeAny()");
                }
                try {
                    retval = c.call();
                    haveRetval = true;
                    break;
                } catch (Throwable t) {
                    lastException = t;
                }
            }

            if (haveRetval) {
                return retval;
            } else {
                throw new ExecutionException(lastException);
            }
        }

    };

    public static final ListeningExecutorService LISTENINGSAMETHREADEXECUTOR = new ListeningExecutorService() {

        @Override
        public void execute(Runnable command) {
            if (command == null) throw new NullPointerException();
            command.run();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <T> ListenableFuture<T> submit(Callable<T> task) {
            Preconditions.checkNotNull(task);
            ListenableFutureTask<T> retval = ListenableFutureTask.create(task);
            retval.run();
            return retval;
        }

        @Override
        public <T> ListenableFuture<T> submit(Runnable task, T result) {
            Preconditions.checkNotNull(task);
            ListenableFutureTask<T> retval = ListenableFutureTask.create(task, result);
            retval.run();
            return retval;
        }

        @Override
        public ListenableFuture<?> submit(Runnable task) {
            Preconditions.checkNotNull(task);
            ListenableFutureTask<Object> retval = ListenableFutureTask.create(task, null);
            retval.run();
            return retval;
        }

        @Override
        public <T> List<Future<T>> invokeAll(
                Collection<? extends Callable<T>> tasks) {
            Preconditions.checkNotNull(tasks);
            List<Future<T>> retval = new ArrayList<Future<T>>(tasks.size());
            for (Callable<T> c : tasks) {
                FutureTask<T> ft = new FutureTask<T>(c);
                retval.add(new FutureTask<T>(c));
                ft.run();
            }
            return retval;
        }

        @Override
        public <T> List<Future<T>> invokeAll(
                Collection<? extends Callable<T>> tasks, long timeout,
                TimeUnit unit) {
            Preconditions.checkNotNull(tasks);
            Preconditions.checkNotNull(unit);

            final long end = System.nanoTime() + unit.toNanos(timeout);

            List<Future<T>> retval = new ArrayList<Future<T>>(tasks.size());
            for (Callable<T> c : tasks) {
                retval.add(new FutureTask<T>(c));
            }

            int size = retval.size();
            int ii = 0;
            for (; ii < size; ii++) {
                @SuppressWarnings("rawtypes")
                FutureTask ft = (FutureTask)retval.get(ii);
                ft.run();
                if (System.nanoTime() > end) break;
            }

            for (; ii < size; ii++) {
                @SuppressWarnings("rawtypes")
                FutureTask ft = (FutureTask)retval.get(ii);
                ft.cancel(false);
            }

            return retval;
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws ExecutionException {
            T retval = null;
            Throwable lastException = null;
            boolean haveRetval = false;
            for (Callable<T> c : tasks) {
                try {
                    retval = c.call();
                    haveRetval = true;
                    break;
                } catch (Throwable t) {
                    lastException = t;
                }
            }

            if (haveRetval) {
                return retval;
            } else {
                throw new ExecutionException(lastException);
            }
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
                long timeout, TimeUnit unit) throws ExecutionException, TimeoutException {
            final long end = System.nanoTime() + unit.toNanos(timeout);
            T retval = null;
            Throwable lastException = null;
            boolean haveRetval = false;
            for (Callable<T> c : tasks) {
                if (System.nanoTime() > end) {
                    throw new TimeoutException("CoreUtils.ListeningExecutorService.invokeAny()");
                }
                try {
                    retval = c.call();
                    haveRetval = true;
                    break;
                } catch (Throwable t) {
                    lastException = t;
                }
            }

            if (haveRetval) {
                return retval;
            } else {
                throw new ExecutionException(lastException);
            }
        }

    };

    public static final ListenableFuture<Object> COMPLETED_FUTURE = new ListenableFuture<Object>() {
        @Override
        public void addListener(Runnable listener, Executor executor) { executor.execute(listener); }
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) { return false; }
        @Override
        public boolean isCancelled() { return false;  }
        @Override
        public boolean isDone() { return true; }
        @Override
        public Object get() { return null; }
        @Override
        public Object get(long timeout, TimeUnit unit) { return null; }
    };

    /**
     * Extension of ScheduledThreadPoolExecutor that handles errors from
     * the threads executing the tasks. Cancellation exceptions are considered normal
     * and only logs at <code>DEBUG</code> level.
     * <p>
     * Default behaviour is to log errors to the <code>HOST</code> logger. Users of the class can provide
     * their own action on errors by setting their own consumers of the errors.
     */
    public static class ErrorHandlingScheduledThreadPool extends  ScheduledThreadPoolExecutor {
        private static final VoltLogger s_logger = new VoltLogger("HOST");
        private volatile Consumer<Throwable> m_consumer;

        public ErrorHandlingScheduledThreadPool(int corePoolSize) {
            super(corePoolSize);
        }

        public ErrorHandlingScheduledThreadPool(int corePoolSize, RejectedExecutionHandler handler) {
            super(corePoolSize, handler);
        }

        public ErrorHandlingScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory) {
            super(corePoolSize, threadFactory);
        }

        public ErrorHandlingScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory,
                RejectedExecutionHandler handler) {
            super(corePoolSize, threadFactory, handler);
        }

        public ErrorHandlingScheduledThreadPool setConsumer(Consumer<Throwable> consumer) {
            m_consumer = consumer;
            return this;
        }

        @Override
        protected void afterExecute(Runnable runnable, Throwable throwable) {
            super.afterExecute(runnable, throwable);
            assert(throwable == null); // ScheduledThreadPoolExecutor always creates a ScheduledFuture with the task,
                                       // so we will never get the throwable here
            assert(runnable instanceof ScheduledFuture);

            // get error, if any
            try {
                ((Future<?>) runnable).get();
            } catch (CancellationException ce) { // If the task got cancelled, assume that was intentional
                if (s_logger.isDebugEnabled()) {
                    s_logger.debug("Task Cancelled: ", ce);
                }
            } catch (ExecutionException ee) {
                throwable = ee.getCause();
            } catch (InterruptedException ie) {
                throwable = ie;
            }

            if (throwable != null) {
                if (m_consumer == null) {
                    s_logger.error("Error from ErrorHandlingScheduledThreadPool thread", throwable);
                } else {
                    m_consumer.accept(throwable);
                }
            }
        }
    }

    public static final Runnable EMPTY_RUNNABLE = () -> {};

    /**
     * Get a single thread executor that caches its thread meaning that the thread will terminate
     * after keepAlive milliseconds. A new thread will be created the next time a task arrives and that will be kept
     * around for keepAlive milliseconds. On creation no thread is allocated, the first task creates a thread.
     *
     * Uses LinkedTransferQueue to accept tasks and has a small stack.
     */
    public static ListeningExecutorService getCachedSingleThreadExecutor(String name, long keepAlive) {
        return MoreExecutors.listeningDecorator(new ThreadPoolExecutor(
                0,
                1,
                keepAlive,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                CoreUtils.getThreadFactory(null, name, SMALL_STACK_SIZE, false, null)));
    }

    /**
     * Create an unbounded single threaded executor
     */
    public static ExecutorService getSingleThreadExecutor(String name) {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                CoreUtils.getThreadFactory(null, name, SMALL_STACK_SIZE, false, null));
    }

    public static ExecutorService getSingleThreadExecutor(String name, int size) {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                CoreUtils.getThreadFactory(null, name, size, false, null));
    }

    /**
     * Create an unbounded single threaded executor
     */
    public static ListeningExecutorService getListeningSingleThreadExecutor(String name) {
        ExecutorService ste =
                new ThreadPoolExecutor(1, 1,
                        0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                        CoreUtils.getThreadFactory(null, name, SMALL_STACK_SIZE, false, null));
        return MoreExecutors.listeningDecorator(ste);
    }

    public static ListeningExecutorService getListeningSingleThreadExecutor(String name, int size) {
        ExecutorService ste =
                new ThreadPoolExecutor(1, 1,
                        0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
                        CoreUtils.getThreadFactory(null, name, size, false, null));
        return MoreExecutors.listeningDecorator(ste);
    }

    /**
     * Create a bounded single threaded executor that rejects requests if more than capacity
     * requests are outstanding.
     */
    public static ListeningExecutorService getBoundedSingleThreadExecutor(String name, int capacity) {
        BlockingQueue<Runnable> lbq = new LinkedBlockingQueue<>(capacity);
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, 1, 0L,
                        TimeUnit.MILLISECONDS, lbq, CoreUtils.getThreadFactory(name));
        return MoreExecutors.listeningDecorator(tpe);
    }

    /*
     * Have shutdown actually means shutdown. Tasks that need to complete should use
     * futures.
     */
    public static ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor(String name, int poolSize, int stackSize) {
        ScheduledThreadPoolExecutor ses = new ScheduledThreadPoolExecutor(
                poolSize, getThreadFactory(null, name, stackSize,
                poolSize > 1, null));
        ses.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        ses.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return ses;
    }

    public static ListeningExecutorService getListeningExecutorService(
            final String name, final int threads) {
        return getListeningExecutorService(name, threads, new LinkedTransferQueue<>(), null);
    }

    public static ListeningExecutorService getListeningExecutorService(
            final String name, final int coreThreads, final int threads) {
        return getListeningExecutorService(name, coreThreads, threads, new LinkedTransferQueue<>(), null);
    }

    public static ListeningExecutorService getListeningExecutorService(
            final String name, final int threads, Queue<String> coreList) {
        return getListeningExecutorService(name, threads, new LinkedTransferQueue<>(), coreList);
    }

    public static ListeningExecutorService getListeningExecutorService(
            final String name,
            int threadsTemp,
            final BlockingQueue<Runnable> queue,
            final Queue<String> coreList) {
        if (coreList != null && !coreList.isEmpty()) {
            threadsTemp = coreList.size();
        }
        final int threads = threadsTemp;
        if (threads < 1) {
            throw new IllegalArgumentException("Must specify > 0 threads");
        } else if (name == null) {
            throw new IllegalArgumentException("Name cannot be null");
        } else {
            return MoreExecutors.listeningDecorator(
                    new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, queue,
                            getThreadFactory(null, name, SMALL_STACK_SIZE,
                                    threads > 1, coreList)));
        }
    }

    public static ListeningExecutorService getListeningExecutorService(
            final String name,
            int coreThreadsTemp,
            int threadsTemp,
            final BlockingQueue<Runnable> queue,
            final Queue<String> coreList) {
        if (coreThreadsTemp < 0) {
            throw new IllegalArgumentException("Must specify >= 0 core threads");
        } else if (coreThreadsTemp > threadsTemp) {
            throw new IllegalArgumentException("Core threads must be <= threads");
        }

        if (coreList != null && !coreList.isEmpty()) {
            threadsTemp = coreList.size();
            if (coreThreadsTemp > threadsTemp) {
                coreThreadsTemp = threadsTemp;
            }
        }
        final int coreThreads = coreThreadsTemp;
        final int threads = threadsTemp;
        if (threads < 1) {
            throw new IllegalArgumentException("Must specify > 0 threads");
        } else if (name == null) {
            throw new IllegalArgumentException("Name cannot be null");
        } else {
            return MoreExecutors.listeningDecorator(
                    new ThreadPoolExecutor(coreThreads, threads, 1L, TimeUnit.MINUTES, queue,
                            getThreadFactory(null, name, SMALL_STACK_SIZE, threads > 1, coreList)));
        }
    }

    /**
     * Create a bounded thread pool executor. The work queue is synchronous and can cause
     * RejectedExecutionException if there is no available thread to take a new task.
     * @param maxPoolSize: the maximum number of threads to allow in the pool.
     * @param keepAliveTime: when the number of threads is greater than the core, this is the maximum
     *                       time that excess idle threads will wait for new tasks before terminating.
     * @param unit: the time unit for the keepAliveTime argument.
     * @param threadFactory: the factory to use when the executor creates a new thread.
     */
    public static ThreadPoolExecutor getBoundedThreadPoolExecutor(
            int maxPoolSize, long keepAliveTime, TimeUnit unit, ThreadFactory tFactory) {
        return new ThreadPoolExecutor(0, maxPoolSize, keepAliveTime, unit,
                                      new SynchronousQueue<>(), tFactory);
    }

    /**
     * Create an ExceutorService that places tasks in an existing task queue for execution. Used
     * to create a bridge for using ListenableFutures in classes already built around a queue.
     * @param taskQueue : place to enqueue Runnables submitted to the service
     */
    public static ExecutorService getQueueingExecutorService(final Queue<Runnable> taskQueue) {
        return new ExecutorService() {
            @Override
            public void execute(Runnable command) {
                taskQueue.offer(command);
            }

            @Override
            public void shutdown() {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<Runnable> shutdownNow() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isShutdown() {
                return false;
            }

            @Override
            public boolean isTerminated() {
                return false;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return true;
            }

            @Override
            public <T> Future<T> submit(Callable<T> task) {
                Preconditions.checkNotNull(task);
                FutureTask<T> retval = new FutureTask<>(task);
                taskQueue.offer(retval);
                return retval;
            }

            @Override
            public <T> Future<T> submit(Runnable task, T result) {
                Preconditions.checkNotNull(task);
                FutureTask<T> retval = new FutureTask<>(task, result);
                taskQueue.offer(retval);
                return retval;
            }

            @Override
            public Future<?> submit(Runnable task) {
                Preconditions.checkNotNull(task);
                ListenableFutureTask<Object> retval = ListenableFutureTask.create(task, null);
                taskQueue.offer(retval);
                return retval;
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> List<Future<T>> invokeAll(
                    Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
                    long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static ThreadFactory getThreadFactory(String name) {
        return getThreadFactory(name, SMALL_STACK_SIZE);
    }

    public static ThreadFactory getThreadFactory(String groupName, String name) {
        return getThreadFactory(groupName, name, SMALL_STACK_SIZE, true, null);
    }

    public static ThreadFactory getThreadFactory(String name, int stackSize) {
        return getThreadFactory(null, name, stackSize, true, null);
    }

    /**
     * Creates a thread factory that creates threads within a thread group if
     * the group name is given. The threads created will catch any unhandled
     * exceptions and log them to the HOST logger.
     *
     * @param groupName
     * @param name
     * @param stackSize
     * @return
     */
    public static ThreadFactory getThreadFactory(
            final String groupName,
            final String name,
            final int stackSize,
            final boolean incrementThreadNames,
            final Queue<String> coreList) {
        ThreadGroup group = null;
        if (groupName != null) {
            group = new ThreadGroup(Thread.currentThread().getThreadGroup(), groupName);
        }
        final ThreadGroup finalGroup = group;

        return new ThreadFactory() {
            private final AtomicLong m_createdThreadCount = new AtomicLong(0);
            private final ThreadGroup m_group = finalGroup;
            @Override
            public synchronized Thread newThread(final Runnable r) {
                final String threadName = name +
                        (incrementThreadNames ? " - " + m_createdThreadCount.getAndIncrement() : "");
                String coreTemp = null;
                if (coreList != null && !coreList.isEmpty()) {
                    coreTemp = coreList.poll();
                }
                final String core = coreTemp;
                Runnable runnable = () -> {
                    if (core != null) {
                        // Remove Affinity for now to make this dependency dissapear from the client.
                        // Goal is to remove client dependency on this class in the medium term.
                        //PosixJNAAffinity.INSTANCE.setAffinity(core);
                    }
                    try {
                        r.run();
                    } catch (Throwable t) {
                        new VoltLogger("HOST").error("Exception thrown in thread " + threadName, t);
                    } finally {
                        m_threadLocalDeallocator.run();
                    }
                };

                Thread t = new Thread(m_group, runnable, threadName, stackSize);
                t.setDaemon(true);
                return t;
            }
        };
    }

    /**
     * Return the local hostname, if it's resolvable.  If not,
     * return the IPv4 address on the first interface we find, if it exists.
     * If not, returns whatever address exists on the first interface.
     * @return the String representation of some valid host or IP address,
     *         if we can find one; the empty string otherwise
     */
    public static String getHostnameOrAddress() {
        final InetAddress addr = m_localAddressSupplier.get();
        if (addr == null) return "";
        return ReverseDNSCache.hostnameOrAddress(addr);
    }

    /**
     * Return the local [hostname]/ip string, attempting a cached lookup
     * to resolve the local hostname
     * @return The [hostname]/ip string representation for some valid local
     *         interface, if we can find one; the empty string otherwise
     */
    public static String getHostnameAndAddress() {
        return addressToString(m_localAddressSupplier.get());
    }

    /**
     * Return [hostname]/ip string for the given {@link InetAddress}. This
     * simulates the value of {@link InetAddress#toString()}, except that it
     * does a cached lookup of the hostname
     * @param addr
     * @return If the provided address is not null, its [hostname]/ip
     *         string representation; the empty string otherwise
     */
    public static String addressToString(InetAddress addr) {
        if (addr == null) return "";

        StringBuilder hostnameAndAddress = new StringBuilder();
        String address = addr.getHostAddress();
        String hostnameOrAddress = ReverseDNSCache.hostnameOrAddress(addr);
        if (!hostnameOrAddress.equals(address)) {
            hostnameAndAddress.append(hostnameOrAddress);
        }
        hostnameAndAddress.append('/').append(address);
        return hostnameAndAddress.toString();
    }

    private static final Supplier<InetAddress> m_localAddressSupplier =
            Suppliers.memoizeWithExpiration(() -> {
                try {
                    return InetAddress.getLocalHost();
                } catch (UnknownHostException e) {
                    try {
                        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                        if (interfaces == null) {
                            return null;
                        }
                        NetworkInterface intf = interfaces.nextElement();
                        Enumeration<InetAddress> addresses = intf.getInetAddresses();
                        while (addresses.hasMoreElements()) {
                            InetAddress address = addresses.nextElement();
                            if (address instanceof Inet4Address) {
                                return address;
                            }
                        }
                        addresses = intf.getInetAddresses();
                        if (addresses.hasMoreElements()) {
                            return addresses.nextElement();
                        }
                        return null;
                    } catch (SocketException e1) {
                        return null;
                    }
                }
            }, 1, TimeUnit.DAYS);

    /**
     * Return the local IP address, if it's resolvable.  If not,
     * return the IPv4 address on the first interface we find, if it exists.
     * If not, returns whatever address exists on the first interface.
     * @return the String representation of some valid host or IP address,
     *         if we can find one; the empty string otherwise
     */
    public static InetAddress getLocalAddress() {
        return m_localAddressSupplier.get();
    }

    public static long getHSIdFromHostAndSite(int host, int site) {
        long HSId = site;
        HSId = (HSId << 32) + host;
        return HSId;
    }

    public static int getHostIdFromHSId(long HSId) {
        return (int) (HSId);
    }

    public static Set<Integer> getHostIdsFromHSIDs(Collection<Long> hsids) {
        Set<Integer> hosts = Sets.newHashSet();
        for (Long id : hsids) {
            hosts.add(getHostIdFromHSId(id));
        }
        return hosts;
    }

    public static String hsIdToString(long hsId) {
        return hsIdToString(hsId, ":");
    }

    public static String hsIdToString(long hsId, String delimiter) {
        return (int) hsId + delimiter + (int) (hsId >> 32);
    }

    public static void hsIdToString(long hsId, StringBuilder sb) {
        sb.append((int)hsId).append(":").append((int)(hsId >> 32));
    }

    public static String hsIdCollectionToString(Collection<Long> ids) {
        List<String> idstrings = new ArrayList<>();
        for (Long id : ids) {
            idstrings.add(hsIdToString(id));
        }
        // Easy hack, sort hsIds lexically.
        Collections.sort(idstrings);
        StringBuilder sb = new StringBuilder();
        boolean first = false;
        for (String id : idstrings) {
            if (!first) {
                first = true;
            } else {
                sb.append(", ");
            }
            sb.append(id);
        }
        return sb.toString();
    }

    public static int getSiteIdFromHSId(long siteId) {
        return (int)(siteId>>32);
    }

    public static int hsIdToInt(long hsId) {
        int hostId = getHostIdFromHSId(hsId);
        return Integer.parseInt((hostId > 0 ? String.valueOf(getHostIdFromHSId(hsId)) : "") +
                getHostIdFromHSId(hsId));
    }

    public static <K,V> ImmutableMap<K, ImmutableList<V>> unmodifiableMapCopy(Map<K, List<V>> m) {
        ImmutableMap.Builder<K, ImmutableList<V>> builder = ImmutableMap.builder();
        for (Map.Entry<K, List<V>> e : m.entrySet()) {
            builder.put(e.getKey(), ImmutableList.<V>builder().addAll(e.getValue()).build());
        }
        return builder.build();
    }

    public static byte[] urlToBytes(String url) {
        if (url == null) {
            return null;
        }
        try {
            // get the URL/path for the deployment and prep an InputStream
            InputStream input;
            try {
                URL inputURL = new URL(url);
                input = inputURL.openStream();
            } catch (MalformedURLException ex) {
                // Invalid URL. Try as a file.
                try {
                    input = new FileInputStream(url);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException ioex) {
                throw new RuntimeException(ioex);
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            byte[] readBytes = new byte[1024 * 8];
            while (true) {
                int read = input.read(readBytes);
                if (read == -1) {
                    break;
                }
                baos.write(readBytes, 0, read);
            }

            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String throwableToString(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    public static String hsIdKeyMapToString(Map<Long, ?> m) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<Long, ?> entry : m.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(CoreUtils.hsIdToString(entry.getKey()));
            sb.append("->").append(entry.getValue());
        }
        sb.append('}');
        return sb.toString();
    }

    public static String hsIdValueMapToString(Map<?, Long> m) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<?, Long> entry : m.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(entry.getKey()).append("->");
            sb.append(CoreUtils.hsIdToString(entry.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }

    public static String hsIdMapToString(Map<Long, Long> m) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<Long, Long> entry : m.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(CoreUtils.hsIdToString(entry.getKey())).append(" -> ");
            sb.append(CoreUtils.hsIdToString(entry.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }

    public static int availableProcessors() {
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }

    public static final class RetryException extends Exception {
        private static final long serialVersionUID = 3651804109132974056L;
        public RetryException() {};
        public RetryException(Throwable cause) {
            super(cause);
        }
        public RetryException(String errMsg) {
            super(errMsg);
        }
    }

    /**
     * A helper for retrying tasks asynchronously returns a settable future
     * that can be used to attempt to cancel the task.
     *
     * The first executor service is used to schedule retry attempts
     * The second is where the task will be subsmitted for execution
     * If the two services are the same only the scheduled service is used
     *
     * @param maxAttempts It is the number of total attempts including the first one.
     * If the value is 0, that means there is no limit.
     */
    public static<T>  ListenableFuture<T> retryHelper(
            final ScheduledExecutorService ses,
            final ExecutorService es,
            final Callable<T> callable,
            final long maxAttempts,
            final long startInterval,
            final TimeUnit startUnit,
            final long maxInterval,
            final TimeUnit maxUnit) {

        SettableFuture<T> future = SettableFuture.create();
        retryHelper(ses, es, callable, maxAttempts, startInterval, startUnit, maxInterval, maxUnit, future);
        return future;
    }

    public static <T> void retryHelper(final ScheduledExecutorService ses, final ExecutorService es,
                                       final Callable<T> callable, final long maxAttempts, final long startInterval, final TimeUnit startUnit,
                                       final long maxInterval, final TimeUnit maxUnit, final SettableFuture<T> future) {
        Preconditions.checkNotNull(maxUnit);
        Preconditions.checkNotNull(startUnit);
        Preconditions.checkArgument(startUnit.toMillis(startInterval) >= 1);
        Preconditions.checkArgument(maxUnit.toMillis(maxInterval) >= 1);
        Preconditions.checkNotNull(callable);
        Preconditions.checkNotNull(future);

        /*
         * Base case with no retry, attempt the task once
         */
        es.execute(() -> {
            try {
                future.set(callable.call());
            } catch (RetryException e) {
                //Now schedule a retry
                retryHelper(ses, es, callable, maxAttempts - 1, startInterval, startUnit, maxInterval, maxUnit, 0,
                        future);
            } catch (Exception e) {
                future.setException(e);
            }
        });
    }

    private static<T> void retryHelper(
            final ScheduledExecutorService ses,
            final ExecutorService es,
            final Callable<T> callable,
            final long maxAttempts,
            final long startInterval,
            final TimeUnit startUnit,
            final long maxInterval,
            final TimeUnit maxUnit,
            final long ii,
            final SettableFuture<T> retval) {
        if (maxAttempts == 0) {
            retval.setException(new RuntimeException("Max attempts reached"));
            return;
        }

        long intervalMax = maxUnit.toMillis(maxInterval);
        final long interval = Math.min(intervalMax, startUnit.toMillis(startInterval) * 2);
        ses.schedule(() -> {
            Runnable task = () -> {
                if (retval.isCancelled()) return;

                try {
                    retval.set(callable.call());
                } catch (RetryException e) {
                    retryHelper(ses, es, callable, maxAttempts - 1,
                            interval, TimeUnit.MILLISECONDS, maxInterval,  maxUnit, ii + 1, retval);
                } catch (Exception e3) {
                    retval.setException(e3);
                }
            };
            if (ses == es) task.run();
            else es.execute(task);
        }, interval, TimeUnit.MILLISECONDS);
    }

    public static final long LOCK_SPIN_MICROSECONDS =
            TimeUnit.MICROSECONDS.toNanos(Integer.getInteger("LOCK_SPIN_MICROS", 0));

    /*
     * Spin on a ReentrantLock before blocking. Default behavior is not to spin.
     */
    public static void spinLock(ReentrantLock lock) {
        if (LOCK_SPIN_MICROSECONDS > 0) {
            long nanos = -1;
            for (;;) {
                if (lock.tryLock()) return;
                if (nanos == -1) {
                    nanos = System.nanoTime();
                } else if (System.nanoTime() - nanos > LOCK_SPIN_MICROSECONDS) {
                    lock.lock();
                    return;
                }
            }
        } else {
            lock.lock();
        }
    }

    public static final long QUEUE_SPIN_MICROSECONDS =
            TimeUnit.MICROSECONDS.toNanos(Integer.getInteger("QUEUE_SPIN_MICROS", 0));

    /*
     * Spin polling a blocking queue before blocking. Default behavior is not to spin.
     */
    public static <T> T queueSpinTake(BlockingQueue<T> queue) throws InterruptedException {
        if (QUEUE_SPIN_MICROSECONDS > 0) {
            T retval;
            long nanos = -1;
            for (;;) {
                if ((retval = queue.poll()) != null) return retval;
                if (nanos == -1) {
                    nanos = System.nanoTime();
                } else if (System.nanoTime() - nanos > QUEUE_SPIN_MICROSECONDS) {
                    return queue.take();
                }
            }
        } else {
            return queue.take();
        }
    }

    /*
     * This method manages the whitelist of all acceptable Throwables (and Exceptions) that
     * will not cause the Server harm if they occur while invoking the initializer of a stored
     * procedure or while calling the stored procedure.
     */
    public static boolean isStoredProcThrowableFatalToServer(Throwable th) {
        if (th instanceof LinkageError || th instanceof AssertionError) {
            return false;
        } else {
            return !(th instanceof Exception);
        }
    };

    /**
     * Utility method to sort the keys and values of a map by their value.
     */
    public static <K extends Comparable< ? super K>,V extends Comparable< ? super V>> List<Entry<K,V>>
            sortKeyValuePairByValue(Map<K,V> map) {
        List<Map.Entry<K,V>> entries = new ArrayList<>(map.entrySet());
        entries.sort((o1, o2) -> {
            if (!o1.getValue().equals(o2.getValue())) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
            return o1.getKey().compareTo(o2.getKey());
        });
        return entries;
    }

    /**
     * Print beautiful (sic) logs surrounded by stars.
     * Single line only. Use only space characters for word separation.
     *
     * @param vLogger   The provided VoltLogger
     * @param msg   Message to be printed out beautifully
     * @param level Logging level
     */
    public static void logWithEmphasis(VoltLogger vLogger, String msg, Level level) {
        if (vLogger == null || msg == null || level == Level.OFF) { return; }

        // Length of stars = msg length, plus 4 for the surrounding "* " and " *"
        int width = msg.length() + 4;
        StringBuilder starBuilder = new StringBuilder();
        for (int i = 0; i < width; i++) {
            starBuilder.append('*');
        }
        String stars = starBuilder.toString();
        String xmsg = "* " + msg + " *";

        // Wrap the message with 2 lines of stars
        vLogger.log(level, stars, null);
        vLogger.log(level, xmsg, null);
        vLogger.log(level, stars, null);
    }

    public static void logProcedureInvocation(VoltLogger log, String userName, String where, String procedure) {
        String msg = "User " + userName + " from " + where +
                " issued a " + procedure;
        if ("@PrepareShutdown".equals(procedure))
            logWithEmphasis(log, msg, Level.INFO);
        else
            log.info(msg);
    }

    // Utility method to figure out if this is a test case.  Various junit targets in
    // build.xml set a environment variable to give us a hint
    public static boolean isJunitTest() {

        //check os environment variable
        if ("true".equalsIgnoreCase(System.getenv().get("VOLT_JUSTATEST"))){
            return true;
        }

        //check system variable
        return "true".equalsIgnoreCase(System.getProperty("VOLT_JUSTATEST"));
    }
}
