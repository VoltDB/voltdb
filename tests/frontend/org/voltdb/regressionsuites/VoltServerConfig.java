/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.voltdb.compiler.VoltProjectBuilder;

/**
 * Interface allowing for the use of a particular configuration/topology
 * of a VoltDB server. For example, an implementation of this class might
 * allow a user to manipulate a 20-node VoltDB cluster in the server room,
 * a 100 node VoltDB cluster in the cloud, or a single process VoltDB
 * instance running on the local machine. This class is given to tests which
 * run generically on top of a VoltServerConfig.
 *
 */
public interface VoltServerConfig {

    /**
     * Build a catalog jar with the required topology according to the
     * configuration parameters of the given VoltProjectBuilder instance.
     *
     * @param builder The VoltProjectBuilder instance describing the project to build.
     */
    public boolean compile(VoltProjectBuilder builder);

    /**
     * Start the instance of VoltDB.
     */
    public void startUp();

    /**
     * Start the instance of VoltDB and optionally clear the
     * data directories first
     */
    public void startUp(boolean clearDataDirectories);

    /**
     * Shutdown the instance of VoltDB.
     */
    public void shutDown() throws InterruptedException;

    /**
     * Get the hostname/ips matching the hostId
     * @param hostId
     * @return The single hostname/ips as string
     */
    public String getListenerAddress(int hostId);

    /**
     * Get the list of hostnames/ips that are listening
     * for the running VoltDB instance.
     *
     * @return A list of hostnames/ips as strings.
     */
    public List<String> getListenerAddresses();

    /**
     * Get the name of this particular configuration. This may be
     * combined with the test name to identify a combination of test
     * and server config to JUnit.
     *
     * @return The name of this config.
     */
    public String getName();

    public void setCallingMethodName(String name);

    /**
     * Get the number of nodes running in this test suite
     */
    public int getNodeCount();

    /**
     * @return Is the underlying instance of VoltDB running HSQL?
     */
    public boolean isHSQL();

    /**
     * @return Is the underlying instance of VoltDB running IPC with Valgrind?
     */
    public boolean isValgrind();

    boolean compileWithPartitionDetection(VoltProjectBuilder builder,
            String snapshotPath,
            String ppdPrefix);

    boolean compileWithAdminMode(VoltProjectBuilder builder, int adminPort,
                                 boolean adminOnStartup);

    /**
     * Create a directory so it is accessible
     * to all voltdb instances represented by this config from within each instances
     * subroot. All necessary intervening directories will
     * be created.
     * @param path
     * @throws IOException
     */
    public void createDirectory(File path) throws IOException;

    /**
     * Delete the directory as seen by all instances represented by this config.
     * This will go into the subroot for each instance and delete the directory and its contents
     * @param path
     * @throws IOException
     */
    public void deleteDirectory(File path) throws IOException;

    /**
     * List the files contained in the specified path as seen by all instances represented
     * by this config. It will go into the specified path in the subroot for each instance,
     * aggregate the list of files, and then return them.
     */
    public List<File> listFiles(File path) throws IOException;

    public File[] getPathInSubroots(File path) throws IOException;

    public void setMaxHeap(int max);

    /**
     * @return the number of logical partitions in this configuration
     * I.e. (sitesPerHost * numHosts) when K safetey is zero. */
    public int getLogicalPartitionCount();
}
