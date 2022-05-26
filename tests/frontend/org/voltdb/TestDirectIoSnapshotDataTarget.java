/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb;

import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.BeforeClass;
import org.voltdb.sysprocs.saverestore.TestTableSaveFile;

/**
 * Reuse {@link TestTableSaveFile} to test that direct IO works and that the data written by the direct IO target can be
 * read by {@link org.voltdb.sysprocs.saverestore.TableSaveFile}
 */
public class TestDirectIoSnapshotDataTarget extends TestTableSaveFile {
    @BeforeClass
    public static void classSetup() {
        if (!DirectIoSnapshotDataTarget.directIoSupported(s_tmpDirectory.getPath())) {
            File objDir = new File("obj/release");
            if (!objDir.isDirectory()) {
                objDir = new File("obj/debug");
            }

            assumeTrue("Could not find fs which supports direct IO",
                    DirectIoSnapshotDataTarget.directIoSupported(objDir.getPath()));
            s_tmpDirectory = objDir.getAbsoluteFile();
        }
    }

    @Override
    protected NativeSnapshotDataTarget createTarget(File file, String pathType, boolean isTerminus, int hostId, String clusterName, String databaseName,
            String tableName, int numPartitions, boolean isReplicated, List<Integer> partitionIds, byte[] schemaBytes,
            long txnId, long timestamp, int[] version) throws IOException {

        NativeSnapshotDataTarget.Factory factory = DirectIoSnapshotDataTarget.factory(file.getParent(), hostId,
                clusterName, databaseName, numPartitions, partitionIds, txnId, timestamp, version, false,
                UnaryOperator.identity());

        return factory.create(file.getName(), tableName, isReplicated, schemaBytes);
    }
}
