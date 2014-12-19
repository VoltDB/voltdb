package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class HardLinkedFileSnapshotDataTarget extends
        DefaultSnapshotDataTarget implements SnapshotDataTarget {

    private final List<Path> m_hardLinks;

    public HardLinkedFileSnapshotDataTarget(List<Path> hardLinks, File file, int hostId,
            String clusterName, String databaseName, String tableName,
            int numPartitions, boolean isReplicated,
            List<Integer> partitionIds, VoltTable schemaTable, long txnId,
            long timestamp) throws IOException {
        super(file, hostId, clusterName, databaseName, tableName, numPartitions,
                isReplicated, partitionIds, schemaTable, txnId, timestamp);
        m_hardLinks = hardLinks;
    }

    public HardLinkedFileSnapshotDataTarget(List<Path> hardLinks, File file, int hostId,
            String clusterName, String databaseName, String tableName,
            int numPartitions, boolean isReplicated,
            List<Integer> partitionIds, VoltTable schemaTable, long txnId,
            long timestamp, int[] version) throws IOException {
        super(file, hostId, clusterName, databaseName, tableName, numPartitions,
                isReplicated, partitionIds, schemaTable, txnId, timestamp, version);
        m_hardLinks = hardLinks;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        super.close();
        Path realFile = m_file.toPath();
        for (Path linkPath: m_hardLinks) {
            Files.createLink(linkPath, realFile);
        }
    }
}
