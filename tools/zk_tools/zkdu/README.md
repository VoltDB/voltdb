ZKDU tool
---------


Usage:

    ./zkdu.sh [optional arguments]

Optional Arguments:

    -h                   print help only
    -H connectstring     connect to the specified hostname:port (default: 127.0.0.1:7181)
    -d depth             print summary to specified depth
    -v                   print the complete list of all znodes and their size


Example:

    ./zkdu.sh
    Jul 12, 2019 11:25:15 AM org.voltcore.logging.VoltUtilLoggingLogger log
    INFO: Initiating client connection, connectString=127.0.0.1:7181 sessionTimeout=2000 watcher=org.voltdb.tools.ZKDU$1@4617c264
    Jul 12, 2019 11:25:15 AM org.voltcore.logging.VoltUtilLoggingLogger log
    INFO: Opening socket connection to server /127.0.0.1:7181
    Jul 12, 2019 11:25:16 AM org.voltcore.logging.VoltUtilLoggingLogger log
    INFO: Socket connection established to localhost/127.0.0.1:7181, initiating session
    Jul 12, 2019 11:25:16 AM org.voltcore.logging.VoltUtilLoggingLogger log
    INFO: Session establishment complete on server localhost/127.0.0.1:7181, sessionid = 0x2a5a120a3c800000, negotiated timeout = 6000
    Connected to 127.0.0.1:7181

    ZooKeeper branch sizes (in bytes) to depth 2:

    ZNODE                                                                                   BYTES
    -------------------------------------------------------------------------------- ------------
    /core/hostids/                                                                             52
    /core/hosts/                                                                               86
    /core/instance_id/                                                                         76
    /core/readyhosts/                                                                          31
    /core/readyjoininghosts/                                                                   23
    /db/action_blockers/                                                                       19
    /db/action_lock/                                                                           15
    /db/buildstring/                                                                           50
    /db/catalogbytes/                                                                       36527
    /db/catalogbytes_previous/                                                                695
    /db/cl_replay/                                                                             13
    /db/cl_replay_barrier/                                                                     21
    /db/cl_replay_complete/                                                                    48
    /db/cluster_metadata/                                                                     433
    /db/commmandlog_init_barrier/                                                              32
    /db/completed_snapshots/                                                                  735
    /db/dr_consumer_partition_migration/                                                       35
    /db/elastic_join/                                                                         242
    /db/fault_log/                                                                             13
    /db/host_ids_be_stopped/                                                                   23
    /db/init_completed/                                                                        18
    /db/iv2appointees/                                                                        188
    /db/iv2masters/                                                                           176
    /db/iv2mpi/                                                                                47
    /db/lastKnownLiveNodes/                                                                    22
    /db/leaders/                                                                              384
    /db/mailboxes/                                                                             13
    /db/nodes_currently_snapshotting/                                                          32
    /db/operation_mode/                                                                        18
    /db/replicationconfig/                                                                     37
    /db/request_truncation_snapshot/                                                           39
    /db/restore/                                                                               40
    /db/restore_barrier/                                                                       19
    /db/restore_barrier2/                                                                      20
    /db/settings/                                                                             126
    /db/start_action/                                                                          29
    /db/sync_snapshots/                                                                        18
    /db/synchronized_states/                                                                   23
    /db/topology/                                                                             403
    /db/unfaulted_hosts/                                                                       19
    /zookeeper/quota/                                                                          16
    -------------------------------------------------------------------------------- ------------
    Total                                                                                   40856
