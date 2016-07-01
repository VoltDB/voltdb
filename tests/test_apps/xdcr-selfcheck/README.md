This app is built to generate XDCR conflicts and check the resolution including the CSV conflict export.

#### Test Cases

All the following conflicting scenarios are covered by this app.

```
    enum ConflictType {
        II_CV,    // insert-insert constraint violation
        IU_CV,    // insert-update constraint violation
        UU_CV,    // update-update constraint violation
        UU_TM,    // update-update timestamp mismatch
        UD_TM_MR, // update-delete timestamp mismatch, updating side
                  // update-delete missing row, deleting side
        DD_MR;    // delete-delete missing row
    }
```

#### Test Setup

The following tables are created for conflicts generation and verification.

| table name | description |
| ---------- | ------------|
| xdcr_partitioned | partitioned table with primary key |
| xdcr_partitioned_conflict_expected | expected XDCR conflicts on partitioned table |
| xdcr_partitioned_conflict_actual | XDCR conflicts from CSV export on on partitioned table|
| xdcr_replicated | replicated table with primary key |
| xdcr_replicated_conflict_expected | expected XDCR conflicts on replicated table |
| xdcr_replicated_conflict_actual | XDCR conflicts from CSV export on on replicated table |

Notes:
* Tables with no primary key are considered trivial from XDCR conflict perspective and not covered by the app for now.


 #### Test Run

 1. For each run the client thread first obtains all the conflicting test cases in a **random** order.
 2. For each conflicting test case the client thread
    1. Stages both databases (local/remote) with 0-2 rows depending on the particular case.
    2. Runs the racing transaction pair against both databases. If a conflict is produced,
       the test case logs the expected outcome in a bookkeeping table, xdcr_partitioned_conflict_expected,
       or xdcr_replicated_conflict_expected in the replicated table case. Also the response from each
       client is collected to verify the transaction is executed as intended.
    3. Under heavy load the racing transaction pair might not be able to produce a conflict, e.g.
       the binary log from the remote cluster arrives and gets applied earlier than the local
       cluster. To determine this situation, each update/delete transaction checks whether the record
       to be changed is as expected and fails the transaction if not. This outcome (no conflict) is
       also logged for later verification.

#### Conflict Report Verification

When all client threads finish their test runs, CSV conflict reports from both clusters are checked to verify the outcome.

1. CSV conflict reports for each cluster is loaded into a table within that cluster, xdcr_partitioned_conflict_actual
   for the partitioned table and xdcr_replicated_conflict_actual for the replicated table.

2. The main thread (or the benchmark thread) runs through each logged **expected** conflict record and compares
   it against the corresponding **actual** records from the CSV reports. For non-conflict case it verifies no XDCR conflict
   is produced.

#### CLI

For help,
```
$ ./run.sh help
Usage: ./run.sh {clean|catalog|xdcr1|xdcr2|client|help}
```
For test runs,

1. Clean the setup
    ```
    ./run.sh clean
    ```

2. Build the schema and test app
    ```
    ./run.sh catalog
    ```

3. Start the primary cluster
    ```
    ./run.sh xdcr1
    ```

4. Start the secondary cluster
    ```
    ./run.sh xdcr2
    ```

5. Run the conflict benchmark
    ```
    ./run.sh client
    ```
6. To plot the conflict graph for either partitioned or replicated table
    ```
    ./run.sh {pp|pr}
    ```

Notes:
* The benchmark can be run either multi-threaded for testing and single-threaded for debugging, which can be configured from within run.sh
* The benchmark fails fasts if it detects an error and dumps the stack trace. Also all the records in both are preserved for manual examination.
