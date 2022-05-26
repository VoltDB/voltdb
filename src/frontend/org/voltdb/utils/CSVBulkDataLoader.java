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

package org.voltdb.utils;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

/**
 * A CSVDataLoader implementation that uses the bulk loader to insert batched rows.
 */
public class CSVBulkDataLoader implements CSVDataLoader {

    private static final VoltLogger log = new VoltLogger(CSVBulkDataLoader.class.getName());
    private final VoltBulkLoader m_loader;
    private final BulkLoaderErrorHandler m_errHandler;
    private final AtomicLong m_failedInsertCount = new AtomicLong(0);
    private final BulkLoaderSuccessCallback m_successCallback;

    public CSVBulkDataLoader(Client client, String tableName, int batchSize,
            BulkLoaderErrorHandler errHandler) throws Exception    {
        this(client, tableName, batchSize, false, errHandler, null);
    }

    public CSVBulkDataLoader(Client client, String tableName, int batchSize, boolean upsertMode,
            BulkLoaderErrorHandler errHandler) throws Exception    {
        this(client, tableName, batchSize, upsertMode, errHandler, null);
    }

    public CSVBulkDataLoader(Client client, String tableName, int batchSize, boolean upsertMode,
            BulkLoaderErrorHandler errHandler, BulkLoaderSuccessCallback successCallback) throws Exception {
        m_loader = client.getNewBulkLoader(tableName, batchSize, upsertMode, new CsvFailureCallback(), successCallback);
        m_errHandler = errHandler;
        m_successCallback = successCallback;
    }

    @Override
    public void setFlushInterval(int delay, int seconds) {
        m_loader.setFlushInterval(delay, seconds);
    }

    @Override
    public void flush() throws ExecutionException, InterruptedException {
        m_loader.flush();
    }

    public class CsvFailureCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response) {

            if (response.getStatus() == ClientResponse.SUCCESS) {
                if (m_successCallback != null) {
                    m_successCallback.success(rowHandle, response);
                }
            }
            else {
                m_failedInsertCount.incrementAndGet();
                m_errHandler.handleError((RowWithMetaData) rowHandle, response, response.getStatusString());
            }
        }
    }

    @Override
    public org.voltdb.VoltType[] getColumnTypes()
    {
        return m_loader.getColumnTypes();
    }

    @Override
    public void insertRow(RowWithMetaData metaData, Object[] values) throws InterruptedException {
        m_loader.insertRow(metaData, values);
    }

    @Override
    public void close() throws Exception {
        m_loader.close();
    }

    @Override
    public long getProcessedRows()
    {
        return m_loader.getCompletedRowCount();
    }

    @Override
    public long getFailedRows()
    {
        return m_failedInsertCount.get();
    }

    @Override
    public Map<Integer, String> getColumnNames()
    {
        return m_loader.getColumnNames();
    }

    @Override
    public void resumeLoading() {
       m_loader.resumeLoading();
    }
}
