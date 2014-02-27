package org.voltdb.client.VoltBulkLoader;

import org.voltdb.client.ClientResponse;


public interface BulkLoaderFailureCallBack {
	
	public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse response);
	
}
