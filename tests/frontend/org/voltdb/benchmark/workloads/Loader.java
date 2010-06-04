package org.voltdb.benchmark.workloads;

import org.voltdb.client.Client;

public interface Loader
{
    public abstract void run(Client client);
}