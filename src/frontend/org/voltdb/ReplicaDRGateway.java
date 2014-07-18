package org.voltdb;

public abstract class ReplicaDRGateway extends Thread implements Promotable {
    public abstract void shutdown();
}
