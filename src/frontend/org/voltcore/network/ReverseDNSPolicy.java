package org.voltcore.network;

/*
 * Enum for specifying how DNS should be handled
 */
public enum ReverseDNSPolicy {
    /*
     * Don't do a reverse DNS lookup
     */
    NONE,
    /*
     * Do the lookup synchronously
     */
    SYNCHRONOUS,
    /*
     * Do it in the background, it may fail or be rejected and never occur
     */
    ASYNCHRONOUS
}