/**
 * 
 */
package org.voltdb.config.state;

/**
 * @author black
 *
 */
public enum VoltState {
	INITIAL,
	
	CREATE,
	
    // Should the execution sites be started in recovery mode
    // (used for joining a node to an existing cluster)
    // If CL is enabled this will be set to true
    // by the CL when the truncation snapshot completes
    // and this node is viable for replay
	REJOIN,
	GROW,
	
	OPERATE,
	
	PAUSE
}
