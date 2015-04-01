/**
 * 
 */
package org.voltdb.config.state;

import org.springframework.stereotype.Component;

/**
 * @author black
 *
 */
@Component
public class VoltStateManager {
	private volatile VoltState currentState = VoltState.INITIAL;
	
	/**
	 * For now, ths method only sets state marker.
	 * In the future, it should perform transition currentState->newState
	 */
	public void moveState(VoltState newState) {
		this.currentState = newState;
	}

	public VoltState getCurrentState() {
		return currentState;
	}
	

}
