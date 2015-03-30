/**
 * 
 */
package org.voltdb.config.topo;

import org.json_voltpatches.JSONObject;

/**
 * @author black
 *
 */
public interface TopologyProvider {

	JSONObject getTopo();

}
