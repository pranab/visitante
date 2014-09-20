/*
 * visitante: Web analytic using Hadoop Map Reduce
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, softwarSessionSummarizere
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.visitante.realtime;

import org.chombo.util.RealtimeUtil;

import backtype.storm.Config;

/**
 * @author pranab
 *
 */
public class VisitTopology {
	public static final String SESSION_ID = "sessionID";
	public static final String VISIT_TIME = "visitTime";
	public static final String VISIT_URL = "visitUrl";
	public static final String PAGE_COUNT = "pageCount";
	
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
    		throw new IllegalArgumentException("Need two arguments: topology name and config file path");
    	}
    	String topologyName = args[0];
    	String configFilePath = args[1];
    	Config conf = RealtimeUtil.buildStormConfig(configFilePath);
    }	
}
