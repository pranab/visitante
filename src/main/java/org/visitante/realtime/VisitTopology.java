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

import org.chombo.util.ConfigUtility;
import org.chombo.util.RealtimeUtil;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author pranab
 *
 */
public class VisitTopology {
	public static final String SESSION_ID = "sessionID";
	public static final String VISIT_TIME = "visitTime";
	public static final String VISIT_URL = "visitUrl";
	public static final String PAGE_ID = "pageId";
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
    	
    	//spout
        TopologyBuilder builder = new TopologyBuilder();
        int spoutThreads = ConfigUtility.getInt(conf, "spout.threads", 1);
        VisitDepthSpout spout  = new VisitDepthSpout();
        spout.withTupleFields(VisitTopology.SESSION_ID, VisitTopology.VISIT_TIME, VisitTopology.VISIT_URL);
        builder.setSpout("visitDepthRedisSpout", spout, spoutThreads);
       
        //visit session bolt
        int visSessTickFreqInSec = ConfigUtility.getInt(conf, "visit.session.tick.freq.sec", 1);
        VisitSessionBolt viSessBolt = new VisitSessionBolt(visSessTickFreqInSec);
        viSessBolt.withTupleFields(VisitTopology.PAGE_ID, VisitTopology.PAGE_COUNT);
        int boltThreads = ConfigUtility.getInt(conf, "visit.session.bolt.threads", 1);
        builder.
        	setBolt("visitSessionBolt", viSessBolt, boltThreads).
        	fieldsGrouping("visitDepthRedisSpout", new Fields(VisitTopology.SESSION_ID));
        
        //visit depth bolt
        int visDepthTickFreqInSec = ConfigUtility.getInt(conf, "visit.depth.tick.freq.sec", 1);
        VisitDepthBolt  viDepthBolt = new VisitDepthBolt(visDepthTickFreqInSec);
        boltThreads = ConfigUtility.getInt(conf, "visit.depth.bolt.threads", 1);
        builder.
    		setBolt("visitDepthBolt", viDepthBolt, boltThreads).
    		shuffleGrouping("visitSessionBolt");
        
        //submit
        RealtimeUtil.submitStormTopology(topologyName, conf,  builder);
    }	
}
