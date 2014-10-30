/*
 * visitante: Web analytic using Hadoop Map Reduce and Storm
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
public class UniqueVisitorTopology {
	public static final String PART_ID = "partitionID";
	public static final String USER_ID = "userID";
	public static final String BOLT_ID = "boltID";
	public static final String UNIQUE_COUNT = "uniqueCount";

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
    	boolean debugOn = ConfigUtility.getBoolean(conf,"debug.on", false);
    	System.out.println("config file:" + configFilePath + " debugOn:" + debugOn);
    	conf.put(Config.TOPOLOGY_DEBUG, debugOn);
    	
    	//spout
        TopologyBuilder builder = new TopologyBuilder();
        int spoutThreads = ConfigUtility.getInt(conf, "spout.threads", 1);
        UniqueVisitorSpout spout = new UniqueVisitorSpout();
        spout.withTupleFields(PART_ID, USER_ID);
        builder.setSpout("unqueVisitorSpout", spout, spoutThreads);
        
        //unique count bolt
        int uniqueCountTickFreqInSec = ConfigUtility.getInt(conf, "unique.user.tick.freq.sec", 1);
        UniqueVisitorCounterBolt uniqueCountBolt = new UniqueVisitorCounterBolt(uniqueCountTickFreqInSec);
        uniqueCountBolt.withTupleFields(BOLT_ID, UNIQUE_COUNT);
        int boltThreads = ConfigUtility.getInt(conf, "unique.count.bolt.threads", 1);
        builder.
    		setBolt("uniqueCountBolt", uniqueCountBolt, boltThreads).
    		fieldsGrouping("unqueVisitorSpout", new Fields(PART_ID));
        
        //unique count aggregation bolt
        UniqueVisitorAggregatorBolt uniqueCountAggrBolt = new UniqueVisitorAggregatorBolt();
        boltThreads = ConfigUtility.getInt(conf, "unique.count.aggr.bolt.threads", 1);
        builder.
			setBolt("uniqueCountAggrBolt", uniqueCountAggrBolt, boltThreads).
			shuffleGrouping("uniqueCountBolt");
        
        
        //submit
        RealtimeUtil.submitStormTopology(topologyName, conf,  builder);
        
    }	
}
