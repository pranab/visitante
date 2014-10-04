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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;
import org.chombo.util.ConfigUtility;
import org.chombo.util.Utility;
import org.hoidla.window.BiasedReservoirWindow;
import org.chombo.util.RealtimeUtil;

import redis.clients.jedis.Jedis;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * Tracks visit length or page count. Generates bounce rate or visit depth
 * distribution
 * @author pranab
 *
 */
public class VisitDepthBolt extends  GenericBolt {
	private int tickFrequencyInSeconds;
	private int windowSize;
	private int minWindowSizeForStat;
	private BiasedReservoirWindow<Integer> window;
	private Map<String, BiasedReservoirWindow<Integer>> windows = new HashMap<String, BiasedReservoirWindow<Integer>>();
	private String  resultType;
	private Jedis jedis;
	private String vistDepthStatQueue;

	private static final Logger LOG = LoggerFactory.getLogger(VisitDepthBolt.class);
	
	public VisitDepthBolt(int tickFrequencyInSeconds) {
		super();
		this.tickFrequencyInSeconds = tickFrequencyInSeconds;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
		return conf;
	}

	@Override
	public void intialize(Map stormConf, TopologyContext context) {
		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
		if (debugOn) {
		}
		windowSize = ConfigUtility.getInt(stormConf, "window.size");
		minWindowSizeForStat = ConfigUtility.getInt(stormConf, "min.window.size.for.stat", (windowSize * 3) / 4);
		resultType = ConfigUtility.getString(stormConf, "result.type");
		
		jedis = RealtimeUtil.buildRedisClient(stormConf);
		vistDepthStatQueue = ConfigUtility.getString(stormConf, "visit.depth.stat.queue");
	}

	@Override
	public boolean process(Tuple input) {
		boolean status = true;
		outputMessages.clear();
		
		if (isTickTuple(input)) {
			for (String pageId : windows.keySet()) {
				window = windows.get(pageId);
				LOG.debug("got tick tuple page ID :" + pageId + " window size:" + window.size());
				if (window.size() >= minWindowSizeForStat) {
					LOG.debug("going to do stat");
					if (resultType.equals("bounceRate")) {
						//bounce rate
						int bounceCount = 0;
						Iterator<Integer> it = window.getIterator();
						while(it.hasNext()) {
							if (it.next() == 1) {
								++bounceCount;
							}
						}
						int bounceRate = (bounceCount * 100) / window.size();
						LOG.debug("bounce rate:" + bounceRate);
						jedis.lpush(vistDepthStatQueue, pageId + ":" + bounceRate);
					} else {
						//depth distribution
						TreeMap<Integer, Integer> depthDistr = new TreeMap<Integer, Integer>();
						Iterator<Integer> it = window.getIterator();
						while(it.hasNext()) {
							Integer depth = it.next();
							Integer count = depthDistr.get(depth);
							if (null == count) {
								depthDistr.put(depth, 1);
							} else {
								depthDistr.put(depth, count+1);
							}
						}		
						//serialize and write to queue
						String distr = Utility.serializeMap(depthDistr, ",", ":");
						jedis.lpush(vistDepthStatQueue, pageId + ":" + distr);
					}
				}
			}
		} else {
			String pageId = input.getStringByField(VisitTopology.PAGE_ID);
			int pageCount = input.getIntegerByField(VisitTopology.PAGE_COUNT);
			LOG.debug("got session pageId:" + pageId + " pageCount:" + pageCount);
			window = windows.get(pageId);
			if (null == window) {
				window = new BiasedReservoirWindow<Integer>(windowSize); 
				LOG.debug("created new window");
				windows.put(pageId, window);
			}
			window.add(pageCount);
			LOG.debug("added to window new size:" + window.size());
		}
		return status;
	}

	@Override
	public List<MessageHolder> getOutput() {
		return null;
	}

}
