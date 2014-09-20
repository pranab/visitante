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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;
import org.chombo.util.ConfigUtility;
import org.hoidla.window.BiasedReservoirWindow;

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
	private BiasedReservoirWindow<Integer> window;
	private String  resultType;
	private static final Logger LOG = Logger.getLogger(VisitDepthBolt.class);
	
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
			LOG.setLevel(Level.INFO);
		}
		windowSize = ConfigUtility.getInt(stormConf, "window.size");
		window = new BiasedReservoirWindow<Integer>(windowSize); 
		resultType = ConfigUtility.getString(stormConf, "result.type");
	}

	@Override
	public boolean process(Tuple input) {
		boolean status = true;
		outputMessages.clear();
		
		if (isTickTuple(input)) {
			if (resultType.equals("bounceRate")) {
				//bounce rate
				int bounceCount = 0;
				Iterator<Integer> it = window.getIterator();
				while(it.hasNext()) {
					if (it.next() == 1) {
						++bounceCount;
					}
				}
				int bounceRate = (bounceCount * 100) / windowSize;
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
			}
		} else {
			int pageCount = input.getIntegerByField(VisitTopology.PAGE_COUNT);
			window.add(pageCount);
		}
		return status;
	}

	@Override
	public List<MessageHolder> getOutput() {
		// TODO Auto-generated method stub
		return null;
	}

}
