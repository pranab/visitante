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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;
import org.chombo.util.ConfigUtility;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class VisitSessionBolt extends  GenericBolt {
	private static final long serialVersionUID = -4001182742881831041L;
	private int tickFrequencyInSeconds;
	private Map<String, List<Long>> sessions = new HashMap<String, List<Long>>();
	private String logOutPattern;
	private long sessionTimeout;
	private MessageHolder msg;
	private List<String> expiredSessions = new ArrayList<String>();
	private static final Logger LOG = Logger.getLogger(VisitSessionBolt.class);

	public VisitSessionBolt(int tickFrequencyInSeconds) {
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
		logOutPattern = ConfigUtility.getString(stormConf, "logOut.pattern");
		sessionTimeout = ConfigUtility.getLong(stormConf, "session.timeOut");
	}

	@Override
	public boolean process(Tuple input) {
		boolean status = true;
		outputMessages.clear();
		
		if (isTickTuple(input)) {
			LOG.info("got tick tuple ");
			//find sessions that have timed out
			expiredSessions.clear();
			long expiryTime = System.currentTimeMillis() - sessionTimeout * 1000;
			for (String sessionID : sessions.keySet()) {
				List<Long> timeStamps = sessions.get(sessionID);
				if ((timeStamps.get(timeStamps.size()-1)) < expiryTime) {
					msg = new MessageHolder();
					msg.setMessage(new Values(timeStamps.size()));
					outputMessages.add(msg);
					expiredSessions.add(sessionID);
				}
			}
			for (String sessionID : expiredSessions) {
				sessions.remove(sessionID);
			}
		} else {
			String sessionID = input.getStringByField(VisitTopology.SESSION_ID);
			long visitTime = input.getLongByField(VisitTopology.VISIT_TIME);
			String url = input.getStringByField(VisitTopology.VISIT_URL);
			List<Long> timeStamps = sessions.get(sessionID);
			if (null == timeStamps) {
				timeStamps = new ArrayList<Long>();
			}
			timeStamps.add(visitTime);
			if (url.contains(logOutPattern)) {
				//send page count
				msg = new MessageHolder();
				msg.setMessage(new Values(timeStamps.size()));
				outputMessages.add(msg);
				sessions.remove(sessionID);
			}
		}
		return status;
	}

	@Override
	public List<MessageHolder> getOutput() {
		// TODO Auto-generated method stub
		return null;
	}

}
