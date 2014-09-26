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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.storm.GenericBolt;
import org.chombo.storm.MessageHolder;
import org.chombo.util.ConfigUtility;
import org.chombo.util.Pair;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Tracks pages visited for a session
 * @author pranab
 *
 */
public class VisitSessionBolt extends  GenericBolt {
	private static final long serialVersionUID = -4001182742881831041L;
	private int tickFrequencyInSeconds;
	private Map<String, SessionDetail> sessions = new HashMap<String, SessionDetail>();
	private String logOutPattern;
	private String pageIdPatternStr;
	private Pattern pageIdPattern;
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
		pageIdPatternStr = ConfigUtility.getString(stormConf, "page.id.pattern");
		pageIdPattern = Pattern.compile(pageIdPatternStr);
		
		logOutPattern = ConfigUtility.getString(stormConf, "logOut.pattern");
		sessionTimeout = ConfigUtility.getLong(stormConf, "session.timeOut");
	}

	@Override
	public boolean process(Tuple input) {
		boolean status = true;
		outputMessages.clear();
		
		if (isTickTuple(input)) {
			//find sessions that have timed out
			LOG.info("got tick tuple ");
			expiredSessions.clear();
			long expiryTime = System.currentTimeMillis() - sessionTimeout * 1000;
			for (String sessionID : sessions.keySet()) {
				SessionDetail sessDetail = sessions.get(sessionID);
				List<Long> timeStamps = sessDetail.getLeft();
				if ((timeStamps.get(timeStamps.size()-1)) < expiryTime) {
					String pageId = sessDetail.getRight();
					if (pageId != null) {
						msg = new MessageHolder();
						msg.setMessage(new Values(timeStamps.size()));
						outputMessages.add(msg);
					}
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
			Matcher matcher = pageIdPattern.matcher(url);
			String pageId = matcher.find()? matcher.group(1) : null;
			SessionDetail sessDetail = sessions.get(sessionID);
			List<Long>  timeStamps = null;
			if (null == sessDetail) {
				//new session
				timeStamps = new ArrayList<Long>();
				sessions.put(sessionID, new SessionDetail(timeStamps, pageId));
			} else {
				timeStamps = sessDetail.getLeft();
				if (null != pageId) {
					sessDetail.setRight(pageId);
				}
			}
			timeStamps.add(visitTime);
			if (url.contains(logOutPattern)) {
				//send page count
				pageId = sessDetail.getRight();
				if (null != pageId) {
					msg = new MessageHolder();
					msg.setMessage(new Values(pageId,timeStamps.size()));
					outputMessages.add(msg);
				}
				sessions.remove(sessionID);
			}
		}
		return status;
	}

	@Override
	public List<MessageHolder> getOutput() {
		return outputMessages;
	}
	
	public static class SessionDetail extends Pair<List<Long>, String> {
		public SessionDetail(List<Long> timeStamps, String pageId) {
			super(timeStamps, pageId);
		}
	}

}
