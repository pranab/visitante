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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.chombo.storm.GenericSpout;
import org.chombo.storm.MessageHolder;
import org.chombo.util.ConfigUtility;
import org.chombo.util.RealtimeUtil;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

public class VisitDepthSpout extends GenericSpout {
	private String logQueue;
	private Jedis jedis;
	private String seesionRegex;
	private DateFormat dF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private int dateOrd;
	private int timeOrd;
	private int urlOrd;
	private Pattern pattern;
	private static final Logger LOG = LoggerFactory.getLogger(VisitDepthSpout.class);
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void intialize(Map stormConf, TopologyContext context) {
		jedis = RealtimeUtil.buildRedisClient(stormConf);
		logQueue = ConfigUtility.getString(stormConf, "redis.log.queue");
		dateOrd = ConfigUtility.getInt(stormConf, "date.ordinal");
		timeOrd = ConfigUtility.getInt(stormConf, "time.ordinal");
		urlOrd = ConfigUtility.getInt(stormConf, "url.ordinal");
		seesionRegex = ConfigUtility.getString(stormConf, "session.regex");
		pattern = Pattern.compile(seesionRegex);
		debugOn = ConfigUtility.getBoolean(stormConf,"debug.on", false);
		if (debugOn) {
		}
	}

	@Override
	public MessageHolder nextSpoutMessage() {
		MessageHolder msgHolder = null;
		String message  = jedis.rpop(logQueue);		
		if(null != message) {
			//message in event queue
			String[] items = message.split("\\s+");
			try {
				Date date = dF.parse(items[dateOrd] + " " + items[timeOrd]);
				long epochTime = date.getTime();
				String url = items[urlOrd];
				Matcher matcher = pattern.matcher(url);
				String session = matcher.matches() ? matcher.group(1) : null;
				if (null != session) {
					Values values = new Values(session, epochTime, url);
					msgHolder = new  MessageHolder(values);
				} else {
					LOG.error("could not extract session id");;
				}
			} catch (ParseException e) {
				LOG.error("invalid date format");;
			}
		}
		return msgHolder;
	}

	@Override
	public void handleFailedMessage(Values tuple) {
		// TODO Auto-generated method stub
		
	}

}
