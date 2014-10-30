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

import java.util.Map;

import org.chombo.storm.GenericSpout;
import org.chombo.storm.MessageHolder;
import org.chombo.storm.MessageQueue;
import org.chombo.util.ConfigUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;

/**
 * Spout for cardinality estimation with probabilistic algorithm
 * @author pranab
 *
 */
public class UniqueVisitorSpout extends GenericSpout {
	private int partOrd;
	private int userIdOrd;
	private MessageQueue msgQueue;
	private static final Logger LOG = LoggerFactory.getLogger(UniqueVisitorSpout.class);
	
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
		String logQueue = ConfigUtility.getString(stormConf, "input.log.queue");
		msgQueue = MessageQueue.createMessageQueue(stormConf, logQueue);
		partOrd = ConfigUtility.getInt(stormConf, "partition.ordinal");
		userIdOrd = ConfigUtility.getInt(stormConf, "user.id.ordinal");
	}

	@Override
	public MessageHolder nextSpoutMessage() {
		MessageHolder msgHolder = null;
		String message  = msgQueue.receive();		
		if(null != message) {
			//message in event queue
			String[] items = message.split("\\s+");
			String partition = items[partOrd];
			String userId = items[userIdOrd];
			Values values = new Values(partition, userId);
			msgHolder = new  MessageHolder(values);
		}
		return msgHolder;
	}

	@Override
	public void handleFailedMessage(Values tuple) {
		// TODO Auto-generated method stub
	}

}
