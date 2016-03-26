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

package org.visitante.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.TextLong;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.visitante.basic.SessionExtractor.SessionIdGroupComprator;
import org.visitante.basic.SessionExtractor.SessionIdPartitioner;

/**
 * Extract user engagement events from web logs with pattern matching
 * @author pranab
 *
 */
public class EngagementEventGenerator extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "web log user engaement event generator  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(EngagementEventGenerator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        
        job.setMapperClass(SessionExtractor.SessionMapper.class);
        job.setReducerClass(EngagementEventGenerator.SessionReducer.class);

        job.setMapOutputKeyClass(TextLong.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SessionIdGroupComprator.class);
        job.setPartitionerClass(SessionIdPartitioner.class);

        job.setNumReduceTasks(job.getConfiguration().getInt("ee.num.reducer", 1));
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class SessionReducer extends Reducer<TextLong, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
		private String userID;
		private String lastUrl;
		private long timeOnPage;
		private long timeStamp;
		private long lastTimeStamp;
		private Map<String, EngagementEvent> events = new HashMap<String, EngagementEvent>();
		private List<EventPattern> eventPatterns = new ArrayList<EventPattern>();
		private int userIDOrd;
		private int urlOrd;
		private int timeStampOrd;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
        	fieldDelim = config.get("field.delim.out", ",");
        	
        	//engaement events e.g. page browsed
        	String[] engageEvents = config.get("eeg.engagement.events").split(",");
        	for (String engageEvent : engageEvents) {
        		String[] items = engageEvent.split(":");
        		eventPatterns.add(new EventPattern(items));
        	}
        	userIDOrd = config.getInt("eeg.user.id.ord", 0);
        	urlOrd = config.getInt("eeg.url.ord", 1);
        	timeStampOrd = config.getInt("eeg.time.stamp.ord", 2);
       }
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(TextLong key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		events.clear();
    		boolean first = true;
    		for (Tuple val : values) {
    			if (first) {
    				userID =  val.getString(userIDOrd);
    				lastUrl = val.getString(urlOrd);
    				lastTimeStamp = val.getLong(timeStampOrd);
    				first = false;
    			} else {
    				timeStamp =  val.getLong(timeStampOrd);
    				timeOnPage = (timeStamp - lastTimeStamp) / 1000;
    				eventFromUrl();
       			    				
    				lastUrl =  val.getString(1);
    				lastTimeStamp = timeStamp;
    			}
    		}
			timeOnPage = 0;
			eventFromUrl();

			//emit all events
			EngagementEvent event = null;
			for (String itemID :  events.keySet()) {
				event = events.get(itemID);
				outVal.set(userID + fieldDelim + itemID + fieldDelim +  event.eventID + fieldDelim + event.value);
				context.write(NullWritable.get(),outVal);
			}			
    	}
    	
    	/**
    	 * extracts events from log line
    	 */
    	private void eventFromUrl() {
    		EngagementEvent event = null;
    		int eventID = 0;
    		int value = 0;
    		String item  = null;
    		
    		for (EventPattern eventPattern : eventPatterns) {
    			if (eventPattern. isMatched(lastUrl)) {
    				eventID = eventPattern.getEventID();
    				value = eventPattern.getValue();
    				item = eventPattern.getMatchedItem();
    				if (null != item) {
    					event = events.get(item);
    					if (null != event) {
    						//event already exists for this item
    						if(event.eventID < eventID) {
        						//more engaging event found for the item
    							event.eventID = eventID;
    							event.value = value;
    						}
    					} else {
    						//first event for this item
    						event = new  EngagementEvent();
   							event.eventID = eventID;
							event.value = value;
   							if (event.value == 0) {
								event.value =(int) timeOnPage;
							}
							events.put(item, event);
    					}
    				} else {
    					//transactional event e.g. entered checkout , completed checkoput
    					for (Map.Entry<String, EngagementEvent>  entry :  events.entrySet()) {
    						event = entry.getValue();
    						if (event.eventID == eventID -1) {
    							//promote to more engaging event
    							event.eventID = eventID;
    							event.value = value;
    						}
    					}
    					
    				}
    			}
    		}
    	}
    	
	}

	/**
	 * @author pranab
	 *
	 */
	private static class EngagementEvent {
		public int eventID;
		public int value;
	}
	
	/**
	 * @author pranab
	 *
	 */
	private static class EventPattern {
		private int eventID;
		private Pattern pattern;
		private String matchedItem;
		private int value;
		private boolean matched;
		
		/**
		 * @param items
		 */
		public EventPattern(String[] items) {
			pattern = Pattern.compile(items[0]);
			eventID = new Integer(items[1]);
			if (items.length == 3) {
				value = new Integer(items[2]);
			}
		}
		
		/**
		 * @param data
		 * @return
		 */
		public boolean isMatched(String data) {
			matchedItem = null;
			matched = false;
			Matcher matcher = pattern.matcher(data);
			matched = matcher.find();
			if ( matched) {
			    for (int  i= 1;  i <= matcher.groupCount();  i++) {
			    	matchedItem = matcher.group(i);
			    }
			}
			return matched;
		}

		/**
		 * @return
		 */
		public String getMatchedItem() {
			return matchedItem;
		}

		/**
		 * @return
		 */
		public int getEventID() {
			return eventID;
		}

		/**
		 * @return
		 */
		public int getValue() {
			return value;
		}
		
	}
	
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new EngagementEventGenerator(), args);
        System.exit(exitCode);
    }

}