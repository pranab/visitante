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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Detects events based on patterns and generates output with surrounding context
 * @author pranab
 *
 */
public class SessionEventDetector  extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(SessionEventDetector.class);

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "web log   event detector  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(SessionEventDetector.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        
        boolean withSession = job.getConfiguration().getBoolean("with.session", true);
        if (withSession) {
        	//log with session e.g. web server log
	        job.setMapperClass(SessionEventDetector.SessionEventMapper.class);
	        job.setReducerClass(SessionEventDetector.SessionEventReducer.class);
	
	        job.setMapOutputKeyClass(Tuple.class);
	        job.setMapOutputValueClass(Text.class);
	
	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);
	
	        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
	        job.setPartitionerClass(SecondarySort.TupleTextPartitioner.class);
	
	        job.setNumReduceTasks(job.getConfiguration().getInt("sed.num.reducer", 1));
        } else {
        	//without session
	        job.setMapperClass(SessionEventDetector.EventMapper.class);
	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);
	        job.setNumReduceTasks(0);
        }
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class EventMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private int beforeMatchContextSize;
		private int afterMatchContextSize;
		private Map<String, Pattern> patterns = new HashMap<String, Pattern>();
		private List<String> records = new ArrayList<String>();
		private int maxBufferSize;
		private String record;
		
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }

        	beforeMatchContextSize = config.getInt("sed.before.match.context.size", 3);
        	afterMatchContextSize = config.getInt("sed.after.match.context.size", 2);
        	maxBufferSize = beforeMatchContextSize + afterMatchContextSize + 1;
        	
        	//patterns
        	buildPatterns(config, patterns);
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	records.add(value.toString());
        	if (records.size() > maxBufferSize) {
        		records.remove(0);
        	}
        	
           	if (records.size() == maxBufferSize) {
           		record = records.get(beforeMatchContextSize);
           		
           		//all patterns
        		for (String patternName : patterns.keySet()) {
    				if (patterns.get(patternName).matcher(record).matches()) {
    					outVal.set(" ");
						context.write(NullWritable.get(),outVal);
						outVal.set("event: " + patternName);
						context.write(NullWritable.get(),outVal);

						for (String rec : records) {
	    					outVal.set(rec);
							context.write(NullWritable.get(),outVal);
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
	public static class SessionEventMapper extends Mapper<LongWritable, Text, Tuple, Text> {
		private String[] items;
		private Tuple outKey = new Tuple();
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private Map<String, String> filedMetaData;
        private static final String itemDelim = ",";
        private static final String keyDelim = ":";
        private int cookieOrd;
        private   int dateOrd;
        private  int timeOrd;
        private SimpleDateFormat dateFormat;
        private Date date;
        private Long timeStamp;
        private String sessionIDName;
        private String userIDName;
        private String cookie;
        private String sessionID;
        private String userID;
        private String[] cookieItems;
        private String cookieSeparator;
        private Long minDateTime;
        private Long maxDateTime;
        private boolean toEmit;
        private List<String> lines = new ArrayList<String>();
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = config.get("field.delim.regex", "\\s+");
        	String fieldMetaSt = config.get("sed.field.meta");
        	System.out.println("fieldMetaSt:" + fieldMetaSt);
        	
        	filedMetaData=Utility.deserializeMap(fieldMetaSt, itemDelim, keyDelim);
        	cookieOrd =new Integer(filedMetaData.get("cookie"));
            dateOrd =new Integer(filedMetaData.get("date"));
            timeOrd =new Integer(filedMetaData.get("time"));
            String dateFormatStr = config.get("sed.date.format.str",  "yyyy-MM-dd HH:mm:ss");
            dateFormat = new SimpleDateFormat(dateFormatStr);
            sessionIDName = config.get("sed.session.id.name");
            userIDName = config.get("sed.user.id.name");
            cookieSeparator = config.get("sed.cookie.separator", ";\\+");
            
            //date time constraint
            try {
	            String minDateTimeStr = config.get("sed.min.date.time");
	            if (null != minDateTimeStr) {
	            	minDateTime = dateFormat.parse(minDateTimeStr).getTime();
	            }
            
	            String maxDateTimeStr = config.get("sed.max.date.time");
	            if (null != maxDateTimeStr) {
	            	maxDateTime = dateFormat.parse(maxDateTimeStr).getTime();
	            }
            } catch (ParseException ex) {
				throw new IOException("Failed to parse date time", ex);
			}
       }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            try {
				date = dateFormat.parse(items[dateOrd] + " " + items[timeOrd]);
				
				//new record
				if (!lines.isEmpty()) {
					//emit previous
					outKey.initialize();
					outKey.add(sessionID, timeStamp);
				
					outVal.set(Utility.join(lines, "\\n"));
   	   				context.write(outKey, outVal);
   	   				lines.clear();
				}
				
				timeStamp = date.getTime();
				toEmit = (null == minDateTime || timeStamp > minDateTime) && 
						(null == maxDateTime || timeStamp < maxDateTime);
				if (toEmit) {
					lines.add(value.toString());
					getSessionID();
				}
			} catch (ParseException ex) {
				//continuation line
				if (toEmit) {
					lines.add(value.toString());
				}
			}
        }
        
        /**
         * 
         */
        private void  getSessionID() {
        	cookie = items[cookieOrd];
        	cookieItems = cookie.split(cookieSeparator);
        	for (String item :  cookieItems) {
        		if (item.startsWith(sessionIDName)) {
        			sessionID = item.split("=")[1];
        		}
        		if (item.startsWith(userIDName)) {
        			userID = item.split("=")[1];
        		}
        	}
        }
        
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class SessionEventReducer extends Reducer<Tuple, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
		private int beforeMatchContextSize;
		private int afterMatchContextSize;
		private Map<String, Pattern> patterns = new HashMap<String, Pattern>();
		private List<String> records = new ArrayList<String>();
		private Matcher matcher;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }
            fieldDelim = config.get("field.delim.out", ",");
        	beforeMatchContextSize = config.getInt("sed.before.match.context.size", 3);
        	afterMatchContextSize = config.getInt("sed.after.match.context.size", 2);
        	
        	//patterns
        	buildPatterns(config, patterns);
       }
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		records.clear();
    		for (Text value : values) {
    			records.add(value.toString());
    		}
    		LOG.debug("number of logs in session:" + records.size());
       		
    		//try matching all patterns
    		for (String patternName : patterns.keySet()) {
    			int i = 0;
    			//all records
    			for (String record : records) {
    				matcher = patterns.get(patternName).matcher(record);
    				if (matcher.find()) {
    					int beg = i - beforeMatchContextSize;
    					beg = beg < 0 ? 0 : beg;
    					int end =  i + afterMatchContextSize;
    					end = end > records.size() -1 ? records.size() -1 : end;
    					LOG.debug("matched beg:" + beg + " end:" + end);
    					
    					outVal.set(" ");
						context.write(NullWritable.get(),outVal);
						outVal.set("event: " + patternName);
						context.write(NullWritable.get(),outVal);
						for (int j = beg;  j <= end; ++j) {
    						outVal.set(records.get(j));
    						context.write(NullWritable.get(),outVal);
						}
    				}
    				++i;
    			}
    		}
    	}
    	
    	
	}
	
	/**
	 * @param config
	 * @param patterns
	 */
	public static void buildPatterns(Configuration config, Map<String, Pattern> patterns ) {
    	//patterns
    	String key = null;
    	String name = null;
    	String regex = null;
    	for (int i = 1;  ; ++i) {
    		key = "event.pattern." + i + ".name";
    		name = config.get(key);
    		if (null == name) {
    			break;
    		}
    		key = "event.pattern." + i + ".regex";
    		regex = config.get(key);
    		patterns.put(name, Pattern.compile(regex));
    	}
   }
	
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SessionEventDetector(), args);
        System.exit(exitCode);
    }
	
}
