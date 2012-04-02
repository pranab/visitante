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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.visitante.basic;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.mr.NumericSorter;
import org.chombo.util.TextLong;
import org.chombo.util.Utility;


public class SessionExtractor extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "web log session extraction  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(NumericSorter.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration());
        
        job.setMapperClass(SessionExtractor.SessionMapper.class);
        job.setReducerClass(SessionExtractor.SessionReducer.class);

        job.setMapOutputKeyClass(TextLong.class);
        job.setMapOutputValueClass(TextLong.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SessionIdGroupComprator.class);
        job.setPartitionerClass(SessionIdPartitioner.class);

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	public static class SessionMapper extends Mapper<LongWritable, Text, TextLong, TextLong> {
		private String[] items;
		private TextLong outKey = new TextLong();
		private TextLong outVal = new TextLong();
        private String fieldDelimRegex;
        private Map<String, String> filedMetaData;
        private static final String itemDelim = ",";
        private static final String keyDelim = ":";
        private int cookieOrd;
        private   int dateOrd;
        private  int timeOrd;
        private  int urlOrd;
        private SimpleDateFormat dateFormat;
        private Date date;
        private long timeStamp;
        private String sessionIDName;
        private String cookie;
        private String sessionID;
        private String[] cookieItems;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelimRegex = context.getConfiguration().get("field.delim.regex", "\\[\\]");
        	String fieldMetaSt = context.getConfiguration().get("field.meta");
        	
        	filedMetaData=Utility.deserializeMap(fieldMetaSt, itemDelim, keyDelim);
           cookieOrd =new Integer(filedMetaData.get("sessionID"));
            dateOrd =new Integer(filedMetaData.get("date"));
            timeOrd =new Integer(filedMetaData.get("time"));
            urlOrd=new Integer(filedMetaData.get("url"));
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sessionIDName = context.getConfiguration().get("session.id.name");
       }

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            try {
				date = dateFormat.parse(items[dateOrd] + " " + items[timeOrd]);
				timeStamp = date.getTime();
				 getSessionID();
				outKey.set(sessionID, timeStamp);
				outVal.set(items[urlOrd], timeStamp);
   	   			context.write(outKey, outVal);
			} catch (ParseException ex) {
				throw new IOException("Failed to parse date time", ex);
			}
        	
        }
        
        private void  getSessionID() {
        	cookie = items[cookieOrd];
        	cookieItems = cookie.split("\\+");
        	for (String item :  cookieItems) {
        		if (item.startsWith(sessionIDName)) {
        			sessionID = item.split("=")[1];
        			break;
        		}
        	}
        }
        
    }	
	
	public static class SessionReducer extends Reducer<TextLong, TextLong, NullWritable, Text> {
		private Text outVal = new Text();
		private StringBuilder stBld;
		private String fieldDelim;
		private String sessionID;
		private long lastTimeStamp;
		private long timeStamp;
		private String lastUrl;
		private long timeOnPage;

		protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim", "[]");
       }
		
    	protected void reduce(TextLong key, Iterable<TextLong> values, Context context)
        	throws IOException, InterruptedException {
    		sessionID = key.getFirst().toString();
    		boolean first = true;
    		for (TextLong val : values) {
    			if (first) {
    				lastUrl = val.getFirst().toString();
    				lastTimeStamp = val.getSecond().get();
    				first = false;
    			} else {
    				timeStamp = val.getSecond().get();
    				timeOnPage = timeStamp - lastTimeStamp;
    				outVal.set( sessionID + fieldDelim + lastUrl + fieldDelim +  timeOnPage);
    				context.write(NullWritable.get(),outVal);
    				
    				lastUrl = val.getFirst().toString();
    				lastTimeStamp = timeStamp;
    			}
    		}
    	}	
	 }
	   
    public static class SessionIdPartitioner extends Partitioner<TextLong, TextLong> {
	     @Override
	     public int getPartition(TextLong key, TextLong value, int numPartitions) {
	    	 //consider only base part of  key
		     return key.baseHashCode()% numPartitions;
	     }
    }
    
    public static class SessionIdGroupComprator extends WritableComparator {
    	protected SessionIdGroupComprator() {
    		super(TextLong.class, true);
    	}

    	@Override
    	public int compare(WritableComparable w1, WritableComparable w2) {
    		//consider only the base part of the key
    		TextLong t1 = ((TextLong)w1);
    		TextLong t2 = ((TextLong)w2);
    		return t1.baseCompareTo(t2);
    	}
     }
    
	
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SessionExtractor(), args);
        System.exit(exitCode);
    }

}
