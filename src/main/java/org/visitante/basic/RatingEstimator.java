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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import org.chombo.util.IntPair;
import org.chombo.util.TextPair;
import org.chombo.util.Utility;

/**
 * @author pranab
 *
 */
public class RatingEstimator extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "web log user item rating estimator  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(EngagementEventGenerator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        
        job.setMapperClass(RatingEstimator.EventMapper.class);
        job.setReducerClass(RatingEstimator.EventReducer.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntPair.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	public static class EventMapper extends Mapper<LongWritable, Text, TextPair, IntPair> {
		private String[] items;
		private TextPair outKey = new TextPair();
		private IntPair outVal = new IntPair();
        private String fieldDelimRegex;

        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelimRegex = context.getConfiguration().get("res.field.delim.regex", ",");
        }
        
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                items  =  value.toString().split(fieldDelimRegex);
                outKey.set(items[0],items[1]);
                outVal.set(new Integer(items[2]), new Integer(items[3]));
   	   			context.write(outKey, outVal);
        }  
        
	}
	
	public static class EventReducer extends Reducer<TextPair, IntPair, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
		private String userID;
		private String itemID;
		private int maxRating;
		private int outFormat;
		private int timeSpent;
		private int eventVal;
		private List<Pair<Integer, Integer>> timeSpentMapping = new ArrayList<Pair<Integer, Integer>>();
		
		protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim.out", ",");
        	outFormat = new Integer(context.getConfiguration().get("res.out.format", "1"));
        	
        	String[] maps = context.getConfiguration().get("res.time.spent.mapping").split(",");
        	ImmutablePair<Integer, Integer> entry = null;
        	for (String map : maps) {
        		String[] mapItems = map.split(":");
        		entry = new ImmutablePair<Integer, Integer>(new Integer(mapItems[0]), new Integer(mapItems[1]));
        		timeSpentMapping.add(entry);
        	}
       }
		
    	protected void reduce(TextPair key, Iterable<IntPair> values, Context context)
            	throws IOException, InterruptedException {
        		userID = key.getFirst().toString();
        		itemID = key.getSecond().toString();
        		maxRating = 0;
        		timeSpent = 0;
        		for (IntPair val : values) {
        			eventVal =  val.getSecond().get();
        			if (val.getFirst().get() == 1) {
        				//browse engagement
        				timeSpent += eventVal; 
        			} else {
        				//more sticky engagement
        				if (eventVal > maxRating) {
        					maxRating = eventVal;
        				}
        			}
        		}
        		
    			//convert time spent to rating
        		if (maxRating == 0) {
        			for (Pair<Integer, Integer> entry  : timeSpentMapping) {
        				if (entry.getLeft() > timeSpent) {
        					maxRating = entry.getRight();
        					break;
        				}
        			}
        		}
        		
        		if (maxRating == 0) {
        			//off the chart
        			maxRating = timeSpentMapping.get(timeSpentMapping.size() - 1).getRight() + 1;
        		}
        		
        		switch (outFormat) {
	        		case 1 :
	        			outVal.set(userID + fieldDelim + itemID + fieldDelim + maxRating);
	        			break;
	        		case 2 :
	        			outVal.set(itemID + fieldDelim + userID + fieldDelim + maxRating);
	        			break;
        		}
				context.write(NullWritable.get(), outVal);
    	}

	}	
	
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RatingEstimator(), args);
        System.exit(exitCode);
    }
	
}
