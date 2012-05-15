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
import org.chombo.util.TextLong;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.visitante.basic.SessionExtractor.SessionIdGroupComprator;
import org.visitante.basic.SessionExtractor.SessionIdPartitioner;

public class UserSessionSummary extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "web log user session summary  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(UserSessionSummary.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        
        job.setMapperClass(UserSessionSummary.SessionMapper.class);
        job.setReducerClass(UserSessionSummary.SessionReducer.class);

        job.setMapOutputKeyClass(TextLong.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SessionExtractor.SessionIdGroupComprator.class);
        job.setPartitionerClass(SessionExtractor.SessionIdPartitioner.class);

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	public static class SessionMapper extends Mapper<LongWritable, Text, TextLong, Tuple> {
		private String[] items;
		private TextLong outKey = new TextLong();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String userID;
        private Long timeStart;
        
        protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelimRegex = context.getConfiguration().get("us.field.delim.regex", ",");
        }
 
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            userID = items[1];
            timeStart = Long.parseLong(items[3]);
			outKey.set(userID, timeStart);
            
			outVal.initialize();
			outVal.add( items[0], new Integer(items[2]), new Integer( items[4]),  items[7], new Integer(items[6]));
	   		context.write(outKey, outVal);
       }

	}
	
	public static class SessionReducer extends Reducer<TextLong, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
		private String userID;
		private String referrer;
		private int totalPages;
		private int totalTime;
		private int status;
		private int count;
		private int avNumPages;
		private StringBuilder stBld = new  StringBuilder();
		private static final int FLOW_COMPLETED = 2;
		
		protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim.out", ",");
       }

    	protected void reduce(TextLong key, Iterable<Tuple> values, Context context)
            	throws IOException, InterruptedException {
        		userID = key.getFirst().toString();
        		boolean first = true;
        		totalPages = 0;
        		totalTime = 0;
        		count = 0;
        		stBld.delete(0, stBld.length());
        		for (Tuple val : values) {
        			if (first) {
        				referrer =  val.getString(3);
        				first = false;
        			}
        			
        			totalPages += val.getInt(1);
        			totalTime  += val.getInt(2);
        			++count;
        			status = val.getInt(4);
        			if (status == FLOW_COMPLETED ) {
        				break;
        			}
        		}  
        		
        		status = status == FLOW_COMPLETED ? 1 : 0;
        		avNumPages = totalPages / count;
        		stBld.append(userID).append(fieldDelim).append(referrer).append(fieldDelim).append(count).append(fieldDelim).
        			append(avNumPages).append(fieldDelim).append(status);
        		
        		outVal.set(stBld.toString());
    			context.write(NullWritable.get(),outVal);
    	}	
    	
	}

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new UserSessionSummary(), args);
        System.exit(exitCode);
    }
	
}
