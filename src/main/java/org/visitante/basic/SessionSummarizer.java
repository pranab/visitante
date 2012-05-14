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
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
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
 * Outputs session, userIID, num of pages, timespent, last page, flow navigation status
 * @author pranab
 *
 */
public class SessionSummarizer  extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "web log session summarizer  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(SessionSummarizer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        
        job.setMapperClass(SessionExtractor.SessionMapper.class);
        job.setReducerClass(SessionSummarizer.SessionReducer.class);

        job.setMapOutputKeyClass(TextLong.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SessionIdGroupComprator.class);
        job.setPartitionerClass(SessionIdPartitioner.class);

        job.setNumReduceTasks(job.getConfiguration().getInt("num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	public static class SessionReducer extends Reducer<TextLong, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
		private String sessionID;
		private String userID;
		private List<String> pages = new ArrayList<String>();
		private String[] flow;
		private int flowStat;
		private String lastPage;
		private long timeStart;
		private long timeEnd;
		private long timeSpent;
		private String referrer;
		private StringBuilder stBld = new  StringBuilder();
		private static final int FLOW_NOT_ENTERED = 0;
		private static final int FLOW_ENTERED = 1;
		private static final int FLOW_COMPLETED = 2;
		
		protected void setup(Context context) throws IOException, InterruptedException {
        	fieldDelim = context.getConfiguration().get("field.delim.out", "[]");
        	flow = context.getConfiguration().get("flow.sequence").split(",");
       }
		
    	protected void reduce(TextLong key, Iterable<Tuple> values, Context context)
        	throws IOException, InterruptedException {
    		sessionID = key.getFirst().toString();
    		boolean first = true;
    		pages.clear();
    		stBld.delete(0, stBld.length());
    		for (Tuple val : values) {
    			if (first) {
    				userID =  val.getString(0);
    				timeStart = val.getLong(2);
    				referrer = val.getString(4);
    				first = false;
    			} else {
    				timeEnd = val.getLong(2);
    			}
    			pages.add(val.getString(1));
    		}    
    		lastPage = pages.get(pages.size()-1);
    		checkFlowStatus();
    		timeSpent =  pages.size() > 1 ?   (timeEnd - timeStart) / 1000 : 0;
    		stBld.append(sessionID).append(fieldDelim).append(userID).append(fieldDelim).append(pages.size()).
    			append(fieldDelim).append(timeStart).append(fieldDelim).append(timeSpent).append(fieldDelim).
    			append(lastPage).append(fieldDelim).append(flowStat).append(fieldDelim).append(referrer);
    		
			//outVal.set(sessionID + fieldDelim  +  userID + fieldDelim + pages.size() +  fieldDelim + timeStart +  fieldDelim + timeSpent + 
			//		fieldDelim + lastPage + fieldDelim +  flowStat + fieldDelim + referrer );
    		outVal.set(stBld.toString());
			context.write(NullWritable.get(),outVal);
    	}
    	
    	private void checkFlowStatus() {
    		int j = 0;
    		flowStat = FLOW_NOT_ENTERED;
    		boolean matched = false;
    		
    		for (int i = 0; i < flow.length; ++i) {
    			while ( j < pages.size()) {
    				matched = false;
    				if (pages.get(j).startsWith(flow[i])) {
    					flowStat =  i == flow.length - 1 ?  FLOW_COMPLETED :  FLOW_ENTERED;
    					matched = true;
    				}
    				++j;
    				if (matched) {
    					break;
    				}
    			}
    		}
    	}
    	
    }	
	

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SessionSummarizer(), args);
        System.exit(exitCode);
    }
	
}
