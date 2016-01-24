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

package org.visitante.customer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.chombo.mr.TimeGapSequenceGenerator;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Calculates avearge gap between transaction, average transaction value and time since
 * most recent transaction.
 * @author pranab
 *
 */
public class TransactionFrequencyRecencyValue extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Time sequence to time gap sequence conversion";
        job.setJobName(jobName);
        
        job.setJarByClass(TransactionFrequencyRecencyValue.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(TimeGapSequenceGenerator.TimeGapMapper.class);
        job.setReducerClass(TransactionFrequencyRecencyValue.TransactionReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
        
        int numReducer = job.getConfiguration().getInt("tfr.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	* @author pranab
	*
	*/
	public static class  TransactionReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		protected Text outVal = new Text();
		protected StringBuilder stBld =  new StringBuilder();;
		protected String fieldDelim;
		private String timeGapUnit;
		private int numIDFields;
		private int numAttributes;
		private long timeGap;
		private long now;
		private List<Long> xactionTimeGaps = new ArrayList<Long>();
		private List<Double> xactionValues = new ArrayList<Double>();
		private long avTimeGap;
		private double avValue;
		private long recency;
		private long gapSum;
		private double valueSum;
		private static final long MS_PER_HOUR = 60L * 1000 * 1000;
		private static final long MS_PER_DAY = MS_PER_HOUR * 24;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			fieldDelim = config.get("field.delim.out", ",");
        	timeGapUnit = config.get("trf.time.gap.unit");
        	numIDFields = Utility.intArrayFromString(config.get("trf.id.field.ordinals"), configDelim).length;
        	numAttributes = Utility.assertIntArrayConfigParam(config, "trf.quant.attr.list", configDelim, 
        			"missing quant attribute list").length;
        	now = System.currentTimeMillis();
		}

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
    		long lastTimeStamp = -1;
    		xactionTimeGaps.clear();
    		xactionValues.clear();
    		for (Tuple val : values) {
    			if (lastTimeStamp > 0) {
    				timeGap = val.getLong(0) - lastTimeStamp;
    				timeGap = convertTimeUnit(timeGap);
    				xactionTimeGaps.add(timeGap);
    			}
    			lastTimeStamp = val.getLong(0);
    			xactionValues.add(Double.parseDouble(val.getString(1)));
    		}
    		
    		//average time gap
    		gapSum = 0;
    		for (Long timeGap : xactionTimeGaps) {
    			gapSum += timeGap;
    		}
    		avTimeGap = gapSum / xactionTimeGaps.size();
    		
    		//average value
    		valueSum = 0;
    		for (double xactValue : xactionValues) {
    			valueSum += xactValue;
    		}
    		avValue = valueSum / xactionValues.size();
    		
    		//recency
    		recency = now - lastTimeStamp;
    		recency = convertTimeUnit(recency);
    		
    		stBld.delete(0, stBld.length());
    		for (int i = 0; i < numIDFields; ++i) {
    			stBld.append(key.getString(i)).append(fieldDelim);
    		}
    		stBld.append(avTimeGap).append(fieldDelim);
    		stBld.append(recency).append(fieldDelim);
    		stBld.append(Utility.formatDouble(avValue, 2));
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
        }
        
        /**
         * @param time
         * @return
         */
        private long convertTimeUnit(long time) {
        	long modTime = time;
			if (timeGapUnit.equals("hour")) {
				modTime /= MS_PER_HOUR;
			} else if (timeGapUnit.equals("day")) {
				modTime /= MS_PER_DAY;
			}
        	return modTime;
        }
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TransactionFrequencyRecencyValue(), args);
		System.exit(exitCode);
	}
	
}
