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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.chombo.mr.TimeGapSequenceGenerator;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Calculates average gap between transaction, average transaction value and time since
 * most recent transaction for every customer
 * @author pranab
 *
 */
public class TransactionFrequencyRecencyValue extends Configured implements Tool {
	private static String configDelim = ",";
    private static final Logger LOG = Logger.getLogger(TransactionFrequencyRecencyValue.class);

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Average gap between transaction, average transaction value and time since most recent transaction";
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
		private long minAvTimeGap;
		private long maxAvTimeGap;
		private double sumTimeGap;
		private double sumSqTimeGap;
		private long timeGapCount;
		private double avValue;
		private double minAvValue;
		private double maxAvValue;
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
            if (config.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }
			fieldDelim = config.get("field.delim.out", ",");
        	timeGapUnit = config.get("trf.time.gap.unit");
        	numIDFields = Utility.intArrayFromString(config.get("trf.id.field.ordinals"), configDelim).length;
        	numAttributes = Utility.assertIntArrayConfigParam(config, "trf.quant.attr.list", configDelim, 
        			"missing quant attribute list").length;
        	now = System.currentTimeMillis();
        	
        	minAvTimeGap = Long.MAX_VALUE;
        	maxAvTimeGap = Long.MIN_VALUE;

        	minAvValue = Double.MAX_VALUE;
        	maxAvValue = Double.MIN_VALUE;
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
            
            stBld.delete(0, stBld.length());
    		stBld.append("minAvTimeGap=").append(minAvTimeGap).append('\n');
    		stBld.append("maxAvTimeGap=").append(maxAvTimeGap).append('\n');
    		stBld.append("minAvValue=").append(minAvValue).append('\n');
    		stBld.append("maxAvValue=").append(maxAvValue).append('\n');
    		
    		double globalAvTimeGap = sumTimeGap / timeGapCount;
    		double variance = sumSqTimeGap / timeGapCount - globalAvTimeGap * globalAvTimeGap;
    		double globalStdDevTimeGap = Math.sqrt(variance);
    		stBld.append("globalAvTimeGap=").append(Utility.formatDouble(globalAvTimeGap, 3)).append('\n');
    		stBld.append("globalStdDevTimeGap=").append(Utility.formatDouble(globalStdDevTimeGap, 3)).append('\n');
    		
        	Configuration config = context.getConfiguration();
            Utility.writeToFile(config, "trf.xaction.stats.file.path", stBld.toString());
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
    		
    		//continue only if there are enough samples
    		if (xactionTimeGaps.size() >= 2) {
	    		//average time gap
	    		averageTimeGap();
	    		
	    		//average value
	    		averageValue();
	    		
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
    		} else {
    			LOG.debug("skipping not enough samples id:" + key.toString(0, numIDFields));
    			context.getCounter("Entity","Not enough samples ").increment(1);
    		}
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
        
        /**
         * 
         */
        private void averageTimeGap() {
    		gapSum = 0;
    		for (Long timeGap : xactionTimeGaps) {
    			gapSum += timeGap;
        		sumTimeGap += timeGap;
        		sumSqTimeGap += timeGap + timeGap;
        		++timeGapCount;
    		}
    		avTimeGap = gapSum / xactionTimeGaps.size();
    		minAvTimeGap = avTimeGap < minAvTimeGap ? avTimeGap : minAvTimeGap;
    		maxAvTimeGap = avTimeGap > maxAvTimeGap ? avTimeGap : maxAvTimeGap;
    		
        }
        
        /**
         * 
         */
        private void averageValue() {
    		valueSum = 0;
    		for (double xactValue : xactionValues) {
    			valueSum += xactValue;
    		}
    		avValue = valueSum / xactionValues.size();
    		minAvValue = avValue < minAvValue ? avValue : minAvValue;
    		maxAvValue = avValue > maxAvValue ? avValue : maxAvValue;
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
