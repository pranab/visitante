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
import java.util.Calendar;
import java.util.GregorianCalendar;

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
 * Calculates acquisition rate, loss rate and retention rate for some time unit
 * e.g. day, month or year
 * @author pranab
 *
 */
public class RetentionRate extends Configured implements Tool {
	private static final int ACQ_SUB_KEY = 0;
	private static final int LOSS_SUB_KEY = 1;
    private static final Logger LOG = Logger.getLogger(RetentionRate.class);
    

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Retention rate calculation";
        job.setJobName(jobName);
        
        job.setJarByClass(RetentionRate.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(RetentionRate.RateMapper.class);
        job.setReducerClass(RetentionRate.RateReducer.class);
        job.setCombinerClass(RetentionRate.RateCombiner.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
        
        int numReducer = job.getConfiguration().getInt("rer.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class RateMapper extends Mapper<LongWritable, Text, Tuple, Tuple> {
		private Tuple outKey = new Tuple();
		private Tuple outVal = new Tuple();
        private String fieldDelimRegex;
        private String[] items;
        private int[] idOrdinals;
        private int timeZoneShiftHours;
        private String timeUnit;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = Utility.getFieldDelimiter(config, "rer.field.delim.regex", "field.delim.regex", ",");
        	idOrdinals = Utility.intArrayFromString(config.get("rer.id.field.ordinals"), Utility.configDelim);
        	timeUnit = config.get("rer.rate.time.unit", "month");
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void map(LongWritable key, Text value, Context context)
        		throws IOException, InterruptedException {
        	items  =  value.toString().split(fieldDelimRegex);
        	
        	for (int i = ACQ_SUB_KEY; i < 2; ++i) {
        		long cycleBondary = Long.parseLong(items[i + idOrdinals.length]);
        		if (cycleBondary > 0) {
        			outKey.initialize();
        			outVal.initialize();
        			
    				Calendar calendar = new GregorianCalendar();
    				calendar.setTimeInMillis(cycleBondary);
        			if (timeUnit.equals("year")) {
        				outKey.add(calendar.get(Calendar.YEAR));
        			} else if (timeUnit.equals("month")) {
        				outKey.add(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH));
        			} else if (timeUnit.equals("day")) {
        				outKey.add(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH));
        			} else {
        				throw new IllegalStateException("invalid time unit");
        			}
        			outKey.append(i);
        			
        			outVal.add(i, 1);
        			context.write(outKey, outVal);
        		}
        	}
        }        
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class RateCombiner extends Reducer<Tuple, Tuple, Tuple, Tuple> {
		private Tuple outVal = new Tuple();
		private int numIDFields;
		private int[] counts = new int[2];
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }
        	numIDFields = Utility.intArrayFromString(config.get("lcf.id.field.ordinals"), Utility.configDelim).length;
		}
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	counts[ACQ_SUB_KEY] = counts[LOSS_SUB_KEY] = 0;
    		for (Tuple val : values) {
    			if (val.getInt(0) == ACQ_SUB_KEY) {
    				++counts[ACQ_SUB_KEY];
    			} else {
    				++counts[LOSS_SUB_KEY];
    			}
    		}
    		for (int i = ACQ_SUB_KEY; i < 2; ++i) {
    			key.set(numIDFields, i);
    			outVal.initialize();
    			outVal.add(i, counts[i]);
    			context.write(key, outVal);
    		}
        }
	}
	
	/**
	* @author pranab
	*
	*/
	public static class  RateReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private StringBuilder stBld =  new StringBuilder();;
		private String fieldDelim;
		private int numIDFields;
		private int[] counts = new int[2];
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }
			fieldDelim = config.get("field.delim.out", ",");
        	numIDFields = Utility.intArrayFromString(config.get("lcf.id.field.ordinals"), Utility.configDelim).length;
		}
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	counts[ACQ_SUB_KEY] = counts[LOSS_SUB_KEY] = 0;
    		for (Tuple val : values) {
    			if (val.getInt(0) == ACQ_SUB_KEY) {
    				++counts[ACQ_SUB_KEY];
    			} else {
    				++counts[LOSS_SUB_KEY];
    			}
    		}
    		stBld.delete(0, stBld.length());
    		stBld.append(key.toString(0, numIDFields)).append(fieldDelim).append(counts[ACQ_SUB_KEY]).
    			append(fieldDelim).append(counts[LOSS_SUB_KEY]).append(fieldDelim).append(counts[ACQ_SUB_KEY] - counts[LOSS_SUB_KEY]);
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
        }		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new RetentionRate(), args);
		System.exit(exitCode);
	}
	
}
