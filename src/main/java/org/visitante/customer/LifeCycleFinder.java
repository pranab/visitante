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
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.chombo.util.NumericalAttrStatsManager;
import org.chombo.util.SecondarySort;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;
import org.hoidla.util.TemporalCluster;
import org.hoidla.util.TemporalClusterFinder;

/**
 * Finds lifecycles from visitation history
 * @author pranab
 *
 */
public class LifeCycleFinder extends Configured implements Tool {
	private static String configDelim = ",";
    private static final Logger LOG = Logger.getLogger(TransactionFrequencyRecencyValue.class);

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Average gap between transaction, average transaction value and time since most recent transaction";
        job.setJobName(jobName);
        
        job.setJarByClass(LifeCycleFinder.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "chombo");
        job.setMapperClass(TimeGapSequenceGenerator.TimeGapMapper.class);
        job.setReducerClass(LifeCycleFinder.FinderReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
        
        int numReducer = job.getConfiguration().getInt("clf.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);

        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	* @author pranab
	*
	*/
	public static class  FinderReducer extends Reducer<Tuple, Tuple, NullWritable, Text> {
		private Text outVal = new Text();
		private StringBuilder stBld =  new StringBuilder();;
		private String fieldDelim;
		private long timeHorizonStart;
		private long timeHorizonEnd;
		private List<Long> visitTimeStamps = new ArrayList<Long>();
		private String lifeCycleOutput;
		private String timeGapUnit;
		private int numIDFields;
		private int numAttributes;
		private long minInterCycleGap;
		private TemporalClusterFinder clusterFinder = new TemporalClusterFinder();
		
		
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
        	timeGapUnit = config.get("lcf.time.gap.unit");
        	numIDFields = Utility.intArrayFromString(config.get("lcf.id.field.ordinals"), configDelim).length;
        	int[] attributes = Utility.intArrayFromString(config.get("lcf.quant.attr.list"), Utility.configDelim);
        	numAttributes = null != attributes ? attributes.length : 0;

        	//time horizon start and end
    		String dateFormatStr = config.get("lcf.time.horizon.date.format.str",  "yyyy-MM-dd HH:mm:ss");
    		SimpleDateFormat refDateFormat = new SimpleDateFormat(dateFormatStr);
    		try {
    			timeHorizonStart = Utility.getEpochTime(config.get("lcf.time.horizon.start"), false, refDateFormat, 0);
    			timeHorizonEnd = Utility.getEpochTime(config.get("lcf.time.horizon.end"), false, refDateFormat, 0);
    		} catch (ParseException ex) {
    			throw new IOException("parsing error with date time field", ex);
    		}
    		
    		lifeCycleOutput = config.get("lcf.life.cycle.output", "current");
    		
    		//min inter cycle time gap 
    		NumericalAttrStatsManager statsManager = new NumericalAttrStatsManager(config, "lcf.visit.time.gap.stats.file.path", 
    				fieldDelim); 
    		double timeGapMean = statsManager.getMean(1);
    		double timeGapStdDev = statsManager.getStdDev(1);
			double maxZscore = config.getFloat("clf.max.zscore", (float)3.0);
			minInterCycleGap = (long)(timeGapMean + maxZscore * timeGapStdDev);
		}
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        protected void reduce(Tuple  key, Iterable<Tuple> values, Context context)
        		throws IOException, InterruptedException {
        	visitTimeStamps.clear();
    		for (Tuple val : values) {
    			visitTimeStamps.add(val.getLong(0));
    		}
    		
    		clusterFinder.initialize(visitTimeStamps, timeHorizonStart, timeHorizonEnd, minInterCycleGap);
    		List<TemporalCluster> clusters = clusterFinder.findClusters();
    		if (lifeCycleOutput.equals("current")) {
    			
    		} else if (lifeCycleOutput.equals("all")) {
    			
    		} else if (lifeCycleOutput.equals("complete")) {
    			
    		} else {
    			
    		}
        }		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new LifeCycleFinder(), args);
		System.exit(exitCode);
	}
	
}
