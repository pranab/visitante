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
    private static final Logger LOG = Logger.getLogger(LifeCycleFinder.class);

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Finds lifecycles from visitation history";
        job.setJobName(jobName);
        
        job.setJarByClass(LifeCycleFinder.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        job.setMapperClass(TimeGapSequenceGenerator.TimeGapMapper.class);
        job.setReducerClass(LifeCycleFinder.FinderReducer.class);
        
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(Tuple.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(SecondarySort.TuplePairGroupComprator.class);
        job.setPartitionerClass(SecondarySort.TuplePairPartitioner.class);
        
        int numReducer = job.getConfiguration().getInt("lcf.num.reducer", -1);
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
		private String lifeCycleOutputType;
		private boolean outputLifeCycleLength;
		private String timeGapUnit;
		private int numIDFields;
		private int numAttributes;
		private long minInterCycleGap;
		private TemporalClusterFinder clusterFinder;
		private long startTime;
		private long endTime;
		private boolean useTimeHorizon;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }
			fieldDelim = config.get("field.delim.out", ",");
        	timeGapUnit = config.get("lcf.time.gap.unit", "day");
        	numIDFields = Utility.intArrayFromString(config.get("lcf.id.field.ordinals"), Utility.configDelim).length;
        	int[] attributes = Utility.intArrayFromString(config.get("lcf.quant.attr.list"), Utility.configDelim);
        	numAttributes = null != attributes ? attributes.length : 0;

        	//time horizon start and end
    		String dateFormatStr = config.get("lcf.time.horizon.date.format.str",  "yyyy-MM-dd HH:mm:ss");
    		SimpleDateFormat refDateFormat = new SimpleDateFormat(dateFormatStr);
    		try {
    			timeHorizonStart = Utility.getEpochTime(config.get("lcf.time.horizon.start"), false, refDateFormat, 0);
    			timeHorizonEnd = Utility.getEpochTime(config.get("lcf.time.horizon.end"), false, refDateFormat, 0);
    		} catch (ParseException ex) {
    			throw new IOException("parsing error with date time configuration parameter", ex);
    		}
    		
    		lifeCycleOutputType = config.get("lcf.life.cycle.output.type", "current");
    		outputLifeCycleLength = config.getBoolean("lcf.output.life.cycle.length", false);
    		
    		//min inter cycle time gap 
    		NumericalAttrStatsManager statsManager = new NumericalAttrStatsManager(config, "lcf.visit.time.gap.stats.file.path", 
    				fieldDelim); 
    		double timeGapMean = statsManager.getMean(1);
    		double timeGapStdDev = statsManager.getStdDev(1);
			double maxZscore = config.getFloat("clf.max.zscore", (float)3.0);
			minInterCycleGap = (long)(timeGapMean + maxZscore * timeGapStdDev);
			clusterFinder = new TemporalClusterFinder(timeHorizonStart, timeHorizonEnd, minInterCycleGap);
			
			useTimeHorizon = config.getBoolean("lcf.use.time.horizon.for.cluster", true);
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
    		
    		//find lifecycles
    		clusterFinder.initialize(visitTimeStamps);
    		List<TemporalCluster> clusters = clusterFinder.findClusters();
    		if (lifeCycleOutputType.equals("current")) {
	    		TemporalCluster lastCluster = clusters.get(clusters.size() - 1);
	    		if (!lastCluster.isEndFound()) {
	    			emitKey(key);
	    			startTime = lastCluster.getStartTime();
	    			endTime = useTimeHorizon ? timeHorizonEnd  : -1;
	    			if (outputLifeCycleLength) {
		    			emitTimeLength(startTime, endTime);
	    			} else {
	    				emitTimeBoundaries(startTime, endTime);
	    			}
	    			emit(context);  
	    		}
    		} else if (lifeCycleOutputType.equals("all")) {
    			for (TemporalCluster cluster : clusters) {
	    			emitKey(key);
    				startTime = cluster.isStartFound() ? cluster.getStartTime() : (useTimeHorizon ? timeHorizonStart  : -1);
    				endTime = cluster.isEndFound() ? cluster.getEndTime() : (useTimeHorizon ? timeHorizonEnd  : -1);
	    			if (outputLifeCycleLength) {
		    			emitTimeLength(startTime, endTime);
	    			} else {
	    				emitTimeBoundaries(startTime, endTime);
	    			}
	    			emit(context);  
    			}
    		} else if (lifeCycleOutputType.equals("complete")) {
    			for (TemporalCluster cluster : clusters) {
    				if (cluster.isStartFound() && cluster.isEndFound()) {
    	    			emitKey(key);
    	    			if (outputLifeCycleLength) {
    		    			emitTimeLength(cluster.getStartTime(), cluster.getEndTime());
    	    			} else {
    	    				emitTimeBoundaries(cluster.getStartTime(), cluster.getEndTime());
    	    			}
    	    			emit(context);   
    	    		}
    			}
    		} else {
    			throw new IllegalStateException("invalid lifecycle output type");
    		}
        }	
        
        /**
         * @param key
         */
        private void emitKey(Tuple  key) {
    		stBld.delete(0, stBld.length());
    		for (int i = 0; i < numIDFields; ++i) {
    			stBld.append(key.getString(i)).append(fieldDelim);
    		}
        }
        
        /**
         * @param startTime
         * @param endTime
         */
        private void emitTimeLength(long startTime, long endTime) {
			long timeLength = 0;
			if (startTime < 0 || endTime < 0) {
				timeLength = -1;
			} else {
				timeLength =  endTime - startTime;
				timeLength = Utility.convertTimeUnit(timeLength, timeGapUnit);
			}
			stBld.append(timeLength);
        }
        
        /**
         * @param startTime
         * @param endTime
         */
        private void emitTimeBoundaries(long startTime, long endTime) {
			stBld.append(Utility.convertTimeUnit(startTime, timeGapUnit)).append(fieldDelim).
				append(Utility.convertTimeUnit(endTime, timeGapUnit));
        }
        
        private void emit(Context context) throws IOException, InterruptedException {
        	outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
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
