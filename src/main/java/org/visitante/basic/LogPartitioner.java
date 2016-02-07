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
 * Partitions log data by some entity value (e.g. session) by some time unit (e.g day, hour)
 * @author pranab
 *
 */
public class LogPartitioner extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(LogPartitioner.class);

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "web log   event detector  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(LogPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        
        //log with session e.g. web server log
	    job.setMapperClass(LogPartitioner.PartitionMapper.class);
	    job.setReducerClass(LogPartitioner.PartitionReducer.class);
	
	    job.setMapOutputKeyClass(Tuple.class);
	    job.setMapOutputValueClass(Text.class);
	
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	
	    job.setNumReduceTasks(job.getConfiguration().getInt("lop.num.reducer", 1));
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class PartitionMapper extends Mapper<LongWritable, Text, Tuple, Text> {
		private Tuple outKey = new Tuple();
        private SimpleDateFormat dateFormat;
        private Long minDateTime;
        private Long maxDateTime;
        private boolean toEmit;
        private String record;
        private Pattern dateTimePattern;
        private boolean partitionByDateTime;
        private String dateTimePartUnit;
        private Pattern partEntityPattern;
        private Matcher matcher;
        private long dateTime;
        private long timePart;
        private String dateTimeStr;
        private String partEntityStr;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            
        	//date time pattern
        	String dateTimePatternStr = Utility.assertStringConfigParam(config, "lop.date.time.pattern", 
        			"missing date time regex");
            dateTimePattern = Pattern.compile(dateTimePatternStr);
            
            //date time format
        	String dateFormatStr = Utility.assertStringConfigParam(config, "lop.date.format.str", 
        			"missing date format string");
            dateFormat = new SimpleDateFormat(dateFormatStr);
            
            partitionByDateTime = config.getBoolean("lop.part.by.date.time", false);
            if (partitionByDateTime) {
            	dateTimePartUnit = config.get("lop.date.time.part.unit", "hour");
            } else {
            	String partEntityPatternStr = Utility.assertStringConfigParam(config, "lop.part.entity.pattern", 
            			"missing paritioning entity regex");
                partEntityPattern = Pattern.compile(partEntityPatternStr);
            }
            
            //date time constraint
            try {
	            String minDateTimeStr = config.get("lop.min.date.time");
	            if (null != minDateTimeStr) {
	            	minDateTime = dateFormat.parse(minDateTimeStr).getTime();
	            }
            
	            String maxDateTimeStr = config.get("lop.max.date.time");
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
            record  = value.toString();
            outKey.initialize();
            
            try {
	        	matcher = dateTimePattern.matcher(record);
	        	if (matcher.find()) {
	        		dateTimeStr = matcher.group(1);
	        		dateTime = dateFormat.parse(dateTimeStr).getTime();
	        	} else {
	        		throw new IOException("date time pattern not matched");
	        	}
	            
				toEmit = (null == minDateTime || dateTime > minDateTime) && 
						(null == maxDateTime || dateTime < maxDateTime);
	        	if (toEmit) {
		            if (partitionByDateTime) {
		            	if (dateTimePartUnit.equals("hour")) {
		            		timePart = dateTime / Utility.MILISEC_PER_HOUR;
		            	} else if (dateTimePartUnit.equals("day")) {
		            		timePart = dateTime / Utility.MILISEC_PER_DAY;
		            	} else {
		            		throw new IllegalArgumentException("invalid partitioning time unit");
		            	}
		            	outKey.append(Utility.formatLong(timePart, 12));
		            } else {
			        	matcher = partEntityPattern.matcher(record);
			        	if (matcher.find()) {
			        		partEntityStr = matcher.group(1);
			        	} else {
			        		throw new IOException("partitoning entity pattern not matched");
			        	}
			        	outKey.append(partEntityStr);
		            }
		            outKey.append(dateTime);
		            context.write(outKey, value);
	        	}
            } catch (ParseException ex) {
        		throw new IOException("Failed to parse date time", ex);
            }
        }
        
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class PartitionReducer extends Reducer<Tuple, Text, NullWritable, Text> {
		private Text outVal = new Text();
		private String fieldDelim;
		private String partition;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
            if (config.getBoolean("debug.on", false)) {
            	LOG.setLevel(Level.DEBUG);
            }
            fieldDelim = config.get("field.delim.out", ",");
       }
		
    	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		partition = key.getString(0);
    		for (Text value : values) {
    			outVal.set(partition + fieldDelim + value.toString());
				context.write(NullWritable.get(),outVal);
    		}
    	}
	}

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LogPartitioner(), args);
        System.exit(exitCode);
    }
	
}
