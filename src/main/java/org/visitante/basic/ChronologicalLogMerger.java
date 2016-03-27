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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Tuple;
import org.chombo.util.Utility;

/**
 * Chronologically sorts and merges log files from multiple sources
 * @author pranab
 *
 */
public class ChronologicalLogMerger extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Log files merger  MR";
        job.setJobName(jobName);
        
        job.setJarByClass(ChronologicalLogMerger.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        
        job.setMapperClass(ChronologicalLogMerger.MergeMapper.class);
        job.setReducerClass(ChronologicalLogMerger.MergeReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        int numReducer = job.getConfiguration().getInt("clm.num.reducer", -1);
        numReducer = -1 == numReducer ? job.getConfiguration().getInt("num.reducer", 1) : numReducer;
        job.setNumReduceTasks(numReducer);
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class MergeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable outKey = new LongWritable();
		private Text outVal = new Text();
		private Pattern dateTimePattern;
		private Matcher matcher;
		private List<String> records = new ArrayList<String>();
		private long epochTime;
        private SimpleDateFormat dateFormat;
        private String fileSourceIdSep;
        private String fileSourceId;
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
           	Configuration config = context.getConfiguration();
            String dateTimeRegex = config.get("clm.date.time.regex", "\\.*(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})\\.*");
            dateTimePattern = Pattern.compile(dateTimeRegex);
            String dateTimeFormatStr = config.get("clm.date.format.str",  "yyyy-MM-dd HH:mm:ss");
            dateFormat = new SimpleDateFormat(dateTimeFormatStr);
            
            //file name prefixed with ID of source (e.g hostname)
            fileSourceIdSep = config.get("clm.file.source.id.sep", "_");
            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            fileSourceId = fileName.split(fileSourceIdSep)[0];
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	matcher = dateTimePattern.matcher(value.toString());
        	if (matcher.matches()) {
        		if (!records.isEmpty()) {
        			outKey.set(epochTime);
        			outVal.set(Utility.join(records, "\\n"));
        			context.write(outKey, outVal);
        		}
        		
        		//extract date time from the first line of a record
        		String dateTimeStr =  matcher.group(1);
        		try {
					Date date = dateFormat.parse(dateTimeStr);
					epochTime = date.getTime();
				} catch (ParseException ex) {
					throw new IOException("error parsing date time", ex);
				}
        		records.clear();
        		records.add(fileSourceId + " " + value.toString());
        	} else {
        		records.add(fileSourceId + " " + value.toString());
        	}
        }       
	}
	
	/**
	 * @author pranab
	 *
	 */
	public static class MergeReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
		
	   	/* (non-Javadoc)
    	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
    	 */
    	protected void reduce(Tuple key, Iterable<Text> values, Context context)
        	throws IOException, InterruptedException {
    		for (Text value : values) {
    			outVal.set(value.toString());
    			context.write(NullWritable.get(), outVal);
    		}
    	}		
	}	
	
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ChronologicalLogMerger(), args);
        System.exit(exitCode);
    }
	
}
