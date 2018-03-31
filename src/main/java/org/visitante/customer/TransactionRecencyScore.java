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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.stats.ExponentialDistribution;
import org.chombo.stats.ProbabilityDistribution;
import org.chombo.stats.StandardNormalDistribution;
import org.chombo.util.Utility;

/**
 * Converts transaction recency i.e, time since last transaction to a score based 
 * on normal distribution of time gap between transaction
 * @author pranab
 *
 */
public class TransactionRecencyScore extends Configured implements Tool {
	private static String configDelim = ",";

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Transaction recency score";
        job.setJobName(jobName);
        
        job.setJarByClass(TransactionRecencyScore.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        job.setMapperClass(TransactionRecencyScore.RecencyScoreMapper.class);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
	}

	/**
	 * @author pranab
	 *
	 */
	public static class RecencyScoreMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String[] items;
        private double recency;
        private double recencyScore;
        private double globalAvTimeGap = -1.0;
        private double globalStdDevTimeGap = -1.0;
        private ProbabilityDistribution transGapDistr;
        private static final int RECENCY_FIELD = 2;
        
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = Utility.getFieldDelimiter(config, "trs.field.delim.regex", "field.delim.regex", ",");
        	
        	
        	//trans stats
        	List<String[]> transStats = Utility.parseFileLines(config, "trs.xaction.stats.file.path", "=");
        	for (String[] items : transStats) {
        		if (items[0].equals("globalAvTimeGap")) {
        			globalAvTimeGap = Double.parseDouble(items[1]);
        		} else if (items[0].equals("globalStdDevTimeGap")) {
        			globalStdDevTimeGap = Double.parseDouble(items[1]);
        		}
        	}
        	if (globalAvTimeGap < 0 && globalStdDevTimeGap < 0) {
        		throw new IllegalStateException("transaction time gap global stats not found");
        	}
        	
        	//trans gap prob distr
        	String distrType = config.get("trs.trans.gap.prob.distr", "normal");
        	if (distrType.equals("normal")) {
        		transGapDistr = new StandardNormalDistribution(globalAvTimeGap, globalStdDevTimeGap);
        	} else if (distrType.equals("exponential")) {
        		transGapDistr = new ExponentialDistribution(globalAvTimeGap);
        	} else {
        		throw new IllegalArgumentException("invalid probability distribution");
        	}
        }

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            
            recency = Double.parseDouble(items[RECENCY_FIELD]);
            recencyScore = 1.0 - transGapDistr.getDistr(recency);
            items[RECENCY_FIELD] = Utility.formatDouble(recencyScore, 3);
            
            outVal.set(Utility.join(items));
			context.write(NullWritable.get(), outVal);
        }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TransactionRecencyScore(), args);
		System.exit(exitCode);
	}
	
}
