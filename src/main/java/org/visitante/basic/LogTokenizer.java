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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.chombo.util.Utility;

/**
 * Tokenizes log line using combination token position and token value
 * @author pranab
 *
 */
public class LogTokenizer extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Log tokenizer";
        job.setJobName(jobName);
        
        job.setJarByClass(LogTokenizer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Utility.setConfiguration(job.getConfiguration(), "visitante");
        job.setMapperClass(LogTokenizer.TokenizerMapper.class);
        
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
	public static class TokenizerMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		private Text outVal = new Text();
        private String fieldDelimRegex;
        private String fieldDelimOut;
        private String[] items;
        private String fileNamePrefix;
        private String generatedKey;
        private String posPrefixedToken;
        private StringBuilder stBld = new StringBuilder();
        private int fileNamePrefixLength;
        private int byteOffsetFormatSize;
        private int tokenPrefixFormatSize;
        
        private static final int FILE_NAME_PREFIX_LENGTH = 8;
        private static final int BYTE_OFFSET_FORMAT_SIZE = 12;
        private static final int TOKEN_PREFIX_FORMAT_SIZE = 3;
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         */
        protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration config = context.getConfiguration();
        	fieldDelimRegex = Utility.getFieldDelimiter(config, "lot.field.delim.regex", "field.delim.regex", "\\s+");
        	fieldDelimOut = Utility.getFieldDelimiter(config, "lot.field.delim.out", "field.delim.out", " ");
        	fileNamePrefixLength = config.getInt("lot.file.name.prefix.length", FILE_NAME_PREFIX_LENGTH);
        	byteOffsetFormatSize = config.getInt("lot.byte.offset.format.size", BYTE_OFFSET_FORMAT_SIZE);
        	tokenPrefixFormatSize = config.getInt("lot.token.prefix.format.size", TOKEN_PREFIX_FORMAT_SIZE);
        	
        	String splitFileName = ((FileSplit)context.getInputSplit()).getPath().getName();
        	if (splitFileName.length() >= fileNamePrefixLength) {
        		fileNamePrefix = splitFileName.substring(0, fileNamePrefixLength);
        	} else {
        		StringBuilder stBld = new StringBuilder(splitFileName);
        		for (int i = 0; i < fileNamePrefixLength - splitFileName.length(); ++i) {
        			stBld.append('x');
        		}
        		fileNamePrefix = stBld.toString();
        	}
        }
        
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            items  =  value.toString().split(fieldDelimRegex);
            stBld.delete(0, stBld.length());
            
            //generate key
            generatedKey = fileNamePrefix + ":" + Utility.formatLong(key.get(), byteOffsetFormatSize);
            stBld.append(generatedKey).append(fieldDelimOut);
            
            //prefix token with position index 
            for (int i = 0; i < items.length; ++i) {
            	posPrefixedToken = Utility.formatLong(i, tokenPrefixFormatSize) + ":" + items[i]; 
                stBld.append(posPrefixedToken).append(fieldDelimOut);
            }
            
            stBld.delete(stBld.length() - fieldDelimOut.length(), stBld.length());
            outVal.set(stBld.toString());
			context.write(NullWritable.get(), outVal);
        }        
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new LogTokenizer(), args);
		System.exit(exitCode);
	}
	

}
