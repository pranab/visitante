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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.visitante.mr.bda;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.visitante.util.Util;

/**
 *
 * @author pranab
 */
public class BayesDiscriminator implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Bayesian discriminator MR";
        job.setJobName(jobName);
        
        job.setJarByClass(BayesDiscriminator.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BayesDiscriminator.BayesDiscriminatorMapper.class);
        job.setReducerClass(BayesDiscriminator.BayesDiscriminatorReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BayesDiscriminator(), args);
        System.exit(exitCode);
    }

    public void setConf(Configuration conf) {
       this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }
 
    public static class BayesDiscriminatorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text keyHolder = new Text();
        private IntWritable valueHolder = new IntWritable(1);
        private Map<String, Integer>  clickCount = new HashMap<String, Integer>();
        private static final int CLICK_COUNT = 0;
        private static final int CONVERTED = 2;
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String keyVal : clickCount.keySet()){
                keyHolder.set(keyVal);
                valueHolder.set(clickCount.get(keyVal));
                context.write(keyHolder, valueHolder);
            }
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] items  =  value.toString().split(",");
            String keyVal = items[CONVERTED] + "," + items[CLICK_COUNT];
            Integer count = clickCount.get(keyVal);
            if (null == count){
               count = 0; 
            }
            count = count + 1;
            clickCount.put(keyVal, count);
            
        }        
    }   
    
    public static class BayesDiscriminatorReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
        private Text valueHolder = new Text();
        private Map<String, Integer> totalCount = new HashMap<String, Integer>();
        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String classValue : totalCount.keySet()){
                int count = totalCount.get(classValue);
                valueHolder.set(classValue + "," + count);
                context.write(NullWritable.get(), valueHolder);
            }
        }    
        
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values){
                count += value.get();
            } 
            String[] items = key.toString().split(",");
            String classVal = items[0];
            Integer classCount = totalCount.get(classVal);
            if (null == classCount){
                classCount = 0;
            }
            classCount = classCount + count;
            totalCount.put(classVal, classCount);
            
            valueHolder.set(key.toString() + "," + count);
            context.write(NullWritable.get(), valueHolder);
                
        }
    }    
    
}
