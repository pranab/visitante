/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.visitante.mr.logit;

import java.io.IOException;
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

/**
 *
 * @author pranab
 */
public class LogitEstimator implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        String jobName = "Logit estimator MR";
        job.setJobName(jobName);
        
        job.setJarByClass(LogitEstimator.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(LogitEstimator.LogitEstimatorMapper.class);
        //job.setCombinerClass(DomainRange.DomainRangeReducer.class);
        job.setReducerClass(LogitEstimator.LogitEstimatorReducer.class);
        
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        
        int status =  job.waitForCompletion(true) ? 0 : 1;
        return status;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LogitEstimator(), args);
        System.exit(exitCode);
    }

    public void setConf(Configuration conf) {
       this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }
    
    public static class LogitEstimatorMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        }        
    }   
    
    public static class LogitEstimatorReducer extends Reducer<Text, Text, NullWritable, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        }
    }    

}
