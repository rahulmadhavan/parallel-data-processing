package org.mapred.a3.drivers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.mapred.a3.mappers.AirlineMapper;
import org.mapred.a3.reducer.FlightDelayReducer;

/**
 * Created by rahulmadhavan on 2/24/15.
 */
public class LearnerDriver extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(LearnerDriver.class);


    @Override
    public int run(String[] args) throws Exception {


        FileSystem fs = FileSystem.get(getConf());
        Job job = new Job(getConf(), "Job1");
        job.setJarByClass(getClass());

        job.setMapperClass(AirlineMapper.class);
        job.setReducerClass(FlightDelayReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(job.waitForCompletion(true)){
            fs.rename(new Path(args[1].concat("/part-r-00000")),new Path(args[1].concat("/model.m")));
            return 1;
        }else {
            return 0;
        }

    }


}
