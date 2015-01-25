package org.rahulmadhavan.a1.drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.rahulmadhavan.a1.mappers.MedianMapper;
import org.rahulmadhavan.a1.models.Record;

import org.rahulmadhavan.a1.reducers.MedianSortReducer;

/**
 * Created by rahulmadhavan on 1/25/15.
 */
public class V2MedianDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(getConf(),"Median Calculator v2");
        job.setJarByClass(getClass());


        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapOutputKeyClass(Record.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MedianMapper.class);
        job.setReducerClass(MedianSortReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        return job.waitForCompletion(true)? 0 : 1;
    }


}

