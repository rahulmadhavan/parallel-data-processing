package org.rahulmadhavan.a0;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by rahulmadhavan on 1/15/15.
 */
public class MedianDriver extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {

        if(args.length != 2){
            System.err.printf("Usage: %s [generic options] <input> <output>\n",getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf(),"Median Calculator");
        job.setJarByClass(getClass());
        job.setGroupingComparatorClass(RecordCategoryGroupingComparator.class);
        job.setSortComparatorClass(RecordComparator.class);
        job.setPartitionerClass(RecordCategoryPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Record.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MedianMapper.class);
        job.setReducerClass(MedianReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);




        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new MedianDriver(), args);
        System.exit(exitCode);
    }
}
