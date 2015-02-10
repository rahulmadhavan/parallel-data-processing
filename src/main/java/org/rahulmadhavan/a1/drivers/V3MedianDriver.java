package org.rahulmadhavan.a1.drivers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.rahulmadhavan.a1.comparators.RecordCategoryGroupingComparator;
import org.rahulmadhavan.a1.comparators.RecordComparator;
import org.rahulmadhavan.a1.mappers.MedianMapper;
import org.rahulmadhavan.a1.models.Record;
import org.rahulmadhavan.a1.partitioners.RecordCategoryPartitioner;
import org.rahulmadhavan.a1.reducers.MedianReducer;

/**
 * Created by rahulmadhavan on 1/15/15.
 */
public class V3MedianDriver extends Configured implements Tool{

    private static final Log log = LogFactory.getLog(V3MedianDriver.class);

    @Override
    public int run(String[] args) throws Exception {

        log.info("V3MedianDriver");

        Job job = new Job(getConf(),"Median Calculator v3");
        job.setJarByClass(getClass());
        job.setGroupingComparatorClass(RecordCategoryGroupingComparator.class);
        job.setSortComparatorClass(RecordComparator.class);
        job.setPartitionerClass(RecordCategoryPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapOutputKeyClass(Record.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MedianMapper.class);
        job.setReducerClass(MedianReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        return job.waitForCompletion(true)? 0 : 1;
    }


}
