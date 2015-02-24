package org.mapred.a1.drivers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.mapred.a1.comparators.RecordCategoryGroupingComparator;
import org.mapred.a1.comparators.RecordComparator;
import org.mapred.a1.mappers.MedianFibbonacciMapper;
import org.mapred.a1.models.Record;
import org.mapred.a1.partitioners.RecordCategoryPartitioner;
import org.mapred.a1.reducers.MedianReducer;

/**
 * Created by mapred on 1/25/15.
 */
public class V4MedianDriver extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(V4MedianDriver.class);

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        configuration.setInt("N",Integer.parseInt(args[1]));

        log.info("V4MedianDriver");

        Job job = new Job(configuration,"Median Calculator v4");
        job.setJarByClass(getClass());
        job.setGroupingComparatorClass(RecordCategoryGroupingComparator.class);
        job.setSortComparatorClass(RecordComparator.class);
        job.setPartitionerClass(RecordCategoryPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.setMapOutputKeyClass(Record.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MedianFibbonacciMapper.class);
        job.setReducerClass(MedianReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        return job.waitForCompletion(true)? 0 : 1;
    }


}
