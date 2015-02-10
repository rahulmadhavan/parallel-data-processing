package org.rahulmadhavan.a1.drivers;

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
import org.rahulmadhavan.a1.combiner.DataSampler;
import org.rahulmadhavan.a1.combiner.PercentageDataSampler;
import org.rahulmadhavan.a1.comparators.RecordCategoryGroupingComparator;
import org.rahulmadhavan.a1.comparators.RecordComparator;
import org.rahulmadhavan.a1.mappers.MedianFibbonacciMapper;
import org.rahulmadhavan.a1.models.Record;
import org.rahulmadhavan.a1.partitioners.RecordCategoryPartitioner;
import org.rahulmadhavan.a1.reducers.MedianReducer;

/**
 * Created by rahulmadhavan on 2/9/15.
 */
public class V5MedianDriver extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(V5MedianDriver.class);

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        int SR = Integer.parseInt(args[1]);
        if(SR < 1 || SR > 100){
            throw new IllegalArgumentException("Sample rate should be between 1 and 100");
        }

        log.info("V5MedianDriver");

        configuration.setInt("SR",Integer.parseInt(args[1]));

        Job job = new Job(configuration,"Approx Median Calculator v5");
        job.setJarByClass(getClass());
        job.setGroupingComparatorClass(RecordCategoryGroupingComparator.class);
        job.setSortComparatorClass(RecordComparator.class);
        job.setCombinerClass(PercentageDataSampler.class);
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
