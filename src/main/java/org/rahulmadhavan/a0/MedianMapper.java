package org.rahulmadhavan.a0;

/**
 * Created by rahulmadhavan on 1/15/15.
 *
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MedianMapper extends Mapper<LongWritable,Text,Record,DoubleWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] record = line.split(",");
        String productCategory = record[0];
        double productPrice = Double.parseDouble(record[1]);

        context.write(new Record(productCategory,productPrice), new DoubleWritable(productPrice));

    }
}
