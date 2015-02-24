package org.mapred.ndcweather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by mapred on 1/14/15.
 */
public class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int max = Integer.MIN_VALUE;

        for(IntWritable intWritable: values){
            max = Math.max(max,intWritable.get());
        }

        context.write(key,new IntWritable(max));

    }
}
