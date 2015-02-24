package org.mapred.a3.reducer;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created and Modified by Shivastuti Koul and rahulmadhavan on 2/23/15.
 */
public class FlightDelayReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {




    @Override
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

        double delays = 0;
        long total = 0;
        for(IntWritable arrDel15: values) {
            if(arrDel15.get() > 0){
                delays++;
            }
            total++;
        }
        double averageDelay = 0.0f;
        if(total > 0)
            averageDelay = delays/(total * 1.0);

        context.write (key, new DoubleWritable(averageDelay));

    }
}
