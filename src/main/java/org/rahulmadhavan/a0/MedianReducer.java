package org.rahulmadhavan.a0;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by rahulmadhavan on 1/15/15.
 */
public class MedianReducer extends Reducer<Record,DoubleWritable,Text,DoubleWritable> {


    private static final Log _log = LogFactory.getLog(MedianReducer.class);

    @Override
    protected void reduce(Record key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        ArrayList<Double> valueList = new ArrayList<Double>();


        StringBuilder sb = new StringBuilder();

        for(DoubleWritable value : values){
            valueList.add(value.get());
            sb.append(" ").append(value.get());
        }

        _log.info("reduce input : "+sb.toString());

        int length = valueList.size();
        double median = (length + 1) % 2 == 0 ? valueList.get(length/2) : (valueList.get(length/2) + valueList.get((length/2) - 1))/2;
        context.write(new Text(key.getCategory()),new DoubleWritable(median));



    }
}
