package org.rahulmadhavan.a0;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by rahulmadhavan on 1/15/15.
 */
public class MedianReducer extends Reducer<Record,DoubleWritable,Text,DoubleWritable> {


    private static final Log _log = LogFactory.getLog(MedianReducer.class);

    @Override
    protected void reduce(Record key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        List<Double> valueList = extractListFromIterable(values);
        Double median = computeMedian(valueList);
        context.write(new Text(key.getCategory()),new DoubleWritable(median));

    }

    private List<Double> extractListFromIterable(Iterable<DoubleWritable> values){

        List<Double> valueList = new LinkedList<Double>();

        for(DoubleWritable value : values){
            valueList.add(value.get());
        }

        return  valueList;

    }

    private Double computeMedian(List<Double> list){
        int length = list.size();
        Double result = null;
        if((length + 1) % 2 == 0 ){
            result = list.get(length/2);
        }else{
            result = (list.get(length/2) + list.get((length/2) - 1))/2;
        }
        return result;

    }
}
