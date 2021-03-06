package org.mapred.a1.reducers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mapred.a1.models.Record;
import org.mapred.a1.utils.MedianUtils;

import java.util.List;

/**
 * Created by mapred on 1/15/15.
 */
public class MedianReducer extends Reducer<Record,DoubleWritable,Text,DoubleWritable> {


    private static final Log log = LogFactory.getLog(MedianReducer.class);

    static enum MedianReducerError {
        MEDIAN_REDUCER_ERROR
    }

    @Override
    protected void reduce(Record key, Iterable<DoubleWritable> values, Context context){

        try {
            List<Double> valueList = MedianUtils.extractListFromIterable(values);
            Double median = MedianUtils.computeMedian(valueList);
            context.write(new Text(key.getCategory()),new DoubleWritable(median));
        } catch (Exception e){
            context.getCounter(MedianReducerError.MEDIAN_REDUCER_ERROR).increment(1);
            log.error("Median Reducer Error");
            log.error(e);
        }

    }


}
