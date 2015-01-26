package org.rahulmadhavan.a1.reducers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.rahulmadhavan.a1.models.Record;
import org.rahulmadhavan.a1.utils.MedianUtils;
import java.util.Collections;
import java.util.List;

/**
 * Created by rahulmadhavan on 1/25/15.
 */
public class MedianSortReducer extends Reducer<Record,DoubleWritable,Text,DoubleWritable> {


    private static final Log log = LogFactory.getLog(MedianSortReducer.class);

    static enum MedianSortReducerError {
        MEDIAN_SORT_REDUCER_ERROR
    }

    @Override
    protected void reduce(Record key, Iterable<DoubleWritable> values, Context context){
        try {
            List<Double> valueList = MedianUtils.extractListFromIterable(values);
            Collections.sort(valueList);
            Double median = MedianUtils.computeMedian(valueList);
            context.write(new Text(key.getCategory()),new DoubleWritable(median));
        } catch (Exception e){
            context.getCounter(MedianSortReducerError.MEDIAN_SORT_REDUCER_ERROR).increment(1);
            log.error("Median Reducer Error");
            log.error(e);
        }
    }



}
