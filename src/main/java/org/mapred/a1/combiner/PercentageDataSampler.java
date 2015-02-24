package org.mapred.a1.combiner;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.mapred.a1.models.Record;

import java.util.ArrayList;

/**
 * Created by mapred on 2/9/15.
 */
public class PercentageDataSampler extends Reducer<Record,DoubleWritable,Record,DoubleWritable> {


    private static final Log log = LogFactory.getLog(PercentageDataSampler.class);

    static enum PercentageDataSamplerError {
        PERCENTAGE_DATA_SAMPLER_ERROR
    }

    @Override
    protected void reduce(Record key, Iterable<DoubleWritable> values, Context context){


        try {
            int SR = context.getConfiguration().getInt("SR", 4);

            ArrayList<DoubleWritable> valueList = new ArrayList<DoubleWritable>();

            for(DoubleWritable price: values){
                valueList.add(price);
            }

            long length = valueList.size();
            long numberOfSamples = (long)(SR/100.0 * length);

            for (int i = 0; i < numberOfSamples; i++) {
                context.write(key,valueList.get(i));
            }

        } catch (Exception e){
            context.getCounter(PercentageDataSamplerError.PERCENTAGE_DATA_SAMPLER_ERROR).increment(1);
            log.error("Median Reducer Error");
            log.error(e);
        }

    }



}

