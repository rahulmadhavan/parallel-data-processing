package org.mapred.a1.combiner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.mapred.a1.models.Record;

/**
 * Created by mapred on 2/9/15.
 */
public class DataSampler extends Reducer<Record,DoubleWritable,Record,DoubleWritable> {


    private static final Log log = LogFactory.getLog(DataSampler.class);

    static enum DataSamplerError {
        DATA_SAMPLER_ERROR
    }

    @Override
    protected void reduce(Record key, Iterable<DoubleWritable> values, Context context){

        long counter = 0;

        try {
            int SR = context.getConfiguration().getInt("SR",4);

            for(DoubleWritable price: values){
                if(counter < SR){
                    context.write(key,price);
                }

                counter++;

                if(counter == 10){
                    counter = 0;
                }
            }

        } catch (Exception e){
            context.getCounter(DataSamplerError.DATA_SAMPLER_ERROR).increment(1);
            log.error("Median Reducer Error");
            log.error(e);
        }

    }



}
