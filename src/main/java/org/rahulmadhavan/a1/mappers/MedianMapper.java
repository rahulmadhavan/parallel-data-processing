package org.rahulmadhavan.a1.mappers;

/**
 * Created by rahulmadhavan on 1/15/15.
 *
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.rahulmadhavan.a1.models.Record;


public class MedianMapper extends Mapper<LongWritable,Text,Record,DoubleWritable> {

    private static final Log log = LogFactory.getLog(MedianMapper.class);

    static enum MedianMapperError {
        INPUT_DATA_FORMAT_ERROR,
        PRICE_PARSE_ERROR,
        MEDIAN_MAPPER_ERROR
    }

    @Override
    protected void map(LongWritable key, Text value, Context context){

        try{

            Record record = parseInputAsRecord(value,context);
            if (null != record){
                context.write(record, new DoubleWritable(record.getPrice()));
            }

        }catch (Exception e){

            context.getCounter(MedianMapperError.MEDIAN_MAPPER_ERROR).increment(1);
            log.error("Median Mapper Error");
            log.error(e);

        }

    }

    private Record parseInputAsRecord(Text value, Context context){

        String productCategory;
        double productPrice;
        Record record = null;

        String line = value.toString();
        String[] input = line.split("\t");

        if(input.length != 6){
            context.getCounter(MedianMapperError.INPUT_DATA_FORMAT_ERROR).increment(1);
        }

        try {
            productCategory = input[3];
            productPrice = Double.parseDouble(input[4]);
            record = new Record(productCategory,productPrice);
        }catch (Exception e){
            context.getCounter(MedianMapperError.PRICE_PARSE_ERROR).increment(1);
        }

        return record;

    }

}
