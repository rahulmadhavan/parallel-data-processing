package org.rahulmadhavan.a1.mappers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.rahulmadhavan.a1.models.Record;
import org.rahulmadhavan.a1.utils.MedianUtils;

/**
 * Created by rahulmadhavan on 1/25/15.
 */
public class MedianFibbonacciMapper extends Mapper<LongWritable,Text,Record,DoubleWritable>{

    private static final Log log = LogFactory.getLog(MedianFibbonacciMapperError.class);

    static enum MedianFibbonacciMapperError {
        INPUT_DATA_FORMAT_ERROR,
        PRICE_PARSE_ERROR,
        MEDIAN_FIBBONACCI_MAPPER_ERROR
    }

    @Override
    protected void map(LongWritable key, Text value, Context context){

        try{

            Record record = parseInputAsRecord(value,context);
            if (null != record){
                context.write(record, new DoubleWritable(record.getPrice()));
            }

            int N = context.getConfiguration().getInt("N",20);
            long fibboOfN = MedianUtils.fibonacci(N);
            //log.info("Fibbonacci number "+N + " is "+fibboOfN);

        }catch (Exception e){

            context.getCounter(MedianFibbonacciMapperError.MEDIAN_FIBBONACCI_MAPPER_ERROR).increment(1);
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
            context.getCounter(MedianFibbonacciMapperError.INPUT_DATA_FORMAT_ERROR).increment(1);
        }else{
            try {
                productCategory = input[3];
                productPrice = Double.parseDouble(input[4]);
                record = new Record(productCategory,productPrice);
            }catch (Exception e){
                context.getCounter(MedianFibbonacciMapperError.PRICE_PARSE_ERROR).increment(1);
            }
        }

        return record;

    }

}
