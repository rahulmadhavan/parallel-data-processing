package org.mapred.a3.mappers;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * Created by rahulmadhavan on 2/23/15.
 */
public class AirlineMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private static final int DELAY_15_INDEX = 33;
    private static final int AIRLINE_ID = 7;
    private static final int ORIGIN_AIRPORT_ID = 11;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        CSVParser parser = CSVParser.parse(value.toString(),CSVFormat.DEFAULT);
        List<CSVRecord> csvRecords = parser.getRecords();

        for(CSVRecord csvRecord : csvRecords){

            try {
                int delayed = Integer.parseInt(csvRecord.get(DELAY_15_INDEX));
                String airlineId = csvRecord.get(AIRLINE_ID);
                if(null != airlineId){
                    context.write(new Text(airlineId),new IntWritable(delayed));
                }
            }catch (NullPointerException e){

            }catch (NumberFormatException e){

            }

        }

    }
}
