package org.mapred.a3.mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by rahulmadhavan on 2/23/15.
 */
public class AirlineMapperTest {

    @Test
    public void validateMapForValidInput() throws IOException, InterruptedException {

        String input = "2012,1,1,1,7,\"2012-01-01\",\"AA\",19805,\"AA\",\"N325AA\",  " +
                "1,12478,1247802,31703,\"JFK\",\"New York, NY\",\"NY\",36,\"New York\",22," +
                "12892,1289203,32575,\"LAX\",\"Los Angeles, CA\",\"CA\",6,\"California\",91,0900," +
                "0855,-5,0,0,-1,\"0900-0959\",13,0908,1138,4,1146,1142,-4,0,0,-2,\"1200-1259\",0,\"\",0,346,347,330,1,2475,10,0,0,0,0,0";

        AirlineMapper airlineMapper = new AirlineMapper();
        AirlineMapper.Context context = mock(AirlineMapper.Context.class);

        airlineMapper.map(new LongWritable(1),new Text(input),context);

        verify(context).write(new Text("19805"),new IntWritable(0));


    }

}
