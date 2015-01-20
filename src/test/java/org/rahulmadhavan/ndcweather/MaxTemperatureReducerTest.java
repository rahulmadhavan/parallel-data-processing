package org.rahulmadhavan.ndcweather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by rahulmadhavan on 1/14/15.
 */
public class MaxTemperatureReducerTest {

    @Test
    public void validateReduce() throws IOException, InterruptedException {

        MaxTemperatureReducer maxTemperatureReducer = new MaxTemperatureReducer();

        MaxTemperatureReducer.Context context = mock(MaxTemperatureReducer.Context.class);

        Text key = new Text("1949");

        List<IntWritable> list = Arrays.asList(new IntWritable(20),new IntWritable(5));

        maxTemperatureReducer.reduce(key,list,context);

        verify(context).write(key,new IntWritable(20));

    }

}
