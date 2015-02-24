package org.mapred.a3.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by rahulmadhavan on 2/23/15.
 */
public class FlightDelayReducerTest {

    @Test
    public void validateReducerForValidInput() throws IOException, InterruptedException {

        FlightDelayReducer flightDelayReducer = new FlightDelayReducer();
        FlightDelayReducer.Context context = mock(FlightDelayReducer.Context.class);
        List<IntWritable> list = Arrays.asList(
                new IntWritable(0),
                new IntWritable(0),
                new IntWritable(0),
                new IntWritable(1),
                new IntWritable(1),
                new IntWritable(1));

        Text key = new Text("1");
        flightDelayReducer.reduce(key,list,context);

        verify(context).write(key,new DoubleWritable(0.5));

    }


}
