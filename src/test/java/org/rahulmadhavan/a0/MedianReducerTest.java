package org.rahulmadhavan.a0;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by rahulmadhavan on 1/15/15.
 */
public class MedianReducerTest {

    @Test
    public void validateReduceForValidEvenListInput() throws IOException, InterruptedException {

        MedianReducer medianReducer = new MedianReducer();

        MedianReducer.Context context = mock(MedianReducer.Context.class);

        Record key = new Record("electronics",12.23);
        List<DoubleWritable> list = Arrays.asList(new DoubleWritable(3.0),new DoubleWritable(4.5));

        medianReducer.reduce(key,list,context);

        verify(context).write(new Text("electronics"),new DoubleWritable(3.75));

    }

    @Test
    public void validateReduceForValidOddListInput() throws IOException, InterruptedException {

        MedianReducer medianReducer = new MedianReducer();

        MedianReducer.Context context = mock(MedianReducer.Context.class);

        Record key = new Record("electronics",12.23);
        List<DoubleWritable> list = Arrays.asList(new DoubleWritable(3.0),new DoubleWritable(4.5),new DoubleWritable(9.1));

        medianReducer.reduce(key,list,context);

        verify(context).write(new Text("electronics"),new DoubleWritable(4.5));

    }

    @Test
    public void validateReduceForValidOddListInput2() throws IOException, InterruptedException {

        MedianReducer medianReducer = new MedianReducer();

        MedianReducer.Context context = mock(MedianReducer.Context.class);

        Record key = new Record("electronics",12.23);

        List<DoubleWritable> list = Arrays.asList(
                new DoubleWritable(12.0),
                new DoubleWritable(13.0),
                new DoubleWritable(15.0),
                new DoubleWritable(16.0),
                new DoubleWritable(17.0),
                new DoubleWritable(24.0));

        medianReducer.reduce(key,list,context);

        verify(context).write(new Text("electronics"),new DoubleWritable(15.5));

    }


}
