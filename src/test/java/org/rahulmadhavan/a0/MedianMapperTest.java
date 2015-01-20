package org.rahulmadhavan.a0;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by rahulmadhavan on 1/15/15.
 */
public class MedianMapperTest {

    @Test
    public void validateMapForValidInput() throws IOException, InterruptedException {

        String input = "electronics,12";

        MedianMapper medianMapper = new MedianMapper();

        MedianMapper.Context context = mock(MedianMapper.Context.class);

        medianMapper.map(new LongWritable(1),new Text(input),context);

        verify(context).write(new Record("electronics",12),new DoubleWritable(12));

    }

}
