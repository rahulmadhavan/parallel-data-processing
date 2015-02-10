package org.rahulmadhavan.a1.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.Test;
import org.rahulmadhavan.a1.models.Record;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Created by rahulmadhavan on 2/9/15.
 */
public class DataSamplerTest {

    @Test
    public void validateSampler()throws IOException, InterruptedException {

        DoubleWritable dw1 = new DoubleWritable(11.0);
        DoubleWritable dw2 = new DoubleWritable(12.0);
        DoubleWritable dw3 = new DoubleWritable(13.0);
        DoubleWritable dw4 = new DoubleWritable(14.0);
        DoubleWritable dw5 = new DoubleWritable(15.0);
        DoubleWritable dw6 = new DoubleWritable(16.0);
        DoubleWritable dw7 = new DoubleWritable(17.0);
        DoubleWritable dw8 = new DoubleWritable(18.0);
        DoubleWritable dw9 = new DoubleWritable(19.0);
        DoubleWritable dw10 = new DoubleWritable(20.0);

        Record record = mock(Record.class);
        List<DoubleWritable> list = Arrays.asList(
                dw1,dw2,dw3,dw4,dw5,
                dw6,dw7,dw8,dw9,dw10);
        DataSampler.Context context = mock(DataSampler.Context.class);
        Configuration configuration = mock(Configuration.class);

        when(configuration.getInt("SR",4)).thenReturn(6);
        when(context.getConfiguration()).thenReturn(configuration);

        DataSampler dataSampler = new DataSampler();



        dataSampler.reduce(record,list,context);

        verify(context).write(record,dw1);
        verify(context).write(record,dw2);
        verify(context).write(record,dw3);
        verify(context).write(record,dw4);
        verify(context).write(record,dw5);
        verify(context).write(record,dw6);
        verify(context,never()).write(record,dw7);
        verify(context,never()).write(record,dw8);
        verify(context,never()).write(record,dw9);
        verify(context,never()).write(record,dw10);


    }

}
