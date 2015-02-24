package org.mapred.ndcweather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;


import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Created by mapred on 1/14/15.
 */
public class MaxTemperatureMapperTest {

    @Test
    public void validateMapForValidNDCRecord() throws IOException, InterruptedException {

        MaxTemperatureMapper maxTemperatureMapper = new MaxTemperatureMapper();

        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + // Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00111+99999999999"); // Temperature ^^^^^

        MaxTemperatureMapper.Context context =  mock(MaxTemperatureMapper.Context.class);

        LongWritable longWritable = new LongWritable(1);

        maxTemperatureMapper.map(longWritable,value,context);

        verify(context).write(new Text("1950"),new IntWritable(-11));

    }

    @Test
    public void validateMapForInvalidNDCRecord() throws IOException, InterruptedException {

        MaxTemperatureMapper maxTemperatureMapper = new MaxTemperatureMapper();

        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" + // Year ^^^^
                "99999V0203201N00261220001CN9999999N9-00112+99999999999"); // Temperature ^^^^^

        MaxTemperatureMapper.Context context =  mock(MaxTemperatureMapper.Context.class);

        LongWritable longWritable = new LongWritable(1);

        maxTemperatureMapper.map(longWritable,value,context);

        verify(context,never()).write(new Text("1950"),new IntWritable(-11));
    }


}
