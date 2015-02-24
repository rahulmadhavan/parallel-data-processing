package org.mapred.a0;

import org.apache.hadoop.io.DoubleWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by mapred on 1/20/15.
 */
public class RecordCategoryPartitionerTest {

    @Test
    public void validateGetPartitions(){

        RecordCategoryPartitioner recordCategoryPartitioner = new RecordCategoryPartitioner();

        Record record = new Record("test",22);
        DoubleWritable doubleWritable = new DoubleWritable(10);

        assertEquals(0,recordCategoryPartitioner.getPartition(record,doubleWritable,1));


    }
}
