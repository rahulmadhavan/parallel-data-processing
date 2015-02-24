package org.mapred.a0;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by mapred on 1/15/15.
 */
public class RecordCategoryPartitioner extends Partitioner<Record, DoubleWritable> {

    @Override
    public int getPartition(Record record, DoubleWritable doubleWritable, int numberOfPartitions) {
        return record.getCategory().hashCode() %  numberOfPartitions;
    }

}
