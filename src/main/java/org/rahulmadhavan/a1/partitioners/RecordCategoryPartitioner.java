package org.rahulmadhavan.a1.partitioners;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.rahulmadhavan.a1.models.Record;

/**
 * Created by rahulmadhavan on 1/15/15.
 */
public class RecordCategoryPartitioner extends Partitioner<Record, DoubleWritable> {

    @Override
    public int getPartition(Record record, DoubleWritable doubleWritable, int numberOfPartitions) {
        return record.getCategory().hashCode() %  numberOfPartitions;
    }

}
