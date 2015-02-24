package org.mapred.a1.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.mapred.a1.models.Record;

/**
 * Created by mapred on 1/15/15.
 */
public class RecordCategoryGroupingComparator extends WritableComparator{

    protected RecordCategoryGroupingComparator(){
        super(Record.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Record record1 = (Record)a;
        Record record2 = (Record)b;

        return record1.getCategory().compareTo(record2.getCategory());
    }
}
