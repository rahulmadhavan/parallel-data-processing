package org.rahulmadhavan.a1.comparators;

import org.junit.Test;
import org.rahulmadhavan.a1.comparators.RecordComparator;
import org.rahulmadhavan.a1.models.Record;

import static org.junit.Assert.assertEquals;

/**
 * Created by rahulmadhavan on 1/20/15.
 */
public class RecordComparatorTest {

    @Test
    public void validateCompareForTheSameRecords(){

        RecordComparator recordComparator = new RecordComparator();

        Record record1 = new Record("test1",22);
        Record record2 = new Record("test1",22);

        assertEquals(0,recordComparator.compare(record1, record2));


    }

    @Test
    public void validateCompareForRecordWithDifferentCategory(){

        RecordComparator recordComparator = new RecordComparator();

        Record record1 = new Record("test1",22);
        Record record2 = new Record("test2",21);

        assertEquals(-1,recordComparator.compare(record1, record2));
    }

    @Test
    public void validateCompareForRecordWithSameCategory(){

        RecordComparator recordComparator = new RecordComparator();

        Record record1 = new Record("test1",22);
        Record record2 = new Record("test1",21);

        assertEquals(1,recordComparator.compare(record1, record2));
    }

}
