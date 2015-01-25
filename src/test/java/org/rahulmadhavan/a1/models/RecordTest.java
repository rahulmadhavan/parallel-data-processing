package org.rahulmadhavan.a1.models;

import org.junit.Test;
import org.rahulmadhavan.a1.models.Record;

import static org.junit.Assert.assertEquals;

/**
 * Created by rahulmadhavan on 1/15/15.
 */
public class RecordTest {

    @Test
    public void validateCompareToForRecordWithDifferentCategory(){
        Record record1 = new Record("test1",22);
        Record record2 = new Record("test2",21);

        int result = record1.compareTo(record2);

        assertEquals(-1,result);
    }

    @Test
    public void validateCompareToForRecordWithSameCategory(){
        Record record1 = new Record("test1",22);
        Record record2 = new Record("test1",21);

        int result = record1.compareTo(record2);

        assertEquals(1,result);
    }

    @Test
    public void validateCompareToForSameRecords(){
        Record record1 = new Record("test1",22);
        Record record2 = new Record("test1",22);

        int result = record1.compareTo(record2);

        assertEquals(0,result);
    }




}
