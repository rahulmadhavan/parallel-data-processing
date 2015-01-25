package org.rahulmadhavan.a1.comparators;

import org.junit.Test;
import org.rahulmadhavan.a1.comparators.RecordCategoryGroupingComparator;
import org.rahulmadhavan.a1.models.Record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by rahulmadhavan on 1/20/15.
 */
public class RecordCategoryGroupingComparatorTest {


    @Test
    public void validateCompareForTheSameRecordCategory(){

        RecordCategoryGroupingComparator recordCategoryGroupingComparator = new RecordCategoryGroupingComparator();

        Record record1 = new Record("test1",22);
        Record record2 = new Record("test1",22);

        assertEquals(0,recordCategoryGroupingComparator.compare(record1, record2));


    }

    @Test
    public void validateCompareForDifferentRecordCategory(){

        RecordCategoryGroupingComparator recordCategoryGroupingComparator = new RecordCategoryGroupingComparator();

        Record record1 = new Record("test1",22);
        Record record2 = new Record("test2",21);

        assertNotEquals(0,recordCategoryGroupingComparator.compare(record1, record2));
    }



}
