package org.mapred.a1.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by mapred on 1/25/15.
 */
public class MedianUtilsTest {

    @Test
    public void validateFibbonacci(){

        long result = 8;

        assertEquals("fibb of 7 should be 8",result,MedianUtils.fibonacci(7));

    }

}
