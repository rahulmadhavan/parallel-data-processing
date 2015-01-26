package org.rahulmadhavan.a1.utils;

import org.apache.hadoop.io.DoubleWritable;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by rahulmadhavan on 1/25/15.
 */
public class MedianUtils {

    public static List<Double> extractListFromIterable(Iterable<DoubleWritable> values){

        List<Double> valueList = new LinkedList<Double>();

        for(DoubleWritable value : values){
            valueList.add(value.get());
        }

        return  valueList;

    }

    static public Double computeMedian(List<Double> list){
        int length = list.size();
        Double result = null;
        if((length + 1) % 2 == 0 ){
            result = list.get(length / 2);
        }else{
            result = (list.get(length / 2) + list.get((length/2) - 1))/2;
        }
        return result;
    }

    static public long fibonacci(int n){
        long[] memo = new long[n];
        memo[0] = 0;
        memo[1] = 1;

        for (int i = 2; i < n; i++) {
            memo[i] = memo[i - 1] + memo[i - 2];
        }

        return memo[n - 1];
    }
}
