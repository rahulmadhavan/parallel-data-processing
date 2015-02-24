package org.mapred.a0;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by mapred on 1/15/15.
 */
public class Record implements WritableComparable<Record>{

    private String category;
    private double price;

    public Record(){

    }

    public Record(String category, double price){
        this.category = category;
        this.price = price;
    }

    @Override
    public String toString(){
        return (new StringBuilder()).append(category).append("-").append(price).toString();
    }

    @Override
    public int hashCode(){
        return category.hashCode()*163 + new Double(price).hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof Record)
        {
            Record record = (Record) o;
            return this.compareTo(record) == 0;
        }
        return false;
    }



    @Override
    public int compareTo(Record o) {
        int result = category.compareTo(o.getCategory());
        if(result == 0){
            return price == o.getPrice() ? 0 : (price > o.getPrice()) ? 1 : -1;
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput,category);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        category = WritableUtils.readString(dataInput);
        price = dataInput.readDouble();
    }

    public String getCategory() {
        return category;
    }

    public double getPrice() {
        return price;
    }
}
