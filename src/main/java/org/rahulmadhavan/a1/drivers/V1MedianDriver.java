package org.rahulmadhavan.a1.drivers;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.rahulmadhavan.a1.models.Record;
import org.rahulmadhavan.a1.utils.MedianUtils;

import java.io.*;
import java.util.*;


/**
 * Created by rahulmadhavan on 1/25/15.
 */
public class V1MedianDriver extends Configured implements Tool {


    private static final Log log = LogFactory.getLog(V1MedianDriver.class);

    @Override
    public int run(String[] args) throws Exception {

        log.info("Begining extraction");
        List<Record> records = extractRecordsFromFile(args[1]);
        log.info("completed extraction. Total records : "+records.size());
        Map<String,Double> medianMap = computeMedianForRecordsByCategories(records);
        log.info("completed computation");
        writeMedianMapToFile(args[2],medianMap);

        return 0;
    }

    private Map<String,Double> computeMedianForRecordsByCategories(List<Record> records){

        Map<String,ArrayList<Double>> map = new HashMap<String, ArrayList<Double>>();

        for (Record record : records){
            ArrayList<Double> list = map.get(record.getCategory());
            if (null == list){
                list = new ArrayList<Double>();
                list.add(record.getPrice());
                map.put(record.getCategory(),list);
            }else{
                list.add(record.getPrice());
            }
        }

        Map<String,Double> mapResult = new HashMap<String, Double>();

        for (String category: map.keySet()){
            ArrayList<Double> list = map.get(category);
            Collections.sort(list);
            double median = MedianUtils.computeMedian(list);
            mapResult.put(category,median);
        }

        return mapResult;

    }

    private void writeMedianMapToFile(String file, Map<String,Double> medianMap) throws IOException {

        BufferedWriter writer = null;

        try {
            writer = new BufferedWriter(new FileWriter(file, false));

            for(String category : medianMap.keySet()){
                writer.write(category+"\t"+medianMap.get(category));
                writer.newLine();
            }
        }finally {
            if(null != writer){
                writer.close();
            }
        }

    }


    private List<Record> extractRecordsFromFile(String file) throws IOException{

        List<Record> list = new ArrayList<Record>();
        BufferedReader br = new BufferedReader(new FileReader(file));

        try {
            // first line is the header
            String line = br.readLine();

            while (line != null) {
                Record record = parseInputAsRecord(line);
                if(null != record)
                    list.add(record);
                line = br.readLine();
            }

        } finally {
            br.close();
        }

        return list;
    }

    private Record parseInputAsRecord(String line){

        String productCategory;
        double productPrice;
        Record record = null;

        String[] input = line.split("\t");

        if(input.length != 6){
            log.error("INPUT_DATA_FORMAT_ERROR " + line);

        }else{
            try {
                productCategory = input[3];
                productPrice = Double.parseDouble(input[4]);
                record = new Record(productCategory,productPrice);
            }catch (Exception e){
                log.error("PRICE_PARSE_ERROR " + input[4].toString());
            }
        }

        return record;

    }


}
