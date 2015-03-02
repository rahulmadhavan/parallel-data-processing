package org.mapred.a3.standalone;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import weka.classifiers.Evaluation;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;
import weka.core.Instances;

import java.io.*;

/**
 * Created by rahulmadhavan on 3/1/15.
 */
public class EvaluationUtils {

    public static void clipData(String inFile,String outFile,boolean isPredict) throws IOException {

        CSVParser csvParser = new CSVParser(new FileReader(new File(inFile)), CSVFormat.DEFAULT);
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outFile)));
        bw.write(getHeaders());
        bw.flush();


        for (final CSVRecord record : csvParser) {
            String compressedLine = clipRecord(record,isPredict);
            // If there is missing data, the function preprocessBuildCompressedString will return null
            if (compressedLine != null){
                bw.write(compressedLine+"\n");
                bw.flush();
            }

        }

        bw.close();
        csvParser.close();
    }


    private static String clipRecord(CSVRecord record,boolean isPredict)  {

        int[] indexColumns = new int[]{2,4,11,20,31,55,44};

        StringBuilder sb = new StringBuilder();
        String comma ="";

        for(int index : indexColumns){

            if(! StringUtils.isEmpty(record.get(index))){
                if(isPredict && index == 44){
                    sb.append(comma).append("?");
                }else{
                    sb.append(comma).append(record.get(index));
                }
                comma = ",";

            }else{
                return null;
            }

        }


        return new String(sb);
    }


    private static String getHeaders() {
        String headers = "@relation airline \n"+
                "@attribute month NUMERIC \n"+
                "@attribute dayofweek NUMERIC \n"+
                "@attribute origin NUMERIC \n"+
                "@attribute destination NUMERIC \n"+
                "@attribute departuredelay NUMERIC \n"+
                "@attribute distancegroup NUMERIC \n"+
                "@attribute Delay {0,1} \n"+
                "@data \n";

        return headers;

    }

    public static Instances buildInstancesFromText(String filePath) throws IOException{

        BufferedReader datafile =  new BufferedReader(new FileReader(filePath));
        Instances instance = new Instances(datafile);
        instance.setClassIndex(instance.numAttributes() - 1);
        return instance;
    }

    public static FilteredClassifier buildDecisionTree(Instances train) throws Exception{

        J48 j48 = new J48();
        FilteredClassifier classifier = new FilteredClassifier();
        classifier.setClassifier(j48);
        classifier.buildClassifier(train);
        return classifier;
    }

    public static Evaluation performEvaluation(FilteredClassifier classifier, Instances train, Instances test) throws Exception {
        Evaluation evaluation = new Evaluation(train);
        evaluation.evaluateModel(classifier, test);
        return evaluation;
    }
}
