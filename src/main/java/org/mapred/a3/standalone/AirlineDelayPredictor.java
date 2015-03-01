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



public class AirlineDelayPredictor{
    //Read file
    //parse file and find the data

    public static void main (String[] args) throws Exception{

        if(args.length != 1) {
            System.exit(1);
        }

        //to test
        String folderPath = "/Users/rahulmadhavan/Documents/developer/ms/parallel/assignments/a3/a3data";

        //String folderPath = args[0];

        //preprocess the training and test files
        String srcTrainData = folderPath + "/data.csv";
        String srcTestData = folderPath + "/check.csv";

        String storeCompressedTrainData =  folderPath +  "/data_compressed.txt";
        String storeCompressedTestData = folderPath +  "/check_compressed.txt";

        preprocess(srcTrainData, storeCompressedTrainData);
        preprocess(srcTestData, storeCompressedTestData);

        Instances train = buildInstancesFromText(storeCompressedTrainData);
        //Form Decision tree
        FilteredClassifier classifier = buildDecisionTree(train);

        //Evaluate
        Instances test = buildInstancesFromText(storeCompressedTestData);
        Evaluation evaluation = performEvaluation(classifier, train, test);
        System.out.println(evaluation.toSummaryString());
    }


    public static void preprocess(String rpath,String wpath) throws IOException{

        CSVParser csvParser = new CSVParser(new FileReader(new File(rpath)), CSVFormat.DEFAULT);
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(wpath)));
        preprocessWriteLegend(bw);

        for (final CSVRecord record : csvParser) {
            String compressedLine = preprocessBuildCompressedString(record);
            // If there is missing data, the function preprocessBuildCompressedString will return null
            if (compressedLine != null){
                bw.write(compressedLine+"\n");
                bw.flush();
            }

        }

        bw.close();
        csvParser.close();
    }

    private static void preprocessWriteLegend(BufferedWriter bw) throws IOException {
        String legend = "@relation airline \n"+
                "@attribute month NUMERIC \n"+
                "@attribute dayofweek NUMERIC \n"+
                "@attribute origin NUMERIC \n"+
                "@attribute destination NUMERIC \n"+
                "@attribute departuredelay NUMERIC \n"+
                "@attribute distancegroup NUMERIC \n"+
                "@attribute Delay {0,1} \n"+
                "@data \n";
        bw.write(legend);
        bw.flush();
    }

    private static String preprocessBuildCompressedString(CSVRecord record)  {

        int[] indexColumns = {2,4,11,20,31,55,44};
        StringBuilder sb = new StringBuilder();
        String comma ="";

        for(int index : indexColumns){

                if(! StringUtils.isEmpty(record.get(index))){
                    sb.append(comma).append(record.get(index));
                    comma = ",";
                }else{
                    return null;
                }

        }

        return new String(sb);
    }

    /* Builds an instance from text file and sets the last column as class*/
    private static Instances buildInstancesFromText(String filePath) throws IOException{

        BufferedReader datafile =  new BufferedReader(new FileReader(filePath));
        Instances instance = new Instances(datafile);
        instance.setClassIndex(instance.numAttributes() - 1);
        return instance;
    }

    private static FilteredClassifier buildDecisionTree(Instances train) throws Exception{

        J48 j48 = new J48();
        FilteredClassifier classifier = new FilteredClassifier();
        classifier.setClassifier(j48);
        classifier.buildClassifier(train);
        return classifier;
    }

    private static Evaluation performEvaluation(FilteredClassifier classifier, Instances train, Instances test) throws Exception {
        Evaluation evaluation = new Evaluation(train);
        evaluation.evaluateModel(classifier, test);
        return evaluation;
    }


}