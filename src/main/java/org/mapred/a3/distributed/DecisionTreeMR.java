package org.mapred.a3.distributed;


import java.io.*;
import java.util.Iterator;


import au.com.bytecode.opencsv.CSVReader;
import org.apache.hadoop.io.Text;
import weka.classifiers.Evaluation;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;
import weka.core.Instances;

import java.io.*;
import java.util.Iterator;

public class DecisionTreeMR {
    //Read file
    //parse file and find the data

    public static FilteredClassifier getDecisionTree(Iterator<Text> linesOfData) throws Exception{

        String storeCompressedTrainData = "/media/mit/Windows/ubuntu/a3data";
        preprocess(linesOfData, storeCompressedTrainData);
        Instances train = buildInstancesFromText(storeCompressedTrainData);
        //Form Decision tree
        FilteredClassifier classifier = buildDecisionTree(train);
        return classifier;

    }


    public static void preprocess(Iterator<Text> linesOfData,String wpath) throws IOException {

        StringBuilder builder = new StringBuilder();
        while(linesOfData.hasNext()){
            builder.append(linesOfData.next().toString());
        }
        CSVReader csv = new CSVReader(new StringReader(builder.toString()));
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(wpath)));

        preprocessWriteLegend(bw);

        String[] lineArray;

        while((lineArray = csv.readNext()) !=null){
            String compressedLine = preprocessBuildCompressedString(lineArray);
            // If there is missing data, the function preprocessBuildCompressedString will return null
            if (compressedLine != null){
                bw.write(compressedLine+"\n");
                bw.flush();
            }
        }
        bw.close();
        csv.close();
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

    public static String preprocessBuildCompressedString(String[] lineArray){

        int[] indexColumns = {2,4,11,20,31,55,44};
        StringBuilder sb = new StringBuilder();
        String comma ="";

        for(int index : indexColumns){
            try{
                if(! "".equals(lineArray[index])){
                    sb.append(comma + lineArray[index]);
                    comma = ",";
                }else{
                    return null;
                }
            }
            catch (Exception e){
                return null;
            }
        }

        return new String(sb);
    }

    /* Builds an instance from text file and sets the last column as class*/
    public static Instances buildInstancesFromText(String data) throws IOException{

        BufferedReader datafile = new BufferedReader(new StringReader(data));
        //BufferedReader datafile =  new BufferedReader(new FileReader(filePath));
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

    /**
     * *
     * @param classifier
     * @param train
     * @param test
     * @return
     * @throws Exception
     */
    public static Evaluation performEvaluation(FilteredClassifier classifier, Instances train, Instances test) throws Exception {
        Evaluation evaluation = new Evaluation(train);
        evaluation.evaluateModel(classifier, test);
        return evaluation;
    }

    /*public static Instances buildInstancesFromCSV(String data) throws IOException {
        CSVLoader loader = new CSVLoader();
        loader.setSource(new File(data));
        return loader.getDataSet();
    }*/
}