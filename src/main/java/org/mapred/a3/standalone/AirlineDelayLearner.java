package org.mapred.a3.standalone;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.meta.FilteredClassifier;
import weka.core.Instances;

import java.io.*;

/**
 * Created by rahulmadhavan on 3/1/15.
 */
public class AirlineDelayLearner {

    private String dataFileName;

    private static final String MODEL_FILE = "/model.m";
    private static final String CLIPPED_DATA_FILE = "/data_c.m";

    public AirlineDelayLearner(String dataFile){
        this.dataFileName = dataFile;
    }

    public void learn() throws Exception{

        File dataFile = new File(this.dataFileName);
        String path =  dataFile.getAbsolutePath().
                substring(0,dataFile.getAbsolutePath().lastIndexOf(File.separator));

        String clippedDataPath = path+ CLIPPED_DATA_FILE;
        String modelPath = path+ MODEL_FILE;


        EvaluationUtils.clipData(dataFileName,clippedDataPath,false);

        Instances train = EvaluationUtils.buildInstancesFromText(clippedDataPath);
        FilteredClassifier classifier = EvaluationUtils.buildDecisionTree(train);

        ObjectOutputStream modelWriter = new ObjectOutputStream(new FileOutputStream(modelPath));
        modelWriter.writeObject(classifier);
        modelWriter.flush();
        modelWriter.close();

    }

    public static class AirlineDelayModel{

        private FilteredClassifier classifier;
        private String modelFile;

        private AirlineDelayModel(String modelFile) throws Exception{
            this.modelFile = modelFile;
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(modelFile));
            classifier =  (FilteredClassifier) ois.readObject();
            ois.close();
        }

        public static FilteredClassifier fetchClassifier(String modelFile) throws Exception{
            return new AirlineDelayModel(modelFile).classifier;
        }

    }

}
