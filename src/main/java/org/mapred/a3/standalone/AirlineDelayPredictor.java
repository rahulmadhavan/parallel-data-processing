package org.mapred.a3.standalone;

import weka.classifiers.meta.FilteredClassifier;
import weka.core.Instances;

import java.io.*;

/**
 * Created by rahulmadhavan on 3/1/15.
 */
public class AirlineDelayPredictor {


    private String toPredictFile;
    private String modelFile;

    private static final String CLIPPED_PREDICT_FILE = "/predict_c.txt";
    private static final String CLIPPED_PREDICTED_FILE = "/predicted_c.txt";


    public AirlineDelayPredictor(String toPredictFile,String modelFile){
        this.toPredictFile = toPredictFile;
        this.modelFile = modelFile;
    }

    public void predict() throws Exception{

        File dataFile = new File(this.toPredictFile);
        String path =  dataFile.getAbsolutePath().
                substring(0,dataFile.getAbsolutePath().lastIndexOf(File.separator));
        String clippedToPredictPath = path+ CLIPPED_PREDICT_FILE;
        String clippedToPredictedPath = path+ CLIPPED_PREDICTED_FILE;

        EvaluationUtils.clipData(toPredictFile,clippedToPredictPath, true);
        Instances toPredict = EvaluationUtils.buildInstancesFromText(clippedToPredictPath);
        Instances predicted = new Instances(toPredict);

        FilteredClassifier classifier = AirlineDelayLearner.AirlineDelayModel.fetchClassifier(this.modelFile);

        for (int i = 0; i < toPredict.numInstances(); i++) {
            double clsLabel = classifier.classifyInstance(toPredict.instance(i));
            predicted.instance(i).setClassValue(clsLabel);
        }

        // save labeled data
        BufferedWriter writer = new BufferedWriter(new FileWriter(clippedToPredictedPath));
        writer.write(predicted.toString());
        writer.newLine();
        writer.flush();
        writer.close();

    }

}
