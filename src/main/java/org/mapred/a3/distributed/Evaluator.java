package org.mapred.a3.distributed;


import au.com.bytecode.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Evaluator {

    public void evaluate(String pFile, String cFile) throws IOException {
        CSVReader brp = new CSVReader(new FileReader(new File(pFile)));
        CSVReader brc = new CSVReader(new FileReader(new File(cFile)));
        int correct = 0;
        int wrong = 0;
        String[] lineP;
        String[] lineC;
        while((lineP = brp.readNext()) != null){
            lineC = brc.readNext();
            if(lineC[44].equals(lineP[44])){
                correct += 1;
            } else {
                wrong += 1;
            }
        }

        System.out.println("Correct Predictions: " + correct);
        System.out.println("Wrong Predictions: " + wrong);
        double accuracy = 100 * (double)correct / (correct+wrong) ;
        System.out.println("Accuracy "+ accuracy + " %");

    }

    public static void evaluator(String predictedFile, String checkFile) throws IOException {
        Evaluator evaluator = new Evaluator();
        System.out.println("Evaluating the predictions..");
        evaluator.evaluate(predictedFile, checkFile);
    }
}