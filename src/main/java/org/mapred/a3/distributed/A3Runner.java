package org.mapred.a3.distributed;


import java.io.IOException;
import java.util.Scanner;

import java.io.IOException;
import java.util.Scanner;

/**
 * Used to build, predict and evaluate the model
 */
public class A3Runner {
    public static void main(String args[]) throws Exception {
        if(args.length != 1){
            System.out.println("Terminating the program as incorrect number of arguments specified. input arguments can be either -learn, -predict or -check.");
            return;
        }
        String option = args[0]; // it could be either of -learn, -predict, -check
        Scanner scanner = new Scanner(System.in);
        if (option.equals("-learn")) {
            learnModel();
        } else if (option.equals("-predict")) {
            predictModel();
        } else if (option.equals("-check")) {
            checkModelForAccuracy();
        } else{
            System.out.println("Terminating the program as incorrect number of arguments specified. input arguments can be either -learn, -predict or -check.");
        }
    }

    /**
     * Runs the evaluator against check file*
     * @throws java.io.IOException
     */
    private static void checkModelForAccuracy() throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the below parameters: \n 1.Location of check file for evaluation");
        String checkFile = scanner.nextLine(); // check.csv

        System.out.println("2. Location of the predict file");
        String predictedFile = scanner.nextLine(); // predict_output.csv

        Evaluator.evaluator(predictedFile, checkFile);
    }

    /**
     * Runs the predictor*
     * @throws Exception
     */
    private static void predictModel() throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the below parameters: \n 1. Location of the test (predict) file");
        String testFile = scanner.nextLine();//predict.csv

        System.out.println("2. Filename where predictions would be written");
        String predictedFile = scanner.nextLine(); //predict_output.csv

        System.out.println("3. Location of the output of reducers");
        String reducerOutput = scanner.nextLine(); //part-00000

        Predictor.predictor(reducerOutput, testFile, predictedFile);
    }

    /**
     * Builds the model*
     * @throws Exception
     */
    private static void learnModel() throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the below parameters: \n 1. Location of the dataset to generate the model");
        String mapperInput = scanner.nextLine();//data.csv

        System.out.println("2. Location to write the output of reducers");
        String reducerOutput = scanner.nextLine(); //output


        A3.job(mapperInput, reducerOutput);
    }
}
