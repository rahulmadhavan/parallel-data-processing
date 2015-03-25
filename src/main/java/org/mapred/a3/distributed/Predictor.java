package org.mapred.a3.distributed;


import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import weka.classifiers.meta.FilteredClassifier;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

import java.io.*;

public class Predictor {


    public void predict(String modelFile, String inputFile, String outputFile) throws Exception {
        //BufferedReader br = new BufferedReader(new FileReader(inputFile));
        CSVReader br = new CSVReader(new FileReader(new File(inputFile)));
        FileOutputStream fos = new FileOutputStream(outputFile);
        //BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        CSVWriter bw = new CSVWriter(new FileWriter(new File(outputFile)));
        String[] line;

        //This line reads the classifier from file. Mit please implement this method.
        FilteredClassifier clf = readClassifier(modelFile);
        System.out.println("Classifier deserailized... Now predicting..");
        //Now we create the weka instances dataset.
        //First we need to tell weka how the data looks. For that, we need to create Attributes and put it in a fastVector obj.
        FastVector attributesVector = new FastVector(6);
        Attribute att1 = new Attribute("month");
        Attribute att2 = new Attribute("dayOfWeek");
        Attribute att3 = new Attribute("origin");
        Attribute att4 = new Attribute("dest");
        Attribute att5 = new Attribute("depDelay");
        Attribute att6 = new Attribute("distGroup");

        // Declare the class attribute along with its values
        FastVector fvClassVal = new FastVector(2);
        fvClassVal.addElement("0");
        fvClassVal.addElement("1");
        Attribute classAttribute = new Attribute("theClass", fvClassVal);


        //Add all the attributes to the attributeList.
        attributesVector.addElement(att1);
        attributesVector.addElement(att2);
        attributesVector.addElement(att3);
        attributesVector.addElement(att4);
        attributesVector.addElement(att5);
        attributesVector.addElement(att6);
        //... Finally add the class values
        attributesVector.addElement(classAttribute);

        //Create the instances object:
        Instances dataSet = new Instances("someData", attributesVector, 6);

        //Set the class index
        dataSet.setClassIndex(6);

        Instance singleInstance;
        String pred;
        /* Read the file line by line and predict for each line. */
        while ((line = br.readNext()) != null) {
            singleInstance = createInstanceFromList(line, attributesVector);
            singleInstance.setDataset(dataSet);
            pred = Integer.toString((int) clf.classifyInstance(singleInstance));
            line[44] = pred;
            bw.writeNext(line);
        }
        br.close();
        bw.close();

    }

    private FilteredClassifier readClassifier(String modelFile) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader(new File(modelFile)));
        String line = br.readLine();
        String lineArray[] = line.split(" ");
        byte[] byteArray = new byte[lineArray.length];
        for (int i = 0; i < lineArray.length; i++) {
            String s = lineArray[i];
            byteArray[i] = (byte) ((Character.digit(s.charAt(0), 16) << 4)
                    + Character.digit(s.charAt(1), 16));
        }
        return (FilteredClassifier) SerializationUtils.deserialize(byteArray);
    }

    private Instance createInstanceFromList(String[] attributes, FastVector attributesVector) {
        //Creates a weka instance with one line of data
        Instance instance = new Instance(7);
        instance.setValue((Attribute) attributesVector.elementAt(0), Double.parseDouble(attributes[2]));
        instance.setValue((Attribute) attributesVector.elementAt(1), Double.parseDouble(attributes[4]));
        instance.setValue((Attribute) attributesVector.elementAt(2), Double.parseDouble(attributes[11]));
        instance.setValue((Attribute) attributesVector.elementAt(3), Double.parseDouble(attributes[20]));
        instance.setValue((Attribute) attributesVector.elementAt(4), Double.parseDouble(attributes[31]));
        instance.setValue((Attribute) attributesVector.elementAt(5), Double.parseDouble(attributes[55]));
        instance.setValue((Attribute) attributesVector.elementAt(6), Double.parseDouble(attributes[44]));
        return instance;
    }

    public static void predictor(String outputOfReducer, String predictFile, String outputFile) throws Exception {
        Predictor predictor = new Predictor();
        predictor.predict(outputOfReducer, predictFile, outputFile);
        System.out.println("Prediction complete");
        //predictor.predict("/media/mit/Windows/ubuntu/a3data/output/part-00000", "/media/mit/Windows/ubuntu/a3data/predict.csv", "/media/mit/Windows/ubuntu/a3data/output/out.csv");
    }

}