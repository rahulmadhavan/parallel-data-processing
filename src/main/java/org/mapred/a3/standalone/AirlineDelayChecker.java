package org.mapred.a3.standalone;

import java.io.*;

/**
 * Created by rahulmadhavan on 3/1/15.
 */
public class AirlineDelayChecker {

    private String predicted;
    private String actual;
    private static final String CLIPPED_ACTUAL_FILE = "/check_c.csv";

    public AirlineDelayChecker(String predicted, String actual){
        this.predicted = predicted;
        this.actual = actual;
    }

    public double check() throws Exception{
        File dataFile = new File(this.predicted);
        String path =  dataFile.getAbsolutePath().
                substring(0,dataFile.getAbsolutePath().lastIndexOf(File.separator));
        String clippedActualFile= path+ CLIPPED_ACTUAL_FILE;
        EvaluationUtils.clipData(actual,clippedActualFile, false);

        return comparator(predicted,clippedActualFile);
    }

    public static double comparator(String predicted, String actual) throws FileNotFoundException {

        FileInputStream fis1 = new FileInputStream(predicted);
        BufferedReader br1 = new BufferedReader(new InputStreamReader(fis1));
        FileInputStream fis2 = new FileInputStream(actual);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(fis2));
        String line1;
        String line2;
        double count = 0;
        double match = 0;
        try{
            br1.readLine();
            br1.readLine();
            for(int i = 0; i<9; i++ ){
                br1.readLine();
                br2.readLine();
            }
            while (((line1 = br1.readLine()) != null)&&((line2 = br2.readLine()) != null)) {

                count++;
                if (line1.equals(line2))
                    match++;
            }
            br2.close();
            br1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (match/count *100);
    }
}
