package org.mapred.a3.standalone;

/**
 * Created by rahulmadhavan on 3/1/15.
 */
public class AirlineDelayRunner {

    public static void main(String[] args) {

        if(args.length < 2){
            inputError();
        }

        try {
            if(args[0].equals("-learn")){
                new AirlineDelayLearner(args[1]).learn();
            }else if(args[0].equals("-predict")){
                if(args.length < 3){
                    inputError();
                }

                new AirlineDelayPredictor(args[1],args[2]).predict();
            }else if(args[0].equals("-check")){
                if(args.length < 3){
                    inputError();
                }

                double accuracy = new AirlineDelayChecker(args[1],args[2]).check();
                System.out.println("Accuracy : " + accuracy);
            }else{
                System.out.println(args[0]);
                inputError();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void inputError() {
        System.out.println("Error : run as " +
                "java -jar [jar_file] -learn   [path to data_file] \n" +
                "java -jar [jar_file] -predict [path to model.m] [path to data_file] \n" +
                "java -jar [jar_file] -check   [path to predicted_data_file] [path to actual_data_file] \n");
        System.exit(-1);
    }
}
