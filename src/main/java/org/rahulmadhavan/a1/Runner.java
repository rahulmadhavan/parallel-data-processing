package org.rahulmadhavan.a1;

import org.apache.hadoop.util.ToolRunner;
import org.rahulmadhavan.a1.drivers.*;

import java.util.Date;

/**
 * Created by rahulmadhavan on 1/25/15.
 */
public class Runner{

    public static void main(String[] args) throws Exception {


        int version = Integer.parseInt(args[0]);
        if(!isArgumentsValid(args)){
            return;
        }

        int exitCode;


        long start = new Date().getTime();


        switch (version) {
            case 1:
                exitCode = ToolRunner.run(new V1MedianDriver(), args);
                break;
            case 2:
                exitCode = ToolRunner.run(new V2MedianDriver(), args);
                break;
            case 3:
                exitCode = ToolRunner.run(new V3MedianDriver(), args);
                break;
            case 4:
                exitCode = ToolRunner.run(new V4MedianDriver(), args);
                break;
            case 5:
                exitCode = ToolRunner.run(new V5MedianDriver(), args);
                break;
            default:
                System.err.printf("version should be either 1, 2, 3, 4 or 5");
                ToolRunner.printGenericCommandUsage(System.err);
                return;
        }

        long end = new Date().getTime();
        System.out.println("Job took "+(end-start) + " milliseconds");


        System.exit(exitCode);

    }

    private static boolean isArgumentsValid(String[] args){

        String messageV123 = "Usage: %s [generic options] <version> <input> <output>\n";
        String messageV4 = "Usage: %s [generic options] N <version> <input> <output>\n";
        String messageV5 = "Usage: %s [generic options] SR[1 - 10] <version> <input> <output>\n";
        int numberOfArguments = 3;
        String errorMessage = messageV123;

        int version = Integer.parseInt(args[0]);

        if(version == 4){
            errorMessage = messageV4;
            numberOfArguments = 4;
        }else if (version == 5){
            errorMessage = messageV5;
            numberOfArguments = 4;
        }

        if(args.length != numberOfArguments){
            System.err.printf(errorMessage, Runner.class.toString());
            ToolRunner.printGenericCommandUsage(System.err);
            return false;
        }

        return true;

    }

}
