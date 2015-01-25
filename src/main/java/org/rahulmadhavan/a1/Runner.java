package org.rahulmadhavan.a1;

import org.apache.hadoop.util.ToolRunner;
import org.rahulmadhavan.a1.drivers.V1MedianDriver;
import org.rahulmadhavan.a1.drivers.V2MedianDriver;
import org.rahulmadhavan.a1.drivers.V3MedianDriver;

/**
 * Created by rahulmadhavan on 1/25/15.
 */
public class Runner{

    public static void main(String[] args) throws Exception {

        if(args.length != 3){
            System.err.printf("Usage: %s [generic options] <version> <input> <output>\n", Runner.class.toString());
            ToolRunner.printGenericCommandUsage(System.err);
            return;
        }

        int version = Integer.parseInt(args[0]);
        int exitCode;

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
            default:
                System.err.printf("version should be either 1, 2, 3 or 4");
                ToolRunner.printGenericCommandUsage(System.err);
                return;
        }

        System.exit(exitCode);

    }

}
