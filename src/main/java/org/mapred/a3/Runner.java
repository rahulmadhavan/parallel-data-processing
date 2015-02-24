package org.mapred.a3;

import org.apache.hadoop.util.ToolRunner;
import org.mapred.a3.drivers.LearnerDriver;

/**
 * Created by rahulmadhavan on 2/24/15.
 */
public class Runner {

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new LearnerDriver(), args);
        System.exit(exitCode);
    }
}
