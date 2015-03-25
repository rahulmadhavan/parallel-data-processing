package org.mapred.a3.distributed;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;


import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;
import weka.core.Instances;


public class A3 {
    /*
    Mapper class which parse the input file and outputs the values of the selected attributes
     */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

        int numberOfDecisionTrees = 1;
        int counter = 0;
        @Override
        public void configure(JobConf jobConf){
            try {
				/*setting number of decision tree in random forest*/
                this.numberOfDecisionTrees = Integer.parseInt(jobConf.get("numberOfDecisionTrees"));
            }catch (NumberFormatException e){
                System.out.println("Number format exception for numberOfDecisionTrees. Setting it to default value  of "+1);
            }
        }

        /**
         *
         * @param key line number
         * @param value line of data from csv
         * @param output decision tree number, extract all attributes that are required
         * @param reporter reporter
         * @throws java.io.IOException
         */
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            int[] attributeColumnIndex = {2,4,11,20,55,44};
            String compressedString = selectRequiredAttributes(value.toString(),attributeColumnIndex);
            //If there is missing data, getCompressedString will return null;
            if (compressedString != null && counter%3 ==0){
                output.collect(new IntWritable(this.numberOfDecisionTrees), new Text(compressedString));
            }
        }

        /**
         * *
         * @param lineOfData a single line of data
         * @param attributeColumnIndex index of the attributes to be selected
         * @return line of data with selected attributes only
         */
        private String selectRequiredAttributes(String lineOfData, int[] attributeColumnIndex) {
            String[] aLineOfData = lineOfData.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            StringBuilder sb = new StringBuilder();
            String comma ="";

            for(int columnIndex : attributeColumnIndex){
                try{
                    if(! com.google.common.base.Strings.isNullOrEmpty(aLineOfData[columnIndex])){
                        sb.append(comma + aLineOfData[columnIndex]);
                        comma = ",";
                    }else{
                        return null;  //Some missingData
                    }
                }
                catch (Exception e){
                    return null; //Some missingData
                }
            }

            return new String(sb);
        }

    }

    /**
     * Reduce takes as input a set of data and creates a decision tree and writes it as the output*
     */
    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, BytesWritable, NullWritable>{

        /**
         *
         * @param key decision tree number
         * @param values line of data with selected attribute
         * @param output ByteArray of decision tree, NullWritable
         * @param reporter reporter
         * @throws IOException
         */
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<BytesWritable, NullWritable> output, Reporter reporter) throws IOException {

            String singleString = formSingleString(values);
            BufferedReader br = new BufferedReader(new StringReader(singleString));
            Instances train = buildInstancesFromReader(br);
            try {
                FilteredClassifier classifier = buildDecisionTree(train);
                byte byteArray[] = SerializationUtils.serialize(classifier);
                BytesWritable bytesWritable =new BytesWritable(byteArray);
                output.collect(bytesWritable, NullWritable.get());

            }catch (Exception e){
                System.err.print("Reducer with key "+key+ " failed to build decision tree");
            }

        }

        /**
         * Builds a decision tree using training data
         * *
         * @param train training data as Instances
         * @return a decision tree as a FilteredClassifier
         * @throws Exception
         */
        private static FilteredClassifier buildDecisionTree(Instances train) throws Exception{

            J48 j48 = new J48();
            FilteredClassifier classifier = new FilteredClassifier();
            classifier.setClassifier(j48);
            classifier.buildClassifier(train);
            return classifier;
        }

        /**
         * Build instances from BufferedReader
         * *
         * @param br BufferedReader
         * @return Instances
         * @throws IOException
         */
        private Instances buildInstancesFromReader(BufferedReader br) throws IOException {
            Instances instances = new Instances(br);
            instances.setClassIndex(instances.numAttributes() - 1);
            return instances;
        }

        /**
         * Convert all the data into format accepted by weka to create a decision tree*
         * @param values Iterator of data
         * @return String with all data concatenated
         */
        public static String formSingleString(Iterator<Text> values){
            StringBuilder sb = new StringBuilder();

            String legend = "@relation airline \n"+
                    "@attribute month NUMERIC \n"+
                    "@attribute dayofweek NUMERIC \n"+
                    "@attribute origin NUMERIC \n"+
                    "@attribute destination NUMERIC \n"+
                    "@attribute distancegroup NUMERIC \n"+
                    "@attribute Delay {0,1} \n"+
                    "@data \n";
            sb.append(legend);
            while(values.hasNext()){
                sb.append(values.next().toString());
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    /**
     * Preprocessing + Running MR Job  *
     * *
     * @param mapperInputPath
     * @param reducerOutput
     * @throws Exception
     */
    public static void job(String mapperInputPath, String reducerOutput) throws Exception {

        long startTime = System.currentTimeMillis();


		/* Set up configuration */
        Configuration conf = new Configuration();

		/* Setup Job Configuration */
        JobConf jobConf = new JobConf(conf, A3.class);
        jobConf.setJobName("a3");

        jobConf.setOutputKeyClass(IntWritable.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.setMapperClass(A3.Map.class);
        jobConf.setReducerClass(A3.Reduce.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, new Path(mapperInputPath));
        FileOutputFormat.setOutputPath(jobConf, new Path(reducerOutput));

        JobClient.runJob(jobConf);

        System.out.println("Time to generate the model:"+(System.currentTimeMillis()-startTime)/1000f);

    }

    /**
     * Main class
     * @param args input args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        job(args[0],args[1]);
    }
}
