//package org.mapred.project.impl;
//
//import org.apache.hadoop.fs.Path;
//import org.mapred.project.apis.Job;
//import org.mapred.project.apis.JobManager;
//import org.mapred.project.apis.Mapper;
//import org.mapred.project.apis.Reducer;
//
///**
// * Created by rahulmadhavan on 3/23/15.
// */
//public class JobImpl implements Job {
//
//    Class<? extends Mapper> mapperClass;
//    Class<? extends Reducer> combinerClass;
//    Class<? extends Reducer> reducerClass;
//    JobManager jobScheduler;
//    boolean standalone;
//
//    @Override
//    public void setInputPath(Path path) {
//
//    }
//
//    @Override
//    public void setOutputPath(Path path) {
//
//    }
//
//    @Override
//    public void setMapper(Class<? extends Mapper> mapperClass) {
//        this.mapperClass = mapperClass;
//    }
//
//    @Override
//    public void setCombiner(Class<? extends Reducer> combinerClass) {
//        this.combinerClass = combinerClass;
//    }
//
//    @Override
//    public void setReducer(Class<? extends Reducer> reducerClass) {
//        this.reducerClass = reducerClass;
//    }
//
//    @OverrideX
//    public boolean isStandalone() {
//        return standalone;
//    }
//
//    @Override
//    public void setStandalone(boolean standalone) {
//        this.standalone = standalone;
//    }
//
//    @Override
//    public boolean execute() {
//        // submit job to JobScheduler
//        return true;
//    }
//
//
//}
