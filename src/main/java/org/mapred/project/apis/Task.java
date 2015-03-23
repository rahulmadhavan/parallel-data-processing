package org.mapred.project.apis;

/**
 * Created by rahulmadhavan on 3/23/15.
 */
public interface Task {

    void getJob();

    void setTaskContext(TaskContext taskContext);

    TaskContext getTaskContext();

    boolean execute();

}
