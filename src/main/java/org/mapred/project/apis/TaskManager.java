package org.mapred.project.apis;

/**
 * Created by rahulmadhavan on 3/23/15.
 *
 * responsible for maintaining a queue of tasks.
 * tracking progress of tasks.
 * Reporting progress of task to JobManager
 * executing tasks in an ordered manner
 *
 */
public interface TaskManager {

    void submit(Task task);

    Task nextTask();

    boolean start();

    boolean stop();

}
