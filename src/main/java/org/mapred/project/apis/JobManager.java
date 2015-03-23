package org.mapred.project.apis;

/**
 * Created by rahulmadhavan on 3/23/15.
 */

import java.util.List;

/**
 * responsible for maintaining queue of jobs
 * and executing jobs in specified order. Executing a job includes, splitting the job into
 * multiple tasks and submitting task to task manager. The JobManager is responsible for
 * creating and executing the tasks of a job in the required sequence and keeping
 * track of the progress of the job
 *
 *
 */
public interface JobManager {

    void submit(Job job);

    Job nextJob();

    List<Task> getTasksForJob(Job job);

    boolean start();

    boolean stop();

}
