package org.mapred.project.apis;

/**
 * Created by rahulmadhavan on 3/23/15.
 */


/**
 * ReducerTask represents a {@link Reducer} running for a single input split
 *
 */
public interface ReducerTask extends Task {

    /**
     * sets the ReducerClass for the given Task
     * the {@link Task#execute()} should invoke the map of the given reducerClass Class
     *
     * @param reducerClass
     */
    void setReducerClass(Class<? extends Reducer> reducerClass);

    /**
     * returns the {@link Reducer} for the Task
     *
     * @return {@link Class<? extends Reducer>}
     */
    Class<? extends Reducer> getReducerClass();

}
