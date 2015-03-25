package org.mapred.project.apis;

import java.util.Map;

/**
 * Created by rahulmadhavan on 3/23/15.
 *
 */


/**
 * Context is a an object used by a {@link Mapper} or {@link Reducer}
 * it represents the state of the {@link Job} for which the
 * {@link Mapper} or {@link Reducer} or reducer is being run.
 *
 */
public interface RaspContext {

    /**
     * sets a context variable <code> key </code> with value as <code>value</code>
     * for the job under which it is being used
     *
     * @param key
     * @param value
     */
    void setContextVariable(String key, Object value);

    /**
     * gets the value for the given context variable in the given context
     *
     * @param key
     * @return the value for the given key, in the form of Object, the user is responsible for
     *          typecasting the value to its original type.
     */
    Object getContextValue(String key);


}
