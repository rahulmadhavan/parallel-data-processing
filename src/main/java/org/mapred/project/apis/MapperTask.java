package org.mapred.project.apis;

/**
 * Created by rahulmadhavan on 3/23/15.
 */

/**
 * MapperTask represents a {@link Mapper} running for a single input split
 *
 */
public interface MapperTask extends Task {

    /**
     * sets the MapperClass for the given Task
     * the {@link Task#execute()} should invoke the map of the given mapperClass Class
     *
     * @param mapperClass
     */
    void setMapperClass(Class<? extends Mapper> mapperClass);

    /**
     * returns the {@link Mapper} for the Task
     *
     * @return {@link Class<? extends Mapper>}
     */
    Class<? extends Mapper> getMapperClass();

}
