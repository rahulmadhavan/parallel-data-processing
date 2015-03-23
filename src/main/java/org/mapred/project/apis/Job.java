package org.mapred.project.apis;

import org.apache.hadoop.fs.Path;

/**
 * Created by rahulmadhavan on 3/23/15.
 */
public interface Job{

    void setInputPath(Path path);

    void setOutputPath(Path path);

    void setMapper(Class<? extends Mapper> mapperClass);

    void setCombiner(Class<? extends Reducer> combinerClass);

    void setReducer(Class<? extends Reducer> reducerClass);

    boolean isStandalone();

    void setStandalone(boolean standalone);

    boolean execute();

}
