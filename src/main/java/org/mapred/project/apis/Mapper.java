package org.mapred.project.apis;

import java.util.List;
import java.util.Map;

/**
 * Created by rahulmadhavan on 3/23/15.
 */


/**
 *
 *
 * @param <KEY_OUT>
 * @param <VALUE_OUT>
 */
public interface Mapper<KEY_OUT,VALUE_OUT> {

    /**
     * This method is invoked once before the map hase
     *
     */
    void setup();

    Map<KEY_OUT,List<VALUE_OUT>> map(Long key,String value, RaspContext raspContext);

    void cleanup();
}


