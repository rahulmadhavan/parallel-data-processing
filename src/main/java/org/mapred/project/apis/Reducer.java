package org.mapred.project.apis;

import java.util.List;
import java.util.Map;

/**
 * Created by rahulmadhavan on 3/23/15.
 */


public interface Reducer<KEY_IN,VALUE_IN,KEY_OUT,VALUE_OUT> {

    void setup();

    Map<KEY_OUT,List<VALUE_OUT>> reduce(KEY_IN key,List<VALUE_IN> value);

    void cleanup();

}
