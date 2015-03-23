package org.mapred.project.apis;

import org.mapred.project.apis.Context;

import java.util.List;
import java.util.Map;

/**
 * Created by rahulmadhavan on 3/23/15.
 */
public interface Mapper<KEY_IN,VALUE_IN,KEY_OUT,VALUE_OUT> {

    void setup();

    Map<KEY_OUT,List<VALUE_OUT>> map(KEY_IN key,VALUE_IN value, Context context);

    void cleanup();
}


