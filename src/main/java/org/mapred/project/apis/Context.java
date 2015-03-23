package org.mapred.project.apis;

import java.util.Map;

/**
 * Created by rahulmadhavan on 3/23/15.
 */
public interface Context{

    Map<String,Object> getContext();

    void setContext(String key, Object Value);

}
