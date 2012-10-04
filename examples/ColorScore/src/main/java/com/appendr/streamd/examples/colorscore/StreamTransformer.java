/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.appendr.streamd.examples.colorscore;

import com.appendr.streamd.connector.InputTransformer;
import com.appendr.streamd.stream.StreamTuple;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author bgordon
 */
public class StreamTransformer implements InputTransformer<String> {
    @Override
    @SuppressWarnings("unchecked")
    public StreamTuple transform(String i) {
        String[] sa = i.split(",");
        int score = Integer.parseInt(sa[2]);
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("user", sa[0]);
        m.put("color", sa[1]);
        m.put("score", Integer.parseInt(sa[2]));
        return new StreamTuple("colors", "color", m);
    }
}
