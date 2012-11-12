package com.appendr.streamd.example;

import com.appendr.streamd.connector.InputTransformer;
import com.appendr.streamd.stream.StreamTuple;
import java.util.HashMap;
import java.util.Map;

public class EventTransformer implements InputTransformer<String> {
    @Override
    public StreamTuple transform(String i) {
        String[] csv = i.split(",");
        Map<String, Object> kvs = new HashMap<String, Object>();
        kvs.put("id", csv[0]);
        kvs.put("color", csv[1]);
        
        float score = Float.parseFloat(csv[2]) / 10;
        kvs.put("score", score);
        return new StreamTuple("colors", "color", kvs);
    }
}
