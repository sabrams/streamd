package com.appendr.streamd.examples.colorscore;

import com.appendr.streamd.conf.Configuration;
import com.appendr.streamd.connector.ByteArrayToStringInput;
import com.appendr.streamd.connector.FileConnector;
import com.appendr.streamd.connector.InputTransformer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Configuration cfg = Configuration.fromFile(args[0]);
        InputTransformer<String> ixf = new StreamTransformer();
        FileConnector connector = new FileConnector(cfg, ByteArrayToStringInput.apply(ixf));
        connector.start(new String[]{args[1]});
        connector.connectorStop();
    }
}
