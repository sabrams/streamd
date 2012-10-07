package com.appendr.streamd.examples.colorscore;

import com.appendr.streamd.component.Server;
import com.appendr.streamd.conf.Configuration;
import com.appendr.streamd.connector.ByteArrayToStringInput;
import com.appendr.streamd.connector.FileConnector;
import com.appendr.streamd.connector.InputTransformer;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.net.URL;

/**
 * Using this as a driver, it is not a unit test
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        try {
            URL datafile = Thread.currentThread().getContextClassLoader().getResource("data.csv");
            Configuration cfg = Configuration.fromResource("app.conf");
            Server s = Server.apply(cfg);
            s.start();
            InputTransformer<String> ixf = new StreamTransformer();
            FileConnector connector = new FileConnector(cfg, ByteArrayToStringInput.apply(ixf));
            connector.start(new String[]{datafile.toString()});
            connector.connectorStop();
            Thread.sleep(5000);
            s.stop();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}