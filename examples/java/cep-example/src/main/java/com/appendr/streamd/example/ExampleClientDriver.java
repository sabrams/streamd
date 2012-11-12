package com.appendr.streamd.example;

import com.appendr.streamd.ClientDriver;
import com.appendr.streamd.conf.Configuration;
import com.appendr.streamd.connector.ByteArrayToStringInput;
import com.appendr.streamd.connector.Connector;
import com.appendr.streamd.connector.FileConnector;
import scala.Option;

public class ExampleClientDriver implements ClientDriver {
    private Connector<byte[]> connector;

    @Override
    public void start(Option<Configuration> cfg) {
        connector = new FileConnector(cfg.get(), new ByteArrayToStringInput(new EventTransformer()));
        Option<String> uri = cfg.get().getString("streamd.driver.params.file");
        connector.start(new String[]{uri.get()});
    }

    @Override
    public void stop() {
        connector.stop();
    }
}
