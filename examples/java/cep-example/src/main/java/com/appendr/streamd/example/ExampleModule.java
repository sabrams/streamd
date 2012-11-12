package com.appendr.streamd.example;

import com.appendr.streamd.cep.CEPModule;
import com.appendr.streamd.sink.Sink;
import com.appendr.streamd.sink.StdOutSink;
import scala.Option;
import scala.Some;

public class ExampleModule extends CEPModule {
    private Option<Sink> sink = new Some<Sink>(new StdOutSink());
    
    public ExampleModule() {
        super(Thread.currentThread().getContextClassLoader().getResource("test.epl").toString());
    }
    
    @Override
    public Option<Sink> sink() {
        return sink;
    }
}
