package com.appendr.streamd.example;

import com.appendr.streamd.module.ModuleContext;
import com.appendr.streamd.sink.Sink;
import com.appendr.streamd.store.Store;
import scala.Option;
import scala.Tuple2;

public class CustomSubscriber implements ModuleContext {
    private Tuple2<Option<Sink>, Option<Store>> tuple2;
    
    @Override
    public Tuple2<Option<Sink>, Option<Store>> moduleContext() {
        return tuple2;
    }

    @Override
    public void moduleContext_$eq(Tuple2<Option<Sink>, Option<Store>> tuple2) {
        this.tuple2 = tuple2;
    }
    
    public void update(String color, Long count, Double score) {
        if (tuple2._1.isDefined()) {
            tuple2._1.get().out(
            String.format("Color: %s, count: %d, score: %f", color, count, score));
        }   
    }
}
