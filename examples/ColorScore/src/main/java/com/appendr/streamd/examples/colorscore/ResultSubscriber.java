/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.appendr.streamd.examples.colorscore;

import com.appendr.streamd.plugin.PluginContextAware;
import com.appendr.streamd.sink.Sink;
import com.appendr.streamd.store.Store;
import scala.Option;
import scala.Tuple2;

/**
 *
 * @author bgordon
 */
public class ResultSubscriber implements PluginContextAware {
    private Tuple2<Option<Sink>, Option<Store>> context;
    
    public void update(String color, long count, double score) {
        String s = "-----> " + count + " " + color + " has an average score of " + score;
        System.out.println(s);
    }

    @Override
    public Tuple2<Option<Sink>, Option<Store>> context() {
        return context;
    }

    @Override
    public void context_$eq(Tuple2<Option<Sink>, Option<Store>> context) {
        this.context = context;
    }
}
