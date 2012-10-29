package com.appendr.streamd.util

import com.appendr.streamd.module.Module
import com.appendr.streamd.cep.CEP
import com.appendr.streamd.sink.StdOutSink
import com.appendr.streamd.store.Store
import java.util.concurrent.ConcurrentHashMap
import sun.misc.AtomicLong

/**
 * Created with IntelliJ IDEA.
 * User: bgordon
 * Date: 10/25/12
 * Time: 4:00 PM
 * To change this template use File | Settings | File Templates.
 */
class TestCEPModule extends Module {
    val cfg = Thread.currentThread().getContextClassLoader.getResource("esper.cfg.xml")
    val mod = Thread.currentThread().getContextClassLoader.getResource("test.epl")

    private val cep = Some(new CEP(Some(cfg.toString), Some(List(mod.toString))))
    private val hash = Some(new HashStore)
    private val out = Some(new StdOutSink)
    def proc = cep
    def sink = out
    def store = hash
    def cport = None
}

class HashStore extends Store {
    private val map = new ConcurrentHashMap[String, AtomicLong]

    def set(key: String, value: Any) {
        map.put(key, value.asInstanceOf[AtomicLong])
    }

    def get(key: String) = {
        val r = map.get(key)
        if (r != null) Some(r)
        else None
    }

    def get(key: (String, String)) = {
        throw new UnsupportedOperationException
    }

    def rem(key: String) = {
        val r = map.remove(key)
        if (r != null) Some(r)
        else None
    }

    def has(key: String) = {
        map.containsKey(key)
    }

    def get(keys: String*) = {
        keys.map(s => {
            val r = map.get(s)
            if (r != null) Some(r)
            else None
        }).toList
    }

    def add(key: String, value: (_, Any)) {
        throw new UnsupportedOperationException
    }

    def rem(key: (String, String)) = {
        throw new UnsupportedOperationException
    }

    def has(key: (String, String)) = {
        throw new UnsupportedOperationException
    }

    def inc(key: String) {
        throw new UnsupportedOperationException
    }

    def inc(key: (String, String)) {
        throw new UnsupportedOperationException
    }

    def inc(key: String, i: Int) {
        throw new UnsupportedOperationException
    }

    def inc(key: (String, String), i: Int) {
        throw new UnsupportedOperationException
    }

    def close() {}

    def open() {}

    def set(key: (String, String), value: Any) {
        throw new UnsupportedOperationException
    }
}