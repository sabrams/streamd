/**
 * Copyright (C) 2011 apendr.com
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 *   _  _  _  _  _  _| _
 *  (_||_)|_)(/_| |(_||
 *     |  |
 */
package com.appendr.streamd.plugin

import com.appendr.streamd.stream.StreamProc
import com.appendr.streamd.sink.Sink
import com.appendr.streamd.store.Store
import com.appendr.streamd.conf.{Configuration, ConfigurableResource}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object StreamPlugin {
    def apply(proc: StreamProc, sink: Option[Sink], store: Option[Store]) = {
        new StreamPlugin(proc, sink, store)
    }
}

class StreamPlugin(
    val proc: StreamProc,
    val sink: Option[Sink],
    val store: Option[Store])
    extends ConfigurableResource {

    def close() {
        proc.close()
        if (sink.isDefined) sink.get.close()
        if (store.isDefined) store.get.close()
    }

    def open(config: Option[Configuration]) {
        proc.open(config.get.getSection("streamd.plugin.processor"))
        if (sink.isDefined) sink.get.open(config.get.getSection("streamd.plugin.sink"))
        if (store.isDefined) store.get.open(config.get.getSection("streamd.plugin.store"))
    }
}
