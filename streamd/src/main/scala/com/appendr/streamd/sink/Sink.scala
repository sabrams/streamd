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
package com.appendr.streamd.sink

import com.appendr.streamd.conf.{Configuration, ConfigurableResource}
import java.io.FileOutputStream

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

trait Sink extends ConfigurableResource {
    def out[T](msg: T)
}

class FileSink extends Sink {
    private var stream: Option[FileOutputStream] = None

    def open(config: Option[Configuration]) {
        if (config.isDefined) {
            val path: Option[String] = config.get.getString("streamd.sink.path")
            if (path.isDefined) stream = Some(new FileOutputStream(path.get))
        }
    }

    def close() {
        if (stream.isDefined) {
            stream.get.flush()
            stream.get.close()
        }
    }

    def out[T](msg: T) {
        if (stream.isDefined) stream.get.write(msg.toString.getBytes)
    }
}
