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
package com.appendr.streamd

import connector.InputTransformer
import component.Client
import com.appendr.streamd.conf.Configuration
import com.appendr.streamd.util.Reflector
import stream.{OneWay, TwoWay}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object Driver {
    val banner =
                   "----------------------------------------" +
            "\n" + "     _|_ _ _ _  _  _|"      +
            "\n" + "    _)|_| (-(_||||(_|"      +
            "\n" + "        stream daemon client driver v0.1" +
            "\n" + "----------------------------------------"

    def main(args: Array[String]) {
        val driver = new DriverShell(args.head)

        Runtime.getRuntime.addShutdownHook(new Thread() {
            override def run() {
                System.out.println("[Boot] Running shutdown hook...")
                driver.stop()
                System.out.println("[Boot] Driver exiting.")
            }
        })

        System.out.println("[Boot] Starting streamd client driver...")
        driver.start()
    }
}

private sealed class DriverShell(conf: String) {
    val config = Configuration.fromFile(conf)
    val driver = Reflector.newInstance[Driver](
        config.apply("streamd.driver.class"))(Thread.currentThread().getContextClassLoader)

    System.out.println(Driver.banner)

    def start() {
        driver.start(config.getSection("streamd.driver.params"))
    }

    def stop() {
        System.out.println("Preparing to stop...")
        driver.stop()
    }
}

trait Driver {
    def start(conf: Option[Configuration])
    def stop()
}

