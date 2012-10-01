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

import component.{ServerComponent, Server}
import conf.Configuration
import java.io.File
import network.netty.NettyTextServer
import com.appendr.streamd.network.TelnetNetworkHandler

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object Main {
    val banner =
               "--------------------------" +
        "\n" + "     _|_ _ _ _  _  _|"      +
        "\n" + "    _)|_| (-(_||||(_|"      +
        "\n" + "        stream daemon v0.1" +
        "\n" + "--------------------------"

    val startupError =
        "[Boot] Starting streamd as daemon requires jsvc." +
        "To run in foreground start with Xdaemon."

    def main(args: Array[String]) {
        val main = new Main

        Runtime.getRuntime.addShutdownHook(new Thread() {
            override def run() {
                System.out.println("[Boot] Running shutdown hook...")
                if (main.app != null) {
                    main.stop()
                    main.destroy()
                }
                System.out.println("[Boot] Exiting.")
            }
        })

        args match {
            case Array(path, xdaemon) => {
                xdaemon match {
                    case "Xdaemon" => {
                        System.out.println("[Boot] Starting streamd in foreground, Xdaemon mode...")
                        main.init(args)
                        main.start()
                    }

                    case _ => {
                        System.out.println(startupError)
                        System.exit(0)
                    }
                }
            }

            case _ => {
                System.out.println(startupError)
                System.exit(0)
            }
        }
    }

}

sealed class Main {
    private var app: App = null

    System.out.println(Main.banner)

    def init(args: Array[String]) {
        app = new App(args)
    }

    def start() {
        app.start()
    }

    def stop() {
        System.out.println("Preparing to stop...")
    }

    def destroy() {
        app.stop()
    }
}

class App(private val args: Array[String]) {
    private var server: Server = null
    private var controlPort: ServerComponent = null

    def start() {
        val loc = args.size match {
            case 0 => System.getProperty("streamd.conf")
            case _ => args.head
        }

        System.out.println("[Boot] Config file is: " + loc)

        if (loc == null) {
            System.out.println("Error: must pass /path/to/conf as arg or set -Dstreamd.conf=/path/to/conf")
            System.exit(0)
        }

        val f = new File(loc)

        if (f.exists() == false) {
            System.out.println("Error: can not find file: " + loc)
            System.exit(0)
        }

        val config = Configuration.fromFile(loc)

        // dump the configuration
        System.out.println(config.map.toString())

        // create the node
        server = Server(config)

        val telnet = new TelnetNetworkHandler
        val plugins = config.getListAny("streamd.control.plugins")

        if (plugins != null) {
            // TODO: create and register plugins
        }

        // create the control port
        controlPort = NettyTextServer()

        // start your engines
        server.start()
        controlPort.start(config.apply("streamd.control.port").toInt, telnet)
    }

    def stop() {
        if (server != null) server.stop()
        if (controlPort != null) controlPort.stop()
    }
}

