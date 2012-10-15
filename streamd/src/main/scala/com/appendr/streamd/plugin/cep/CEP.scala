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
package com.appendr.streamd.plugin.cep

import com.appendr.streamd.stream.{StreamTuple, StreamProc}
import com.appendr.streamd.store.Store
import com.appendr.streamd.sink.Sink
import com.appendr.streamd.conf.{ConfigurableResource, Configuration}
import com.appendr.streamd.plugin.PluginContextAware
import com.espertech.esper.client.{ Configuration => EsperConf }
import com.espertech.esper.client.deploy.DeploymentOptions
import com.espertech.esper.client.EPServiceProvider
import com.espertech.esper.client.EPServiceProviderManager
import java.util.{Date, UUID}
import java.net.URL
import collection.mutable
import com.espertech.esper.client.EPStatement
import java.lang.annotation.Annotation
import com.appendr.streamd.util.Reflector
import com.espertech.esper.client.deploy.DeploymentInformation
import com.espertech.esper.client.deploy.DeploymentState
import com.espertech.esper.client.deploy.EPDeploymentAdmin
import com.appendr.streamd.controlport.TelnetHandler

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class CEP extends StreamProc with PluginContextAware {
    private val engine: Engine = new Engine

    def proc(t: StreamTuple): Option[StreamTuple] = {
        engine.event(t._1, t._3)
        None
    }

    def coll(t: StreamTuple) {
        // no-op
    }

    def open(conf: Option[Configuration]) {
        engine.ctx = context
        engine.open(conf)
    }

    def close() {
        engine.close()
    }

    class Engine extends ConfigurableResource {
        private[CEP] var ctx: (Option[Sink], Option[Store]) = (None, None)
        private val cfg: EsperConf = new EsperConf
        private val options = {
            val opts = new DeploymentOptions
            opts.setCompile(true)
            opts.setFailFast(true)
            opts.setRollbackOnFail(true)
            opts
        }

        private val esper: EPServiceProvider =
            EPServiceProviderManager.getProvider(UUID.randomUUID().toString, cfg)

        import com.appendr.streamd.conf.Configuration
        def open(config: Option[Configuration]) {
            val urlstr = config.get.getString("cep.config")
            if (urlstr != null && urlstr.isDefined) cfg.configure(new URL(urlstr.get))
            else cfg.configure()

            esper.initialize()

            val modules = config.get.getList("cep.modules")
            if (modules != null) modules.foreach(m => activate(load(new URL(m))))
        }

        def close() {
            esper.destroy()
        }

        def event(eventName: String, event: Map[_, _]) {
            try {
                import collection.JavaConversions.mapAsJavaMap
                esper.getEPRuntime.sendEvent(event, eventName)
            }
            catch {
                case e: Exception => e.printStackTrace()
            }
        }

        def getInstanceId = {
            esper.getURI
        }

        def getModule(id: String): ModuleInfo = {
            getInfo(admin.getDeployment(id))
        }

        def getModules: List[ModuleInfo] = {
            deploymentInfo.map(i => getInfo(i))
        }

        def load(url: URL) = {
            admin.add(admin.read(url))
        }

        def parse(m: String) = {
            admin.add(admin.parse(m))
        }

        def activate(id: String) {
            if (!isDeployed(id)) {
                // activate dependencies
                val info = admin.getDeployment(id)
                val uses = {
                    if(info != null) info.getModule.getUses
                    else null
                }

                if (uses != null) {
                    for (s <- uses.toArray) {
                        if (!admin.isDeployed(s.toString))
                            throw new RuntimeException(String.format("module: %s is not deployed.", s.toString))
                    }
                }

                // activate module
                val res = admin.deploy(id, options)

                // add subscribers
                import collection.JavaConversions.asScalaBuffer
                val si: mutable.Buffer[EPStatement] = res.getStatements
                for (s <- si) {
                    s.getAnnotations.foreach {
                        a: Annotation => {
                            if (a.isInstanceOf[Subscriber]) {
                                val sub = Reflector[Object](a.asInstanceOf[Subscriber].value)
                                if (sub.isInstanceOf[PluginContextAware]) {
                                    sub.asInstanceOf[PluginContextAware].context = ctx
                                }
                                s.setSubscriber(sub)
                            }
                        }
                    }
                }
            }
        }

        private def getInfo(di: DeploymentInformation): ModuleInfo = {
            new ModuleInfo {
                val isActive = di.getState.equals(DeploymentState.DEPLOYED)
                val deployed = di.getAddedDate.getTime
                val name = di.getModule.getName
                val statements = di.getItems.map(i => i.getExpression).toList
                val id = di.getDeploymentId
            }
        }

        private def isDeployed(id: String): Boolean = {
            val inf: DeploymentInformation = admin.getDeployment(id)
            (inf != null) && (admin.isDeployed(inf.getModule.getName))
        }

        private def admin: EPDeploymentAdmin = {
            esper.getEPAdministrator.getDeploymentAdmin
        }

        private def deploymentInfo: List[DeploymentInformation] = {
            esper.getEPAdministrator.getDeploymentAdmin.getDeploymentInformation.toList
        }
    }
}

object DynaModule {
    def apply(statement: String, subscriber: Class[_]) = {
        new DynaModule(statement, subscriber)
    }
}

class DynaModule(
    val statement: String,
    val subscriber: Class[_],
    val name: Option[String] = None,
    val schema: Option[String] = None) {
    private val mname = name.getOrElse(UUID.randomUUID().toString)
    val epl = toString

    override def toString = {
        val sb = new StringBuilder("module ")
        sb.append(mname.replace("-", ".m")).append(";\n")
        sb.append("import ").append(classOf[Subscriber].getName).append(";\n")
        sb.append("import ").append(subscriber.getName).append(";\n")
        if (schema.isDefined) sb.append(schema.get)
        sb.append(String.format("@Name('%s')\n", name))
        sb.append("@Description('Dynamic Esper Module')\n")
        sb.append(String.format("@Subscriber('%s')\n", subscriber.getName))
        sb.append(statement).append(";\n")
        sb.toString()
    }
}

abstract class ModuleInfo {
    val id: String
    val name: String
    val deployed: Date
    val statements: List[String]
    val isActive: Boolean
}

class ControlPort extends TelnetHandler {
    val module = "CEP"
    def commands = List("info", "list", "load", "unload", "activate")
    def command(cmd: Array[String]) = {
        cmd.head match {
            case "info" => doInfo(cmd.tail)
            case "list" => doList(cmd.tail)
            case "load" => doLoad(cmd.tail)
            case "unload" => doUnload(cmd.tail)
            case "activate" => doActivate(cmd.tail)
            case _ => "Unknown command: " + cmd.head
        }
    }

    def shutdown() {
    }

    private def doInfo(cmd: Array[String]) = {
        "Not implemented"
    }

    private def doList(cmd: Array[String]) = {
        "Not implemented"
    }

    private def doLoad(cmd: Array[String]) = {
        "Not implemented"
    }

    private def doUnload(cmd: Array[String]) = {
        "Not implemented"
    }

    private def doActivate(cmd: Array[String]) = {
        "Not implemented"
    }
}
