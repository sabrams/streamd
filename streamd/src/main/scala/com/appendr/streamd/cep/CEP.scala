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
package com.appendr.streamd.cep

import com.appendr.streamd.stream.{StreamTuple, StreamProc}
import com.appendr.streamd.store.Store
import com.appendr.streamd.sink.Sink
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
import com.appendr.streamd.module.{Module, ModuleContext}
import com.appendr.streamd.cep.CEP.{CEPService, Engine}
import com.appendr.streamd.network.services.Service

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object CEP {
    def apply(confFile: Option[String] = None, modFiles: Option[List[String]] = None) = {
        new CEP(confFile, modFiles)
    }

    class Engine {
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

        private[CEP] def init(config: Option[String]) {
            val defCfg = System.getProperty("streamd.cep.configuration")
            if (config.isDefined) cfg.configure(new URL(config.get))
            else if (defCfg != null && !defCfg.isEmpty) cfg.configure(new URL(defCfg))
            else cfg.configure()
            esper.initialize()
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

        def close() {
            esper.destroy()
        }

        def getInstanceId = esper.getURI
        def getModule(id: String): CEPModuleInfo = getInfo(admin.getDeployment(id))
        def getModules: List[CEPModuleInfo] = deploymentInfo.map(i => getInfo(i))
        def load(url: URL) = admin.add(admin.read(url))
        def unload(id: String) = admin.undeployRemove(id)
        def parse(m: String) = admin.add(admin.parse(m))
        def deactivate(id: String) = admin.undeploy(id).getDeploymentId
        def activate(id: String) = {
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
                                if (sub.isInstanceOf[ModuleContext]) {
                                    sub.asInstanceOf[ModuleContext].moduleContext = ctx
                                }
                                s.setSubscriber(sub)
                            }
                        }
                    }
                }
            }

            id
        }

        private def getInfo(di: DeploymentInformation): CEPModuleInfo = {
            new CEPModuleInfo {
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
    class CEPService(private val engine: CEP) extends Service {
        val name = "cep"
        private val help = List(
            "activate   <id>     activate an epl module by deployment id",
            "deactivate <id>     deactivate an epl module by deployment id",
            "epl        <epl>    raw epl statement",
            "info                print engine instance id",
            "list                list all epl modules",
            "load       <url>    load a module file from a url",
            "unload     <id>     unload and deactivate an epl module by deployment id",
            "help                prints this message")
        private val cmds = List("activate", "deactivate", "epl", "info", "list", "load", "unload", "help")

        def command(cmd: Array[String]) = {
            cmd.head match {
                case "epl" => doCmd(cmd.tail, doEPL)
                case "info" => doCmd(cmd.tail, doInfo)
                case "list" => doCmd(cmd.tail, doList)
                case "load" => doCmd(cmd.tail, doLoad)
                case "unload" => doCmd(cmd.tail, doUnload)
                case "activate" => doCmd(cmd.tail, doActivate)
                case "deactivate" => doCmd(cmd.tail, doDeactivate)
                case "help" => commandHelp.reduceLeft[String]((acc, s) => acc + "\n" + s)
                case _ => "Unknown command: " + cmd.head
            }
        }

        def commands = cmds
        def commandHelp = help
        def shutdown() {}
        private def doLoad(cmd: Array[String]) = engine.engine.load(new URL(cmd.head))
        private def doUnload(cmd: Array[String]) = engine.engine.unload(cmd.head).getDeploymentId
        private def doActivate(cmd: Array[String]) = engine.engine.activate(cmd.head)
        private def doDeactivate(cmd: Array[String]) = engine.engine.deactivate(cmd.head)
        private def doEPL(cmd: Array[String]) = engine.engine.parse(cmd.mkString(" "))
        private def doInfo(cmd: Array[String]) = "Engine id: " + engine.engine.getInstanceId
        private def doList(cmd: Array[String]) = {
            val sb = new mutable.StringBuilder()
            engine.engine.getModules.foreach(m => sb.append("\n" + m.toString))
            sb.toString()
        }

        private def doCmd(cmd: Array[String], fn:(Array[String]) => String) = {
            try { fn(cmd) + "\n"}
            catch { case e: Exception => e.getMessage }
        }
    }
}

class CEP(confFile: Option[String] = None, modFiles: Option[List[String]] = None)
    extends StreamProc {
    private val engine: Engine = new Engine

    engine.init(confFile)

    def proc(t: StreamTuple): Option[StreamTuple] = {
        engine.event(t._1, t._3)
        None
    }

    def coll(t: StreamTuple) {}
    def close() {
        engine.close()
    }
    def open() {
        engine.ctx = moduleContext
        if (modFiles.isDefined) {
            modFiles.get.foreach(m => engine.activate(engine.load(new URL(m))))
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

abstract class CEPModuleInfo {
    val id: String
    val name: String
    val deployed: Date
    val statements: List[String]
    val isActive: Boolean

    override def toString = {
        val sb = new mutable.StringBuilder("EPL info: \n")
        sb.append("deployment id: ").append(id).append("\n")
        sb.append("name: ").append(name).append("\n")
        sb.append("deployed on: ").append(deployed).append("\n")
        sb.append("active: ").append(isActive).append("\n")
        sb.append("statements: ").append("\n")
        statements.foreach(s => sb.append("    ").append(s).append("\n"))
        sb.toString()
    }
}

class CEPModule(private val cep: Option[CEP]) extends Module {
    private val svc = Some(new CEPService(cep.get))
    def this() { this(Some(CEP())) }
    def this(conf: Option[String], eplModules: Option[List[String]]) { this(Some(CEP(conf, eplModules))) }
    def proc() = cep
    def service() = svc
    def sink(): Option[Sink] = None
    def store(): Option[Store] = None
}