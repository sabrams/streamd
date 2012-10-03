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

package com.appendr.streamd.proc.cep.esper

import java.util.UUID
import java.net.URL
import java.lang.annotation.Annotation
import com.appendr.streamd.conf.ConfigurableResource
import com.espertech.esper.client.{EPStatement, Configuration, EPServiceProviderManager, EPServiceProvider}
import com.espertech.esper.client.deploy.{DeploymentState, DeploymentOptions, EPDeploymentAdmin, DeploymentInformation}
import com.appendr.streamd.util.Reflector

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class Engine extends ConfigurableResource {
    private val cfg: Configuration = new Configuration
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
        val urlstr = config.get.getString("streamd.cep.config")
        if (urlstr != null && urlstr.isDefined) cfg.configure(new URL(urlstr.get))
        else cfg.configure()

        esper.initialize()

        val modStr = config.get.getString("streamd.cep.module")
        if (modStr != null && modStr.isDefined) activate(load(new URL(modStr.get)))
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
        val info = esper.getEPAdministrator.getDeploymentAdmin.getDeploymentInformation.toList
        info.map(i => getInfo(i))
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
            val si = asScalaBuffer[EPStatement](res.getStatements)
            for (s <- si) {
                s.getAnnotations.foreach {
                    a: Annotation => {
                        if (a.isInstanceOf[Subscriber])
                            s.setSubscriber(Reflector[Object](a.asInstanceOf[Subscriber].value))
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
}
