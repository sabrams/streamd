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

package com.appendr.streamd.conf

import scala.collection.mutable

/**
 * A ConfigSpec object contains vars for configuring an object of type T,
 * and an apply() method which turns this ConfigSpec into an object of type T.
 *
 * The trait also contains a few useful implicits.
 *
 * Define fields that are required but that don't have a default value with:
 * class NodeConfig extends ConfigSpec[Node] {
 * var host = required[String]
 * var port = required[Int]
 * ....
 * }
 *
 * Optional fields can be defined with:
 * var something = optional[Thing]
 *
 * Fields that are dependent on other fields and have a default value computed
 * from an expression should be marked as computed:
 *
 * var level = required[Int]
 * var nextLevel = computed { level + 1 }
 *
 * Code from Twitter Commons with some modifications.
 */
object ConfigSpec {

    object Spec {
        def unapply[A](spec: Spec[A]) = Some(spec.value)
    }

    sealed trait Required[+A] {
        def value: A
        def isSpecified: Boolean
    }

    class Spec[+A](f: => A) extends Required[A] {
        lazy val value = f
        def isSpecified = true
    }

    case object Unspec extends Required[scala.Nothing] {
        def value = throw new NoSuchElementException
        def isSpecified = false
    }

    class Missing(names: Seq[String]) extends Exception(names.mkString(","))

    class Nothing extends ConfigSpec[scala.Nothing] {
        def apply() = throw new UnsupportedOperationException
    }

    implicit def toSpec[A](value: => A) = new Spec(value)
    implicit def toSpecOption[A](value: => A) = new Spec(Some(value))
    implicit def fromRequired[A](req: Required[A]) = req.value
    implicit def toOption[A](item: A): Option[A] = Some(item)
    implicit def fromOption[A](item: Option[A]): A = item.get
    implicit def toList[A](item: A): List[A] = List(item)
}

trait ConfigSpec[T] extends (() => T) {
    import ConfigSpec.{ Required, Spec, Unspec, Missing }

    def required[A]: Required[A] = Unspec
    def optional[A]: Option[A] = None
    def computed[A](f: => A): Required[A] = new Spec(f)

    implicit def toSpec[A](value: => A) = new Spec(value)
    implicit def toSpecOption[A](value: => A) = new Spec(Some(value))
    implicit def fromRequired[A](req: Required[A]) = req.value
    implicit def toOption[A](item: A): Option[A] = Some(item)
    implicit def fromOption[A](item: Option[A]): A = item.get
    implicit def toList[A](item: A): List[A] = List(item)

    def missingValues: Seq[String] = {
        val visited = mutable.Set[AnyRef]()
        val returnTypes = Seq(classOf[Required[_]], classOf[ConfigSpec[_]])
        val listBuffer = new mutable.ListBuffer[String]
        def collect(prefix: String, Configuration: ConfigSpec[_]) {
            if (!visited.contains(Configuration)) {
                visited += Configuration
                val nullaryMethods = Configuration.getClass.getMethods.toSeq filter { _.getParameterTypes.isEmpty }

                for (method <- nullaryMethods) {
                    val name = method.getName
                    val rt = method.getReturnType
                    if (name != "required" &&
                        !name.endsWith("$outer") &&
                        returnTypes.exists(_.isAssignableFrom(rt))) {
                        method.invoke(Configuration) match {
                            case Unspec =>
                                listBuffer += (prefix + name)
                            case Spec(sub: ConfigSpec[_]) =>
                                collect(prefix + name + ".", sub)
                            case sub: ConfigSpec[_] =>
                                collect(prefix + name + ".", sub)
                            case _ =>
                        }
                    }
                }
            }
        }
        collect("", this)
        listBuffer.toList
    }

    def validate() {
        val missing = missingValues
        if (!missing.isEmpty) throw new Missing(missing)
    }
}
