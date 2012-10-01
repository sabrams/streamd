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
// Based on Configgy by Robey Pointer.
// Copyright 2009 Robey Pointer <robeypointer@gmail.com>
// http://www.apache.org/licenses/LICENSE-2.0

package com.appendr.streamd.conf

import java.io.{BufferedReader, File, FileInputStream, InputStream, InputStreamReader}
import scala.collection.mutable
import scala.util.parsing.combinator.RegexParsers

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

object Configuration {
    val DefaultPath = new File(".").getCanonicalPath
    val DefaultImporter = new FilesystemImporter(DefaultPath)

    def it = this

    def load(data: String, importer: Importer = DefaultImporter): Configuration = {
        val parser = new ConfigParser(importer = importer)
        new Configuration(parser parse data)
    }

    def fromFile(filename: String, importer: Importer): Configuration = {
        load(importer.importFile(filename), importer)
    }

    def fromFile(path: String, filename: String): Configuration = {
        val importer = new FilesystemImporter(path)
        fromFile(filename, importer)
    }

    def fromFile(filename: String): Configuration = {
        val n = filename.lastIndexOf('/')
        if (n < 0) {
            fromFile(DefaultPath, filename)
        } else {
            fromFile(filename.substring(0, n), filename.substring(n + 1))
        }
    }

    def fromResource(filename: String): Configuration = {
        fromResource(filename, ClassLoader.getSystemClassLoader)
    }

    def fromResource(filename: String, classLoader: ClassLoader): Configuration = {
        val importer = new ResourceImporter(classLoader)
        fromFile(filename, importer)
    }

    def fromMap(map: Map[String, Any]) = {
        new Configuration(map)
    }

    def fromString(data: String): Configuration = {
        load(data)
    }

    def apply(pairs: (String, Any)*) = {
        new Configuration(Map(pairs: _*))
    }
}

class Configuration(val map: Map[String, Any]) {
    private val trueValues = Set("true", "on")
    private val falseValues = Set("false", "off")

    def ++(other: Configuration) = new Configuration(map ++ other.map)

    def contains(key: String): Boolean = map contains key

    def keys: Iterable[String] = map.keys

    def getAny(key: String): Option[Any] = {
        try {
            Some(map(key))
        } catch {
            case _: Throwable => None
        }
    }

    def getAny(key: String, defaultValue: Any): Any =
        getAny(key).getOrElse(key, defaultValue)

    def getListAny(key: String): Seq[Any] = {
        try {
            map(key).asInstanceOf[Seq[Any]]
        } catch {
            case _: Throwable => Seq.empty[Any]
        }
    }

    def getString(key: String): Option[String] = map.get(key).map(_.toString)

    def getString(key: String, defaultValue: String): String =
        getString(key).getOrElse(defaultValue)

    def getList(key: String): Seq[String] = {
        try {
            map(key).asInstanceOf[Seq[String]]
        } catch {
            case _: Throwable => Seq.empty[String]
        }
    }

    def getInt(key: String): Option[Int] = {
        try {
            Some(map(key).toString.toInt)
        } catch {
            case _: Throwable => None
        }
    }

    def getInt(key: String, defaultValue: Int): Int =
        getInt(key).getOrElse(defaultValue)

    def getLong(key: String): Option[Long] = {
        try {
            Some(map(key).toString.toLong)
        } catch {
            case _: Throwable => None
        }
    }

    def getLong(key: String, defaultValue: Long): Long =
        getLong(key).getOrElse(defaultValue)

    def getFloat(key: String): Option[Float] = {
        try {
            Some(map(key).toString.toFloat)
        } catch {
            case _: Throwable => None
        }
    }

    def getFloat(key: String, defaultValue: Float): Float =
        getFloat(key).getOrElse(defaultValue)

    def getDouble(key: String): Option[Double] = {
        try {
            Some(map(key).toString.toDouble)
        } catch {
            case _: Throwable => None
        }
    }

    def getDouble(key: String, defaultValue: Double): Double =
        getDouble(key).getOrElse(defaultValue)

    def getBoolean(key: String): Option[Boolean] = {
        getString(key) flatMap {
            s => {
                val isTrue = trueValues.contains(s)
                if (!isTrue && !falseValues.contains(s)) None
                else Some(isTrue)
            }
        }
    }

    def getBoolean(key: String, defaultValue: Boolean): Boolean =
        getBool(key).getOrElse(defaultValue)

    def getBool(key: String): Option[Boolean] = getBoolean(key)

    def getBool(key: String, defaultValue: Boolean): Boolean =
        getBoolean(key, defaultValue)

    def apply(key: String): String = getString(key) match {
        case None => throw new RuntimeException("undefined config: " + key)
        case Some(v) => v
    }

    def apply(key: String, defaultValue: String) = getString(key, defaultValue)

    def apply(key: String, defaultValue: Int) = getInt(key, defaultValue)

    def apply(key: String, defaultValue: Long) = getLong(key, defaultValue)

    def apply(key: String, defaultValue: Boolean) = getBool(key, defaultValue)

    def getSection(name: String): Option[Configuration] = {
        val l = name.length + 1
        val pattern = name + "."
        val m = map.collect {
            case (k, v) if k.startsWith(pattern) => (k.substring(l), v)
        }
        if (m.isEmpty) {
            None
        }
        else {
            Some(new Configuration(m))
        }
    }
}

trait Importer {
    def importFile(filename: String): String

    private val BUFFER_SIZE = 8192

    protected def streamToString(in: InputStream): String = {
        try {
            val reader = new BufferedReader(new InputStreamReader(in, "UTF-8"))
            val buffer = new Array[Char](BUFFER_SIZE)
            val sb = new StringBuilder
            var n = 0
            while (n >= 0) {
                n = reader.read(buffer, 0, buffer.length)
                if (n >= 0) {
                    sb.appendAll(buffer, 0, n)
                }
            }
            in.close()
            sb.toString()
        } catch {
            case x: Throwable => throw new RuntimeException(x.toString)
        }
    }
}

/**
 * An Importer that looks for imported config files in the filesystem.
 * This is the default importer.
 */
class FilesystemImporter(val baseDir: String) extends Importer {
    def importFile(filename: String): String = {
        val f = new File(filename)
        val file = if (f.isAbsolute) f else new File(baseDir, filename)
        streamToString(new FileInputStream(file))
    }
}

/**
 * An Importer that looks for imported config files in the java resources
 * of the system class loader (usually the jar used to launch this app).
 */
class ResourceImporter(classLoader: ClassLoader) extends Importer {
    def importFile(filename: String): String = {
        val stream = classLoader.getResourceAsStream(filename)
        streamToString(stream)
    }
}

class ConfigParser(
    var prefix: String = "",
    map: mutable.Map[String, Any] = mutable.Map.empty[String, Any],
    importer: Importer) extends RegexParsers {

    val sections = mutable.Stack[String]()

    def createPrefix() {
        prefix = if (sections.isEmpty) "" else sections.toList.reverse.mkString("", ".", ".")
    }

    override val whiteSpace = """(\s+|#[^\n]*\n)+""".r

    // tokens
    val numberToken: Parser[String] = """-?\d+(\.\d+)?""".r
    val stringToken: Parser[String] = ("\"" + """([^\\\"]|\\[^ux]|\\\n|\\u[0-9a-fA-F]{4}|\\x[0-9a-fA-F]{2})*""" + "\"").r
    val booleanToken: Parser[String] = "(true|on|false|off)".r
    val identToken: Parser[String] = """([\da-zA-Z_/][-\w]*)(\.[a-zA-Z_/][-/\w]*)*""".r
    val assignToken: Parser[String] = "=".r
    val sectionToken: Parser[String] = """[a-zA-Z_/][-/\w]*""".r

    // values
    def value: Parser[Any] = number | string | list | boolean

    def number = numberToken

    def string = stringToken ^^ {
        s => s.substring(1, s.length - 1)
    }

    def list = "[" ~> repsep(string | numberToken, opt(",")) <~ (opt(",") ~ "]")

    def boolean = booleanToken

    def root = rep(includeFile | assignment | sectionOpen | sectionClose)

    def includeFile = "include" ~> string ^^ {
        case filename: String =>
            new ConfigParser(prefix, map, importer) parse importer.importFile(filename)
    }

    def assignment = identToken ~ assignToken ~ value ^^ {
        case k ~ a ~ v => map(prefix + k) = v
    }

    def sectionOpen = sectionToken <~ "{" ^^ {
        name =>
            sections.push(name)
            createPrefix()
    }

    def sectionClose = "}" ^^ {
        _ =>
            if (sections.isEmpty) {
                failure("dangling close tag")
            } else {
                sections.pop()
                createPrefix()
            }
    }

    def parse(in: String): Map[String, Any] = {
        parseAll(root, in) match {
            case Success(result, _) => map.toMap
            case x@Failure(msg, _) => throw new RuntimeException(x.toString())
            case x@Error(msg, _) => throw new RuntimeException(x.toString())
        }
    }
}
