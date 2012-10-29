package com.appendr.streamd.cep

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.appendr.streamd.connector.InputTransformer
import com.appendr.streamd.stream.StreamTuple
import io.Source
import com.appendr.streamd.conf
import conf.{ModuleSpec, Configuration}
import com.appendr.streamd.module.{Module, ModuleContext}

@RunWith(classOf[JUnitRunner])
class CEPTest extends FunSuite with BeforeAndAfter {
    val cfg = Thread.currentThread().getContextClassLoader.getResource("esper.cfg.xml")
    val mod = Thread.currentThread().getContextClassLoader.getResource("test.epl")

    private val proc = new CEP(Some(cfg.toString), Some(List(mod.toString)))
    private val ixf = new InputTransformer[String] {
        def transform(in: String): StreamTuple = {
            val v: Array[String] = in.split(",")
            val score: java.lang.Float = v(2).toFloat/10
            StreamTuple("colors", "color", Map[String, Object]("user"->v(0), "color"->v(1), "score"->score))
        }
    }

    before {
        proc.open()
    }

    after {
        proc.close()
    }

    test("engine evaluates events") {
        val dataStream = getClass.getClassLoader.getResourceAsStream("data.csv")
        val iter = Source.fromInputStream(dataStream).getLines()
        for (s <- iter) proc.proc(ixf.transform(s))
        Thread.sleep(5000)
    }
}

@RunWith(classOf[JUnitRunner])
class CEPTest2 extends FunSuite with BeforeAndAfter {
    private var modules: List[Module] = null

    private val ixf = new InputTransformer[String] {
        def transform(in: String): StreamTuple = {
            val v: Array[String] = in.split(",")
            val score: java.lang.Float = v(2).toFloat/10
            StreamTuple("colors", "color", Map[String, Object]("user"->v(0), "color"->v(1), "score"->score))
        }
    }

    before {
        val conf = Configuration.fromResource("test.conf")
        val spec = new ModuleSpec(conf)
        modules = spec.apply()
        modules.foreach(m => m.open())
    }

    after {
        modules.foreach(m => m.close())
    }

    test("engine evaluates events") {
        val dataStream = getClass.getClassLoader.getResourceAsStream("data.csv")
        val iter = Source.fromInputStream(dataStream).getLines()
        for (s <- iter) modules.foreach(m => m.proc.get.proc(ixf.transform(s)))
        Thread.sleep(5000)
    }
}

class TestSubscriber extends ModuleContext {
    def update(color: String, count: Long, score: Double) {
        if (moduleContext._1.isDefined) moduleContext._1.get.out(count + " " + color + " has an average score of " + score)
        else System.out.println("-----> " + count + " " + color + " has an average score of " + score)
    }
}

class MapSubscriber extends ModuleContext {
    def update(m: java.util.Map[Object, Object]) {
        if (moduleContext._1.isDefined) moduleContext._1.get.out(m)
        else System.out.println("-----> " + m)
    }
}

class ArraySubscriber extends ModuleContext {
    def update(a: Array[Object]) {
        val s = a.reduceLeft { (acc, n) => acc + ", " + n }

        if (moduleContext._1.isDefined) moduleContext._1.get.out(s)
        else System.out.println("-----> " + s)
    }
}