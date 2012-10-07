package com.appendr.streamd.cep

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.appendr.streamd.connector.InputTransformer
import com.appendr.streamd.stream.StreamTuple
import io.Source
import com.appendr.streamd.conf
import com.appendr.streamd.plugin.PluginContextAware
import com.appendr.streamd.plugin.cep.CEP
import conf.Configuration
import collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class CEPTest extends FunSuite with BeforeAndAfter {
    private val proc = new CEP
    private val ixf = new InputTransformer[String] {
        def transform(in: String): StreamTuple = {
            val v: Array[String] = in.split(",")
            val score: java.lang.Float = v(2).toFloat/10
            StreamTuple("colors", "color", Map[String, Object]("user"->v(0), "color"->v(1), "score"->score))
        }
    }

    before {
        val conf = Configuration.fromResource("test.conf")
        proc.open(conf.getSection("streamd.plugin.processor"))
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

class TestSubscriber extends PluginContextAware {
    def update(color: String, count: Long, score: Double) {
        System.out.println("-----> " + count + " " + color + " has an average score of " + score)
    }
}

class MapSubscriber extends PluginContextAware {
    def update(m: java.util.Map[Object, Object]) {
        System.out.println("-----> " + m)
    }
}

class ArraySubscriber extends PluginContextAware {
    def update(a: Array[Object]) {
        System.out.println(a.reduceLeft { (acc, n) => acc + ", " + n })
    }
}