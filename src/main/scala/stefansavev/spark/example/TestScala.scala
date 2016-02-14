package stefansavev.spark.example

import java.net.URL

import org.apache.spark.rdd.RDD

import scala.math.random

import org.apache.spark._

import scala.reflect.internal.util.ScalaClassLoader.{URLClassLoader =>ScalaUrlClassLaoder}

/** Computes an approximation to pi */
object SparkPi {

  //TODO: make the serialization more efficient (look for Data Serialization in the guide below)
  //https://spark.apache.org/docs/0.8.1/tuning.html

  class MyCounter(var v1: Int, var v2: Int) extends Serializable{
    def addTo(inc1: Int, inc2: Int): Unit = {
      v1 += inc1
      v2 += inc2
    }

    def mergeInto(other: MyCounter): Unit = {
      v1 += other.v1
      v2 += other.v2
    }
  }

  object MyCounter{
    def zero(): MyCounter = new MyCounter(0, 0)

    def fromValues(v1: Int, v2: Int): MyCounter = {
      new MyCounter(v1, v2)
    }
  }


  implicit object MyCounterAccumulatorParam extends AccumulatorParam[MyCounter] {
    def addInPlace(t1: MyCounter, t2: MyCounter): MyCounter = {
      new MyCounter(t1.v1 + t2.v1, t1.v2 + t2.v2)
    }

    def zero(initialValue: MyCounter): MyCounter = MyCounter.zero()
  }


  def myaccum(spark: SparkContext, slices: Int = 2): Unit = {
    val singleData = 1 to 10000
    val tupleData = singleData.zip(singleData)
    val distData = spark.parallelize(tupleData, slices)
    val a1 = spark.accumulator(0)
    val a2 = spark.accumulator(0)
    val a12 = spark.accumulator(MyCounter.zero())

    distData.foreach{case (v1,v2) => {
      a1 += v1
      a2 += v2
      a12 += (MyCounter.fromValues(v1, v2))
    }}
    println(a1.value)
    println(a2.value)
    val v12 = a12.value
    println(s"value: ${v12.v1} --- ${v12.v2}")
  }


  def reduceByKeyExample(spark: SparkContext, slices: Int = 2): Unit = {
    //see: countApproxDistinctByKey in PairRDDFunctions

    val keys = 1 to 10000
    val values = (1 to 10000).map(_ + 1)
    val tupleData = keys.zip(values)
    val distData = spark.parallelize(tupleData, slices)

    def mergeCounters(a: MyCounter, b: MyCounter): MyCounter = {
      val copy = new MyCounter(a.v1, a.v2)
      copy.mergeInto(b)
      copy
    }

    val out: RDD[(Int, String)] = distData.map{case (k,v) => (k, new MyCounter(v,v))}.
                                      reduceByKey(mergeCounters).
                                      map{case (k,c) => (k,c.v1 + ", " + c.v2)}

    val some100: Array[(Int, String)] = out.take(100)
    for(kv <- some100){
      println(kv)
    }
    /*
    def merge(a: Int, b: Int): Int = {
      a + b
    }

    val out: RDD[(Int, Int)] = distData.reduceByKey(merge)
    val some100 = out.take(100)
    for(kv <- some100){
      println(kv)
    }
    */
  }

  def run(spark: SparkContext, slices: Int = 2): Unit = {
    /* run with reflection
    val cl = new ScalaUrlClassLaoder(Seq(new URL("jar:file:///home/stefan2/spark/myexample/spark-example/target/my-spark-example-0.0.1-SNAPSHOT.jar!/")), sc.getClass.getClassLoader)
    val clazz = cl.loadClass("stefansavev.spark.example.Test")
    val args: Array[String] = Array()
    val clazzes: Array[Class[_]] = Array(args.getClass() )
    val m = clazz.getMethod("main", clazzes :_*)
    m.invoke(null, args)

    val clazz1 = cl.loadClass("stefansavev.spark.example.SparkPi")
    val clazzIntObj = Class.forName("java.lang.Integer")


    val intClass = clazzIntObj.getField("TYPE").get(null).asInstanceOf[Class[_]]

    val method = clazz1.getMethod("run", SparkContext.getClass, intClass)
    clazz1.getMethods()(2).invoke(null, sc, 2.asInstanceOf[Object])
    */

    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    //run(spark, slices)
    //myaccum(spark, slices)
    reduceByKeyExample(spark, slices)
    spark.stop()
  }
}
