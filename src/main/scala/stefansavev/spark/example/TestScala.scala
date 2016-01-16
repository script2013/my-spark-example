package stefansavev.spark.example

import java.net.URL

import scala.math.random

import org.apache.spark._

import scala.reflect.internal.util.ScalaClassLoader.{URLClassLoader =>ScalaUrlClassLaoder}

/** Computes an approximation to pi */
object SparkPi {
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
    run(spark, slices)
    spark.stop()
  }
}
