/*
Edmond La Chance UQAC 2019
Knight and Peasants algorithm in Apache Spark
 */
package coloring

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object testAlgo extends App {

  class creature(val name : String) extends Serializable {
    var spells =  ArrayBuffer[String]()
    def addspell(spell : String) : Unit = {
      spells += spell
    }
  }

  var Solar = new creature("Solar")
  Solar.addspell("cure light wounds")
  Solar.addspell("heal")

  var Planetar = new creature("Planetar")
  Planetar.addspell("cure light wounds")
  Planetar.addspell("attackPlanetar")

  var Lillend = new creature("Planetar")
  Lillend.addspell("heal")
  Lillend.addspell("attackLili")

  val conf = new SparkConf()
    .setAppName("Petersen Graph version Examen")
    .setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)
  sc.setLogLevel("ERROR")

  val data = Array(Lillend, Planetar, Solar)

  val data2 = Array(("Lillend","spell1 spell2"), ("Planetar","spell2 spell3"), ("Solar","spell1 spell3"),("Orc","spell1 spell2 spell3 spell4"))
  val RDD=sc.parallelize(data2)
  println("")
  println("printing first rdd\n")
  RDD.collect().foreach(println)
  val RDDinverse=RDD.flatMapValues(creature => creature.split(" ")).groupBy(_._2).mapValues(seq => seq.map(_._1))
  println("")
  print("printing inverted index \n")
  RDDinverse.collect().foreach(println)
  //val test=RDDinverse.groupBy(_._2).mapValues(seq => seq.map(_._1))
  //test.collect().foreach(println)


}


