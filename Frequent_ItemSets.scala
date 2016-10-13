/**
  * Created by vasu on 10/9/16.
  */
package Vasuprada_Vijayakumar_spark
import java.io.{File, PrintWriter}

import org.apache.spark
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions.mapAsScalaMap


object Frequent_ItemSets {
  def main(args: Array[String]) {

    val result_map = new java.util.HashMap[String,Int]
    val actor_movie_map = new java.util.HashMap[String,ListBuffer[String]]
    val director_movie_map = new java.util.HashMap[String,ListBuffer[String]]
    val sparkconf = new SparkConf().setAppName("FrequentItemSets")
    val sc = new SparkContext(sparkconf)
    val text_actress = sc.textFile(args(0)).collect().toList
    val text_director = sc.textFile(args(1)).collect().toList
    val support = args(2).toInt
    var count = 0
    var ncount = 0

    for (actor_movie_pair <- text_actress)
    {
      var a = 1
      var key = ""
      var value = ""
      var isKey = false
      var isValue = false
      var len = actor_movie_pair.length

      var startOfKey = actor_movie_pair(1)
      var startOfValue = actor_movie_pair(len-2)
      key += startOfKey

      var i = 2
      while(actor_movie_pair(i) != startOfKey || actor_movie_pair(i+1) != ',' || actor_movie_pair(i+3) != startOfValue) {
        key += actor_movie_pair(i)
        i = i + 1
      }
      key += actor_movie_pair(i)
      i = i+2

      while ( i < len -1 )
      {
        value += actor_movie_pair(i)
        i = i+1
      }


      if (!actor_movie_map.containsKey(key)){
        var ls1 = new ListBuffer[String]
        //ls1.add(value)
        ls1 += value

        actor_movie_map.put(key,ls1)
      }
      else
      {
        ncount = ncount + 1
        val ls = actor_movie_map.get(key)
        //ls.add(value)
        ls+= value
        actor_movie_map.put(key,ls)
      }

    }
    //actor_movie_map.foreach(kv => println(kv._1 + " -> " + kv._2))

    var actor_pairs_list = new ListBuffer[List[String]]
    actor_movie_map.foreach(kv =>
    {
      if (kv._2.length > 2)
      {
        for (a <- kv._2.toArray.toSeq.combinations(2))
        {
          //println(a.toList)
          actor_pairs_list += a.toList

        }
      }
      else if (kv._2.length == 2)
      {
        //println(kv._2.toList)
        actor_pairs_list += kv._2.toList
      }
      else
      {}
    })
    /*for ( value_pair <- actor_pairs_list){
      count = count + 1
    }
    */

    var distinct_items = actor_pairs_list.distinct
    for ( item <- distinct_items)
    {
      if (actor_pairs_list.count(_ == item) >= support){
        result_map.put("(" + item(0) + ',' + item(1) + ")",actor_pairs_list.count(_ == item))
      }
    }

    ncount = 0

    for (director_movie_pair <- text_director)
    {
      var a = 1
      var key = ""
      var value = ""
      var isKey = false
      var isValue = false
      var len = director_movie_pair.length

      var startOfKey = director_movie_pair(1)
      var startOfValue = director_movie_pair(len-2)
      key += startOfKey

      var i = 2
      while(director_movie_pair(i) != startOfKey || director_movie_pair(i+1) != ',' || director_movie_pair(i+3) != startOfValue) {
        key += director_movie_pair(i)
        i = i + 1
      }
      key += director_movie_pair(i)
      i = i+2

      while ( i < len -1 )
      {
        value += director_movie_pair(i)
        i = i+1
      }


      if (!director_movie_map.containsKey(key)){
        var ls1 = new ListBuffer[String]
        //ls1.add(value)
        ls1 += value

        director_movie_map.put(key,ls1)
      }
      else
      {
        ncount = ncount + 1
        val ls = director_movie_map.get(key)
        //ls.add(value)
        ls+= value
        director_movie_map.put(key,ls)
      }

    }
    //actor_movie_map.foreach(kv => println(kv._1 + " -> " + kv._2))

    var director_pairs_list = new ListBuffer[List[String]]
    director_movie_map.foreach(kv =>
    {
      if (kv._2.length > 2)
      {
        for (a <- kv._2.toArray.toSeq.combinations(2))
        {
          //println(a.toList)
          director_pairs_list += a.toList

        }
      }
      else if (kv._2.length == 2)
      {
        //println(kv._2.toList)
        director_pairs_list += kv._2.toList
      }
      else
      {}
    })

    var distinct_items_director = director_pairs_list.distinct
    for ( item <- distinct_items_director)
    {
      if (director_pairs_list.count(_ == item) >= support){
        result_map.put("(" + item(0) + ',' + item(1) + ")",director_pairs_list.count(_ == item))
      }
    }


    // DIRECTOR - ACTOR PAIRS
    var dcount = 0
    var director_actors_list = new ListBuffer[List[String]]
    director_movie_map.foreach(kv =>
      if (actor_movie_map.containsKey(kv._1))
      {
        val temp = actor_movie_map.get(kv._1).toList ::: director_movie_map.get(kv._1).toList
        if (temp.length > 2)
        {
          for(a <- temp.toArray.toSeq.combinations(2))
          {
            director_actors_list += a.toList
          }
        }
        else
        {
          director_actors_list += temp
        }

      })

    var distinct_items_da = director_actors_list.distinct
    for ( item <- distinct_items_da)
    {
      if (director_actors_list.count(_ == item) >= support){
        result_map.put("(" + item(0) + ',' + item(1) + ")",director_actors_list.count(_ == item))
      }
    }

    val sorted_map = result_map.toSeq.sortWith(_._2 < _._2)
    sc.parallelize(sorted_map).saveAsTextFile("Vasuprada_Vijayakumar_spark")

  }
}
