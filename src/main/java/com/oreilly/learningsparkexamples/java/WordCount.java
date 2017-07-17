/**
 * Illustrates a wordcount in Java
 */
package com.oreilly.learningsparkexamples.java;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {
    public interface SerializableComparator<T> extends Comparator<T>, Serializable {

        static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
            return comparator;
        }

    }
  public static void main(String[] args) throws Exception {
		String master = args[0];
		JavaSparkContext sc = new JavaSparkContext(
      master, "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));
      JavaRDD<String> rdd = sc.textFile(args[1]);
      JavaPairRDD<String, Integer> counts = rdd
              .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
              .mapToPair(word -> new Tuple2<>(word, 1))
              .reduceByKey((a, b) -> a + b);


      List<Tuple2<String, Integer>> topN = counts.top(10,
              SerializableComparator.serialize((o1, o2)->o1._2 - o2._2));

      System.out.print("==== size of topN: \n" + topN.size());
      for (Tuple2<String,Integer> tuple: topN) {
          System.out.println(tuple._1 + " : " + tuple._2);
      }

	}
}
