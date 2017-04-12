package aval.spark.q5

import org.apache.spark._
import org.apache.spark.SparkContext._
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.ling.CoreAnnotations._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import java.util.Properties

object BiGram {
  def main(args: Array[String]) {

    val stopwordfile = args(0)
    val inputtextfile = args(1)
    val outputFile = args(2)
    val threshold = args(3).toInt

    val sc = new SparkContext("local[*]", "bigram")
    val plainText = sc.textFile(inputtextfile)

    val source = scala.io.Source.fromFile(stopwordfile).mkString
    val stopwords = source.split(",").toSet

    val lemmatized = plainText.map(plainTextToLemmas(_, stopwords))
    val result = lemmatized.map(bigrams)
      .flatMap(x => x)
      .reduceByKey((x, y) => x + y)
      .filter(x => x._2 > threshold)

    result.saveAsTextFile(outputFile)
  }

  def bigrams(s: String): Array[((String, String), Int)] = {

    s.split("""\.""")
      .map(_.split(" ")
        .filter(_.nonEmpty)
        .map(_.replaceAll("""\W""", "").toLowerCase)
        .filter(_.nonEmpty)
        .sliding(2)
        .filter(_.size == 2)
        .map { case Array(a, b) => ((a, b), 1) })
      .flatMap(x => x)
  }

  def plainTextToLemmas(text: String, stopWords: Set[String]): String = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    var lemmas = ""
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma)) {
        lemmas += " " + lemma.toLowerCase
      }
    }
    lemmas

  }
}