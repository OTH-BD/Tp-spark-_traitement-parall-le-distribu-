package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class AppE1 {
    public static void main(String[] args)  {

        /*SparkConf conf=new SparkConf().setAppName("Word Count").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> rddLines=sc.textFile("text.txt");
        JavaRDD<String> rddWords=rddLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> wordsPairRdd=rddWords.mapToPair(word -> new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> wordWordCount= wordsPairRdd.reduceByKey((elem, accuml) -> elem+accuml);
        wordWordCount.foreach(elem -> System.out.println(elem._1+" "+elem._2));*/


        ////////////////////////////////////////////////////////////////////////////////////////////////////////
        SparkConf conf = new SparkConf().setAppName("Sale").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Extraire la ville et le prix
        JavaRDD<String> rddLines=sc.textFile("text.txt");
        // Transformation : (ville, prix)
        JavaPairRDD<String, Double> ventes = rddLines.mapToPair(line -> {
            String[] parts = line.split(",");
            String city = parts[1]; // Exemple : "Fes"
         // Exemple : "Fes"
           // System.out.println(city);

            double price = Double.parseDouble(parts[3]); // Exemple : 10000.0
           // System.out.println(price);
            return new Tuple2<>(city, price);
        });

        // Agrégation : total des ventes par ville
        JavaPairRDD<String, Double> totalParVille = ventes.reduceByKey(Double::sum);

        // Affichage du résultat
        totalParVille.collect().forEach(tuple ->
                System.out.println("City : " + tuple._1 + " | Total des ventes : " + tuple._2)
        );

        sc.close();
       /* SparkConf conf = new SparkConf().setAppName("Sale").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 3. Transformation : (ville, prix)
        JavaRDD<String> rddLines=sc.textFile("sales.txt");

        JavaPairRDD<String, Double> ventesParVille = rddLines.mapToPair(
                ligne -> {
                    String[] parts = ligne.split(" ");
                    if(parts.length == 4){
                        String city = parts[2];
                        double price = Double.parseDouble(parts[4]);
                        return new Tuple2<>(city, price);
                    }else{
                        return new Tuple2<>("Invalide",0.0);
                    }
                }
        ).filter(t ->!t._1.equals("Invalid"));

        //Regrouper par ville et sommer les ventes

        JavaPairRDD<String, Double> totalParVille = ventesParVille.reduceByKey(Double::sum);

        //affichier les resultat

        totalParVille.collect().forEach(tuple-> System.out.println(tuple._1 + " : " + tuple._2 + "$"));

        sc.stop();*/

    }
}