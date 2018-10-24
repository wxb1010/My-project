import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //通过log4j设置日志输出的级别
    Logger.getLogger("org").setLevel(Level.ERROR)
    //创建Spark的配置对象SparkConf，设置Spark程序运行时的配置信息
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    //创建SparkContext 对象
    val sc = new SparkContext(conf)
    //通过SparkContext读取数据源创建RDD
    val lines = sc.textFile("src/main/resources/wordCount.txt")
    //对初始的RDD进行Transformation级别的处理,这里拆分单词，并过滤掉空格
    val words = lines.flatMap(_.split(" ")).filter(word => word != " ")
    //在单词拆分的基础上对每个单词实例计数为1, 也就是 word => (word, 1)
    val pairs = words.map(word => (word, 1))
    //在每个单词实例计数为1的基础之上统计每个单词在文件中出现的总次数, 即key相同的value相加
    val wordscount = pairs.reduceByKey(_ + _)
    //打印结果，使用collect会将集群中的数据收集到当前运行drive的机器上，需要保证单台机器能放得下所有数据
    wordscount.collect.foreach(println)
    //释放资源
    sc.stop()

  }

}
