import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import java.io.File
import org.apache.hadoop.fs._;
import org.apache.spark.sql.DataFrame


object driveClientesApp {

	def saveDfToCsv(df: DataFrame, tsvOutput: String, sc: SparkContext,
		        sep: String = ",", header: Boolean = false): Unit = {
	    val tmpParquetDir = "Posts.tmp.parquet"
	  
	    df.repartition(1).write.
		format("com.databricks.spark.csv").
		option("header", header.toString).
		option("delimiter", sep).
		mode("overwrite").
		save(tmpParquetDir)
	  
	    val fs = FileSystem.get(sc.hadoopConfiguration);
	    
 	    val fileName = new java.io.File(tmpParquetDir).listFiles.filter(_.getName.endsWith(".csv"))(0).getName()
	    fs.rename(new Path(tmpParquetDir+"/"+ fileName), new Path(tsvOutput));
	    
	}


	def main (args: Array[String] ) {
		val conf = new SparkConf().setAppName("driveClientes")
		val sc = new SparkContext(conf)
		val SQLContext = new SQLContext(sc)

		val spark = org.apache.spark.sql.SparkSession.builder()
				.master("local")
				.appName("driveClientes")
				.getOrCreate();

		import SQLContext.implicits._

		val dataInicio = args(0)
		val dataFim = args(1)

		val vendasDF = spark.read.format("csv").option("header","true").option("delimiter",",").csv("/home/bigdata/Documentos/dados/comprasProdutos.csv")

		val vendasDFDATE = vendasDF.withColumn("DATA", date_format(unix_timestamp($"DATA","dd-mm-yyyy").cast("timestamp"),"YYYY-MM-DD")).select("CLIENTE","DATA","TIPOPRODUTO","GRUPO","SUBGRUPO")

		val vendasDFFiltrado = vendasDFDATE.filter($"DATA".between(dataInicio,dataFim))

		val clientesTipo = vendasDFFiltrado.groupBy("CLIENTE","TIPOPRODUTO").count 
		var maxval = clientesTipo.groupBy("CLIENTE").max("`count`")
		var cliFilter = clientesTipo.join(maxval, clientesTipo("CLIENTE") === maxval("CLIENTE")).select(clientesTipo("CLIENTE"), clientesTipo("TIPOPRODUTO"), col("count").alias("countVal"), col("max(count)").alias("COUNTTIPO"))
		val clientesDriveTipoAux = cliFilter.filter("countVal = COUNTTIPO")
		val clientesDriveTipo = clientesDriveTipoAux.select(clientesDriveTipoAux("CLIENTE"),clientesDriveTipoAux("TIPOPRODUTO"),clientesDriveTipoAux("COUNTTIPO")).filter($"COUNTTIPO">1)
	
		val clientesGrupo = vendasDF.groupBy("CLIENTE","GRUPO").count
		maxval = clientesGrupo.groupBy("CLIENTE").max("count")
		cliFilter = clientesGrupo.join(maxval, clientesGrupo("CLIENTE") === maxval("CLIENTE")).select(clientesGrupo("CLIENTE"), clientesGrupo("GRUPO"), col("count").alias("countVal"), col("max(count)").alias("COUNTGRUPO"))
		val clientesDriveGrupoAux = cliFilter.filter("countVal = COUNTGRUPO")
		val clientesDriveGrupo = clientesDriveGrupoAux.select(clientesDriveGrupoAux("CLIENTE"),clientesDriveGrupoAux("GRUPO"),clientesDriveGrupoAux("COUNTGRUPO")).filter($"COUNTGRUPO">1)

		val clientesSubgrupo = vendasDF.groupBy("CLIENTE","SUBGRUPO").count
		maxval = clientesSubgrupo.groupBy("CLIENTE").max("count")
		cliFilter = clientesSubgrupo.join(maxval, clientesSubgrupo("CLIENTE") === maxval("CLIENTE")).select(clientesSubgrupo("CLIENTE"), clientesSubgrupo("SUBGRUPO"), col("count").alias("countVal"), col("max(count)").alias("COUNTSUBGRUPO"))
		val clientesDriveSubgrupoAux = cliFilter.filter("countVal = COUNTSUBGRUPO")
		val clientesDriveSubgrupo = clientesDriveSubgrupoAux.select(clientesDriveSubgrupoAux("CLIENTE"),clientesDriveSubgrupoAux("SUBGRUPO"),clientesDriveSubgrupoAux("COUNTSUBGRUPO")).filter($"COUNTSUBGRUPO">1)

		val cliJoin1 = clientesDriveTipo.join(clientesDriveGrupo, clientesDriveGrupo("CLIENTE") === clientesDriveTipo("CLIENTE")).select(clientesDriveTipo("CLIENTE"),clientesDriveTipo("TIPOPRODUTO"),clientesDriveTipo("COUNTTIPO"),col("GRUPO"),clientesDriveGrupo("COUNTGRUPO"))

		val clientesDrive = cliJoin1.join(clientesDriveSubgrupo, clientesDriveSubgrupo("CLIENTE") === cliJoin1("CLIENTE")).select(cliJoin1("CLIENTE"),cliJoin1("TIPOPRODUTO"),cliJoin1("COUNTTIPO"),cliJoin1("GRUPO"),cliJoin1("COUNTGRUPO"),col("SUBGRUPO"),col("COUNTSUBGRUPO"))

		val clientesDriveFinal = clientesDrive.withColumn("DriveCli", when($"COUNTTIPO" >= $"COUNTGRUPO" && $"COUNTTIPO" >= $"COUNTSUBGRUPO", $"TIPOPRODUTO").otherwise(when($"COUNTGRUPO" >= $"COUNTTIPO" && $"COUNTGRUPO" >= $"COUNTSUBGRUPO", $"GRUPO").otherwise($"SUBGRUPO"))).select("CLIENTE","DriveCli")

		val drivesClientes = clientesDriveFinal.groupBy("DriveCli").count.orderBy(desc("count"))

		//drivesClientes.repartition(1).write.format("com.databricks.spark.csv", SaveMode.Overwrite).option("header","true").save("/home/bigdata/Documentos/dados/resultados/drivesCount.csv")

		//clientesDriveFinal.repartition(1).write.format("com.databricks.spark.csv", SaveMode.Overwrite).option("header","true").save("/home/bigdata/Documentos/dados/resultados/resultado.csv")

		saveDfToCsv (drivesClientes, "/home/bigdata/Documentos/dados/resultados/drive.csv", sc)
		saveDfToCsv (clientesDriveFinal, "/home/bigdata/Documentos/server/public/resultado.csv", sc)

		drivesClientes.take(10).foreach(println)
		clientesDriveFinal.take(10).foreach(println)
	}
}
