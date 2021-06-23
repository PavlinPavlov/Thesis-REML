package bg.p_pavlov.thesis.real_estate_ml;

import bg.p_pavlov.thesis.real_estate_ml.common.schema.SchemaHolder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class RealEstateDataProcessor {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder()
                .appName("Spark Real Estate Processing App")
                .master("spark://spark-master:7077")
                .getOrCreate();

        Dataset<Row> kafkaDataFrame = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-broker-0:19092")
                .option("subscribe", "real-estate-entries")
                .load();

        Dataset<Row> baseDataFrame = kafkaDataFrame
                .select(
                        from_json(col("value").cast(DataTypes.StringType), SchemaHolder.getSchema())
                                .as("real_estate_event")
                );

        StreamingQuery query = baseDataFrame.writeStream()
                .outputMode("append")
                .queryName("writing_to_es")
                .format("org.elasticsearch.spark.sql")
                .option("checkpointLocation", "/tmp")
                .option("es.resource", "/es_spark")
                .option("es.nodes", "es01:9200")
                .option("es.index.auto.create", "true")
                .start();

        query.awaitTermination();
    }

}
