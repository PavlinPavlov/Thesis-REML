package bg.p_pavlov.thesis.real_estate_ml;

import bg.p_pavlov.thesis.real_estate_ml.common.schema.SchemaHolder;
import bg.p_pavlov.thesis.real_estate_ml.common.transformations.DataCategorizer;
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
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

public class RealEstateDataProcessor {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder()
                .appName("Spark Real Estate Processing App")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> kafkaDataFrame = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "real-estate-entries")
                .load();

        Dataset<Row> baseDataFrame = kafkaDataFrame
                .select(
                        from_json(
                                col("value").cast(DataTypes.StringType),
                                SchemaHolder.getSchema()
                        ).as("tmp_real_estate_data")
                )
                .select("tmp_real_estate_data.*");


        baseDataFrame = new DataCategorizer().encodeColumns(baseDataFrame);
        baseDataFrame = new ModelLoader().load().transform(baseDataFrame);


        baseDataFrame = baseDataFrame
                .select(struct(col("id"), col("price")).as("id_price_prediction"))
                .select(to_json(col("id_price_prediction")).as("value"));


        StreamingQuery query = baseDataFrame.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("checkpointLocation", "C:\\Users\\Pavlin\\Projects\\thesis\\reml-stream-processor\\checkpoints")
                .option("topic", "real-estate-evaluated")
                .start();

        query.awaitTermination();
    }

}
