package bg.p_pavlov.thesis.real_estate_ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;

public class RealEstateDataProcessor {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        Dataset<Row> baseDataFrame = new DataFrameLoader().loadRealEstateData();

        baseDataFrame = new ModelLoader()
                .load()
                .transform(baseDataFrame);

        baseDataFrame.printSchema();


        baseDataFrame = baseDataFrame
                .select(struct(col("id"), col("prediction")).as("id_price_prediction"))
                .select(to_json(col("id_price_prediction")).as("value"));

        baseDataFrame.printSchema();

        StreamingQuery query = baseDataFrame.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("checkpointLocation", "C:\\Users\\Pavlin\\Projects\\thesis\\reml-stream-processor\\checkpoints")
                .option("topic", "real-estate-evaluated-2")
                .start();

        query.awaitTermination();
    }

}
