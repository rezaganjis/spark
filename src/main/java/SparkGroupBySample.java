import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkGroupBySample {
    private static final String CSV_URL = "D:/projects/others/apache-spark/bonus.csv";
    private static final String ACTUAL_DEPOSIT_FUNCTION="getActualDepositType";
    public static void main(String args[]) {

        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> csv = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(CSV_URL);

        csv.groupBy(col("deposit type")).sum("amount").show();

    }
}
