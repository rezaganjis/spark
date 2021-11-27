import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSimpleAggregation {
    private static final String CSV_URL="bonus.csv";
    public static void main(String args[]){
        SparkSession spark= SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> csv = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(CSV_URL);
        csv.filter("amount >102").show();

    }
    }