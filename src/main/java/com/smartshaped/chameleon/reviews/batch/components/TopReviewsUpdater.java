package com.smartshaped.chameleon.reviews.batch.components;

import java.time.Instant;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.smartshaped.chameleon.batch.BatchUpdater;
import com.smartshaped.chameleon.batch.exception.BatchUpdaterException;
import com.smartshaped.chameleon.common.preprocessing.Preprocessor;
import com.smartshaped.chameleon.common.preprocessing.exception.PreprocessorException;
import com.smartshaped.chameleon.common.utils.exception.CassandraException;
import com.smartshaped.chameleon.common.utils.exception.ConfigurationException;

public class TopReviewsUpdater extends BatchUpdater {

  Preprocessor preprocessor;
  String parquetPath;

  public TopReviewsUpdater() throws ConfigurationException, CassandraException {
    super();
    this.preprocessor = new ReviewPreprocessor();
    this.parquetPath = this.getConfigurationUtils().getKafkaConfig().get("topicReviews.path");
  }

  @Override
  public Dataset<Row> updateBatch(Dataset<Row> df) throws BatchUpdaterException {

    SparkSession sparkSession = SparkSession.getActiveSession().get();

    try {
      df = preprocessor.preprocess(df);
    } catch (PreprocessorException e) {
      throw new BatchUpdaterException("Error preprocessig datastream", e);
    }

    Dataset<Row> savedDf;

    StructType schema =
        new StructType()
            .add("asin", DataTypes.StringType)
            .add("rating", DataTypes.DoubleType)
            .add("user_id", DataTypes.StringType);

    savedDf = sparkSession.read().schema(schema).parquet(parquetPath);

    savedDf.show();

    df = df.union(savedDf);

    df = df.drop("user_id");

    df = df.groupBy("asin").agg(functions.avg("rating").alias("meanrating"));

    df = df.orderBy(functions.desc("meanrating")).limit(10);

    df = df.withColumn("calculatedat", functions.lit(Instant.now()));

    return df;
  }
}
