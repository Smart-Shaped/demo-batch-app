package com.smartshaped.chameleon.reviews.batch.components;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.smartshaped.chameleon.preprocessing.Preprocessor;
import com.smartshaped.chameleon.preprocessing.exception.PreprocessorException;

public class ReviewPreprocessor extends Preprocessor {

	@Override
	public Dataset<Row> preprocess(Dataset<Row> df) throws PreprocessorException {
		
		StructType schema = new StructType()
				.add("sort_timestamp", DataTypes.LongType, true)
	            .add("rating", DataTypes.DoubleType, true)
	            .add("helpful_votes", DataTypes.IntegerType, true)
	            .add("title", DataTypes.StringType, true)
	            .add("text", DataTypes.StringType, true)
	            .add("images", DataTypes.StringType, true)
	            .add("asin", DataTypes.StringType, true)
	            .add("verified_purchase", DataTypes.BooleanType, true)
	            .add("parent_asin", DataTypes.StringType, true)
	            .add("user_id", DataTypes.StringType, true);
		
		df = df.selectExpr("CAST(value as STRING) as jsonData")
				.select(functions.from_json(functions.col("jsonData"), schema).as("data"))
				.select("data.asin", "data.rating", "data.user_id");
		
		df.printSchema();
		
		return df;
	}

}
