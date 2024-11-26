package com.smartshaped.chameleon.reviews.batch;

import com.smartshaped.chameleon.batch.exception.BatchLayerException;
import com.smartshaped.chameleon.batch.exception.BatchUpdaterException;
import com.smartshaped.chameleon.batch.exception.HdfsSaverException;
import com.smartshaped.chameleon.common.exception.ConfigurationException;
import com.smartshaped.chameleon.common.exception.KafkaConsumerException;
import com.smartshaped.chameleon.preprocessing.exception.PreprocessorException;

public class ReviewsBatchApp {

	public static void main(String[] args) throws ConfigurationException, BatchLayerException, KafkaConsumerException, BatchUpdaterException, PreprocessorException, HdfsSaverException {
		
		ReviewsBatchLayer batchLayer = new ReviewsBatchLayer();
		
		batchLayer.start();
	}

}
