package com.smartshaped.chameleon.reviews.batch.utils;

import java.time.Instant;

import com.smartshaped.chameleon.common.utils.TableModel;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TopTenModel extends TableModel{

	private String asin;
	
	private Double meanRating;
	
	private Instant calculatedAt; 
	
	@Override
	protected String choosePrimaryKey() {
		return "asin";
	}
	
}
