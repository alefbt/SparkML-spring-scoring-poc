package com.alefbt.solutions.sparkspring.domain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.alefbt.solutions.sparkspring.utils.DataMapper;

/**
 * Prediction response
 * 
 * @author yehuda
 *
 */
public class PredictionResponse {
	// schema
	private ArrayList<PipelineConfigSchemaItem> schema = new ArrayList<>();

	// data key-val
	private ArrayList<HashMap<String, Object>> data = new ArrayList<>();

	/**
	 * 
	 * Prediction Response Constructor
	 * 
	 * @param pipelineConfig
	 * @param predictionResult
	 * @throws IOException
	 */
	public PredictionResponse(PipelineConfig pipelineConfig, Dataset<Row> predictionResult) throws IOException {
		setSchema(pipelineConfig);
		setData(pipelineConfig, predictionResult);
	}

	/**
	 * Set schema
	 * 
	 * @param pipelineConfig
	 */
	private void setSchema(PipelineConfig pipelineConfig) {
		ArrayList<PipelineConfigSchemaItem> o = new ArrayList<>();

		List<String> l = Arrays.asList(pipelineConfig.getOutput());
		pipelineConfig.getSchema().stream().filter(i -> l.contains(i.getName())).forEach(i -> o.add(i));

		setSchema(o);
	}

	/**
	 * get schema
	 * 
	 * @return
	 */
	public ArrayList<PipelineConfigSchemaItem> getSchema() {
		return schema;
	}

	/**
	 * set schema
	 * @param schema
	 */
	public void setSchema(ArrayList<PipelineConfigSchemaItem> schema) {
		this.schema = schema;
	}

	/**
	 * get data
	 * @return
	 */
	public ArrayList<HashMap<String, Object>> getData() {
		return data;
	}

	/**
	 * Set data from dataset
	 * @param pipelineConfig
	 * @param data
	 * @throws IOException
	 */
	public void setData(PipelineConfig pipelineConfig, Dataset<Row> data) throws IOException {

		ArrayList<HashMap<String, Object>> retData = DataMapper.convertDatasetToHashMapArrayBySchema(pipelineConfig,
				data);

		setData(retData);
	}

	/**
	 * Set data raw
	 * @param data
	 */
	public void setData(ArrayList<HashMap<String, Object>> data) {
		this.data = data;
	}

}
