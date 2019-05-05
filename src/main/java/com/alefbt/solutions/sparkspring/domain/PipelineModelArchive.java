package com.alefbt.solutions.sparkspring.domain;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alefbt.solutions.sparkspring.services.SparkService;
import com.alefbt.solutions.sparkspring.utils.DataMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
/**
 * PipelineModelArchive
 * 
 * @author yehuda
 *
 */
public class PipelineModelArchive {
	private File pipelineFolder;
	private ObjectMapper objectMapper;
	private PipelineModel pipelineModel;
	private SparkService sparkService;
	private StructType schema;
	private PipelineConfig pipelineConfig;

	/**
	 * Constructor
	 * 
	 * @param configFile
	 * @param sparkService
	 * @throws Exception
	 */
	public PipelineModelArchive(File configFile, SparkService sparkService) throws Exception {
		this.sparkService = sparkService;

		this.objectMapper = new ObjectMapper();
		this.pipelineConfig = objectMapper.readValue(configFile, PipelineConfig.class);

		Path p = Paths.get(configFile.getParentFile().getAbsolutePath(), pipelineConfig.getPipeline());
		this.pipelineFolder = p.toFile();

		if (!this.pipelineFolder.exists() || !this.pipelineFolder.isDirectory())
			throw new Exception(String.format("Pipeline [%s] not found in [%s]", pipelineConfig.getPipeline(),
					configFile.getParentFile().getAbsolutePath()));

		// Get pipeline model
		this.pipelineModel = PipelineModel.load(this.pipelineFolder.getAbsolutePath());

		this.generateSchema();
		this.runCollectSampleData();
	}

	/**
	 * Generating Spark Schema from json
	 * 
	 * @throws Exception
	 */
	private void generateSchema() throws Exception {
		ArrayList<StructField> fields = new ArrayList<>();

		for (PipelineConfigSchemaItem configSchemaItem : this.pipelineConfig.getSchema()) {

			DataType dt = DataMapper.convertPipelineConfigSchemaItemTypeToSparkDataType(configSchemaItem);
			fields.add(DataTypes.createStructField(configSchemaItem.getName(), dt, configSchemaItem.getIsNullable()));
		}

		this.schema = DataTypes.createStructType(fields);
	}

	/**
	 * 
	 * Convert JsonNode to array of key-value map
	 * 
	 * @param jsonNode
	 * @return
	 * @throws Exception
	 */
	public ArrayList<HashMap<String, Object>> convertJsonToArrayHashMap(JsonNode jsonNode) throws Exception {
		ArrayList<HashMap<String, Object>> data = new ArrayList<>();

		if (jsonNode.getNodeType() == JsonNodeType.ARRAY) {
			Iterator<JsonNode> elms = jsonNode.elements();
			while (elms.hasNext()) {
				JsonNode jsonSubNode = elms.next();
				data.add(convertJsonToHashMap(jsonSubNode));
			}
		} else {
			data.add(convertJsonToHashMap(jsonNode));
		}

		return data;
	}

	/**
	 * This is only for leaf node that has one-row-data
	 * 
	 * @param rowNode
	 * @return
	 * @throws Exception
	 */
	public HashMap<String, Object> convertJsonToHashMap(JsonNode rowNode) throws Exception {
		HashMap<String, Object> sampleDataMap = new HashMap<>();
		Iterator<Entry<String, JsonNode>> itter = rowNode.fields();

		while (itter.hasNext()) {
			Entry<String, JsonNode> field = itter.next();

			int fieldSchemaIdx = this.schema.fieldIndex(field.getKey());
			DataType fieldDataType = this.schema.fields()[fieldSchemaIdx].dataType();

			try {
				sampleDataMap.put(field.getKey(),
						DataMapper.convertJsonFieldBySparkDataType(field.getValue(), fieldDataType));
			} catch (Exception ex) {
				throw new Exception(String.format("Undefined DataType (%s) for field (%s) in convertion",
						field.getKey(), fieldDataType), ex);
			}
		}

		return sampleDataMap;
	}

	/**
	 * Run collect on simple data for warm up
	 * 
	 * @return
	 * @throws Exception
	 */
	public Dataset<Row> runCollectSampleData() throws Exception {
		List<HashMap<String, Object>> data = convertJsonToArrayHashMap(pipelineConfig.getSample());
		Dataset<Row> df = sparkService.createDF(data, this.schema);
		Dataset<Row> prediction = pipelineModel.transform(df);
		Dataset<Row> predictionOutput = selectReleventOutput(prediction);
		predictionOutput.collect();
		return predictionOutput;
	}

	/**
	 * Create array from sample single Hashmap
	 * 
	 * @param sample
	 * @return
	 */
	private List<HashMap<String, Object>> convertJsonToArrayHashMap(HashMap<String, Object> sample) {
		ArrayList<HashMap<String, Object>> data = new ArrayList<>();
		data.add(sample);

		return Arrays.asList(sample);
	}

	/**
	 * Predict by json
	 * 
	 * @param sjson
	 * @return
	 * @throws Exception
	 */
	public PredictionResponse predict(String sjson) throws Exception {
		JsonNode jsonNode = objectMapper.readTree(sjson);
		ArrayList<HashMap<String, Object>> data = convertJsonToArrayHashMap(jsonNode);
		Dataset<Row> df = sparkService.createDF(data, this.schema);
		
		return predict(df);
	}
	
	/**
	 * Predict by dataframe/dataset
	 * @param df
	 * @return
	 * @throws Exception
	 */
	public PredictionResponse predict(Dataset<Row> df) throws Exception {
		Dataset<Row> prediction = pipelineModel.transform(df);
		Dataset<Row> predictionResult = selectReleventOutput(prediction);
		return new PredictionResponse(this.pipelineConfig, predictionResult);
	}

	/**
	 * Select relevant columns (according JSON configuration file) in `output`
	 * attribute
	 * 
	 * @param prediction
	 * @return
	 */
	public Dataset<Row> selectReleventOutput(Dataset<Row> prediction) {
		Dataset<Row> predictionOutput = prediction;

		if (this.pipelineConfig.getOutput() != null && this.pipelineConfig.getOutput().length > 0) {
			System.out.println(this.pipelineConfig.getOutput());
			predictionOutput = prediction.selectExpr(this.pipelineConfig.getOutput());
		}
		return predictionOutput;
	}
}
