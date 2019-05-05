package com.alefbt.solutions.sparkspring.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.alefbt.solutions.sparkspring.domain.BasicKVMap;
import com.alefbt.solutions.sparkspring.domain.PipelineConfig;
import com.alefbt.solutions.sparkspring.domain.PipelineConfigSchemaItem;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataMapper {

	public static Object convertJsonFieldBySparkDataType(JsonNode field, DataType fieldDataType) throws Exception {

		if (fieldDataType == DataTypes.StringType) {
			return field.asText();
		} else if (fieldDataType == DataTypes.BooleanType) {
			return field.asBoolean();
		} else if (fieldDataType == DataTypes.LongType) {
			return field.asLong();
		} else if (fieldDataType == DataTypes.FloatType || fieldDataType == DataTypes.DoubleType) {
			return field.asDouble();
		} else if (fieldDataType == DataTypes.IntegerType) {
			return field.asInt();
		} else {
			throw new Exception(String.format("Undefined DataType (%s) in convertion", fieldDataType));
		}
	}

	public static DataType convertPipelineConfigSchemaItemTypeToSparkDataType(PipelineConfigSchemaItem configSchemaItem)
			throws Exception {

		DataType dt = null;

		switch (configSchemaItem.getType()) {
		case DOUBEL:
			dt = DataTypes.DoubleType;
			break;
		case FLOAT:
			dt = DataTypes.FloatType;
			break;
		case INT:
			dt = DataTypes.IntegerType;
			break;
		case STRING:
			dt = DataTypes.StringType;
			break;
		default:
			throw new Exception("configSchemaItem type not found");

		}

		return dt;

	}

	private static ObjectMapper objectMapper = new ObjectMapper();

	public static ObjectMapper getObjMapper() {
		if (objectMapper == null)
			objectMapper = new ObjectMapper();

		return objectMapper;
	}

	public static ArrayList<HashMap<String, Object>> convertDatasetToHashMapArrayBySchema(PipelineConfig config,
			Dataset<Row> data) throws IOException {

		ArrayList<HashMap<String, Object>> retData = new ArrayList<>();

		
		for (String row : data.toJSON().collectAsList()) {
			HashMap<String, Object> rowMap = new HashMap<>();

			BasicKVMap k = getObjMapper().readValue(row, BasicKVMap.class);

			if (config.getOutput() == null || config.getOutput().length == 0) {
				rowMap = k;
			} else {
				for (String outputField : config.getOutput()) {
					if (k.containsKey(outputField))
						rowMap.put(outputField, k.get(outputField));
				}
			}

			retData.add(rowMap);
		}
		return retData;
	}
}
