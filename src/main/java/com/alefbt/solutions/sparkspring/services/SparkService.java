package com.alefbt.solutions.sparkspring.services;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

@Service
public class SparkService {

	/**
	 * Spark session holder
	 */
	private SparkSession sparkSession;
	/**
	 * Models
	 */
	private HashMap<String, PipelineModel> models = new HashMap<>();
	/**
	 * Simp
	 */
	public Dataset<Row> simpleDataset;

	private JavaSparkContext sparkContext;

	@Value("classpath:data/pipline-archive.zip")
	private Resource resourceFile;
	
	public PipelineModel getPipelineModel(String name) {
		return models.get(name);
	}

	@PostConstruct
	public void init() throws IOException {
		setSparkSession(
				SparkSession.builder().master("local[*]").appName("Java Spark SQL basic example").getOrCreate());

	
		PipelineModel pl = PipelineModel.load(Paths.get(resourceFile.getFile().getParent(),"simple_pipline1").toFile().getAbsolutePath());
		models.put("test", pl);

		simpleDataset = getSparkSession().read().json(Paths.get(resourceFile.getFile().getParent(),"test1.json").toFile().getAbsolutePath());

		executePredict("test", simpleDataset);

	}

	public Dataset<Row> jsonToDf(String jsonString) {
		List<String> jsonList = new ArrayList<>();

		String t1Record = jsonString; // "{\"name\": \"Bob\"}";
		jsonList.add(t1Record);

		JavaRDD<String> t1RecordRDD = sparkContext.parallelize(jsonList);

		return sparkSession.read().json(t1RecordRDD);
	}

	public String executePredict(String modelId, Dataset<Row> dataset) {
		Dataset<Row> ds = getPipelineModel(modelId).transform(dataset);

		List<String> results = ds.toJSON().collectAsList();

		return results.get(0);
	}

	public SparkSession getSparkSession() {
		return sparkSession;
	}

	private void setSparkSession(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
		this.sparkContext = new JavaSparkContext(sparkSession.sparkContext());
	}

	public Dataset<Row> createDF(List<HashMap<String, Object>> dataArr, StructType schema) {

		List<Row> stringAsList = new ArrayList<>();

		for (HashMap<String, Object> data : dataArr) {
			
			ArrayList<Object> objs = new ArrayList<>();
			for (StructField field : schema.fields()) {
				if (data.containsKey(field.name())) 
					objs.add(data.get(field.name()));
				else
					objs.add(null);
			}

			stringAsList.add(RowFactory.create(objs.toArray()));
		}
		return sparkSession.createDataFrame(stringAsList, schema);

	}
}
