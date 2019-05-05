package com.alefbt.solutions.sparkspring.domain;

import java.util.ArrayList;
import java.util.HashMap;
/**
 * 
 * Pipeline config represents mlserving.config 
 * you can see it on src/main/resource/mlserving.json
 * 
 * <pre>
 * {
    "name":"Test pipeline",
    "pipeline":"simple_pipline1",
    "schema":[
        {"name":"name","type":"STRING", "isNullable":false}
    ],
    "sample":{
        "name":"Bob"
    },
    "output": ["name", "features"]
}
 * </pre>
 * 
 * @author yehuda
 *
 */
public class PipelineConfig {
	private String name;
	private String pipeline;

	private ArrayList<PipelineConfigSchemaItem> schema;
	private HashMap<String, Object> sample;
	private String[] output;

	public String getPipeline() {
		return pipeline;
	}

	public String getName() {
		return name;
	}

	public ArrayList<PipelineConfigSchemaItem> getSchema() {
		return schema;
	}

	public HashMap<String, Object> getSample() {
		return sample;
	}

	public String[] getOutput() {
		return this.output;
	}
}
