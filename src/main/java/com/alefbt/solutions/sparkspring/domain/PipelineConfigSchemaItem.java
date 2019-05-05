package com.alefbt.solutions.sparkspring.domain;

/**
 * 
 * Pipeline config represents mlserving.config/schema
 * 
 * you can see it on src/main/resource/mlserving.json
 * 
 * <pre>
    "schema":[
        {"name":"name","type":"STRING", "isNullable":false}
    ]
 * </pre>
 * 
 * @author yehuda
 *
 */
public class PipelineConfigSchemaItem {
	private String name;
	private PipelineConfigSchemaItemType type;
	private Boolean isNullable = true;

	public Boolean getIsNullable() {
		return isNullable;
	}

	public String getName() {
		return name;
	}

	public PipelineConfigSchemaItemType getType() {
		return type;
	}
}
