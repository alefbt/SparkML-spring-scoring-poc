package com.alefbt.solutions.sparkspring.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.alefbt.solutions.sparkspring.domain.PredictionResponse;
import com.alefbt.solutions.sparkspring.services.PipelineModelService;

/**
 * 
 * PredictionController
 * 
 * @author yehuda
 *
 */
@RestController
public class PredictionController {

	/**
	 * Pipeline model service
	 */
	@Autowired
	private PipelineModelService pipelineModelService;

	/**
	 * 
	 * Example model pipline-archive
	 * 
	 * @param modelId
	 * @param httpEntity
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value = "/predict/{modelId}", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	@ResponseBody
	public PredictionResponse prdict(@PathVariable String modelId, HttpEntity<String> httpEntity) throws Exception {
		String sjson = httpEntity.getBody();
		return pipelineModelService.getPipeline(modelId).predict(sjson);
	}

}
