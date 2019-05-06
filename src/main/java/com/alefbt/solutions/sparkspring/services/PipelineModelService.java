package com.alefbt.solutions.sparkspring.services;

import java.io.File;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.alefbt.solutions.sparkspring.domain.PipelineModelArchive;

@Service
public class PipelineModelService {

	@Autowired
	private ZipService zipService;

	@Autowired
	private SparkService sparkService;

	@Value("${pipelines.tempfolder:/tmp/pipelines}")
	private String tempFolder;
	
	@Value("${pipelines.folder}")
	private String pipelinesFolder;

	/**
	 * Get pipline
	 * @param name
	 * @return
	 * @throws Exception
	 */
	public PipelineModelArchive getPipeline(String name) throws Exception {
		return retrivePipeline(name);
	}

	/**
	 * Retrive piplene
	 * 
	 * @param name
	 * @return
	 * @throws Exception
	 */
	private PipelineModelArchive retrivePipeline(String name) throws Exception {
		File zipfile = Paths.get(pipelinesFolder, String.format("%s.zip", name)).toFile();

		if (!zipfile.exists())
			throw new Exception("Pipeline not exists in repo " + zipfile.getAbsolutePath());

		File dest = Paths.get(tempFolder, name).toFile();

		FileUtils.forceDeleteOnExit(dest);

		File unzipped = zipService.unzip(zipfile, dest);

		File configFile = Paths.get(unzipped.getAbsolutePath(), "mlserving.json").toFile();

		PipelineModelArchive out = new PipelineModelArchive(configFile, sparkService);

		return out;
	}

}
