package com.alefbt.solutions.sparkspring.services;

import java.io.File;

import org.springframework.stereotype.Service;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

@Service
public class ZipService {

	public File unzip(File zipfile, File dest) throws ZipException {
		ZipFile zfile = new ZipFile(zipfile);

		if (!zfile.isValidZipFile()) {
			throw new ZipException("Invalid zip file");
		}
		
	
		if (dest.isDirectory() && !dest.exists()) {
			dest.mkdirs();
		}
		if (zfile.isEncrypted()) {
			throw new ZipException("Encrypted zip file");
		}

		zfile.extractAll(dest.getAbsolutePath());
		
		if(dest.list().length == 1) 
			return dest.listFiles()[0];
		
		return dest;
	}
}
