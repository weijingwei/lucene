package com.lucene.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource(value = "classpath:lucene.properties", ignoreResourceNotFound=false)
public class LucenePropertiesConfigure {
	@Autowired
	private Environment env;
	
	@Bean
	public String getIndexDir() {
		return env.getProperty("index_dir");
	}
	
	@Bean
	public String getIndexNRTDir() {
		return env.getProperty("index_NRT_dir");
	}
	
	@Bean
	public String getDocDir() {
		return env.getProperty("doc_dir");
	}
	
}