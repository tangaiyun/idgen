package com.jswym.demo;

import org.springframework.beans.BeansException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

@SpringBootApplication
public class IdGenApplication implements ApplicationContextAware, ApplicationRunner{
	private ApplicationContext applicationContext;
	public static void main(String[] args) {
		SpringApplication.run(IdGenApplication.class, args);
		
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		applicationContext = context;
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		IDGenerator idGen = applicationContext.getBean(IDGenerator.class);
		for(int i=0; i < 40; i++) {
			System.out.println(idGen.nextId());
		}
	}
}
