package com.lordofthejars.nosqlunit.demo.hbase;

import static com.lordofthejars.nosqlunit.hbase.EmbeddedHBase.EmbeddedHBaseRuleBuilder.newEmbeddedHBaseRule;
import static com.lordofthejars.nosqlunit.hbase.HBaseRule.HBaseRuleBuilder.newHBaseRule;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.hbase.EmbeddedHBase;
import com.lordofthejars.nosqlunit.hbase.HBaseRule;

public class WhenPersonWantsToKnowItsCar {

	@ClassRule
	public static EmbeddedHBase embeddedHBase = newEmbeddedHBaseRule().build();
	
	@Rule
	public HBaseRule hBaseRule = newHBaseRule().defaultEmbeddedHBase(this);
	
	@Inject
	private Configuration configuration;
	
	
	@Test
	@UsingDataSet(locations={"persons.json"}, loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	public void car_should_be_returned() throws IOException {

		PersonManager personManager = new PersonManager(configuration);
		String car = personManager.getCarByPersonName("john");
		
		assertThat(car, is("toyota"));
		
	}
	
}
