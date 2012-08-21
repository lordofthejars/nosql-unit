package com.lordofthejars.nosqlunit.demo.cassandra;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static com.lordofthejars.nosqlunit.cassandra.EmbeddedCassandra.EmbeddedCassandraRuleBuilder.newEmbeddedCassandraRule;
import static com.lordofthejars.nosqlunit.cassandra.EmbeddedCassandraConfigurationBuilder.newEmbeddedCassandraConfiguration;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.cassandra.CassandraRule;
import com.lordofthejars.nosqlunit.cassandra.EmbeddedCassandra;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;

public class WhenPersonWantsToKnowItsCar {

	@ClassRule
	public static EmbeddedCassandra embeddedCassandraRule = newEmbeddedCassandraRule().build();
	
	@Rule
	public CassandraRule cassandraRule = new CassandraRule(newEmbeddedCassandraConfiguration().clusterName("Test Cluster").build());
	
	
	@Test
	@UsingDataSet(locations="persons.json", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	public void car_should_be_returned() {
		
		PersonManager personManager = new PersonManager("Test Cluster", "persons", "localhost:9171");
		String car = personManager.getCarByPersonName("mary");
		
		assertThat(car, is("ford"));
		
	}
	
}
