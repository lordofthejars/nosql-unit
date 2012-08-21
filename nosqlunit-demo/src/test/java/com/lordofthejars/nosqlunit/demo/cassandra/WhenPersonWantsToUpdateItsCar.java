package com.lordofthejars.nosqlunit.demo.cassandra;

import static com.lordofthejars.nosqlunit.cassandra.ManagedCassandraConfigurationBuilder.newManagedCassandraConfiguration;
import static com.lordofthejars.nosqlunit.cassandra.ManagedCassandra.ManagedCassandraRuleBuilder.newManagedCassandraRule;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.annotation.ShouldMatchDataSet;
import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.cassandra.CassandraRule;
import com.lordofthejars.nosqlunit.cassandra.ManagedCassandra;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;

public class WhenPersonWantsToUpdateItsCar {

	static {
		System.setProperty("CASSANDRA_HOME", "/opt/cassandra");
	}
	
	@ClassRule
	public static ManagedCassandra managedCassandra = newManagedCassandraRule().build();
	
	@Rule
	public CassandraRule cassandraRule = new CassandraRule(newManagedCassandraConfiguration().clusterName("Test Cluster").build());
	
	@Test
	@UsingDataSet(locations="persons.json", loadStrategy=LoadStrategyEnum.CLEAN_INSERT)
	@ShouldMatchDataSet(location="expected-persons.json")
	public void new_car_should_be_updated() {
		
		PersonManager personManager = new PersonManager("Test Cluster", "persons", "localhost:9171");
		personManager.updateCarByPersonName("john", "opel");
		
	}
	
}
