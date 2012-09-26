package com.lordofthejars.nosqlunit.cassandra;


import static com.lordofthejars.nosqlunit.cassandra.EmbeddedCassandraConfigurationBuilder.newEmbeddedCassandraConfiguration;
import static com.lordofthejars.nosqlunit.cassandra.ManagedCassandraConfigurationBuilder.newManagedCassandraConfiguration;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import me.prettyprint.cassandra.service.CassandraHost;

import org.junit.Test;

public class WhenDefaultCassandraRuleConfigurationIsCreated {

	@Test
	public void embedded_parameter_values_should_contain_default_values() {
		
		CassandraConfiguration embeddedConfiguration = newEmbeddedCassandraConfiguration().clusterName("cluster").build();
		
		assertThat(embeddedConfiguration.getHost(), is("127.0.0.1"));
		assertThat(embeddedConfiguration.getPort(), is(EmbeddedCassandra.DEFAULT_PORT));
		assertThat(embeddedConfiguration.getClusterName(), is("cluster"));
	}
	
	@Test
	public void managed_parameter_values_should_contain_default_values() {
		
		CassandraConfiguration managedConfiguration = newManagedCassandraConfiguration().clusterName("cluster").build();
		
		assertThat(managedConfiguration.getHost(), is("127.0.0.1"));
		assertThat(managedConfiguration.getPort(), is(CassandraHost.DEFAULT_PORT));
		assertThat(managedConfiguration.getClusterName(), is("cluster"));
		
	}
}