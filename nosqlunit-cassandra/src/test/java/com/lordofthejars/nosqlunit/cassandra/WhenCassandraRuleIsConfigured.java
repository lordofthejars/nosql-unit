package com.lordofthejars.nosqlunit.cassandra;

import static com.lordofthejars.nosqlunit.cassandra.EmbeddedCassandraConfigurationBuilder.newEmbeddedCassandraConfiguration;
import static com.lordofthejars.nosqlunit.cassandra.ManagedCassandraConfigurationBuilder.newManagedCassandraConfiguration;
import static com.lordofthejars.nosqlunit.cassandra.RemoteCassandraConfigurationBuilder.newRemoteCassandraConfiguration;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import me.prettyprint.cassandra.service.CassandraHost;

import org.junit.Test;

public class WhenCassandraRuleIsConfigured {

	@Test
	public void embedded_cassandra_should_be_configured_with_default_parameters() {
		
		CassandraConfiguration embeddedConfiguration = newEmbeddedCassandraConfiguration().clusterName("test").build();
		
		assertThat(embeddedConfiguration.getClusterName(), is("test"));
		assertThat(embeddedConfiguration.getHost(), is("127.0.0.1"));
		assertThat(embeddedConfiguration.getPort(), is(EmbeddedCassandra.PORT));
		
	}
	
	@Test
	public void embedded_cassandra_should_be_configured_with_set_parameters() {
		
		CassandraConfiguration embeddedConfiguration = newEmbeddedCassandraConfiguration().clusterName("test").connectionIdentifier("connect").port(11).build();
		
		assertThat(embeddedConfiguration.getClusterName(), is("test"));
		assertThat(embeddedConfiguration.getHost(), is("127.0.0.1"));
		assertThat(embeddedConfiguration.getPort(), is(11));
		assertThat(embeddedConfiguration.getConnectionIdentifier(), is("connect"));
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void embedded_cassandra_should_throw_an_exception_if_not_set_cluster_name() {
		newEmbeddedCassandraConfiguration().build();
	}
	
	@Test
	public void managed_cassandra_should_be_configured_with_default_parameters() {
		CassandraConfiguration managedConfiguration = newManagedCassandraConfiguration().clusterName("test").build();
		
		assertThat(managedConfiguration.getClusterName(), is("test"));
		assertThat(managedConfiguration.getHost(), is("127.0.0.1"));
		assertThat(managedConfiguration.getPort(), is(CassandraHost.DEFAULT_PORT));
		
	}
	
	@Test
	public void managed_cassandra_should_be_configured_with_set_parameters() {
		CassandraConfiguration managedConfiguration = newManagedCassandraConfiguration().clusterName("test").connectionIdentifier("connect").port(11).build();
		
		assertThat(managedConfiguration.getClusterName(), is("test"));
		assertThat(managedConfiguration.getHost(), is("127.0.0.1"));
		assertThat(managedConfiguration.getPort(), is(11));
		assertThat(managedConfiguration.getConnectionIdentifier(), is("connect"));
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void managed_cassandra_should_throw_an_exception_if_not_set_cluster_name() {
		newManagedCassandraConfiguration().build();
	}
	
	@Test
	public void remote_cassandra_should_be_configured_with_default_parameters() {
		CassandraConfiguration managedConfiguration = newRemoteCassandraConfiguration().clusterName("test").host("192.168.1.1").build();
		
		assertThat(managedConfiguration.getClusterName(), is("test"));
		assertThat(managedConfiguration.getHost(), is("192.168.1.1"));
		assertThat(managedConfiguration.getPort(), is(CassandraHost.DEFAULT_PORT));
		
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void remote_cassandra_should_throw_an_exception_if_not_set_cluster_name() {
		newRemoteCassandraConfiguration().build();
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void remote_cassandra_should_throw_an_exception_if_not_set_host() {
		newRemoteCassandraConfiguration().clusterName("test").build();
	}
	
}
