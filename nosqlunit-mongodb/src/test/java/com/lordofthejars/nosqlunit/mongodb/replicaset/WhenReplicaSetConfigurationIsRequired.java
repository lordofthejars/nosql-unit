package com.lordofthejars.nosqlunit.mongodb.replicaset;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetBuilder.replicaSet;
import static com.lordofthejars.nosqlunit.mongodb.replicaset.ReplicaSetConfigurationBuilder.SettingsBuilder.settings;

import java.util.concurrent.TimeUnit;


import org.junit.Test;

import com.lordofthejars.nosqlunit.mongodb.ManagedMongoDbLifecycleManager;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class WhenReplicaSetConfigurationIsRequired {

	@Test
	public void eligible_Server_Should_Be_Added() {

		ManagedMongoDbLifecycleManager managedInstance = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance.getHost()).thenReturn("localhost");
		when(managedInstance.getPort()).thenReturn(21017);

		ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs0").eligible(managedInstance).get();

		DBObject configuration = replicaSetManagedMongoDb.getConfigurationDocument().getConfiguration();
		String serializedConfiguration = JSON.serialize(configuration);
		assertThat(
				serializedConfiguration,
				is("{ \"_id\" : \"rs0\" , \"version\" : 1 , \"members\" : [ { \"_id\" : 0 , \"host\" : \"localhost:21017\"}]}"));
	}

	@Test
	public void secondary_Server_Should_Be_Added() {

		ManagedMongoDbLifecycleManager managedInstance = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance.getHost()).thenReturn("localhost");
		when(managedInstance.getPort()).thenReturn(21017);

		ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs0").secondary(managedInstance).get();

		DBObject configuration = replicaSetManagedMongoDb.getConfigurationDocument().getConfiguration();
		String serializedConfiguration = JSON.serialize(configuration);
		assertThat(
				serializedConfiguration,
				is("{ \"_id\" : \"rs0\" , \"version\" : 1 , \"members\" : [ { \"_id\" : 0 , \"host\" : \"localhost:21017\" , \"priority\" : 0}]}"));
	}

	@Test
	public void hidden_Server_Should_Be_Added() {

		ManagedMongoDbLifecycleManager managedInstance = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance.getHost()).thenReturn("localhost");
		when(managedInstance.getPort()).thenReturn(21017);

		ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs0").hidden(managedInstance).get();

		DBObject configuration = replicaSetManagedMongoDb.getConfigurationDocument().getConfiguration();
		String serializedConfiguration = JSON.serialize(configuration);

		assertThat(
				serializedConfiguration,
				is("{ \"_id\" : \"rs0\" , \"version\" : 1 , \"members\" : [ { \"_id\" : 0 , \"host\" : \"localhost:21017\" , \"priority\" : 0 , \"hidden\" : true}]}"));
	}

	@Test
	public void arbiter_Server_Should_Be_Added() {

		ManagedMongoDbLifecycleManager managedInstance = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance.getHost()).thenReturn("localhost");
		when(managedInstance.getPort()).thenReturn(21017);

		ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs0").arbiter(managedInstance).get();

		DBObject configuration = replicaSetManagedMongoDb.getConfigurationDocument().getConfiguration();
		String serializedConfiguration = JSON.serialize(configuration);

		assertThat(
				serializedConfiguration,
				is("{ \"_id\" : \"rs0\" , \"version\" : 1 , \"members\" : [ { \"_id\" : 0 , \"host\" : \"localhost:21017\" , \"arbiterOnly\" : true}]}"));
	}

	@Test
	public void none_Voter_Server_Should_Be_Added() {

		ManagedMongoDbLifecycleManager managedInstance = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance.getHost()).thenReturn("localhost");
		when(managedInstance.getPort()).thenReturn(21017);

		ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs0").noneVoter(managedInstance).get();

		DBObject configuration = replicaSetManagedMongoDb.getConfigurationDocument().getConfiguration();
		String serializedConfiguration = JSON.serialize(configuration);

		assertThat(
				serializedConfiguration,
				is("{ \"_id\" : \"rs0\" , \"version\" : 1 , \"members\" : [ { \"_id\" : 0 , \"host\" : \"localhost:21017\" , \"votes\" : 0}]}"));
	}

	@Test
	public void delayed_Server_Should_Be_Added() {

		ManagedMongoDbLifecycleManager managedInstance = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance.getHost()).thenReturn("localhost");
		when(managedInstance.getPort()).thenReturn(21017);

		ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs0").delayed(managedInstance, 20,
				TimeUnit.MINUTES).get();

		DBObject configuration = replicaSetManagedMongoDb.getConfigurationDocument().getConfiguration();
		String serializedConfiguration = JSON.serialize(configuration);

		assertThat(
				serializedConfiguration,
				is("{ \"_id\" : \"rs0\" , \"version\" : 1 , \"members\" : [ { \"_id\" : 0 , \"host\" : \"localhost:21017\" , \"priority\" : 0 , \"slaveDelay\" : 1200}]}"));
	}

	@Test
	public void custom_Server_Should_Be_Added() {

		ManagedMongoDbLifecycleManager managedInstance = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance.getHost()).thenReturn("localhost");
		when(managedInstance.getPort()).thenReturn(21017);

		ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs0").server(managedInstance).arbiterOnly()
				.buildIndexes().tags("prop1", "val1", "prop2", "val2").configure().get();

		DBObject configuration = replicaSetManagedMongoDb.getConfigurationDocument().getConfiguration();
		String serializedConfiguration = JSON.serialize(configuration);

		assertThat(
				serializedConfiguration,
				is("{ \"_id\" : \"rs0\" , \"version\" : 1 , \"members\" : [ { \"_id\" : 0 , \"host\" : \"localhost:21017\" , \"arbiterOnly\" : true , \"buildIndexes\" : true , \"tags\" : { \"prop1\" : \"val1\" , \"prop2\" : \"val2\"}}]}"));
	}

	@Test
	public void settings_Parameters_Should_Be_Added() {
		
		ManagedMongoDbLifecycleManager managedInstance = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance.getHost()).thenReturn("localhost");
		when(managedInstance.getPort()).thenReturn(21017);

		ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs0", settings().getLastErrorModes("{\"DRSafe\":{\"region\":2}}").get()).eligible(managedInstance).get();

		DBObject configuration = replicaSetManagedMongoDb.getConfigurationDocument().getConfiguration();
		String serializedConfiguration = JSON.serialize(configuration);
		
		assertThat(
				serializedConfiguration,
				is("{ \"_id\" : \"rs0\" , \"version\" : 1 , \"members\" : [ { \"_id\" : 0 , \"host\" : \"localhost:21017\"}] , \"settings\" : { \"getLastErrorModes\" : { \"DRSafe\" : { \"region\" : 2}}}}"));
		
	}
	
	@Test
	public void complex_Scenarios_Should_Be_Build() {
		
		ManagedMongoDbLifecycleManager managedInstance1 = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance1.getHost()).thenReturn("localhost");
		when(managedInstance1.getPort()).thenReturn(21017);
		
		ManagedMongoDbLifecycleManager managedInstance2 = mock(ManagedMongoDbLifecycleManager.class);
		when(managedInstance2.getHost()).thenReturn("localhost");
		when(managedInstance2.getPort()).thenReturn(21018);

		ReplicaSetManagedMongoDb replicaSetManagedMongoDb = replicaSet("rs0").eligible(managedInstance1).secondary(managedInstance2).get();

		DBObject configuration = replicaSetManagedMongoDb.getConfigurationDocument().getConfiguration();
		String serializedConfiguration = JSON.serialize(configuration);
		
		assertThat(
				serializedConfiguration,
				is("{ \"_id\" : \"rs0\" , \"version\" : 1 , \"members\" : [ { \"_id\" : 0 , \"host\" : \"localhost:21017\"} , { \"_id\" : 1 , \"host\" : \"localhost:21018\" , \"priority\" : 0}]}"));
		
	}
	
}
