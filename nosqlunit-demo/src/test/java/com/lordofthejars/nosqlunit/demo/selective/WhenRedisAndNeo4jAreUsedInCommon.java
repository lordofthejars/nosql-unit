package com.lordofthejars.nosqlunit.demo.selective;


public class WhenRedisAndNeo4jAreUsedInCommon {

	/*@ClassRule
	public static final ManagedNeoServer MANAGED_NEO_SERVER = newManagedNeo4jServerRule().neo4jPath("/opt/neo4j-community-1.7.2").build();
	
	@ClassRule
	public static final ManagedRedis MANAGED_REDIS = newManagedRedisRule().redisPath("/opt/redis-2.4.17").build();
	
	private final Neo4jConfiguration neo4jConfiguration = newManagedNeoServerConfiguration().connectionIdentifier("neo4j").build();
	@Rule
	public final Neo4jRule neo4jRule = newNeo4jRule().configure(neo4jConfiguration).build();
	
	private final RedisConfiguration redisConfiguration = newManagedRedisConfiguration().connectionIdentifier("redis").build();
	@Rule
	public final RedisRule redisRule = newRedisRule().configure(redisConfiguration).build();
	

	@Test
	@UsingDataSet(withSelectiveLocations = {
	        @Selective(identifier = "neo4j", locations = "matrix.xml"),
	        @Selective(identifier = "redis", locations = "matrix.json") }, 
	    loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
	public void cached_friends_should_be_returned() {
		
		Jedis jedis = new Jedis(this.redisConfiguration.getHost(), this.redisConfiguration.getPort());
		RestGraphDatabase graphDatabaseService = new RestGraphDatabase(this.neo4jConfiguration.getUri());
		
		CachedMatrixManager cachedMatrixManager = new CachedMatrixManager(graphDatabaseService, jedis);
		int countNeoFriends = cachedMatrixManager.countNeoFriends();
		
		assertThat(countNeoFriends, is(10));
	}
	
	@Test
	@UsingDataSet(withSelectiveLocations = {
	        @Selective(identifier = "neo4j", locations = "matrix.xml"),
	        @Selective(identifier = "redis", locations = "matrix.json") }, 
	    loadStrategy = LoadStrategyEnum.CLEAN_INSERT)
	public void real_node_should_be_returned_from_neo_server() {
		
		Jedis jedis = new Jedis(this.redisConfiguration.getHost(), this.redisConfiguration.getPort());
		RestGraphDatabase graphDatabaseService = new RestGraphDatabase(this.neo4jConfiguration.getUri());
		
		CachedMatrixManager cachedMatrixManager = new CachedMatrixManager(graphDatabaseService, jedis);
		Node neoNode = cachedMatrixManager.getNeoNode();
		
		assertThat((String)neoNode.getProperty("name"), is("Thomas Anderson"));
		
	}
	
	*/
}
