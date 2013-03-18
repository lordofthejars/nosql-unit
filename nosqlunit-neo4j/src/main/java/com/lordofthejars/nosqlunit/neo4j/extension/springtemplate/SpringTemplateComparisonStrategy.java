package com.lordofthejars.nosqlunit.neo4j.extension.springtemplate;

import static ch.lambdaj.Lambda.selectFirst;
import static ch.lambdaj.Lambda.having;
import static org.hamcrest.CoreMatchers.equalTo;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.springframework.data.neo4j.config.JtaTransactionManagerFactoryBean;
import org.springframework.data.neo4j.conversion.EndResult;
import org.springframework.data.neo4j.support.Neo4jTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.neo4j.Neo4jComparisonStrategy;
import com.lordofthejars.nosqlunit.neo4j.Neo4jConnectionCallback;

public class SpringTemplateComparisonStrategy implements Neo4jComparisonStrategy {

	@Override
	public boolean compare(Neo4jConnectionCallback connection, InputStream dataset) throws NoSqlAssertionError,
			Throwable {

		DataParser dataParser = new DataParser();
		List<Object> expectedObjects = dataParser.readValues(dataset);

		Multimap<Class<?>, Object> expectedGroupByClass = groupByClass(expectedObjects);

		Set<Class<?>> expectedClasses = expectedGroupByClass.keySet();

		for (Class<?> expectedClass : expectedClasses) {

			Collection<Object> expectedObjectsByClass = expectedGroupByClass.get(expectedClass);
			List<Object> insertedObjects = findAndFetchAllEntitiesByClass(neo4jTemplate(connection), expectedClass);

			for (Object expectedObject : expectedObjectsByClass) {
				
				Object selectFirst = selectFirst(insertedObjects, equalTo(expectedObject));
				
				if(selectFirst == null) {
					throw new NoSqlAssertionError(String.format("Object %s is not found in graph.", expectedObject.toString()));
				}
				
			}
			
		}

		return true;
	}

	private List<Object> findAndFetchAllEntitiesByClass(final Neo4jTemplate neo4jTemplate, final Class<?> entityClass) {

		TransactionTemplate transactionalTemplate = transactionalTemplate(neo4jTemplate.getGraphDatabaseService());
		return transactionalTemplate.execute(new TransactionCallback<List<Object>>() {

			@Override
			public List<Object> doInTransaction(TransactionStatus status) {

				EndResult<?> allEntities = neo4jTemplate.findAll(entityClass);
				final List<Object> fetchedData = fetchData(neo4jTemplate, allEntities);
				return fetchedData;
			}

			private List<Object> fetchData(final Neo4jTemplate neo4jTemplate, EndResult<?> allEntities) {
				final List<Object> fetchedData = new ArrayList<Object>();
				Iterator<?> iterator = allEntities.iterator();

				while (iterator.hasNext()) {
					Object entity = iterator.next();
					fetchedData.add(neo4jTemplate.fetch(entity));
				}
				return fetchedData;
			}
		});
	}

	private TransactionTemplate transactionalTemplate(GraphDatabaseService graphDatabaseService) {

		try {
			JtaTransactionManagerFactoryBean jtaTransactionManagerFactoryBean = new JtaTransactionManagerFactoryBean(
					graphDatabaseService);
			return new TransactionTemplate(jtaTransactionManagerFactoryBean.getObject());
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}

	}

	private Multimap<Class<?>, Object> groupByClass(List<Object> objects) {

		Multimap<Class<?>, Object> groupByClass = ArrayListMultimap.create();

		for (Object object : objects) {
			groupByClass.put(object.getClass(), object);
		}

		return groupByClass;
	}

	private Neo4jTemplate neo4jTemplate(Neo4jConnectionCallback connection) {
		GraphDatabaseService graphDatabaseService = connection.graphDatabaseService();
		Neo4jTemplate neo4jTemplate = new Neo4jTemplate(graphDatabaseService);

		return neo4jTemplate;
	}

}
