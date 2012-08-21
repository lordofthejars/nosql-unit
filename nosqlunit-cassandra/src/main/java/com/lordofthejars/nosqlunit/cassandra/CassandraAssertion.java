package com.lordofthejars.nosqlunit.cassandra;

import static ch.lambdaj.Lambda.having;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.selectUnique;
import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.SuperCountQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

import org.apache.cassandra.tools.GetVersion;
import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.model.ColumnFamilyModel;
import org.cassandraunit.model.ColumnModel;
import org.cassandraunit.model.RowModel;
import org.cassandraunit.model.SuperColumnModel;
import org.cassandraunit.serializer.GenericTypeSerializer;
import org.cassandraunit.type.GenericType;

import com.lordofthejars.nosqlunit.core.FailureHandler;

public class CassandraAssertion {

	private CassandraAssertion() {
		super();
	}

	public static void strictAssertEquals(DataSet dataset, Cluster cluster, Keyspace keyspace) {
		KeyspaceDefinition keyspaceDefinition = getKeyspaceDefinition(cluster, keyspace);

		checkKeyspaceName(dataset.getKeyspace().getName(), keyspace.getKeyspaceName());
		checkColumnsSize(dataset.getColumnFamilies(), keyspaceDefinition.getCfDefs());

		checkColumns(dataset.getColumnFamilies(), keyspace, keyspaceDefinition);

	}

	private static void checkColumns(List<ColumnFamilyModel> expectedColumnFamilies, Keyspace keyspace,
			KeyspaceDefinition keyspaceDefinition) {

		List<ColumnFamilyDefinition> columnFamilyDefinitions = keyspaceDefinition.getCfDefs();

		for (ColumnFamilyModel expectedColumnFamilyModel : expectedColumnFamilies) {

			ColumnFamilyDefinition columnFamily = checkColumnFamilyName(columnFamilyDefinitions,
					expectedColumnFamilyModel);

			ColumnType columnType = checkColumnFamilyType(expectedColumnFamilyModel, columnFamily);

			String expectedColumnFamilyName = expectedColumnFamilyModel.getName();

			List<RowModel> expectedRows = expectedColumnFamilyModel.getRows();

			checkNumberOfRowsIntoColumnFamily(keyspace, expectedColumnFamilyName, expectedRows.size());

			if (ColumnType.STANDARD == columnType) {

				for (RowModel expectedRowModel : expectedRows) {
					checkStandardColumns(keyspace, expectedColumnFamilyName, expectedRowModel);
				}

			} else {
				if (ColumnType.SUPER == columnType) {

					for (RowModel expectedRowModel : expectedRows) {
						List<ColumnModel> expectedColumns = expectedRowModel.getColumns();
						checkNotStandardColumnsInSuperColumns(expectedRowModel, expectedColumns.size());
						checkSuperColumns(keyspace, expectedColumnFamilyName, expectedRowModel);
					}

				} else {
					throw new IllegalArgumentException("Column type is not STANDARD or SUPER.");
				}
			}

		}

	}

	private static void checkNotStandardColumnsInSuperColumns(RowModel expectedRowModel, int size) throws Error {
		if(size > 0) {
			throw FailureHandler.createFailure("Standard columns for key %s are not allowed because is defined as super column.",
					expectedRowModel.getKey().getValue());
		}
	}

	private static void checkSuperColumns(Keyspace keyspace, String expectedColumnFamilyName, RowModel expectedRowModel)
			throws Error {
		List<SuperColumnModel> expectedSuperColumns = expectedRowModel.getSuperColumns();
		checkNumberOfSuperColumns(keyspace, expectedColumnFamilyName, expectedRowModel,
				expectedSuperColumns.size());

		for (SuperColumnModel expectedSuperColumnModel : expectedSuperColumns) {
			GenericType expectedSuperColumnName = expectedSuperColumnModel.getName();

			SuperColumnQuery<byte[], byte[], byte[], byte[]> createSuperColumnQuery = HFactory
					.createSuperColumnQuery(keyspace, BytesArraySerializer.get(),
							BytesArraySerializer.get(), BytesArraySerializer.get(),
							BytesArraySerializer.get());
			createSuperColumnQuery.setColumnFamily(expectedColumnFamilyName);
			createSuperColumnQuery.setKey(getBytes(expectedRowModel.getKey()));
			createSuperColumnQuery.setSuperName(getBytes(expectedSuperColumnName));
			QueryResult<HSuperColumn<byte[], byte[], byte[]>> supercolumn = createSuperColumnQuery
					.execute();

			List<ColumnModel> expectedColumns = expectedSuperColumnModel.getColumns();
			HSuperColumn<byte[], byte[], byte[]> hSuperColumn = supercolumn.get();
			
			checkSuperColumnNameAndKey(expectedSuperColumnName, hSuperColumn);
			
			List<HColumn<byte[], byte[]>> columns = hSuperColumn.getColumns();
			
			checkNumberOfColumnsInsideSuperColumn( expectedSuperColumnModel.getName().getValue(), expectedRowModel.getKey().getValue(), expectedColumns.size(), columns.size());
			checkColumnsOfSuperColumn(expectedRowModel, expectedSuperColumnModel, expectedColumns,
					columns);
			
		}
	}

	private static void checkSuperColumnNameAndKey(GenericType expectedSuperColumnName,
			HSuperColumn<byte[], byte[], byte[]> hSuperColumn) throws Error {
		if(hSuperColumn == null) {
			throw FailureHandler.createFailure("Supercolumn %s is not found into database.",
					expectedSuperColumnName.getValue());
		}
	}

	private static void checkColumnsOfSuperColumn(RowModel expectedRowModel, SuperColumnModel expectedSuperColumnModel,
			List<ColumnModel> expectedColumns, List<HColumn<byte[], byte[]>> columns) throws Error {
		for (HColumn<byte[], byte[]> hColumn : columns) {
			if(!areLoadValuesOnExpectedList(expectedColumns, hColumn.getName(), hColumn.getValue())){
				throw FailureHandler.createFailure("Row with key %s and supercolumn %s does not contain expected column.",
						expectedRowModel.getKey().getValue(),expectedSuperColumnModel.getName().getValue());
			}
		}
	}

	private static void checkNumberOfColumnsInsideSuperColumn(String supercolumnName, String key, int expectedSize,
			int size) {
		if (expectedSize != size) {
			throw FailureHandler.createFailure(
					"Expected number of columns inside supercolumn %s for key %s is %s but was counted %s.",
					supercolumnName, key, size, expectedSize);
		}
	}

	private static void checkNumberOfSuperColumns(Keyspace keyspace, String expectedColumnFamilyName,
			RowModel expectedRowModel, int size) throws Error {
		int countNumberOfSuperColumnsByKey = countNumberOfColumnsByKey(keyspace, expectedColumnFamilyName,
				expectedRowModel);

		if (countNumberOfSuperColumnsByKey != size) {
			throw FailureHandler.createFailure("Expected number of supercolumns for key %s is %s but was counted %s.",
					expectedRowModel.getKey().getValue(), size, countNumberOfSuperColumnsByKey);
		}
	}

	private static void checkStandardColumns(Keyspace keyspace, String expectedColumnFamilyName,
			RowModel expectedRowModel) throws Error {

		checkNumberOfColumns(keyspace, expectedColumnFamilyName, expectedRowModel);

		List<ColumnModel> expectedColumns = expectedRowModel.getColumns();

		for (ColumnModel expectedColumnModel : expectedColumns) {

			ColumnQuery<byte[], byte[], byte[]> columnQuery = HFactory.createColumnQuery(keyspace,
					BytesArraySerializer.get(), BytesArraySerializer.get(), BytesArraySerializer.get());
			columnQuery.setColumnFamily(expectedColumnFamilyName).setKey(getBytes(expectedRowModel.getKey()))
					.setName(getBytes(expectedColumnModel.getName()));
			QueryResult<HColumn<byte[], byte[]>> result = columnQuery.execute();

			HColumn<byte[], byte[]> hColumn = result.get();
			
			checkColumnName(expectedColumnModel, hColumn);
			checkColumnValue(expectedRowModel, hColumn);
		}
	}

	private static void checkColumnName(ColumnModel expectedColumnModel, HColumn<byte[], byte[]> hColumn) throws Error {
		if(hColumn == null) {
			throw FailureHandler.createFailure("Expected name of column is %s but was not found.",
					expectedColumnModel.getName().getValue());
		}
	}

	private static byte[] getBytes(GenericType genericType) {

		return GenericTypeSerializer.get().toBytes(genericType);

	}

	private static void checkColumnValue(RowModel expectedRowModel, HColumn<byte[], byte[]> hColumn) throws Error {
		byte[] expectedColumnName = hColumn.getName();
		byte[] expectedColumnValue = hColumn.getValue();

		if (!areLoadValuesOnExpectedList(expectedRowModel.getColumns(), expectedColumnName, expectedColumnValue)) {
			throw FailureHandler.createFailure("Row with key %s does not contain column with name %s and value %s.",
					expectedRowModel.getKey().getValue(), new String(expectedColumnName), new String(
							expectedColumnValue));
		}
	}

	private static boolean areLoadValuesOnExpectedList(List<ColumnModel> columns, byte[] expectedColumnName,
			byte[] expectedColumnValue) {

		boolean found = false;

		for (ColumnModel columnModel : columns) {
			byte[] columnName = getBytes(columnModel.getName());
			byte[] columnValue = getBytes(columnModel.getValue());

			boolean equalsName = Arrays.equals(expectedColumnName, columnName);
			boolean equalsValue = Arrays.equals(expectedColumnValue, columnValue);

			if (equalsName && equalsValue) {
				found = true;
				break;
			}

		}
		return found;
	}

	private static void checkNumberOfColumns(Keyspace keyspace, String expectedColumnFamilyName,
			RowModel expectedRowModel) throws Error {
		int numberOfColumns = countNumberOfColumnsByKey(keyspace, expectedColumnFamilyName, expectedRowModel);

		int expectedNumberOfColumns = expectedRowModel.getColumns().size();
		if (numberOfColumns != expectedNumberOfColumns) {
			throw FailureHandler.createFailure("Expected number of columns for key %s is %s but was counted %s.",
					expectedRowModel.getKey().getValue(), expectedNumberOfColumns, numberOfColumns);
		}
	}

	// change to bytearray instead of string
	private static int countNumberOfColumnsByKey(Keyspace keyspace, String expectedColumnFamilyName,
			RowModel expectedRowModel) {
		QueryResult<Integer> qr = HFactory.createCountQuery(keyspace, StringSerializer.get(), StringSerializer.get())
				.setColumnFamily(expectedColumnFamilyName).setKey(expectedRowModel.getKey().getValue())
				.setRange(null, null, 1000000000).execute();

		int numberOfColumns = qr.get();
		return numberOfColumns;
	}

	private static void checkNumberOfRowsIntoColumnFamily(Keyspace keyspace, String expectedColumnFamilyName,
			int expectedSize) throws Error {
		int currentRowsSize = countNumberOfRowsByFamilyColumn(keyspace, expectedColumnFamilyName);
		if (expectedSize != currentRowsSize) {
			throw FailureHandler.createFailure("Expected keys for column family %s is %s but was counted %s.",
					expectedColumnFamilyName, expectedSize, currentRowsSize);
		}
	}

	private static ColumnType checkColumnFamilyType(ColumnFamilyModel expectedColumnFamilyModel,
			ColumnFamilyDefinition columnFamily) {
		ColumnType expectedColumnType = expectedColumnFamilyModel.getType();
		ColumnType columnType = columnFamily.getColumnType();

		if (expectedColumnType != columnType) {
			throw FailureHandler.createFailure("Expected column family type is %s but was found %s.",
					expectedColumnType, columnType);
		}

		return columnType;

	}

	private static ColumnFamilyDefinition checkColumnFamilyName(List<ColumnFamilyDefinition> columnFamilyDefinitions,
			ColumnFamilyModel expectedColumnFamilyModel) throws Error {
		ColumnFamilyDefinition columnFamily = selectUnique(columnFamilyDefinitions,
				having(on(ColumnFamilyDefinition.class).getName(), equalTo(expectedColumnFamilyModel.getName())));

		if (columnFamily == null) {
			throw FailureHandler.createFailure("Expected name of column family is %s but was not found.",
					expectedColumnFamilyModel.getName());
		}
		return columnFamily;
	}

	private static void checkColumnsSize(List<ColumnFamilyModel> expectedColumnFamilies,
			List<ColumnFamilyDefinition> columnFamilyDefinitions) throws Error {
		if (expectedColumnFamilies.size() != columnFamilyDefinitions.size()) {
			throw FailureHandler.createFailure("Expected number of column families is %s but was %s.",
					expectedColumnFamilies.size(), columnFamilyDefinitions.size());
		}
	}

	private static void checkKeyspaceName(String expectedKeysaceName, String keyspaceName) {
		if (!expectedKeysaceName.equals(keyspaceName)) {
			throw FailureHandler.createFailure("Expected keyspace name is %s but was %s.", expectedKeysaceName,
					keyspaceName);
		}
	}

	private static KeyspaceDefinition getKeyspaceDefinition(Cluster cluster, Keyspace keyspaceName) {
		return cluster.describeKeyspace(keyspaceName.getKeyspaceName());
	}

	private static int countNumberOfRowsByFamilyColumn(Keyspace keyspace, String columnFamilyName) {

		CqlQuery<String, String, Long> cqlQuery = new CqlQuery<String, String, Long>(keyspace, StringSerializer.get(),
				StringSerializer.get(), new LongSerializer());
		cqlQuery.setQuery("SELECT COUNT(*) FROM " + columnFamilyName);
		QueryResult<CqlRows<String, String, Long>> result = cqlQuery.execute();
		return result.get().getAsCount();

	}

}
