package com.lordofthejars.nosqlunit.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.lordofthejars.nosqlunit.hbase.model.ParsedColumnFamilyModel;
import com.lordofthejars.nosqlunit.hbase.model.ParsedColumnModel;
import com.lordofthejars.nosqlunit.hbase.model.ParsedDataModel;
import com.lordofthejars.nosqlunit.hbase.model.ParsedRowModel;

public class DataLoader {

    protected Configuration configuration;

    public DataLoader(Configuration configuration) {
        this.configuration = configuration;
    }

    public void load(ParsedDataModel parsedDataModel) throws IOException {

        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);

        byte[] tableName = parsedDataModel.getName().getBytes();

        if (!hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.createTable(new HTableDescriptor(tableName));
        }

        hBaseAdmin.disableTable(tableName);
        HTableDescriptor tableDescriptor = hBaseAdmin.getTableDescriptor(tableName);

        List<ParsedColumnFamilyModel> columnFamilies = parsedDataModel.getColumnFamilies();

        for (ParsedColumnFamilyModel parsedColumnFamilyModel : columnFamilies) {

            byte[] familyName = parsedColumnFamilyModel.getName().getBytes();

            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);

            if (existsColumnFamily(tableDescriptor, parsedColumnFamilyModel)) {
                hBaseAdmin.modifyColumn(tableName, hColumnDescriptor);
            } else {
                hBaseAdmin.addColumn(tableName, hColumnDescriptor);
            }

        }

        hBaseAdmin.enableTable(tableName);

        HTable table = new HTable(configuration, tableName);

        for (ParsedColumnFamilyModel parsedColumnFamilyModel : columnFamilies) {
            byte[] familyName = parsedColumnFamilyModel.getName().getBytes();

            List<ParsedRowModel> rows = parsedColumnFamilyModel.getRows();

            for (ParsedRowModel parsedRowModel : rows) {
                byte[] key = parsedRowModel.getKeyInBytes();

                List<ParsedColumnModel> columns = parsedRowModel.getColumns();

                Put column = new Put(key);
                for (ParsedColumnModel parsedColumnModel : columns) {

                    byte[] columnName = parsedColumnModel.getName().getBytes();
                    byte[] columnValuee = parsedColumnModel.getValueInBytes();

                    column.add(familyName, columnName, columnValuee);
                }

                table.put(column);
                table.flushCommits();
            }

        }

        table.close();

    }

    private boolean existsColumnFamily(HTableDescriptor tableDescriptor, ParsedColumnFamilyModel parsedColumnFamilyModel) {
        return tableDescriptor.getFamily(parsedColumnFamilyModel.getName().getBytes()) != null;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

}
