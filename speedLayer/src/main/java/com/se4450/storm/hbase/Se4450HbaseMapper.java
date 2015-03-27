package com.se4450.storm.hbase;

import static org.apache.storm.hbase.common.Utils.toBytes;
import static org.apache.storm.hbase.common.Utils.toLong;

import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class Se4450HbaseMapper implements HBaseMapper {
    /**
	 * 
	 */
	private static final long serialVersionUID = -7254118384063529095L;

	private static final Logger LOG = LoggerFactory.getLogger(SimpleHBaseMapper.class);
    
    private String rowKeyField;
    private String columnQualifierField;
    private String valueField;
    private byte[] columnFamily;

    public Se4450HbaseMapper(){
    }

    public Se4450HbaseMapper withColunQualifierField(String columnQualifierField){
        this.columnQualifierField = columnQualifierField;
        return this;
    }
    
    public Se4450HbaseMapper withValueField(String valueField){
        this.valueField = valueField;
        return this;
    }

    public Se4450HbaseMapper withRowKeyField(String rowKeyField){
        this.rowKeyField = rowKeyField;
        return this;
    }

    public Se4450HbaseMapper withColumnFamily(String columnFamily){
        this.columnFamily = columnFamily.getBytes();
        return this;
    }

    @Override
    public byte[] rowKey(Tuple tuple) {
        Object objVal = tuple.getValueByField(this.rowKeyField);
        return toBytes(objVal);
    }

    @Override
    public ColumnList columns(Tuple tuple) {
        ColumnList cols = new ColumnList();
        if(this.columnQualifierField != null && this.columnQualifierField.length() > 0 && this.valueField != null && this.valueField.length() > 0){
        	if(tuple.contains(columnQualifierField) && tuple.contains(valueField)){
                cols.addColumn(this.columnFamily, toBytes(tuple.getValueByField(this.columnQualifierField)), toBytes(tuple.getValueByField(this.valueField)));
        	}
        }
        return cols;
    }
}
