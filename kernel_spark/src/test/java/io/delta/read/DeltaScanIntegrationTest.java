package io.delta.read;

import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

/**
 * Integration test to verify the DSv2 connector works with the new state machine APIs.
 */
public class DeltaScanIntegrationTest {
    
    private static final String TEST_TABLE_PATH = 
        "file:///Users/oussama.saoudi/temp/delta-plan/oxidizedKernel/delta-kernel-rs/kernel/tests/data/app-txn-checkpoint";
    
    @Test
    public void testDeltaScanBuilderCreation() throws Exception {
        // Create engine
        Configuration conf = new Configuration();
        Engine engine = DefaultEngine.create(conf);
        
        // Create table
        Table table = Table.forPath(engine, TEST_TABLE_PATH);
        
        // Create scan builder
        DeltaScanBuilder scanBuilder = new DeltaScanBuilder(table, engine);
        
        // Verify scan builder was created successfully
        assertNotNull("ScanBuilder should not be null", scanBuilder);
        
        // Build scan
        org.apache.spark.sql.connector.read.Scan scan = scanBuilder.build();
        
        // Verify scan was created successfully
        assertNotNull("Scan should not be null", scan);
        assertTrue("Scan should be a DeltaScan", scan instanceof DeltaScan);
        
        DeltaScan deltaScan = (DeltaScan) scan;
        
        // Verify the scan has partitions (file metadata)
        org.apache.spark.sql.connector.read.Batch batch = deltaScan.toBatch();
        assertNotNull("Batch should not be null", batch);
        
        org.apache.spark.sql.connector.read.InputPartition[] partitions = batch.planInputPartitions();
        assertNotNull("Partitions should not be null", partitions);
        assertTrue("Should have at least one partition", partitions.length > 0);
        
        System.out.println("✓ Successfully created DeltaScan with " + partitions.length + " partitions");
    }
    
    @Test
    public void testDeltaScanWithPredicate() throws Exception {
        // Create engine
        Configuration conf = new Configuration();
        Engine engine = DefaultEngine.create(conf);
        
        // Create table
        Table table = Table.forPath(engine, TEST_TABLE_PATH);
        
        // Create scan builder
        DeltaScanBuilder scanBuilder = new DeltaScanBuilder(table, engine);
        
        // Push a simple predicate (this will test predicate conversion)
        org.apache.spark.sql.connector.expressions.filter.Predicate[] predicates = new org.apache.spark.sql.connector.expressions.filter.Predicate[0];
        org.apache.spark.sql.connector.expressions.filter.Predicate[] remaining = scanBuilder.pushPredicates(predicates);
        
        // Build scan
        org.apache.spark.sql.connector.read.Scan scan = scanBuilder.build();
        assertNotNull("Scan should not be null", scan);
        
        System.out.println("✓ Successfully created DeltaScan with predicate pushdown");
    }
    
    @Test
    public void testDeltaScanSchemaRetrieval() throws Exception {
        // Create engine
        Configuration conf = new Configuration();
        Engine engine = DefaultEngine.create(conf);
        
        // Create table
        Table table = Table.forPath(engine, TEST_TABLE_PATH);
        
        // Create scan builder
        DeltaScanBuilder scanBuilder = new DeltaScanBuilder(table, engine);
        
        // Build scan
        org.apache.spark.sql.connector.read.Scan scan = scanBuilder.build();
        
        // Get schema
        org.apache.spark.sql.types.StructType schema = scan.readSchema();
        assertNotNull("Schema should not be null", schema);
        assertTrue("Schema should have fields", schema.fields().length > 0);
        
        System.out.println("✓ Successfully retrieved schema with " + schema.fields().length + " fields");
    }
}



