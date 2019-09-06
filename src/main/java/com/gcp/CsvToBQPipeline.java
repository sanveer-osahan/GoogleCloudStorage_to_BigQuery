/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gcp;

import java.io.IOException;

import java.util.ArrayList;

import java.util.Arrays;

import java.util.List;


import org.apache.beam.runners.dataflow.DataflowRunner;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.io.TextIO;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import org.apache.beam.sdk.options.PipelineOptions;

import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.apache.beam.sdk.transforms.Create;

import org.apache.beam.sdk.transforms.DoFn;

import org.apache.beam.sdk.transforms.MapElements;

import org.apache.beam.sdk.transforms.ParDo;

import org.apache.beam.sdk.transforms.SimpleFunction;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;


import com.google.api.services.bigquery.model.TableReference;

import com.google.api.services.bigquery.model.TableRow;

import com.google.api.services.bigquery.model.TableSchema;

import com.google.api.services.bigquery.model.TableFieldSchema;


public class CsvToBQPipeline {
     private static final Logger LOG = LoggerFactory.getLogger(CsvToBQPipeline.class);
     private static String HEADERS = "Row_ID,Order_ID,Order_Date,Ship_Date,Ship_Mode,"
     		+ "Customer_ID,Customer_Name,Segment,Country,City,State,Postal_Code,Region,"
     		+ "Product_ID,Category,Sub_Category,Product_Name,Sales,Quantity,Discount,Profit";


    public static class FormatForBigquery extends DoFn<String, TableRow> {


        private static String[] columnNames = HEADERS.split(",");


        @ProcessElement
         public void processElement(ProcessContext c) {
             TableRow row = new TableRow();
             String[] parts = c.element().split(",");
             //LOG.info("Processing row (static class): " + c.element());


            if (!c.element().contains(columnNames[0])) {
                 for (int i = 0; i < parts.length; i++) {
                     // No typy conversion at the moment.
                		 row.set(columnNames[i], parts[i].replaceAll("\\|", ","));
                 }
                 c.output(row);
             }
         }


        /** Defines the BigQuery schema used for the output. */


        static TableSchema getSchema() {
             List<TableFieldSchema> fields = new ArrayList<>();
             // Currently store all values as String
             /*fields.add(new TableFieldSchema().setName("ID").setType("STRING"));
             fields.add(new TableFieldSchema().setName("Code").setType("STRING"));
             fields.add(new TableFieldSchema().setName("Value").setType("STRING"));
             fields.add(new TableFieldSchema().setName("Date").setType("STRING"));*/
             for(String fieldName: columnNames)
             {
            	 fields.add(new TableFieldSchema().setName(fieldName).setType("STRING"));
             }


            return new TableSchema().setFields(fields);
         }
     }


    public static void main(String[] args) throws Throwable {
         // Currently hard-code the variables, this can be passed into as parameters
         String sourceFilePath = "gs://bucket_name/sample.csv";
         String tempLocationPath = "gs://bucket_name/tmp";
         boolean isStreaming = false;
         TableReference tableRef = new TableReference();
         // Replace this with your own GCP project id
         tableRef.setProjectId("project_id");
         tableRef.setDatasetId("bigquery_dataset");
         tableRef.setTableId("table_name");


       /* PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
         // This is required for BigQuery
         options.setTempLocation(tempLocationPath);
         options.setJobName("csvtobq");
         */
         
      // Create and set your PipelineOptions.
         DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

         // For Cloud execution, set the Cloud Platform project, staging location,
         // and specify DataflowRunner.
         options.setProject("project_id");
         options.setStagingLocation("gs://bucket_name/dataflow_staging");
         options.setJobName("csv-to-bigquery");
         options.setRunner(DataflowRunner.class);
         Pipeline p = Pipeline.create(options); 
        
         p.apply("Read CSV File", TextIO.read().from(sourceFilePath))
                 .apply("Log messages", ParDo.of(new DoFn<String, String>() {
                     @ProcessElement
                     public void processElement(ProcessContext c) {
                         LOG.info("Processing row: " + c.element());
                         c.output(c.element());
                     }
                 })).apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))
                 .apply("Write into BigQuery",
                         BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatForBigquery.getSchema())
                                 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                 .withWriteDisposition(isStreaming ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
                                         : BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));


        p.run().waitUntilFinish();


    }

}

