# GoogleCloudStorage_to_BigQuery #

## Intro ##
Cloud Dataflow is a managed service for executing a wide variety of data processing patterns. 

This documentation shows you how to deploy your batch pipelines using Cloud Dataflow in Java. The goal here is to create a pipeline to load data from a CSV file to BigQuery table.

## Prerequisites ##
* [Eclipse 4.6 +](https://www.eclipse.org/photon/)
* [JDK 1.8+](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* GCP account
  * GCS and BigQuery enabled
  * Service account
  
## Detailed steps ##

### Install Cloud SDK ###

Follow this page to install Cloud SDK.

https://cloud.google.com/sdk/docs/quickstart-windows

### Install Cloud Tools for Eclipse ###

Install Cloud Tools for Eclipse by following this page:

https://cloud.google.com/eclipse/docs/quickstart

### Create a DataFlow project ###

Create a new project through New Project wizard.

Select Google Cloud Dataflow Java Project wizard. Click Next to continue.

![NewDataflowProject](https://raw.githubusercontent.com/sanveerosahan/image-resources/master/GoogleCloudStorage_to_BigQuery/NewDataflowProject.png)

Input the details for this project:

![ProjectDetails](https://raw.githubusercontent.com/sanveerosahan/image-resources/master/GoogleCloudStorage_to_BigQuery/ProjectDetails.png)


Click Next.
Setup account details:

![AccountDetails](https://raw.githubusercontent.com/sanveerosahan/image-resources/master/GoogleCloudStorage_to_BigQuery/AccountDetails.png)

Click Finish to complete the wizard.

### Build the project ###
Run Maven Install to install the dependencies. You can do this through Run Configurations or Maven command line interfaces.


### Create a pipeline to load CSV file in GCS to BigQuery ###

Upload the CSV file into a bucket.

Refer the code from class [CsvToBQPipeline](https://github.com/sanveerosahan/GoogleCloudStorage_to_BigQuery/blob/master/src/main/java/com/gcp/CsvToBQPipeline.java) and create a new one.

Create a run configuration for Dataflow pipeline:

![RunConfiguration](https://raw.githubusercontent.com/sanveerosahan/image-resources/master/GoogleCloudStorage_to_BigQuery/RunConfiguration.png)

For Pipeline Arguments tab, choose DirectRunner to run job on local machine.

![PipelineArgumentsTab](https://raw.githubusercontent.com/sanveerosahan/image-resources/master/GoogleCloudStorage_to_BigQuery/PipelineArgumentsTab.png)

Check Use default Dataflow options and then run the code.

### View the job in Console ###
You can also run through DataflowRunner (set through Pipeline Arguments tab). The job will then be submitted to Dataflow in GCP.

![ConsoleJob](https://raw.githubusercontent.com/sanveerosahan/image-resources/master/GoogleCloudStorage_to_BigQuery/ConsoleJob.png)

### Verify the result in BigQuery ###
Once data is loaded, you can run the following query to query it:

``SELECT * FROM `project-id.dataset.table-name` LIMIT 1000``

## Conclusion ##

This demonstrates using a fully managed and reliable service to transform data in batch modes. We can create a pipeline to process historical data which can be generated from various applications.

Once the data is ingested in BigQuery, it can be used for further analysis, visualization, training ML models, etc.




