
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Bucket.BlobWriteOption;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;

import com.google.api.client.util.Charsets;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class CapgeminiHackathon 
{
	final static String PROJECT_NAME="BigQuerySandBox";
	final static String PROJECT_ID="bigquerysandbox-294807";

	final static String DATASET_NAME="Covid_19_Dataset";
	final static String DATASET_ID="bigquerysandbox-294807:Covid_19_Dataset";

	final static String BUCKET_NAME = "bucket-bigquerysandbox-294807-10";
	//final static String BUCKET_NAME = "bucket-dukes-goolge-project-10";

	final static String TABLE_NAME="Covid19Table";
	final static String LOCATION="us";

	//final static Path KEY_PATH_1=FileSystems.getDefault().getPath("D:\\DATA_ENGINEER\\GCP\\keys", "dukes-goolge-project-b90d3a165957.json");
	final static Path KEY_PATH_2=FileSystems.getDefault().getPath("D:\\DATA_ENGINEER\\GCP\\keys", "bigquerysandbox-294807-6cf40475b486.json");

	final static String BUCKET_FILE_PATH="gs://bucket-bigquerysandbox-294807-10/main.csv";
	
	final static Path DATA_FILE_CSV=FileSystems.getDefault().getPath("D:\\DATA_ENGINEER\\datasets", "TestData.csv");
	final static Path DATA_FILE_PATH=FileSystems.getDefault().getPath("D:\\DATA_ENGINEER\\datasets\\", "main.csv");

	final static Field[] TABLE_COLUMNS =
			new Field[] {
					Field.of("EmployeeId", LegacySQLTypeName.STRING),
					Field.of("LastName", LegacySQLTypeName.STRING),
					Field.of("FirstName", LegacySQLTypeName.STRING),
					Field.of("Title", LegacySQLTypeName.STRING),
					Field.of("ReportsTo", LegacySQLTypeName.STRING),
					Field.of("BirthDate", LegacySQLTypeName.STRING),
					Field.of("HireDate", LegacySQLTypeName.STRING),
					Field.of("Address", LegacySQLTypeName.STRING),
					Field.of("City", LegacySQLTypeName.STRING),
					Field.of("State", LegacySQLTypeName.STRING),
					Field.of("Country", LegacySQLTypeName.STRING),
					Field.of("PostalCode", LegacySQLTypeName.STRING),
					Field.of("Phone", LegacySQLTypeName.STRING),
					Field.of("Fax", LegacySQLTypeName.STRING),
					Field.of("Email", LegacySQLTypeName.STRING)
	};

	public static void main(String[] args) 
	{
		try {
			//Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
			Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID)
					.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(KEY_PATH_2.toString())))
					.build()
					.getService();

			String bucketId=createStorageBucket(storage,PROJECT_ID,BUCKET_NAME);
			System.out.println("Bucket with "+bucketId + " Created !");

			long blobsize=uploadFileToStorageBucket(PROJECT_ID, storage,BUCKET_NAME, "main", DATA_FILE_PATH.toString());
			System.out.println("File of size "+blobsize + " uploaded to bucket !");
			
			//BigQuery default instance
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

			//Dataset Creation
			checkArgument(!isNullOrEmpty(DATASET_NAME), " Provided name is null or empty");
			Map<String,String> datasetInfo= createDataset(bigquery,DATASET_NAME);
			System.out.println("Dataset  " +datasetInfo.toString() + " created successfully");

			//Table Creation
			checkArgument(!isNullOrEmpty(TABLE_NAME), " given table name is null or empty");
			if(TABLE_COLUMNS !=null && TABLE_COLUMNS.length !=0) 
			{
				createTable(bigquery, DATASET_NAME, TABLE_NAME, TABLE_COLUMNS);
				System.out.println(TABLE_NAME + " Table Created !");
			}

			//get csv file path

			//Data written to table
			if (DATA_FILE_CSV !=null && DATA_FILE_CSV.isAbsolute()) 
			{
				long data=writeFileToTable(bigquery, DATASET_NAME, TABLE_NAME, DATA_FILE_CSV, LOCATION);
				System.out.println(data+" rows has been written to table !");
			}



		} 
		catch (Exception  ex) 
		{
			ex.printStackTrace();
		}
	}


	public static Map<String,String> createDataset(BigQuery bigquery,String datasetName) 
	{
		DatasetId datasetId=null;
		String newDatasetName=null;
		Map<String, String> datasetinfo=new HashMap<String, String>();
		try {

			DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();

			Dataset newDataset = bigquery.create(datasetInfo);
			newDatasetName = newDataset.getDatasetId().getDataset();

			datasetId=newDataset.getDatasetId();

			datasetinfo.put(datasetId.toString(),newDatasetName);

		}
		catch (BigQueryException e) 
		{
			System.out.println("Dataset was not created. \n" + e.toString());
		}
		return datasetinfo;
	}


	public static Table createTable(BigQuery bigquery,String datasetName, String tableName, Field[] fields) {
		// [START bigquery_create_table]
		TableId tableId = TableId.of(datasetName, tableName);
		// Table field definition
		//Field field = Field.of(fieldName, LegacySQLTypeName.STRING);
		// Table schema definition
		Schema schema = Schema.of(fields);
		TableDefinition tableDefinition = StandardTableDefinition.of(schema);
		TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
		Table table = bigquery.create(tableInfo);
		// [END bigquery_create_table]
		return table;
	}


	public static long writeFileToTable(BigQuery bigquery,String datasetName, String tableName, Path csvPath, String location)
			throws IOException, InterruptedException, TimeoutException 
	{
		// [START bigquery_load_from_file]
		TableId tableId = TableId.of(datasetName, tableName);
		WriteChannelConfiguration writeChannelConfiguration =
				WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).build();
		// The location must be specified; other fields can be auto-detected.
		JobId jobId = JobId.newBuilder().setLocation(location).build();
		TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
		// Write data to writer
		try (OutputStream stream = Channels.newOutputStream(writer)) {
			Files.copy(csvPath, stream);
		}
		// Get load job
		Job job = writer.getJob();
		job = job.waitFor();
		LoadStatistics stats = job.getStatistics();
		return stats.getOutputRows();
		// [END bigquery_load_from_file]
	}

	//create  storage bucket and transfer file to bucket

	public static String createStorageBucket(Storage storage ,String projectId, String bucketName) 
	{
		Bucket bucket =null;
		try {
			/*		Storage storage = StorageOptions.newBuilder()
					.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(KEY_PATH.toString())))
					.build()
					.getService();*/

			bucket =storage.create(BucketInfo.newBuilder(bucketName)
					.setStorageClass(StorageClass.COLDLINE)
					.setLocation(LOCATION)
					.build());


			System.out.println(
					"Created bucket "
							+ bucket.getName()
							+ " in "
							+ bucket.getLocation()
							+ " with storage class "
							+ bucket.getStorageClass());
		}  catch (Exception e) 
		{
			e.printStackTrace();
		}
		return bucket.getGeneratedId();
	}


	public static long uploadFileToStorageBucket(String projectId,  
			Storage storage,String bucketName, String objectName, String filePath) throws IOException 
	{
		BlobId blobId = BlobId.of(bucketName, objectName);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
		Blob blob=storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));

		composeObject(bucketName, "firstObjectName", "secondObjectName", "targetObjectName", projectId);
		//uploadToStorage(storage, new File(DATA_FILE_PATH.toString()), blobInfo);

		System.out.println( "File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);
		return blob.getSize();
	}
	//write from bucket to table
	//https://storage.googleapis.com/covid19-open-data/v2/${key}/main.json
	public static Long writeStorageBucketFileToBigQueryTable(BigQuery bigquery,String datasetName, String tableName)
			throws InterruptedException {
		TableId tableId = TableId.of(datasetName, tableName);
		// Table field definition
		Field[] fields =
				new Field[] {
						Field.of("name", LegacySQLTypeName.STRING),
						Field.of("post_abbr", LegacySQLTypeName.STRING)
		};
		// Table schema definition
		Schema schema = Schema.of(fields);
		LoadJobConfiguration configuration =
				LoadJobConfiguration.builder(tableId, BUCKET_FILE_PATH)
				.setFormatOptions(FormatOptions.json())
				.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
				.setSchema(schema)
				.build();
		// Load the table
		Job loadJob = bigquery.create(JobInfo.of(configuration));
		loadJob = loadJob.waitFor();
		// Check the table
		System.out.println("State: " + loadJob.getStatus().getState());
		return ((StandardTableDefinition) bigquery.getTable(tableId).getDefinition()).getNumRows();
		// [END bigquery_load_table_gcs_json]
	}

	public static void composeObject(
			String bucketName,
			String firstObjectName,
			String secondObjectName,
			String targetObjectName,
			String projectId) 
	{
		BlobId blobId = BlobId.of(bucketName, "");
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
		Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		//Blob blob=storage.create(blobInfo,new FileInputStream(new File("")),);
		
		Storage.ComposeRequest composeRequest =
				Storage.ComposeRequest.newBuilder()
				// addSource takes varargs, so you can put as many objects here as you want, up to the
				// max of 32
				.addSource(firstObjectName, secondObjectName)
				.setTarget(BlobInfo.newBuilder(bucketName, targetObjectName).build())
				.build();

		Blob compositeObject = storage.compose(composeRequest);

		System.out.println("New composite object "+ compositeObject.getName()+ " was created by combining part request objects");
	}
}
