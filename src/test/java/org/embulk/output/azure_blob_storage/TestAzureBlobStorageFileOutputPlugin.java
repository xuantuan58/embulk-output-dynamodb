package org.embulk.output.azure_blob_storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin.PluginTask;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.standards.CsvParserPlugin;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

public class TestAzureBlobStorageFileOutputPlugin
{
    private static String AZURE_ACCOUNT_NAME;
    private static String AZURE_ACCOUNT_KEY;
    private static String AZURE_CONTAINER;
    private static String AZURE_CONTAINER_DIRECTORY;
    private static String AZURE_PATH_PREFIX;
    private static String LOCAL_PATH_PREFIX;
    private FileOutputRunner runner;

    /*
     * This test case requires environment variables
     *   AZURE_ACCOUNT_NAME
     *   AZURE_ACCOUNT_KEY
     *   AZURE_CONTAINER
     *   AZURE_CONTAINER_DIRECTORY
     */
    @BeforeClass
    public static void initializeConstant()
    {
        AZURE_ACCOUNT_NAME = System.getenv("AZURE_ACCOUNT_NAME");
        AZURE_ACCOUNT_KEY = System.getenv("AZURE_ACCOUNT_KEY");
        AZURE_CONTAINER = System.getenv("AZURE_CONTAINER");
        // skip test cases, if environment variables are not set.
        assumeNotNull(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER);

        AZURE_CONTAINER_DIRECTORY = System.getenv("AZURE_CONTAINER_DIRECTORY") != null ? getDirectory(System.getenv("AZURE_CONTAINER_DIRECTORY")) : getDirectory("");
        AZURE_PATH_PREFIX = AZURE_CONTAINER_DIRECTORY + "sample_";
        LOCAL_PATH_PREFIX = Resources.getResource("sample_01.csv").getPath();
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private AzureBlobStorageFileOutputPlugin plugin;

    @Before
    public void createResources() throws GeneralSecurityException, NoSuchMethodException, IOException
    {
        plugin = new AzureBlobStorageFileOutputPlugin();
        runner = new FileOutputRunner(runtime.getInstance(AzureBlobStorageFileOutputPlugin.class));
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "azure_blob_storage")
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(AZURE_ACCOUNT_NAME, task.getAccountName());
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "azure_blob_storage")
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test
    public void testTransactionCreateNonexistsContainer() throws Exception
    {
        String container = "non-exists-container";
        deleteContainerIfExists(container);

        assertEquals(false, isExistsContainer(container));

        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "azure_blob_storage")
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", container)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());

        assertEquals(true, isExistsContainer(container));
        deleteContainerIfExists(container);
    }

    @Test
    public void testResume()
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        ConfigDiff configDiff = plugin.resume(task.dump(), 0, new FileOutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        //assertEquals("in/aa/a", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testCleanup()
    {
        PluginTask task = config().loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test(expected = RuntimeException.class)
    public void testCreateAzureClientThrowsConfigException()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "azure_blob_storage")
                .set("account_name", "invalid-account-name")
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test
    public void testAzureFileOutputByOpen() throws Exception
    {
        ConfigSource configSource = config();
        PluginTask task = configSource.loadConfig(PluginTask.class);
        Schema schema = configSource.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSource, schema, 0, new Control());

        TransactionalFileOutput output = plugin.open(task.dump(), 0);

        output.nextFile();

        FileInputStream is = new FileInputStream(LOCAL_PATH_PREFIX);
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        output.add(buffer);

        output.finish();
        output.commit();

        String remotePath = AZURE_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();
        assertRecords(remotePath);
    }

    public ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "azure_blob_storage")
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());
    }

    private class Control
            implements OutputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource)
        {
            return Lists.newArrayList(Exec.newTaskReport());
        }
    }

    private ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", LOCAL_PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        builder.add(ImmutableMap.of("name", "json_column", "type", "json"));
        return builder.build();
    }

    private ImmutableMap<String, Object> formatterConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("header_line", "false");
        builder.put("timezone", "Asia/Tokyo");
        return builder.build();
    }

    private void assertRecords(String azurePath) throws Exception
    {
        ImmutableList<List<String>> records = getFileContentsFromAzure(azurePath);
        assertEquals(6, records.size());
        {
            List<String> record = records.get(1);
            assertEquals("1", record.get(0));
            assertEquals("32864", record.get(1));
            assertEquals("2015-01-27 19:23:49", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk", record.get(4));
            assertEquals("{\"k\":true}", record.get(5));
        }

        {
            List<String> record = records.get(2);
            assertEquals("2", record.get(0));
            assertEquals("14824", record.get(1));
            assertEquals("2015-01-27 19:01:23", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk jruby", record.get(4));
            assertEquals("{\"k\":1}", record.get(5));
        }

        {
            List<String> record = records.get(3);
            assertEquals("{\"k\":1.23}", record.get(5));
        }

        {
            List<String> record = records.get(4);
            assertEquals("{\"k\":\"v\"}", record.get(5));
        }

        {
            List<String> record = records.get(5);
            assertEquals("{\"k\":\"2015-02-03 08:13:45\"}", record.get(5));
        }
    }

    private ImmutableList<List<String>> getFileContentsFromAzure(String path) throws Exception
    {
        Method method = AzureBlobStorageFileOutputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        method.setAccessible(true);
        CloudBlobClient client = (CloudBlobClient) method.invoke(plugin, AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY);
        CloudBlobContainer container = client.getContainerReference(AZURE_CONTAINER);
        CloudBlob blob = container.getBlockBlobReference(path);

        ImmutableList.Builder<List<String>> builder = new ImmutableList.Builder<>();

        InputStream is =  blob.openInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = reader.readLine()) != null) {
            List<String> records = Arrays.asList(line.split(",", 0));

            builder.add(records);
        }
        return builder.build();
    }

    private boolean isExistsContainer(String containerName) throws Exception
    {
        Method method = AzureBlobStorageFileOutputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        method.setAccessible(true);
        CloudBlobClient client = (CloudBlobClient) method.invoke(plugin, AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY);
        CloudBlobContainer container = client.getContainerReference(containerName);
        return container.exists();
    }

    private void deleteContainerIfExists(String containerName) throws Exception
    {
        Method method = AzureBlobStorageFileOutputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        method.setAccessible(true);
        CloudBlobClient client = (CloudBlobClient) method.invoke(plugin, AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY);
        CloudBlobContainer container = client.getContainerReference(containerName);
        if (container.exists()) {
            container.delete();
            // container could not create same name after deletion.
            Thread.sleep(30000);
        }
    }

    private static String getDirectory(String dir)
    {
        if (!dir.isEmpty() && !dir.endsWith("/")) {
            dir = dir + "/";
        }
        if (dir.startsWith("/")) {
            dir = dir.replaceFirst("/", "");
        }
        return dir;
    }

    private byte[] convertInputStreamToByte(InputStream is) throws IOException
    {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        byte [] buffer = new byte[1024];
        while (true) {
            int len = is.read(buffer);
            if (len < 0) {
                break;
            }
            bo.write(buffer, 0, len);
        }
        return bo.toByteArray();
    }
}
