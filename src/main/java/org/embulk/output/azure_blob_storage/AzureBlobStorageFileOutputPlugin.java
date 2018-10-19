package org.embulk.output.azure_blob_storage;

import com.google.common.base.Throwables;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;

public class AzureBlobStorageFileOutputPlugin
        implements FileOutputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("account_name")
        String getAccountName();

        @Config("account_key")
        String getAccountKey();

        @Config("container")
        String getContainer();

        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\"%03d.%02d\"")
        String getSequenceFormat();

        @Config("max_connection_retry")
        @ConfigDefault("10") // 10 times retry to connect Azure Blob Storage if failed.
        int getMaxConnectionRetry();
    }

    private static final Logger log = Exec.getLogger(AzureBlobStorageFileOutputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        try {
            CloudBlobClient blobClient = newAzureClient(task.getAccountName(), task.getAccountKey());
            String containerName = task.getContainer();
            CloudBlobContainer container = blobClient.getContainerReference(containerName);
            if (!container.exists()) {
                log.info("container {} doesn't exist and is created.", containerName);
                container.createIfNotExists();
            }
        }
        catch (StorageException | URISyntaxException ex) {
            throw new ConfigException(ex);
        }

        return resume(task.dump(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileOutputPlugin.Control control)
    {
        control.run(taskSource);

        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    private static CloudBlobClient newAzureClient(String accountName, String accountKey)
    {
        String connectionString = "DefaultEndpointsProtocol=https;" +
                "AccountName=" + accountName + ";" +
                "AccountKey=" + accountKey;

        CloudStorageAccount account;
        try {
            account = CloudStorageAccount.parse(connectionString);
        }
        catch (InvalidKeyException | URISyntaxException ex) {
            throw new ConfigException(ex.getMessage());
        }
        return account.createCloudBlobClient();
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        CloudBlobClient client = newAzureClient(task.getAccountName(), task.getAccountKey());
        return new AzureFileOutput(client, task, taskIndex);
    }

    public static class AzureFileOutput implements TransactionalFileOutput
    {
        private final CloudBlobClient client;
        private final String containerName;
        private final String pathPrefix;
        private final String sequenceFormat;
        private final String pathSuffix;
        private final int maxConnectionRetry;
        private BufferedOutputStream output = null;
        private int fileIndex;
        private File file;
        private String filePath;
        private int taskIndex;

        public AzureFileOutput(CloudBlobClient client, PluginTask task, int taskIndex)
        {
            this.client = client;
            this.containerName = task.getContainer();
            this.taskIndex = taskIndex;
            this.pathPrefix = task.getPathPrefix();
            this.sequenceFormat = task.getSequenceFormat();
            this.pathSuffix = task.getFileNameExtension();
            this.maxConnectionRetry = task.getMaxConnectionRetry();
        }

        @Override
        public void nextFile()
        {
            closeFile();

            try {
                String suffix = pathSuffix;
                if (!suffix.startsWith(".")) {
                    suffix = "." + suffix;
                }
                filePath = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + suffix;
                file = Exec.getTempFileSpace().createTempFile(filePath, ".tmp");
                log.info("Writing local file {}", file.getAbsolutePath());
                output = new BufferedOutputStream(new FileOutputStream(file));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private void closeFile()
        {
            if (output != null) {
                try {
                    output.close();
                    fileIndex++;
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
            }
        }

        @Override
        public void add(Buffer buffer)
        {
            try {
                output.write(buffer.array(), buffer.offset(), buffer.limit());
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
            finally {
                buffer.release();
            }
        }

        @Override
        public void finish()
        {
            close();
            uploadFile();
        }

        private Void uploadFile()
        {
            if (filePath != null) {
                try {
                    return retryExecutor()
                            .withRetryLimit(maxConnectionRetry)
                            .withInitialRetryWait(500)
                            .withMaxRetryWait(30 * 1000)
                            .runInterruptible(new Retryable<Void>() {
                                @Override
                                public Void call() throws StorageException, URISyntaxException, IOException, RetryGiveupException
                                {
                                    CloudBlobContainer container = client.getContainerReference(containerName);
                                    CloudBlockBlob blob = container.getBlockBlobReference(filePath);
                                    log.info("Upload start {} to {}", file.getAbsolutePath(), filePath);
                                    try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
                                        blob.upload(in, file.length());
                                        log.info("Upload completed {} to {}", file.getAbsolutePath(), filePath);
                                    }
                                    if (file.exists()) {
                                        if (!file.delete()) {
                                            log.warn("Couldn't delete local file " + file.getAbsolutePath());
                                        }
                                    }
                                    return null;
                                }

                                @Override
                                public boolean isRetryableException(Exception exception)
                                {
                                    return true;
                                }

                                @Override
                                public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                        throws RetryGiveupException
                                {
                                    if (exception instanceof  FileNotFoundException || exception instanceof URISyntaxException || exception instanceof ConfigException) {
                                        throw new RetryGiveupException(exception);
                                    }
                                    String message = String.format("Azure Blob Storage put request failed. Retrying %d/%d after %d seconds. Message: %s",
                                            retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                                    if (retryCount % 3 == 0) {
                                        log.warn(message, exception);
                                    }
                                    else {
                                        log.warn(message);
                                    }
                                }

                                @Override
                                public void onGiveup(Exception firstException, Exception lastException)
                                        throws RetryGiveupException
                                {
                                }
                            });
                }
                catch (RetryGiveupException ex) {
                    throw Throwables.propagate(ex.getCause());
                }
                catch (InterruptedException ex) {
                    throw Throwables.propagate(ex);
                }
            }
            return null;
        }

        @Override
        public void close()
        {
            closeFile();
        }

        @Override
        public void abort() {}

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }
    }
}
