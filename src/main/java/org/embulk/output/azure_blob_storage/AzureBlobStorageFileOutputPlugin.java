package org.embulk.output.azure_blob_storage;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockSearchMode;
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
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.embulk.spi.util.RetryExecutor.retryExecutor;

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
        try {
            CloudBlobClient blobClient = newAzureClient(task.getAccountName(), task.getAccountKey());
            CloudBlobContainer container = blobClient.getContainerReference(task.getContainer());

            return new AzureFileOutput(container, task, taskIndex);
        }
        catch (Exception e) {
            throw new DataException(e);
        }
    }

    public static class AzureFileOutput implements TransactionalFileOutput
    {
        private final int blockSize;
        private final CloudBlobContainer container;
        private final String pathPrefix;
        private final String sequenceFormat;
        private final String pathSuffix;
        private final int maxConnectionRetry;
        private BufferedOutputStream output = null;
        private CloudBlockBlob blockBlob;
        private int fileIndex;
        private final int taskIndex;
        private File file;
        private int blockIndex = 0;
        private final List<BlockEntry> blocks = new ArrayList<>();

        public AzureFileOutput(CloudBlobContainer container, PluginTask task, int taskIndex)
        {
            this.container = container;
            this.taskIndex = taskIndex;
            this.pathPrefix = task.getPathPrefix();
            this.sequenceFormat = task.getSequenceFormat();
            this.pathSuffix = task.getFileNameExtension();
            this.maxConnectionRetry = task.getMaxConnectionRetry();
            // ~ 90M. init here for unit test changes it
            this.blockSize = 90 * 1024 * 1024;
        }

        @Override
        public void nextFile()
        {
            // close and commit current file
            closeCurrentFile();
            commitCurrentBlock();

            // prepare for next new file
            newTempFile();
            newBlockBlob();
            fileIndex++;
        }

        private void newBlockBlob()
        {
            try {
                blockBlob = container.getBlockBlobReference(newBlobName());
                blockIndex = 0;
                blocks.clear();
            }
            catch (Exception e) {
                throw new DataException(e);
            }
        }

        private String newBlobName()
        {
            String suffix = pathSuffix;
            if (!suffix.startsWith(".")) {
                suffix = "." + suffix;
            }
            return pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + suffix;
        }

        private void closeCurrentFile()
        {
            if (output != null) {
                try {
                    output.close();
                }
                catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        private void newTempFile()
        {
            try {
                file = Exec.getTempFileSpace().createTempFile();
                output = new BufferedOutputStream(new FileOutputStream(file));
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void add(Buffer buffer)
        {
            try {
                output.write(buffer.array(), buffer.offset(), buffer.limit());

                // upload this block if the size reaches limit (data still in the buffer)
                if (file.length() > blockSize) {
                    closeCurrentFile();
                    uploadFile();
                    newTempFile();
                }
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            finally {
                buffer.release();
            }
        }

        @Override
        public void finish()
        {
            closeCurrentFile();
            uploadFile();
            commitCurrentBlock();
        }

        private void commitCurrentBlock()
        {
            // commit blob
            if (!blocks.isEmpty()) {
                try {
                    blockBlob.commitBlockList(blocks);
                    blocks.clear();
                }
                catch (StorageException e) {
                    throw new DataException(e);
                }
            }
        }

        private Void uploadFile()
        {
            if (file.length() == 0) {
                log.warn("Skipped empty block {}", file.getName());
                return null;
            }

            try {
                return retryExecutor()
                        .withRetryLimit(maxConnectionRetry)
                        .withInitialRetryWait(500)
                        .withMaxRetryWait(30 * 1000)
                        .runInterruptible(new Retryable<Void>() {
                            @Override
                            public Void call() throws IOException, StorageException
                            {
                                String blockId = Base64.getEncoder().encodeToString(String.format("%10d", blockIndex).getBytes());
                                blockBlob.uploadBlock(blockId, new BufferedInputStream(new FileInputStream(file)), file.length());
                                blocks.add(new BlockEntry(blockId, BlockSearchMode.UNCOMMITTED));
                                log.info("Uploaded block file: {}, id: {}, size ~ {}kb", file.getName(), blockId, file.length() / 1024);
                                blockIndex++;
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
                            {
                            }
                        });
            }
            catch (RetryGiveupException ex) {
                throw new RuntimeException(ex.getCause());
            }
            catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            finally {
                if (file.exists()) {
                    if (!file.delete()) {
                        log.warn("Couldn't delete local file " + file.getAbsolutePath());
                    }
                }
            }
        }

        @Override
        public void close()
        {
            closeCurrentFile();
        }

        @Override
        public void abort() {}

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        @VisibleForTesting
        public boolean isTempFileExist()
        {
            return file.exists();
        }
    }
}
