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

import org.slf4j.Logger;

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
                log.info(String.format("container [%s] is not exists and created.", containerName));
                container.createIfNotExists();
            }
        }
        catch (StorageException | URISyntaxException | ConfigException ex) {
            Throwables.propagate(ex);
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
        return new AzureFileOutput(task, taskIndex);
    }

    public static class AzureFileOutput implements TransactionalFileOutput
    {
        private final String pathPrefix;
        private final String sequenceFormat;
        private final String pathSuffix;
        private final CloudBlobClient client;
        private CloudBlobContainer container = null;
        private BufferedOutputStream output = null;
        private int fileIndex;
        private File file;
        private String filePath;
        private int taskIndex;

        public AzureFileOutput(PluginTask task, int taskIndex)
        {
            this.taskIndex = taskIndex;
            this.pathPrefix = task.getPathPrefix();
            this.sequenceFormat = task.getSequenceFormat();
            this.pathSuffix = task.getFileNameExtension();
            this.client = newAzureClient(task.getAccountName(), task.getAccountKey());
            try {
                this.container = client.getContainerReference(task.getContainer());
            }
            catch (URISyntaxException | StorageException ex) {
                Throwables.propagate(ex);
            }
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
                file = new File(filePath);

                String parentPath = file.getParent();
                File dir = new File(parentPath);
                if (!dir.exists()) {
                    dir.mkdir();
                }
                log.info(String.format("Writing local file [%s]", filePath));
                output = new BufferedOutputStream(new FileOutputStream(filePath));
            }
            catch (FileNotFoundException ex) {
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
            closeFile();
            if (filePath != null) {
                try {
                    CloudBlockBlob blob = container.getBlockBlobReference(filePath);
                    log.info(String.format("Upload start [%s]", filePath));
                    blob.upload(new FileInputStream(file), file.length());
                    log.info(String.format("Upload completed [%s]", filePath));
                    file.delete();
                    log.info(String.format("Delete completed local file [%s]", filePath));
                }
                catch (StorageException | URISyntaxException | IOException ex) {
                    Throwables.propagate(ex);
                }
            }
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
