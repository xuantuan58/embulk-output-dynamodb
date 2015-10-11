package org.embulk.output.azure_blob_storage;

import java.io.File;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;
import com.google.common.base.Throwables;
import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskReport;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.Buffer;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;
import org.slf4j.Logger;

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
    private static CloudBlobClient blobClient;
    private static CloudBlobContainer container;

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount,
            FileOutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        blobClient = newAzureClient(task.getAccountName(), task.getAccountKey());
        String containerName = task.getContainer();

        try {
            container = blobClient.getContainerReference(containerName);
            if (!container.exists()) {
                log.info(String.format("container [%s] is not exists and created.", containerName));
                container.createIfNotExists();
            }
        } catch (StorageException | URISyntaxException ex) {
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
        } catch (InvalidKeyException | URISyntaxException ex) {
            throw new ConfigException(ex.getMessage());
        }
        return account.createCloudBlobClient();
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        final String pathPrefix = task.getPathPrefix();
        final String sequenceFormat = task.getSequenceFormat();
        final String pathSuffix = task.getFileNameExtension();

        return new TransactionalFileOutput() {
            private int fileIndex = 0;
            private BufferedOutputStream output = null;
            private File file;
            private String filePath;

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
                } catch (FileNotFoundException ex) {
                    throw Throwables.propagate(ex);
                }
            }

            private void closeFile()
            {
                if (output != null) {
                    try {
                        output.close();
                    } catch (IOException ex) {
                        throw Throwables.propagate(ex);
                    }
                }
            }

            public void add(Buffer buffer)
            {
                try {
                    output.write(buffer.array(), buffer.offset(), buffer.limit());
                } catch (IOException ex) {
                    throw Throwables.propagate(ex);
                } finally {
                    buffer.release();
                }
            }

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
                    } catch (StorageException | URISyntaxException | IOException ex) {
                        Throwables.propagate(ex);
                    }
                }
            }

            public void close()
            {
                closeFile();
            }

            public void abort() {}

            public TaskReport commit()
            {
                return Exec.newTaskReport();
            }
        };
    }
}
