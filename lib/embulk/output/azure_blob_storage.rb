Embulk::JavaPlugin.register_output(
  "azure_blob_storage", "org.embulk.output.azure_blob_storage.AzureBlobStorageFileOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
