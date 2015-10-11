# Azure blob Storage file output plugin for Embulk

[Embulk](http://www.embulk.org/) file output plugin stores files on [Microsoft Azure](https://azure.microsoft.com/) [blob Storage](https://azure.microsoft.com/en-us/documentation/articles/storage-introduction/#blob-storage)


## Overview

* **Plugin type**: file output
* **Resume supported**: no
* **Cleanup supported**: no

## Configuration

First, create Azure [Storage Account](https://azure.microsoft.com/en-us/documentation/articles/storage-create-storage-account/).

- **account_name**: storage account name (string, required)
- **account_key**: primary access key (string, required)
- **container**: container name (string, required)
- **path_prefix**: prefix of target keys (string, required) (string, required)
- **file_ext**: e.g. "csv.gz, json.gz" (string, required)


### Auto create container

container will create automatically when container isn't exists.
 
When a container is deleted, a container with the same name cannot be created for at least 30 seconds.
It's a [service specification](https://technet.microsoft.com/en-us/library/dd179408.aspx#Anchor_3) of Azure blob Storage.

## Example

```yaml
out:
  type: azure_blob_storage
  account_name: myaccount
  account_key: myaccount_key
  container: my-container
  path_prefix: logs/csv-
  file_ext: csv.gz
  formatter:
    type: csv
    header_line: false
  encoders:
  - {type: gzip}
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
