# 1. Overview

awsP9 is an api of [AWS SDK (Boto3)](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html). We can save our time from learning AWS SDK.

Please also read [helper_AWS-CLI.md](https://github.com/lankahsu520/HelperX/blob/master/helper_AWS-CLI.md) and [helper_AWS-SDK.md](https://github.com/lankahsu520/HelperX/blob/master/helper_AWS-SDK.md).

# 2. Depend on

- [pythonX9](https://github.com/lankahsu520/pythonX9)
-  [AWS SDK (Boto3)](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) (Boto3 1.26.123)

# 3. Current Status

#### A. DynamoDB

```mermaid
flowchart TD
	subgraph Amazon
		DynamoDB
	end

	subgraph awsP9
		subgraph awsxDB
			subgraph Show
				



			end
			subgraph Table
				dydb_create_table
				dydb_delete_table
				dydb_list_table
				dydb_describe_table
			end

			subgraph Item
				dydb_del_item
				dydb_put_item
				dydb_update_item
				dydb_remove_attributes

				dydb_get_item
				dydb_query_item
				dydb_scan_item
			end
			
			subgraph attrX
				dydb_attrX_addS
				dydb_attrX_addN
				dydb_attrX_addBoolean
				dydb_attrX_addListS
				dydb_attrX_free
			end
		end
	end
	DynamoDB <--> awsxDB

	classDef yellow fill:#FFFFCC
	classDef pink fill:#FFCCCC
	classDef blue fill:#0000FF
	classDef lightblue fill:#ADD8E6

	class DynamoDB pink
	class awsxDB pink
```

#### B. S3

```mermaid
flowchart TD
	subgraph Amazon
		S3
	end

	subgraph awsP9
		subgraph awsxS3
			s3_copy_object
			s3_delete_object
			s3_get_object
			s3_put_object
			s3_pull_object
		end
	end

	S3<-->awsxS3

	classDef yellow fill:#FFFFCC
	classDef pink fill:#FFCCCC
	classDef blue fill:#0000FF
	classDef lightblue fill:#ADD8E6

	class S3 lightblue
	class awsxS3 lightblue
```


# 4. Build
```bash
Do nothing
```
# 5. Example or Usage

#### - awsp9-db_123.py - an example of how to access DynamoDB.

> TableName: MusicBak，進行 table 檢查或建立，之後再對 table 進行新增刪除項目。 

```bash
$ make awsp9-db_123.py
or
$ ./awsp9-db_123.py -d 3
```

#### - awsp9-s3_123.py - an example of how to access S3.

> 一個很簡單的範例程式，檢查 bucket - lambdax9 是否存在，提取該 lambdax9 訊息

```bash
$ make awsp9-s3_123.py
or
$ ./awsp9-s3_123.py -d 3
```

# 6. License

> awsP9 is under the New BSD License (BSD-3-Clause).


# 7. Documentation
> Run an example and read it.
