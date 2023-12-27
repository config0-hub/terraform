**Description**

  - This stack parses the Terraform tfstate file stored in the Config0 resources database and inserts the parsed data as individual entries in the Config0 resources table.
  - For example, if the tfstate file created a VPC and multiple subnets within it, this stack will extract the subnet IDs from the tfstate file and insert them as separate entries in the Config0 resources database.

**Required**

| argument      | description                            | var type | default      |
| ------------- | -------------------------------------- | -------- | ------------ |
| tbd   | tbd                 | string   | None         |

**Optional**

| argument           | description                            | var type |  default      |
| ------------- | -------------------------------------- | -------- | ------------ |
| tbd   | tbd                 | string   | None         |
