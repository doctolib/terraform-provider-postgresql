---
layout: "postgresql"
page_title: "PostgreSQL: postgresql_script"
sidebar_current: "docs-postgresql-resource-postgresql_script"
description: |-
  Execute a SQL script
---

# postgresql\_script

The ``postgresql_script`` execute a script given as parameter. This script will be executed each time it changes.

If one command of the batch fails, the provider will send a `ROLLBACK` command to the database, and retry, according to the tries / backoff_delay configuration.

## Usage

```hcl
resource "postgresql_script" "foo" {
  commands = [
    "command 1",
    "command 2"
  ]
  tries = 1
  backoff_delay = 1
}
```

## Argument Reference

* `commands` - (Required) An array of commands to execute, one by one.
* `tries` - (Optional) Number of tries of a command before raising an error.
* `backoff_delay` - (Optional) In case of failure, time in second to wait before a retry.

## Examples

Revoke default accesses for public schema:

```hcl
resource "postgresql_script" "foo" {
  commands = [
    "BEBIN",
    "SELECT * FROM table",
    "COMMIT"
  ]
  tries = 3
  backoff_delay = 1
}
```
