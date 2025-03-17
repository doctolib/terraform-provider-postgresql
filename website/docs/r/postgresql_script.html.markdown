---
layout: "postgresql"
page_title: "PostgreSQL: postgresql_script"
sidebar_current: "docs-postgresql-resource-postgresql_script"
description: |-
  Execute a SQL scipt
---

# postgresql\_script

The ``postgresql_script`` execute a script given as parameter.

## Usage

```hcl
resource "postgresql_script" "foo" {
  commands = [
    "command 1",
    "command 2"
  ]
  tries = 1
  timeout = 1
}
```

## Argument Reference

* `commands` - (Required) An array of commands to execute, one by one.
* `tries` - (Optional) Number of tries of a command before raising an error.
* `timeout` - (Optional) Time in second between two failing commands.

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
  timeout = 1
}
```
