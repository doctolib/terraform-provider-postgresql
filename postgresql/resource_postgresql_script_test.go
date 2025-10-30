package postgresql

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccPostgresqlScript_basic(t *testing.T) {
	config := `
	resource "postgresql_script" "test" {
		commands = [
			"SELECT 1;"
		]
		tries = 2
		backoff_delay = 4
	}
	`

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.0", "SELECT 1;"),
					resource.TestCheckResourceAttr("postgresql_script.test", "tries", "2"),
					resource.TestCheckResourceAttr("postgresql_script.test", "backoff_delay", "4"),
				),
			},
		},
	})
}

func TestAccPostgresqlScript_multiple(t *testing.T) {
	config := `
	resource "postgresql_script" "test" {
		commands = [
			"SELECT 1;",
			"SELECT 2;",
			"SELECT 3;"
		]
		tries = 2
		backoff_delay = 4
	}
	`

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.0", "SELECT 1;"),
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.1", "SELECT 2;"),
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.2", "SELECT 3;"),
					resource.TestCheckResourceAttr("postgresql_script.test", "tries", "2"),
					resource.TestCheckResourceAttr("postgresql_script.test", "backoff_delay", "4"),
				),
			},
		},
	})
}

func TestAccPostgresqlScript_default(t *testing.T) {
	config := `
	resource "postgresql_script" "test" {
		commands = [
			"SELECT 1;"
		]
	}
	`

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.0", "SELECT 1;"),
					resource.TestCheckResourceAttr("postgresql_script.test", "tries", "1"),
					resource.TestCheckResourceAttr("postgresql_script.test", "backoff_delay", "1"),
				),
			},
		},
	})
}

func TestAccPostgresqlScript_reapply(t *testing.T) {
	config := `
	resource "postgresql_script" "test" {
		commands = [
			"SELECT 1;"
		]
	}
	`

	configChange := `
	resource "postgresql_script" "test" {
		commands = [
			"SELECT 2;"
		]
	}
	`

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.0", "SELECT 1;"),
				),
			},
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.0", "SELECT 1;"),
				),
			},
			{
				Config: configChange,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.0", "SELECT 2;"),
				),
			},
		},
	})
}

func TestAccPostgresqlScript_invalid(t *testing.T) {
	config := `
	resource "postgresql_script" "invalid" {
		commands = [
			""
		]
		tries = 2
		backoff_delay = 2
	}
	`

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config:      config,
				ExpectError: regexp.MustCompile("element <nil> is not a string"),
			},
		},
	})
}

func TestAccPostgresqlScript_fail(t *testing.T) {
	config := `
	resource "postgresql_script" "invalid" {
		commands = [
			"SLC FROM nowhere;"
		]
		tries = 2
		backoff_delay = 2
	}
	`

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config:      config,
				ExpectError: regexp.MustCompile("syntax error"),
			},
		},
	})
}

func TestAccPostgresqlScript_failMultiple(t *testing.T) {
	config := `
	resource "postgresql_script" "invalid" {
		commands = [
			"BEGIN",
			"SLC FROM nowhere;",
			"COMMIT"
		]
		tries = 2
		backoff_delay = 2
	}
	`

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config:      config,
				ExpectError: regexp.MustCompile("syntax error"),
			},
		},
	})
}

func TestAccPostgresqlScript_timeout(t *testing.T) {
	config := `
	resource "postgresql_script" "invalid" {
		commands = [
			"BEGIN",
			"SELECT pg_sleep(2);",
			"COMMIT"
		]
		timeout = 1
	}
	`

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config:      config,
				ExpectError: regexp.MustCompile("canceling statement"),
			},
		},
	})
}

func TestAccPostgresqlScript_withDatabase(t *testing.T) {
	config := `
	resource "postgresql_database" "test_db" {
		name = "test_script_db"
	}

	resource "postgresql_script" "test" {
		database = postgresql_database.test_db.name
		commands = [
			"CREATE TABLE test_table (id INT);",
			"INSERT INTO test_table VALUES (1);"
		]
		depends_on = [postgresql_database.test_db]
	}

    resource "postgresql_script" "test_default" {
        commands = [
            "CREATE TABLE default_db_table (id INT);",
            "INSERT INTO default_db_table VALUES (1);",
            "INSERT INTO default_db_table VALUES (2);"
        ]
        depends_on = [postgresql_database.test_db]
    }
	`

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckScriptTablesDestroyed,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_script.test", "database", "test_script_db"),
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.0", "CREATE TABLE test_table (id INT);"),
					resource.TestCheckResourceAttr("postgresql_script.test", "commands.1", "INSERT INTO test_table VALUES (1);"),
					resource.TestCheckResourceAttr("postgresql_script.test_default", "database", "postgres"),
					resource.TestCheckResourceAttr("postgresql_script.test_default", "commands.0", "CREATE TABLE default_db_table (id INT);"),
					resource.TestCheckResourceAttr("postgresql_script.test_default", "commands.1", "INSERT INTO default_db_table VALUES (1);"),
					resource.TestCheckResourceAttr("postgresql_script.test_default", "commands.2", "INSERT INTO default_db_table VALUES (2);"),
					testAccCheckTableExistsInDatabase("test_script_db", "test_table"),
					testAccCheckTableHasRecords("test_script_db", "test_table", 1),
					testAccCheckTableExistsInDatabase("postgres", "default_db_table"),
					testAccCheckTableHasRecords("postgres", "default_db_table", 2),
				),
			},
		},
	})
}

func testAccCheckScriptTablesDestroyed(s *terraform.State) error {
	return testAccDropTables(map[string][]string{
		"test_script_db": {"test_table"},
		"postgres":       {"default_db_table"},
	})
}

func testAccDropTables(tablesToDrop map[string][]string) error {
	client := testAccProvider.Meta().(*Client)

	for dbName, tables := range tablesToDrop {
		dbClient := client.config.NewClient(dbName)
		db, err := dbClient.Connect()
		if err != nil {
			continue // Skip if we can't connect to the database
		}

		for _, tableName := range tables {
			_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		}
	}

	return nil
}

func testAccCheckTableExistsInDatabase(dbName, tableName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)
		dbClient := client.config.NewClient(dbName)
		db, err := dbClient.Connect()
		if err != nil {
			return fmt.Errorf("Error connecting to database %s: %s", dbName, err)
		}

		var exists bool
		query := "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)"
		err = db.QueryRow(query, tableName).Scan(&exists)
		if err != nil {
			return fmt.Errorf("Error checking if table %s exists: %s", tableName, err)
		}

		if !exists {
			return fmt.Errorf("Table %s does not exist in database %s", tableName, dbName)
		}

		return nil
	}
}

func testAccCheckTableHasRecords(dbName, tableName string, expectedCount int) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)
		dbClient := client.config.NewClient(dbName)
		db, err := dbClient.Connect()
		if err != nil {
			return fmt.Errorf("Error connecting to database %s: %s", dbName, err)
		}

		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
		err = db.QueryRow(query).Scan(&count)
		if err != nil {
			return fmt.Errorf("Error counting records in table %s: %s", tableName, err)
		}

		if count != expectedCount {
			return fmt.Errorf("Expected %d records but got %d in table %s", expectedCount, count, tableName)
		}

		return nil
	}
}

func TestAccPostgresqlScript_setLocalRoleWorks(t *testing.T) {
	// This test demonstrates that SET LOCAL ROLE works across separate commands
	// because they are concatenated into a single SQL statement to be executed on a single connection
	config := `
	resource "postgresql_role" "test_role" {
		name = "test_owner_role"
	}

	resource "postgresql_script" "grant_role" {
		commands = [
			"GRANT test_owner_role TO CURRENT_USER",
			"GRANT CREATE ON SCHEMA public TO test_owner_role"
		]
		depends_on = [postgresql_role.test_role]
	}

    resource "postgresql_script" "test_with_set_local_separate" {
		commands = [
			"BEGIN",
			"SET LOCAL ROLE test_owner_role",
			"CREATE TABLE test_set_locals (id INT)",
			"COMMIT"
		]
		depends_on = [postgresql_script.grant_role]
	}
	`

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckSetLocalRoleTablesDestroyed,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckTableExistsInDatabase("postgres", "test_set_locals"),
					// Both commands should now work with SET LOCAL ROLE since commands are concatenated
					testAccCheckTableOwner("postgres", "test_set_locals", "test_owner_role"),
				),
			},
		},
	})
}

func testAccCheckTableOwner(dbName, tableName, expectedOwner string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)
		dbClient := client.config.NewClient(dbName)
		db, err := dbClient.Connect()
		if err != nil {
			return fmt.Errorf("Error connecting to database %s: %s", dbName, err)
		}

		var owner string
		query := `SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = $1`
		err = db.QueryRow(query, tableName).Scan(&owner)
		if err != nil {
			return fmt.Errorf("Error checking owner of table %s: %s", tableName, err)
		}

		if owner != expectedOwner {
			return fmt.Errorf("Expected table %s to be owned by %s but got %s", tableName, expectedOwner, owner)
		}

		return nil
	}
}

func testAccCheckSetLocalRoleTablesDestroyed(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)
	db, err := client.Connect()
	if err != nil {
		return nil // Skip if we can't connect
	}

	_, _ = db.Exec("DROP TABLE IF EXISTS test_set_local_separate")
	_, _ = db.Exec("DROP TABLE IF EXISTS test_set_local_single")
	_, _ = db.Exec("DROP ROLE IF EXISTS test_owner_role")

	return nil
}
