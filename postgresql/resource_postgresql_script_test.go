package postgresql

import (
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccPostgresqlScript_basic(t *testing.T) {
	config := `
	resource "postgresql_script" "test" {
		commands = [
			"SELECT 1;"
		]
		tries = 2
		timeout = 4
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
					resource.TestCheckResourceAttr("postgresql_script.test", "timeout", "4"),
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
		timeout = 4
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
					resource.TestCheckResourceAttr("postgresql_script.test", "timeout", "4"),
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
					resource.TestCheckResourceAttr("postgresql_script.test", "timeout", "1"),
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

func TestAccPostgresqlScript_fail(t *testing.T) {
	config := `
	resource "postgresql_script" "invalid" {
		commands = [
			"SLC FROM nowhere;"
		]
		tries = 2
		timeout = 2
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
