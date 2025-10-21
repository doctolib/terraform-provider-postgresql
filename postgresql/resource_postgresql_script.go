package postgresql

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const (
	scriptCommandsAttr     = "commands"
	scriptDatabaseAttr     = "database"
	scriptTriesAttr        = "tries"
	scriptBackoffDelayAttr = "backoff_delay"
	scriptTimeoutAttr      = "timeout"
	scriptShasumAttr       = "shasum"
)

func resourcePostgreSQLScript() *schema.Resource {
	return &schema.Resource{
		CreateContext: PGResourceContextFunc(resourcePostgreSQLScriptCreateOrUpdate),
		Read:          PGResourceFunc(resourcePostgreSQLScriptRead),
		UpdateContext: PGResourceContextFunc(resourcePostgreSQLScriptCreateOrUpdate),
		Delete:        PGResourceFunc(resourcePostgreSQLScriptDelete),

		Schema: map[string]*schema.Schema{
			scriptDatabaseAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "The database to execute commands in (defaults to provider's configured database)",
			},
			scriptCommandsAttr: {
				Type:        schema.TypeList,
				Required:    true,
				Description: "List of SQL commands to execute",
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			scriptTriesAttr: {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     1,
				Description: "Number of tries for a failing command",
			},
			scriptBackoffDelayAttr: {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     1,
				Description: "Number of seconds between two tries of the batch of commands",
			},
			scriptTimeoutAttr: {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     5 * 60,
				Description: "Number of seconds for a batch of command to timeout",
			},
			scriptShasumAttr: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Shasum of commands",
			},
		},
	}
}

func resourcePostgreSQLScriptCreateOrUpdate(ctx context.Context, db *DBConnection, d *schema.ResourceData) diag.Diagnostics {
	commands, err := toStringArray(d.Get(scriptCommandsAttr).([]any))
	tries := d.Get(scriptTriesAttr).(int)
	backoffDelay := d.Get(scriptBackoffDelayAttr).(int)
	timeout := d.Get(scriptTimeoutAttr).(int)

	if err != nil {
		return diag.Diagnostics{diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Commands input is not valid",
			Detail:   err.Error(),
		}}
	}

	// Get the target database connection
	database := getDatabase(d, db.client.databaseName)

	targetDB := db
	if database != "" && database != db.client.databaseName {
		client := db.client.config.NewClient(database)
		newDB, err := client.Connect()
		if err != nil {
			return diag.Diagnostics{diag.Diagnostic{
				Severity: diag.Error,
				Summary:  "Failed to connect to database",
				Detail:   err.Error(),
			}}
		}
		targetDB = newDB
	}

	if err := executeCommands(ctx, targetDB, commands, tries, backoffDelay, timeout); err != nil {
		return diag.Diagnostics{diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Commands execution failed",
			Detail:   err.Error(),
		}}
	}

	sum := shasumCommands(commands)
	d.SetId(sum)

	if err := resourcePostgreSQLScriptReadImpl(db, d); err != nil {
		return diag.Diagnostics{diag.Diagnostic{
			Severity: diag.Error,
			Summary:  "Failed to read script state",
			Detail:   err.Error(),
		}}
	}

	return nil
}

func resourcePostgreSQLScriptRead(db *DBConnection, d *schema.ResourceData) error {
	return resourcePostgreSQLScriptReadImpl(db, d)
}

func resourcePostgreSQLScriptReadImpl(db *DBConnection, d *schema.ResourceData) error {
	commands, err := toStringArray(d.Get(scriptCommandsAttr).([]any))
	if err != nil {
		return err
	}
	newSum := shasumCommands(commands)

	database := getDatabase(d, db.client.databaseName)

	d.Set(scriptShasumAttr, newSum)
	d.Set(scriptDatabaseAttr, database)

	return nil
}

func resourcePostgreSQLScriptDelete(db *DBConnection, d *schema.ResourceData) error {
	return nil
}

func executeCommands(ctx context.Context, db *DBConnection, commands []string, tries int, backoffDelay int, timeout int) error {
	for try := 1; ; try++ {
		err := executeBatch(ctx, db, commands, timeout)
		if err == nil {
			return nil
		} else {
			if try >= tries {
				return err
			}
			time.Sleep(time.Duration(backoffDelay) * time.Second)
		}
	}
}

func executeBatch(ctx context.Context, db *DBConnection, commands []string, timeout int) error {
	timeoutContext, timeoutCancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer timeoutCancel()
	for _, command := range commands {
		log.Printf("[DEBUG] Executing %s", command)
		_, err := db.ExecContext(timeoutContext, command)
		log.Printf("[DEBUG] Result %s: %v", command, err)
		if err != nil {
			log.Println("[DEBUG] Error catched:", err)
			if _, rollbackError := db.Query("ROLLBACK"); rollbackError != nil {
				log.Println("[DEBUG] Rollback raised an error:", rollbackError)
			}
			return err
		}
	}
	return nil
}

func shasumCommands(commands []string) string {
	sha := sha1.New()
	for _, command := range commands {
		sha.Write([]byte(command))
	}
	return hex.EncodeToString(sha.Sum(nil))
}

func toStringArray(array []any) ([]string, error) {
	strings := make([]string, 0, len(array))
	for _, elem := range array {
		str, ok := elem.(string)
		if !ok {
			return nil, fmt.Errorf("element %v is not a string", elem)
		}
		strings = append(strings, str)
	}
	return strings, nil
}
