package postgresql

import (
	"crypto/sha1"
	"encoding/hex"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const (
	scriptCommandsAttr = "commands"
	scriptTriesAttr    = "tries"
	scriptTimeoutAttr  = "timeout"
	scriptShasumAttr   = "shasum"
)

func resourcePostgreSQLScript() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLScriptCreateOrUpdate),
		Read:   PGResourceFunc(resourcePostgreSQLScriptRead),
		Update: PGResourceFunc(resourcePostgreSQLScriptCreateOrUpdate),
		Delete: PGResourceFunc(resourcePostgreSQLScriptDelete),

		Schema: map[string]*schema.Schema{
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
			scriptTimeoutAttr: {
				Type:        schema.TypeInt,
				Optional:    true,
				Default:     1,
				Description: "Number of seconds between two tries for a command",
			},
			scriptShasumAttr: {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Shasum of commands",
			},
		},
	}
}

func resourcePostgreSQLScriptCreateOrUpdate(db *DBConnection, d *schema.ResourceData) error {
	commands := d.Get(scriptCommandsAttr).([]any)
	tries := d.Get(scriptTriesAttr).(int)
	timeout := d.Get(scriptTimeoutAttr).(int)

	sum := shasumCommands(commands)

	if err := executeCommands(db, commands, tries, timeout); err != nil {
		return err
	}

	d.Set(scriptShasumAttr, sum)
	d.SetId(sum)
	return nil
}

func resourcePostgreSQLScriptRead(db *DBConnection, d *schema.ResourceData) error {
	commands := d.Get(scriptCommandsAttr).([]any)
	newSum := shasumCommands(commands)
	d.Set(scriptShasumAttr, newSum)

	return nil
}

func resourcePostgreSQLScriptDelete(db *DBConnection, d *schema.ResourceData) error {
	return nil
}

func executeCommands(db *DBConnection, commands []any, tries int, timeout int) error {
	for i := 1; ; i++ {
		var err error
		for _, command := range commands {
			log.Printf("[DEBUG] Executing (%d try) %s", i, command.(string))
			_, err = db.Query(command.(string))

			if err != nil {
				log.Println("[DEBUG] Error catched:", err)
				if _, err := db.Query("ROLLBACK"); err != nil {
					log.Println("[DEBUG] Rollback raised an error:", err)
				}
				if i >= tries {
					return err
				}
				time.Sleep(time.Duration(timeout) * time.Second)
				break
			}
		}
		if err == nil {
			return nil
		}
	}
}

func shasumCommands(commands []any) string {
	sha := sha1.New()
	for _, command := range commands {
		sha.Write([]byte(command.(string)))
	}
	return hex.EncodeToString(sha.Sum(nil))
}
