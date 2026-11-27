package postgresql

import (
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

// pgVersionNum16 is server_version_num of PostgreSQL 16.0, the first version where a role
// membership can exist without INHERIT.
const pgVersionNum16 = 160000

func testServerVersionNum(t *testing.T) int {
	config := getTestConfig(t)
	db, err := sql.Open("postgres", config.connStr("postgres"))
	if err != nil {
		t.Fatalf("could not create connection pool: %v", err)
	}
	defer db.Close()

	var version int
	if err := db.QueryRow("SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		t.Fatalf("could not read server_version_num: %v", err)
	}
	return version
}

// testAccPublicationDestroyOwnedByOtherRole destroys a publication that is owned by a role
// whose privileges the connected user does not hold, which is what happens to the outbox
// publications created by the terraform database module (owner: <db>-role-admin). Before the
// owner's privileges were granted for the time of the DROP this failed with
// "must be owner of publication".
//
// membershipSQL is run, after the publication has been given to the owner role, to put the
// connected user in the membership state under test.
func testAccPublicationDestroyOwnedByOtherRole(t *testing.T, membershipSQL string) {
	skipIfNotAcc(t)

	dbSuffix, teardown := setupTestDatabase(t, true, true)
	config := getTestConfig(t)
	dbName, _ := getTestDBNames(dbSuffix)
	ownerRole := "pub_owner_" + dbSuffix

	dbExecute(t, config.connStr("postgres"), fmt.Sprintf(`CREATE ROLE "%s" NOLOGIN`, ownerRole))
	// Deferred calls run last-in first-out: the database (and the publication it contains)
	// is dropped before the role that owns it.
	defer dbExecute(t, config.connStr("postgres"), fmt.Sprintf(`DROP ROLE IF EXISTS "%s"`, ownerRole))
	defer teardown()

	publicationConfig := fmt.Sprintf(`
resource "postgresql_publication" "test" {
	name     = "publication"
	database = "%s"
}
`, dbName)

	// Give the publication to the owner role out of band, then leave the connected user in
	// the membership state under test.
	reown := func() {
		dbExecute(t, config.connStr("postgres"), fmt.Sprintf(`GRANT "%s" TO CURRENT_USER`, ownerRole))
		// The new owner of a publication needs CREATE on the database (the owner role of the
		// terraform module owns the database, so it has it).
		dbExecute(t, config.connStr("postgres"), fmt.Sprintf(`GRANT CREATE ON DATABASE "%s" TO "%s"`, dbName, ownerRole))
		dbExecute(t, config.connStr(dbName), fmt.Sprintf(`ALTER PUBLICATION "publication" OWNER TO "%s"`, ownerRole))
		dbExecute(t, config.connStr("postgres"), fmt.Sprintf(membershipSQL, ownerRole))
	}

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePublication)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlPublicationDestroy,
		Steps: []resource.TestStep{
			{
				Config: publicationConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlPublicationExists("postgresql_publication.test"),
				),
			},
			{
				PreConfig: reown,
				Config:    publicationConfig,
				Destroy:   true,
			},
		},
	})
}

// The connected user is not a member of the role owning the publication.
func TestAccPostgresqlPublication_DestroyOwnedByRoleWithoutMembership(t *testing.T) {
	testAccPublicationDestroyOwnedByOtherRole(t, `REVOKE "%s" FROM CURRENT_USER`)
}

// PostgreSQL 16+: the connected user is a member of the owning role, but without INHERIT,
// which does not give the privileges needed to drop the publication. The membership must be
// left as it was found once the publication has been dropped.
func TestAccPostgresqlPublication_DestroyOwnedByRoleMemberWithoutInherit(t *testing.T) {
	skipIfNotAcc(t)
	if testServerVersionNum(t) < pgVersionNum16 {
		t.Skipf("Skip test: role memberships have no INHERIT option before PostgreSQL 16 (server_version_num %s)",
			strconv.Itoa(testServerVersionNum(t)))
	}
	testAccPublicationDestroyOwnedByOtherRole(t, `GRANT "%s" TO CURRENT_USER WITH INHERIT FALSE, SET TRUE`)
}
