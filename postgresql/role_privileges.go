package postgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/lib/pq"
)

// pgVersionNumGrantOptions is the first server_version_num (PostgreSQL 16) where role
// membership carries its own INHERIT and SET options, and where ownership checks only
// honour a membership that inherits (has_privs_of_role). Before PostgreSQL 16 a membership
// always grants the privileges of the role.
const pgVersionNumGrantOptions = 160000

func serverVersionNum(db QueryAble) (int, error) {
	var v int
	if err := db.QueryRow("SELECT current_setting('server_version_num')::int").Scan(&v); err != nil {
		return 0, fmt.Errorf("could not read server_version_num: %w", err)
	}
	return v, nil
}

// hasRolePrivileges reports whether member holds the privileges of role (directly or through
// an inherited membership). This is what ownership checks use, unlike isMemberOfRole which
// only tells that a pg_auth_members row exists.
func hasRolePrivileges(db QueryAble, role, member string) (bool, error) {
	var ok bool
	if err := db.QueryRow("SELECT pg_has_role($1::name, $2::name, 'USAGE')", member, role).Scan(&ok); err != nil {
		return false, fmt.Errorf("could not check privileges of role %s for %s: %w", role, member, err)
	}
	return ok, nil
}

// ownMembership is a membership row of a role that the connected user granted to itself.
type ownMembership struct {
	admin, inherit, set bool
}

// getOwnMembership returns the membership of member in role granted by member itself
// (PostgreSQL 16+), or nil if there is none.
func getOwnMembership(db QueryAble, role, member string) (*ownMembership, error) {
	m := &ownMembership{}
	err := db.QueryRow(`
SELECT m.admin_option, m.inherit_option, m.set_option
FROM pg_auth_members m
WHERE m.roleid  = (SELECT oid FROM pg_roles WHERE rolname = $1)
  AND m.member  = (SELECT oid FROM pg_roles WHERE rolname = $2)
  AND m.grantor = (SELECT oid FROM pg_roles WHERE rolname = $2)`,
		role, member,
	).Scan(&m.admin, &m.inherit, &m.set)
	switch {
	case err == nil:
		return m, nil
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	default:
		return nil, fmt.Errorf("could not read membership of %s in %s: %w", member, role, err)
	}
}

// withRolePrivileges runs fn while the connected user holds the privileges of every role in
// roles, which is what dropping or altering an object owned by one of these roles requires.
//
// The code path depends on the server version:
//   - before PostgreSQL 16 a membership is enough, so this is withRolesGranted;
//   - from PostgreSQL 16 an existing membership may not inherit (no INHERIT option), so it is
//     not enough: the privileges are checked with pg_has_role and, when missing, the
//     connected user grants itself the role WITH INHERIT TRUE. Only the grant made by the
//     connected user is touched: any other grant of the same role is left alone, and a
//     previous self-grant is restored to its original options afterwards.
//
// Either way the temporary grants are undone as soon as fn has finished.
func withRolePrivileges(txn *sql.Tx, roles []string, fn func() error) error {
	if len(roles) == 0 {
		return fn()
	}

	version, err := serverVersionNum(txn)
	if err != nil {
		return err
	}
	if version < pgVersionNumGrantOptions {
		return withRolesGranted(txn, roles, fn)
	}

	currentUser, err := getCurrentUser(txn)
	if err != nil {
		return err
	}
	superuser, err := isSuperuser(txn, currentUser)
	if err != nil {
		return err
	}
	if superuser {
		return fn()
	}

	type undo struct {
		role string
		prev *ownMembership
	}
	var undos []undo

	for _, role := range roles {
		if role == currentUser {
			continue
		}
		has, err := hasRolePrivileges(txn, role, currentUser)
		if err != nil {
			return err
		}
		if has {
			continue
		}
		roleIsSuper, err := isSuperuser(txn, role)
		if err != nil {
			return err
		}
		if roleIsSuper {
			log.Printf("withRolePrivileges: WARN role %s is a superuser and cannot be granted to %s", role, currentUser)
			continue
		}

		prev, err := getOwnMembership(txn, role, currentUser)
		if err != nil {
			return err
		}

		log.Printf("withRolePrivileges: granting %s to %s WITH INHERIT TRUE", role, currentUser)
		grant := fmt.Sprintf("GRANT %s TO %s WITH INHERIT TRUE", pq.QuoteIdentifier(role), pq.QuoteIdentifier(currentUser))
		if _, err := txn.Exec(grant); err != nil {
			return fmt.Errorf("Error granting role %s to %s: %w", role, currentUser, err)
		}
		undos = append(undos, undo{role: role, prev: prev})
	}

	if err := fn(); err != nil {
		return err
	}

	for _, u := range undos {
		var stmt string
		if u.prev == nil {
			stmt = fmt.Sprintf("REVOKE %s FROM %s", pq.QuoteIdentifier(u.role), pq.QuoteIdentifier(currentUser))
		} else {
			stmt = fmt.Sprintf("GRANT %s TO %s WITH ADMIN %t, INHERIT %t, SET %t",
				pq.QuoteIdentifier(u.role), pq.QuoteIdentifier(currentUser), u.prev.admin, u.prev.inherit, u.prev.set)
		}
		if _, err := txn.Exec(stmt); err != nil {
			return fmt.Errorf("Error restoring role %s for %s: %w", u.role, currentUser, err)
		}
	}
	return nil
}
