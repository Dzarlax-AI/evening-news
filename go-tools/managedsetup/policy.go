package managedsetup

import _ "embed"

// GrantPolicy is bundled into the maintenance binary so provisioning cannot
// accidentally execute a mutable policy from the current working directory.
//
//go:embed grants.sql
var GrantPolicy string
