package ledger

import (
	"fmt"
	"regexp"
)

// validIdentifier matches safe SQL/MongoDB identifiers: alphanumeric and underscore,
// not starting with a digit.
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidateName checks that name is a safe identifier for use as a table or collection name.
func ValidateName(name string) error {
	if !validIdentifier.MatchString(name) {
		return fmt.Errorf("%w: %q", ErrInvalidName, name)
	}
	return nil
}
