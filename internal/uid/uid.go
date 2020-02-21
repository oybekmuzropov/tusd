package uid

import (
	"github.com/google/uuid"
)

// uid returns a unique id. These ids consist of 128 bits from a
// cryptographically strong pseudo-random generator and are like uuids, but
// without the dashes and significant bits.
//
// See: http://en.wikipedia.org/wiki/UUID#Random_UUID_probability_of_duplicates
func Uid() string {
	for ;; {
		id, err := uuid.NewRandom()

		if err == nil {
			return id.String()
		}
	}
}
