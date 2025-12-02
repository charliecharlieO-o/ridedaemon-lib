//go:build debug

package logging

import "log"

func init() {
	Printf = log.Printf
}
