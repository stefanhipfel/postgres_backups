package util

import (
	"math/rand"
	"time"
)

func Retry(attempts int, sleep time.Duration, f func() error) error {
	if err := f(); err != nil {

		if attempts--; attempts > 0 {
			// Add some randomness to prevent creating a Thundering Herd
			jitter := time.Duration(rand.Int63n(int64(sleep)))
			sleep = sleep + jitter/2

			time.Sleep(sleep)
			return Retry(attempts, 2*sleep, f)
		}
		return err
	}

	return nil
}
