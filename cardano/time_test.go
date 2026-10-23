package cardano

import (
	"testing"
	"time"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/tj/assert"
)

func TestTimeToSlotEnv(t *testing.T) {
	// These tests change global CLI options and must not run in parallel.
	opts := sundaecli.CommonOpts
	t.Cleanup(func() { sundaecli.CommonOpts = opts })

	const customOffset uint64 = 1700000000
	tests := []struct {
		name       string
		env        string
		defaultEnv string
		slotOffset uint64
		wantOffset uint64
		wantError  string
	}{
		{name: "preview", env: "preview", slotOffset: customOffset, wantOffset: SlotOffsetPreview},
		{name: "preprod", env: "preprod", slotOffset: customOffset, wantOffset: SlotOffsetPreprod},
		{name: "mainnet", env: "mainnet", slotOffset: customOffset, wantOffset: SlotOffsetMainnet},
		{name: "cardano-tom", env: "cardano-tom", slotOffset: customOffset, wantOffset: SlotOffsetMainnet},
		{name: "default network", defaultEnv: "preview", wantOffset: SlotOffsetPreview},
		{name: "explicit network overrides default", env: "preprod", defaultEnv: "mainnet", wantOffset: SlotOffsetPreprod},
		{name: "custom environment", env: "musashi", slotOffset: customOffset, wantOffset: customOffset},
		{name: "default custom environment", defaultEnv: "musashi", slotOffset: customOffset, wantOffset: customOffset},
		{name: "empty environment with offset", slotOffset: customOffset, wantOffset: customOffset},
		{name: "custom environment without offset", env: "musashi", wantError: "unrecognized environment musashi"},
		{name: "default custom environment without offset", defaultEnv: "musashi", wantError: "unrecognized environment musashi"},
		{name: "empty environment without offset", wantError: "unrecognized environment "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sundaecli.CommonOpts.Env = tt.defaultEnv
			sundaecli.CommonOpts.SlotOffset = tt.slotOffset
			instant := time.Unix(1800000000, 0)

			slot, err := TimeToSlotEnv(instant, tt.env)
			if tt.wantError != "" {
				assert.EqualError(t, err, tt.wantError)
				assert.Equal(t, uint64(0), slot)
				return
			}
			if err != nil {
				t.Fatalf("TimeToSlotEnv() error = %v", err)
			}
			assert.Equal(t, uint64(1800000000)-tt.wantOffset, slot)

			roundTrip, err := SlotToTimeEnv(slot, tt.env)
			assert.NoError(t, err)
			assert.True(t, instant.Equal(roundTrip))
		})
	}
}

func TestDateTimeSlotCustomEnvironment(t *testing.T) {
	// DateTime.Slot resolves the environment through global CLI options.
	opts := sundaecli.CommonOpts
	t.Cleanup(func() { sundaecli.CommonOpts = opts })
	sundaecli.CommonOpts.Env = "musashi"
	sundaecli.CommonOpts.SlotOffset = 1700000000

	d := DateTime{Instant: time.Unix(1700000123, 0)}
	slot, err := d.Slot()
	assert.NoError(t, err)
	assert.Equal(t, int32(123), slot)

	sundaecli.CommonOpts.SlotOffset = 0
	slot, err = d.Slot()
	assert.EqualError(t, err, "unrecognized environment musashi")
	assert.Equal(t, int32(0), slot)
}
