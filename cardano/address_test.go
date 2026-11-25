package cardano

import (
	"testing"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/tj/assert"
)

func Test_SplitAddress(t *testing.T) {
	b, err := HasStakeAddressPointer("addr_test1grz30s2p0kr7tty6e3gk6azm7gy8wrwrp27ff3qjlmxxlxqqqqqqlj0q2c")
	assert.Nil(t, err)
	assert.True(t, b)
	b, err = HasStakeAddress("addr_test1grz30s2p0kr7tty6e3gk6azm7gy8wrwrp27ff3qjlmxxlxqqqqqqlj0q2c")
	assert.Nil(t, err)
	assert.False(t, b)
	b, err = HasNoStakeAddress("addr_test1grz30s2p0kr7tty6e3gk6azm7gy8wrwrp27ff3qjlmxxlxqqqqqqlj0q2c")
	assert.Nil(t, err)
	assert.False(t, b)

	b, err = HasStakeAddressPointer("addr_test1zr866xg5kkvarzll69xjh0tfvqvu9zvuhht2qve9ehmgp0zh43aarelw2hze3z0sy4ejx3h37gtaly55dzcnt64dy9qqw0w3q4")
	assert.Nil(t, err)
	assert.False(t, b)
	b, err = HasStakeAddress("addr_test1zr866xg5kkvarzll69xjh0tfvqvu9zvuhht2qve9ehmgp0zh43aarelw2hze3z0sy4ejx3h37gtaly55dzcnt64dy9qqw0w3q4")
	assert.Nil(t, err)
	assert.True(t, b)
	b, err = HasNoStakeAddress("addr_test1zr866xg5kkvarzll69xjh0tfvqvu9zvuhht2qve9ehmgp0zh43aarelw2hze3z0sy4ejx3h37gtaly55dzcnt64dy9qqw0w3q4")
	assert.Nil(t, err)
	assert.False(t, b)

	b, err = HasStakeAddressPointer("addr_test1wqzdedqpy2ljhf70dw89p399z836dvmgs7sn6qyr0gndu7gp9k7en")
	assert.Nil(t, err)
	assert.False(t, b)
	b, err = HasStakeAddress("addr_test1wqzdedqpy2ljhf70dw89p399z836dvmgs7sn6qyr0gndu7gp9k7en")
	assert.Nil(t, err)
	assert.False(t, b)
	b, err = HasNoStakeAddress("addr_test1wqzdedqpy2ljhf70dw89p399z836dvmgs7sn6qyr0gndu7gp9k7en")
	assert.Nil(t, err)
	assert.True(t, b)

}

func Test_EnvToNetworkID(t *testing.T) {
	previous := sundaecli.CommonOpts.Env
	t.Cleanup(func() { sundaecli.CommonOpts.Env = previous })

	tests := []struct {
		name       string
		env        string
		defaultEnv string
		want       byte
	}{
		{name: "mainnet", env: "mainnet", want: 1},
		{name: "cardano-tom", env: "cardano-tom", want: 1},
		{name: "preview", env: "preview", want: 0},
		{name: "preprod", env: "preprod", want: 0},
		{name: "testnet without a named case", env: "musashi", want: 0},
		{name: "empty env falls back to the process env", defaultEnv: "mainnet", want: 1},
		{name: "empty env on a testnet process", defaultEnv: "musashi", want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sundaecli.CommonOpts.Env = tt.defaultEnv
			got, err := EnvToNetworkID(tt.env)
			assert.Nil(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
