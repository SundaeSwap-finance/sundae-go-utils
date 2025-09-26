package cardano

import (
	"testing"

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
