package tarpit

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// fakeDDB implements only GetItemWithContext; every other method panics, which
// guarantees the request path never calls Scan or any other operation.
type fakeDDB struct {
	dynamodbiface.DynamoDBAPI
	rows     map[string]map[string]*dynamodb.AttributeValue
	getCalls int
}

func (f *fakeDDB) GetItemWithContext(_ aws.Context, in *dynamodb.GetItemInput, _ ...request.Option) (*dynamodb.GetItemOutput, error) {
	f.getCalls++
	pk := *in.Key[attrPK].S
	if item, ok := f.rows[pk]; ok {
		return &dynamodb.GetItemOutput{Item: item}, nil
	}
	return &dynamodb.GetItemOutput{}, nil
}

func TestDynamoSourceFlagsActiveRowAndCaches(t *testing.T) {
	f := &fakeDDB{rows: map[string]map[string]*dynamodb.AttributeValue{
		BaitPK("1.2.3.4"): {
			attrPK:        {S: aws.String(BaitPK("1.2.3.4"))},
			attrBaitUntil: {N: aws.String("0")}, // permanent
		},
	}}
	src := NewDynamoSource(f, "tbl", time.Minute)

	for i := 0; i < 3; i++ {
		bait, err := src.IsBait(context.Background(), "1.2.3.4")
		if err != nil {
			t.Fatalf("IsBait error: %v", err)
		}
		if !bait {
			t.Fatal("active bait row not reported as bait")
		}
	}
	if f.getCalls != 1 {
		t.Fatalf("GetItem calls = %d, want 1 (result must be cached)", f.getCalls)
	}
}

func TestDynamoSourceCachesNonBait(t *testing.T) {
	f := &fakeDDB{rows: map[string]map[string]*dynamodb.AttributeValue{}}
	src := NewDynamoSource(f, "tbl", time.Minute)

	for i := 0; i < 3; i++ {
		bait, err := src.IsBait(context.Background(), "9.9.9.9")
		if err != nil {
			t.Fatalf("IsBait error: %v", err)
		}
		if bait {
			t.Fatal("absent row reported as bait")
		}
	}
	if f.getCalls != 1 {
		t.Fatalf("GetItem calls = %d, want 1 (a normal IP must be negatively cached)", f.getCalls)
	}
}

func TestDynamoSourceExpiredRowIsNotBait(t *testing.T) {
	f := &fakeDDB{rows: map[string]map[string]*dynamodb.AttributeValue{
		BaitPK("1.2.3.4"): {
			attrPK:        {S: aws.String(BaitPK("1.2.3.4"))},
			attrBaitUntil: {N: aws.String("1")}, // expired in 1970
		},
	}}
	src := NewDynamoSource(f, "tbl", time.Minute)

	bait, err := src.IsBait(context.Background(), "1.2.3.4")
	if err != nil {
		t.Fatalf("IsBait error: %v", err)
	}
	if bait {
		t.Fatal("expired bait row reported as bait")
	}
}
