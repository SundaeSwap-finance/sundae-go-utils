package syncV2Consumer

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

type captureS3 struct {
	s3iface.S3API
	input *s3.GetObjectInput
	body  *trackedBody
}

func (s *captureS3) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	s.input = input
	return &s3.GetObjectOutput{Body: s.body}, nil
}

func TestDownloaderBucketAndPrefix(t *testing.T) {
	for _, tc := range []struct{ name, bucket, wantBucket, prefix string }{
		{"default", "", "musashi-sundae-sync-v2-000000000000-us-east-2", ""},
		{"override", "musashi-test-2-sundae-sync-v2", "musashi-test-2-sundae-sync-v2", ""},
		{"uri", "s3://archive", "archive", ""},
		{"prefix", "s3://archive/history/", "archive", "history/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &trackedBody{Reader: strings.NewReader("block")}
			api := &captureS3{body: body}
			d := S3Downloader{S3: api, Env: "musashi", Account: "000000000000", Bucket: tc.bucket}
			data, err := d.DownloadBlockSync(make([]byte, 32))
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != "block" || !body.closed {
				t.Fatal("body not read and closed")
			}
			if aws.StringValue(api.input.Bucket) != tc.wantBucket {
				t.Fatalf("bucket: %s", aws.StringValue(api.input.Bucket))
			}
			want := tc.prefix + "blocks/by-hash/00/" + strings.Repeat("0", 64) + ".cbor"
			if aws.StringValue(api.input.Key) != want {
				t.Fatalf("key: %s", aws.StringValue(api.input.Key))
			}
		})
	}
}
