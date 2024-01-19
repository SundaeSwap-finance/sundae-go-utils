package sundaereport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"time"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/rs/zerolog"
)

type GenerateCallback func(ctx context.Context) (interface{}, error)

type Handler struct {
	service sundaecli.Service
	logger  zerolog.Logger
	s3      s3iface.S3API

	reportName string

	generate GenerateCallback
}

func ReportKey(serviceName, reportName string, timestamp time.Time) string {
	return fmt.Sprintf("%v/%v/%v/%v/%v", serviceName, reportName, timestamp.Format("2006-01-02"), timestamp.Format("15"), timestamp.Format("2006-01-02-15:04:05.json"))
}

func NewHandler(
	service sundaecli.Service,
	reportName string,
	generate GenerateCallback,
) *Handler {
	session := session.Must(session.NewSession(aws.NewConfig()))
	return &Handler{
		service:    service,
		logger:     sundaecli.Logger(service),
		s3:         s3.New(session),
		reportName: reportName,
		generate:   generate,
	}
}

func (h *Handler) Generate(ctx context.Context, _ json.RawMessage) error {
	h.logger.Info().Msg("generating report")
	report, err := h.generate(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("failed to generate report")
		return err
	}
	reportBytes, err := json.Marshal(report)
	if err != nil {
		h.logger.Warn().Err(err).Msg("failed to marshal report")
		return err
	}

	now := time.Now()
	var filename string
	if sundaecli.CommonOpts.Dry {
		if ReportOpts.OutFile == "" {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(report); err != nil {
				return err
			}
		} else {
			filename = fmt.Sprintf("%v-%v.json", h.reportName, now.Format("2006-01-02-15:04:05"))
			if err := os.MkdirAll(path.Dir(filename), 0755); err != nil {
				return err
			}
			h.logger.Info().Str("bucket", ReportOpts.Bucket).Str("filename", filename).Int("size", len(reportBytes)).Msg("dry run, saving report locally")
			if err := os.WriteFile(filename, reportBytes, 0644); err != nil {
				return err
			}
		}
	} else {
		filename := ReportKey(h.service.Name, h.reportName, now)
		h.logger.Info().Str("bucket", ReportOpts.Bucket).Str("filename", filename).Int("size", len(reportBytes)).Msg("saving report to s3")
		_, err = h.s3.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(ReportOpts.Bucket),
			Body:   bytes.NewReader(reportBytes),
			Key:    aws.String(filename),
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func GetRawAsOf(ctx context.Context, s3Api s3iface.S3API, bucket, servicename, reportName string, timestamp time.Time) ([]byte, string, error) {
	count := 0
	for {
		prefix := fmt.Sprintf("%v/%v/%v", servicename, reportName, timestamp.Format("2006-01-02"))
		listInput := s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			MaxKeys: aws.Int64(1000),
			Prefix:  aws.String(prefix),
		}
		listOutput, err := s3Api.ListObjectsV2WithContext(ctx, &listInput)
		if err != nil {
			return nil, "", fmt.Errorf("failed to read most recent exchange-feed: failed to list objects: %w", err)
		}

		if len(listOutput.Contents) == 0 {
			yesterday := timestamp.AddDate(0, 0, -1)
			timestamp = time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 0, time.UTC)
			if count > 5 {
				return nil, "", fmt.Errorf("failed to find latest report after 5 days: %v", timestamp)
			}
			count += 1
			continue
		}

		// Grab the last
		sort.Slice(listOutput.Contents, func(i, j int) bool {
			return aws.StringValue(listOutput.Contents[i].Key) > aws.StringValue(listOutput.Contents[j].Key)
		})

		firstKey := listOutput.Contents[0].Key

		input := s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    firstKey,
		}
		output, err := s3Api.GetObjectWithContext(ctx, &input)
		if err != nil {
			return nil, "", fmt.Errorf("failed to read most recent file in %v: failed to get object, %v: %w", prefix, aws.StringValue(firstKey), err)
		}
		bytes, err := io.ReadAll(output.Body)
		if err != nil {
			return nil, "", fmt.Errorf("failed to read most recent file in %v: failed to read s3 response, %v: %w", prefix, aws.StringValue(firstKey), err)
		}
		return bytes, aws.StringValue(firstKey), nil
	}
}

func GetLatest(ctx context.Context, s3Api s3iface.S3API, bucket, serviceName, reportName string, obj any) (string, error) {
	now := time.Now().UTC()
	bytes, filename, err := GetRawAsOf(ctx, s3Api, bucket, serviceName, reportName, now)
	if err != nil {
		return "", err
	}
	if err := json.Unmarshal(bytes, obj); err != nil {
		return "", fmt.Errorf("failed to unmarshal latest report: %w", err)
	}
	return filename, nil
}

func (h *Handler) Start() error {
	if ReportOpts.GetLatest {
		reportBytes, filename, err := GetRawAsOf(context.Background(), h.s3, ReportOpts.Bucket, h.service.Name, h.reportName, time.Now().UTC())
		if err != nil {
			return err
		}
		if ReportOpts.OutFile == "" {
			var prettyBytes bytes.Buffer
			if err := json.Indent(&prettyBytes, reportBytes, "", "  "); err != nil {
				return err
			}
			os.Stdout.Write(prettyBytes.Bytes())
		} else {
			if err := os.MkdirAll(path.Dir(filename), 0755); err != nil {
				return err
			}
			if err := os.WriteFile(ReportOpts.OutFile, reportBytes, 0644); err != nil {
				return err
			}
		}
		return nil
	}

	switch {
	case sundaecli.CommonOpts.Console:
		return h.Generate(context.Background(), nil)

	default:
		lambda.Start(h.Generate)
	}
	return nil
}
