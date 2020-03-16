package cli

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/oybekmuzropov/tusd/cmd/tusd/cli/hooks"
	"github.com/oybekmuzropov/tusd/pkg/handler"
	"os"
	"strconv"
	"strings"
)

var hookHandler hooks.HookHandler = nil

func hookTypeInSlice(a hooks.HookType, list []hooks.HookType) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func preCreateCallback(info handler.HookEvent) error {
	if output, err := invokeHookSync(hooks.HookPreCreate, info, true); err != nil {
		if hookErr, ok := err.(hooks.HookError); ok {
			return hooks.NewHookError(
				fmt.Errorf("pre-create hook failed: %s", err),
				hookErr.StatusCode(),
				hookErr.Body(),
			)
		}
		return fmt.Errorf("pre-create hook failed: %s\n%s", err, string(output))
	}

	return nil
}

func SetupHookMetrics() {
	MetricsHookErrorsTotal.WithLabelValues(string(hooks.HookPostFinish)).Add(0)
	MetricsHookErrorsTotal.WithLabelValues(string(hooks.HookPostTerminate)).Add(0)
	MetricsHookErrorsTotal.WithLabelValues(string(hooks.HookPostReceive)).Add(0)
	MetricsHookErrorsTotal.WithLabelValues(string(hooks.HookPostCreate)).Add(0)
	MetricsHookErrorsTotal.WithLabelValues(string(hooks.HookPreCreate)).Add(0)
}

func SetupPreHooks(config *handler.Config) error {
	if Flags.FileHooksDir != "" {
		stdout.Printf("Using '%s' for hooks", Flags.FileHooksDir)

		hookHandler = &hooks.FileHook{
			Directory: Flags.FileHooksDir,
		}
	} else if Flags.HttpHooksEndpoint != "" {
		stdout.Printf("Using '%s' as the endpoint for hooks", Flags.HttpHooksEndpoint)

		hookHandler = &hooks.HttpHook{
			Endpoint:   Flags.HttpHooksEndpoint,
			MaxRetries: Flags.HttpHooksRetry,
			Backoff:    Flags.HttpHooksBackoff,
		}
	} else if Flags.GrpcHooksEndpoint != "" {
		stdout.Printf("Using '%s' as the endpoint for gRPC hooks", Flags.GrpcHooksEndpoint)

		hookHandler = &hooks.GrpcHook{
			Endpoint:   Flags.GrpcHooksEndpoint,
			MaxRetries: Flags.GrpcHooksRetry,
			Backoff:    Flags.GrpcHooksBackoff,
		}
	} else if Flags.PluginHookPath != "" {
		stdout.Printf("Using '%s' to load plugin for hooks", Flags.PluginHookPath)

		hookHandler = &hooks.PluginHook{
			Path: Flags.PluginHookPath,
		}
	} else {
		return nil
	}

	var enabledHooksString []string
	for _, h := range Flags.EnabledHooks {
		enabledHooksString = append(enabledHooksString, string(h))
	}

	stdout.Printf("Enabled hook events: %s", strings.Join(enabledHooksString, ", "))

	if err := hookHandler.Setup(); err != nil {
		return err
	}

	config.PreUploadCreateCallback = preCreateCallback

	return nil
}

func SetupPostHooks(handler *handler.Handler) {
	go func() {
		for {
			select {
			case info := <-handler.CompleteUploads:
				invokeHookAsync(hooks.HookPostFinish, info)
			case info := <-handler.TerminatedUploads:
				invokeHookAsync(hooks.HookPostTerminate, info)
			case info := <-handler.UploadProgress:
				invokeHookAsync(hooks.HookPostReceive, info)
			case info := <-handler.CreatedUploads:
				invokeHookAsync(hooks.HookPostCreate, info)
			}
		}
	}()
}

func invokeHookAsync(typ hooks.HookType, info handler.HookEvent) {
	go func() {
		// Error handling is taken care by the function.
		_, _ = invokeHookSync(typ, info, false)
	}()
}

func invokeHookSync(typ hooks.HookType, info handler.HookEvent, captureOutput bool) ([]byte, error) {
	if !hookTypeInSlice(typ, Flags.EnabledHooks) {
		return nil, nil
	}

	id := info.Upload.ID
	size := info.Upload.Size

	switch typ {
	case hooks.HookPostFinish:
		logEv(stdout, "UploadFinished", "id", id, "size", strconv.FormatInt(size, 10))
		upload(info.Upload)
		os.Remove(Flags.UploadDir + "/" + info.Upload.ID + ".info")
		os.Remove(Flags.UploadDir + "/" + info.Upload.ID + ".mp4")
	case hooks.HookPostTerminate:
		logEv(stdout, "UploadTerminated", "id", id)
	}

	if hookHandler == nil {
		return nil, nil
	}

	name := string(typ)
	if Flags.VerboseOutput {
		logEv(stdout, "HookInvocationStart", "type", name, "id", id)
	}

	output, returnCode, err := hookHandler.InvokeHook(typ, info, captureOutput)

	if err != nil {
		logEv(stderr, "HookInvocationError", "type", string(typ), "id", id, "error", err.Error())
		MetricsHookErrorsTotal.WithLabelValues(string(typ)).Add(1)
	} else if Flags.VerboseOutput {
		logEv(stdout, "HookInvocationFinish", "type", string(typ), "id", id)
	}

	if typ == hooks.HookPostReceive && Flags.HooksStopUploadCode != 0 && Flags.HooksStopUploadCode == returnCode {
		logEv(stdout, "HookStopUpload", "id", id)

		info.Upload.StopUpload()
	}

	return output, err
}

func upload(f handler.FileInfo) {
	metadatas := make(map[string]*string)

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("AWS_REGION")) ,
		Credentials: credentials.NewStaticCredentials(os.Getenv("AWS_S3_ID"), os.Getenv("AWS_S3_SECRET"), ""),
	})

	if err != nil {
		fmt.Println("aws create session error: ", err)
	}

	uploader := s3manager.NewUploader(sess)

	up, err := os.Open(Flags.UploadDir + "/" + f.ID + ".mp4")
	if err != nil {
		logEv(stderr, "OpenUploadFile", "id", f.ID, "err", err.Error())
		return
	}

	for key, value := range f.MetaData {
		metadatas[key] = &value
	}

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(os.Getenv("BUCKET_NAME")),
		Key:    aws.String(f.ID),
		ContentType: aws.String("video/mp4"),
		Body:   up,
		Metadata: metadatas,
	})

	if err != nil {
		logEv(stderr, "UploadFileError", "id", f.ID, "err", err.Error())
		return
	}
	logEv(stdout, "UploadS3Successful", "id", f.ID)
}
