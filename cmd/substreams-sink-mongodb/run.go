package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/cli"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-mongodb/mongo"
	"github.com/streamingfast/substreams-sink-mongodb/sinker"
	"go.uber.org/zap"
)

var sinkRunCmd = Command(sinkRunE,
	"run <dsn> <database_name> <schema> <endpoint> <manifest> <module> [<start>:<stop>]",
	"Runs MongoDB sink process",
	RangeArgs(6, 7),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
	}),
	OnCommandErrorLogAndExit(zlog),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

	sink.RegisterMetrics()
	sinker.RegisterMetrics()

	mongoDSN := args[0]
	databaseName := args[1]
	schema := args[2]
	endpoint := args[3]
	manifestPath := args[4]
	outputModuleName := args[5]
	blockRange := ""
	if len(args) > 6 {
		blockRange = args[6]
	}

	schemaContent, err := os.ReadFile(schema)
	if err != nil {
		return fmt.Errorf("reading schema file: %w", err)
	}

	var tables mongo.Tables
	if err := json.Unmarshal(schemaContent, &tables); err != nil {
		return fmt.Errorf("unmarshalling schema file: %w", err)
	}

	mongoLoader, err := mongo.NewMongoDB(mongoDSN, databaseName, zlog)
	if err != nil {
		return fmt.Errorf("unable to create mongo loader: %w", err)
	}

	sink, err := sink.NewFromViper(
		cmd,
		"sf.substreams.sink.database.v1.DatabaseChanges",
		endpoint, manifestPath, outputModuleName, blockRange,
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("unable to setup sinker: %w", err)
	}

	mongoSinker, err := sinker.New(sink, mongoLoader, tables, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup mongo sinker: %w", err)
	}

	mongoSinker.OnTerminating(app.Shutdown)
	app.OnTerminating(func(err error) {
		mongoSinker.Shutdown(err)
	})

	go func() {
		mongoSinker.Run(ctx)
	}()

	zlog.Info("ready, waiting for signal to quit")

	signalHandler, isSignaled, _ := cli.SetupSignalHandler(0*time.Second, zlog)
	select {
	case <-signalHandler:
		go app.Shutdown(nil)
		break
	case <-app.Terminating():
		zlog.Info("run terminating", zap.Bool("from_signal", isSignaled.Load()), zap.Bool("with_error", app.Err() != nil))
		break
	}

	zlog.Info("waiting for run termination")
	select {
	case <-app.Terminated():
	case <-time.After(30 * time.Second):
		zlog.Warn("application did not terminate within 30s")
	}

	if err := app.Err(); err != nil {
		return err
	}

	zlog.Info("run terminated gracefully")
	return nil
}
