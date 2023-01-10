package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/derr"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams-sink-mongodb/mongo"
	"github.com/streamingfast/substreams-sink-mongodb/sinker"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	"go.uber.org/zap"
)

var SinkRunCmd = Command(sinkRunE,
	"run <dsn> <database_name> <schema> <endpoint> <manifest> <module> [<start>:<stop>]",
	"Runs extractor code",
	RangeArgs(6, 7),
	Flags(func(flags *pflag.FlagSet) {
		flags.BoolP("insecure", "k", false, "Skip certificate validation on GRPC connection")
		flags.BoolP("plaintext", "p", false, "Establish GRPC connection in plaintext")
		flags.Int("undo-buffer-size", 0, "Number of blocks to keep buffered to handle fork reorganizations")
	}),
	AfterAllHook(func(_ *cobra.Command) {
		sinker.RegisterMetrics()
	}),
)

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := shutter.New()

	ctx, cancelApp := context.WithCancel(cmd.Context())
	app.OnTerminating(func(_ error) {
		cancelApp()
	})

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
	err = json.Unmarshal(schemaContent, &tables)
	if err != nil {
		return fmt.Errorf("unmarshalling schema file: %w", err)
	}

	mongoLoader, err := mongo.NewMongoDB(mongoDSN, databaseName, zlog)
	if err != nil {
		return fmt.Errorf("unable to create mongo loader: %w", err)
	}

	zlog.Info("reading substreams manifest", zap.String("manifest_path", manifestPath))
	pkg, err := manifest.NewReader(manifestPath).Read()
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	graph, err := manifest.NewModuleGraph(pkg.Modules.Modules)
	if err != nil {
		return fmt.Errorf("create substreams moduel graph: %w", err)
	}

	zlog.Info("validating output store", zap.String("output_store", outputModuleName))
	module, err := graph.Module(outputModuleName)
	if err != nil {
		return fmt.Errorf("get output module %q: %w", outputModuleName, err)
	}
	if module.GetKindMap() == nil {
		return fmt.Errorf("ouput module %q is *not* of  type 'Mapper'", outputModuleName)
	}

	if module.Output.Type != "proto:sf.substreams.database.v1.DatabaseChanges" {
		return fmt.Errorf("sync only supports maps with output type 'proto:sf.substreams.database.v1.DatabaseChanges'")
	}
	hashes := manifest.NewModuleHashes()
	outputModuleHash := hashes.HashModule(pkg.Modules, module, graph)

	resolvedStartBlock, resolvedStopBlock, err := readBlockRange(module, blockRange)
	if err != nil {
		return fmt.Errorf("resolve block range: %w", err)
	}
	zlog.Info("resolved block range",
		zap.Int64("start_block", resolvedStartBlock),
		zap.Uint64("stop_block", resolvedStopBlock),
	)

	apiToken := readAPIToken()

	config := &sinker.Config{
		DBLoader:         mongoLoader,
		DBName:           databaseName,
		BlockRange:       blockRange,
		DDL:              tables,
		Pkg:              pkg,
		OutputModule:     module,
		OutputModuleName: outputModuleName,
		OutputModuleHash: outputModuleHash,
		UndoBufferSize:   viper.GetInt("run-undo-buffer-size"),
		ClientConfig: client.NewSubstreamsClientConfig(
			endpoint,
			apiToken,
			viper.GetBool("run-insecure"),
			viper.GetBool("run-plaintext"),
		),
	}

	mongoSinker, err := sinker.New(config, zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to create sinker: %w", err)
	}
	mongoSinker.OnTerminating(app.Shutdown)

	app.OnTerminating(func(err error) {
		zlog.Info("application terminating, shutting down sinker", zap.Error(err))
		mongoSinker.Shutdown(err)
	})

	go func() {
		if err := mongoSinker.Start(ctx); err != nil {
			zlog.Error("sinker start failed", zap.Error(err))
			mongoSinker.Shutdown(err)
		}
	}()

	signalHandler := derr.SetupSignalHandler(0 * time.Second)
	zlog.Info("ready, waiting for signal to quit")
	select {
	case <-signalHandler:
		zlog.Info("received termination signal, quitting application")
		go app.Shutdown(nil)
	case <-app.Terminating():
		NoError(app.Err(), "application shutdown unexpectedly, quitting")
	}

	zlog.Info("waiting for app termination")
	select {
	case <-app.Terminated():
	case <-ctx.Done():
	case <-time.After(30 * time.Second):
		zlog.Error("application did not terminated within 30s, forcing exit")
	}

	return nil
}
