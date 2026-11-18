package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"lamina/internal/engine"
	"os"
	"time"
)

// migrateCommand implements the operational surface for format conversion:
//
//	laminad upgrade  -data DIR [-cut-tail] [-json]
//	laminad rollback -data DIR
//	laminad recover  -data DIR [-json]   (read-only diagnosis)
//
// The serving command never converts anything implicitly.
func migrateCommand(name string, args []string) error {
	switch name {
	case "upgrade":
		return upgradeCommand(args)
	case "rollback":
		return rollbackCommand(args)
	case "recover":
		return recoverCommand(args)
	default:
		return fmt.Errorf("unknown command %q", name)
	}
}

func migrateFlags(name string) (*flag.FlagSet, *string, *bool) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	dir := fs.String("data", "", "data directory")
	jsonOut := fs.Bool("json", false, "print the report as JSON")
	return fs, dir, jsonOut
}

func upgradeCommand(args []string) error {
	fs, dir, jsonOut := migrateFlags("upgrade")
	cut := fs.Bool("cut-tail", false, "discard an incomplete legacy journal tail after reporting it")
	assume := fs.Bool("assume-continuous", false, "adopt legacy material whose journal does not cover the checkpoint history (recorded as an assumption, not a verification)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("upgrade requires -data")
	}
	report, err := engine.Upgrade(*dir, time.Now(), *cut, *assume)
	if err != nil {
		return err
	}
	return printReport(report, *jsonOut)
}

func rollbackCommand(args []string) error {
	fs, dir, jsonOut := migrateFlags("rollback")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("rollback requires -data")
	}
	report, err := engine.Rollback(*dir)
	if err != nil {
		return err
	}
	return printReport(report, *jsonOut)
}

func recoverCommand(args []string) error {
	fs, dir, jsonOut := migrateFlags("recover")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("recover requires -data")
	}
	report, err := engine.Recover(*dir)
	if err != nil {
		return err
	}
	return printReport(report, *jsonOut)
}

func printReport(v any, asJSON bool) error {
	if asJSON {
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return nil
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(append(b, '\n'))
	return err
}
