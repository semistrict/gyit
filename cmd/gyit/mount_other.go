//go:build !darwin

package main

import "context"

func nativeGitHubMount(context.Context, string, int64, string) (bool, error) { return false, nil }
