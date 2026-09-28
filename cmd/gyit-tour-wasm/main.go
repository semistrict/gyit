//go:build js && wasm

// The browser bridge exposes the shared reader; it never shells out to Git.
package main

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"syscall/js"
	"time"

	"gyit/internal/tour"
)

func value(v any) js.Value {
	b, _ := json.Marshal(v)
	return js.Global().Get("JSON").Call("parse", string(b))
}
func main() {
	engine, initErr := tour.New()
	var busy atomic.Bool
	run := js.FuncOf(func(this js.Value, args []js.Value) any {
		command := ""
		delay := 0
		if len(args) > 0 {
			command = args[0].String()
		}
		if len(args) > 1 {
			delay = args[1].Int()
		}
		executor := js.FuncOf(func(this js.Value, p []js.Value) any {
			resolve := p[0]
			if initErr != nil {
				resolve.Invoke(value(tour.Result{Error: initErr.Error()}))
				return nil
			}
			if !busy.CompareAndSwap(false, true) {
				resolve.Invoke(value(tour.Result{Error: "a command is already running"}))
				return nil
			}
			go func() {
				defer busy.Store(false)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				r := engine.Run(ctx, command, time.Duration(delay)*time.Millisecond, func(e tour.Event) {
					callback := js.Global().Get("gyitOnEvent")
					if callback.Type() == js.TypeFunction {
						callback.Invoke(value(e))
					}
				})
				resolve.Invoke(value(r))
			}()
			return nil
		})
		promise := js.Global().Get("Promise").New(executor)
		executor.Release()
		return promise
	})
	js.Global().Set("gyitRun", run)
	select {}
}
