package main

import (
	"HaystackAtHome/internal/build_version"
	"HaystackAtHome/internal/gw/api"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	// 1. Define the method, URL, and body (optional)
	args := os.Args
	if len(args) < 3 {
		fmt.Printf("Usage: %s <method> <url>\n", args[0])
		return
	}
	method := args[1]
	url := args[2]

	// 2. Create the request object
	// This does NOT send the request yet.
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		fmt.Printf("Error creating request: %s\n", err)
		return
	}

	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)
	slog.Info("Service started", "version", build_version.Get())
	// 3. Customize the request (optional)
	now := time.Now()
	req.Header.Add("x-date", now.UTC().Format(time.RFC3339))
	req.Header.Add("AccessKey", "admin")
	// You now have a populated *http.Request object 'req'
	// that you can inspect, pass to other functions, or test.
	fmt.Printf("Created %s request for: %s\n", req.Method, req.URL)
	sign, err := api.SignReq(req, "admin")
	if err != nil {
		fmt.Printf("Sign err %v\n", err)
		return
	}
	fmt.Printf("err %v sign %v\n", err, sign)
	req.Header.Add("Signature", sign)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request: %s\n", err)
		return
	}
	defer resp.Body.Close()
	fmt.Printf("Status Code: %d\n", resp.StatusCode)

}
