package main

import (
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/registryguard"
	"os"
)

func main() {
	repo := os.Getenv("GHCR_REPOSITORY")
	if repo == "" {
		repo = registryguard.DefaultRepository
	}
	exists, err := registryguard.Exists(registryguard.Client(), os.Getenv("IMAGE_TAG"), os.Getenv("GHCR_USERNAME"), os.Getenv("GHCR_TOKEN"), repo)
	if err != nil {
		fmt.Fprintln(os.Stderr, "::error::Could not verify image SHA tag ("+err.Error()+")")
		os.Exit(1)
	}
	if exists {
		fmt.Fprintln(os.Stderr, "::error::The immutable image SHA tag already exists")
		os.Exit(1)
	}
	fmt.Println("Image SHA tag is confirmed absent")
}
