/*
Copyright 2024 Andrew Meredith

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bufio"
	"html/template"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// commentCodeCmd represents the commentCode command
var commentCodeCmd = &cobra.Command{
	Use:   "comment-code",
	Short: "Add license comments to source code files.",
	Long:  `Scans source code files for license comments and adds them if they are missing.`,
	Run: func(cmd *cobra.Command, args []string) {
		commentLang := cmd.Flag("lang").Value.String()
		switch commentLang {
		case "go":
			// OK to continue
		default:
			log.Fatalf("unsupported language: %s", commentLang)
		}

		rootDir := os.Getenv("CANTER_ROOT")
		if rootDir == "" {
			log.Fatalf("CANTER_ROOT environment variable not set. Are you running this from the correct directory?")
		}

		// Read and compile the license template.
		templateFile := path.Join(rootDir, "config", "license.gotpl")
		tmpl, err := template.ParseFiles(templateFile)
		if err != nil {
			log.Fatalf("error parsing license template: %v", err)
		}
		// Get template vars: year, author (from git config).
		year := time.Now().Year()
		author := "Unknown"
		authorCmd := exec.Command("git", "config", "user.name")
		authorCmd.Dir = rootDir
		authorOut, err := authorCmd.Output()
		if err != nil {
			log.Printf("error getting author name: %v", err)
		} else {
			author = strings.TrimSpace(string(authorOut))
		}

		var licenseTextBuilder strings.Builder
		if err := tmpl.Execute(&licenseTextBuilder, map[string]any{
			"Year":   year,
			"Author": author,
		}); err != nil {
			log.Fatalf("error executing license template: %v", err)
		}
		licenseText := licenseTextBuilder.String()

		filename := cmd.Flag("file").Value.String()
		if filename != "" {
			// Add license comment to a single file.
			applyLicenseComment(filename, licenseText)
		} else if cmd.Flag("all-files").Value.String() == "true" {
			// Recursive scan for files in the current directory.
			// For each file, check for a license comment.
			filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
				if err != nil {
					log.Printf("error accessing a path %q: %v\n", path, err)
					return err
				}
				if info.IsDir() {
					return nil
				}
				if filepath.Ext(path) != ".go" {
					return nil
				}

				return applyLicenseComment(path, licenseText)
			})
		} else {
			log.Fatalf("no file specified and --all-files not set")
		}
	},
}

func applyLicenseComment(filename string, licenseText string) error {
	// Open the file and read the first few lines.
	f, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		log.Printf("error opening file %q: %v\n", filename, err)
		return nil
	}
	defer f.Close()

	// Read the first few lines.
	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > 5 {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("error reading file %q: %v\n", filename, err)
		return nil
	}

	// Check for a license comment.
	if len(lines) > 2 && strings.HasPrefix(lines[0], "/*") &&
		strings.Contains(lines[3], "Licensed under the Apache License, Version 2.0") {
		return nil
	}

	// Add the license comment.
	// Read the entire file into memory.
	f.Seek(0, 0)
	contents, err := io.ReadAll(f)
	if err != nil {
		log.Printf("error reading file %q: %v\n", filename, err)
		return nil
	}
	_ = contents

	// Truncate the file and write the license comment.
	if err := f.Truncate(0); err != nil {
		log.Printf("error truncating file %q: %v\n", filename, err)
		return nil
	}
	if _, err := f.Seek(0, 0); err != nil {
		log.Printf("error seeking to beginning of file %q: %v\n", filename, err)
		return nil
	}

	// Write the license comment.
	f.Write([]byte("/*\n"))
	if _, err := f.Write([]byte(licenseText)); err != nil {
		log.Printf("error writing license comment to file %q: %v\n", filename, err)
	}
	f.Write([]byte("*/\n\n"))

	// Write the original file contents.
	if _, err := f.Write(contents); err != nil {
		log.Printf("error writing file contents to file %q: %v\n", filename, err)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(commentCodeCmd)

	commentCodeCmd.Flags().StringP("lang", "l", "go", "Language of the source code files")
	commentCodeCmd.Flags().StringP("file", "f", "", "File to add license comment to")
	commentCodeCmd.Flags().Bool("all-files", false, "Run on all files in the current directory")
}
