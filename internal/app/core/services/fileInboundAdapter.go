/*
Copyright 2025 The Synapse Authors
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

package services

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"reflect"

	"strconv"

	"github.com/apache/synapse-go/internal/pkg/artifacts"
	"github.com/apache/synapse-go/internal/pkg/consolelogger"
	"github.com/apache/synapse-go/internal/pkg/mediationengine"
	"github.com/apache/synapse-go/internal/pkg/synapsecontext"
)

type FileInboundAdapter struct {
	inbound artifacts.Inbound
}

func (adapter *FileInboundAdapter) PollFile(ctx context.Context) error {
	waitgroup := ctx.Value("waitGroup").(*sync.WaitGroup)
	if adapter.inbound.Protocol == "file" {

		interval, found := getIntervalParameterValue(adapter.inbound)
		if found {
			consolelogger.InfoLog(fmt.Sprintf("Polling file every %d milliseconds", interval))
		} else {
			consolelogger.ErrorLog("Interval parameter not found")
			return errors.New("interval parameter not found")
		}

		fileURI, found := getFileURIParameterValue(adapter.inbound)
		if found {
			consolelogger.InfoLog(fmt.Sprintf("File URI: %s", fileURI))
		} else {
			consolelogger.ErrorLog("File URI parameter not found")
			return errors.New("file URI parameter not found")
		}

		moveAfterFailure, found := getMoveAfterFailureParameterValue(adapter.inbound)
		if found {
			consolelogger.InfoLog(fmt.Sprintf("Move after failure: %s", moveAfterFailure))
		} else {
			consolelogger.ErrorLog("Move after failure parameter not found")
			return errors.New("move after failure parameter not found")
		}

		moveAfterProcess, found := getMoveAfterProcessParameterValue(adapter.inbound)
		if found {
			consolelogger.InfoLog(fmt.Sprintf("Move after process: %s", moveAfterProcess))
		} else {
			consolelogger.ErrorLog("Move after process parameter not found")
			return errors.New("move after process parameter not found")
		}

		pattern, found := getFileNamePatternParameterValue(adapter.inbound)
		if found {
			consolelogger.InfoLog(fmt.Sprintf("File name pattern: %s", pattern))
		} else {
			consolelogger.ErrorLog("File name pattern parameter not found")
			return errors.New("file name pattern parameter not found")
		}

		ticker := time.NewTicker(time.Duration(interval) * time.Second) // Ensures precise polling
		defer ticker.Stop()

		for range ticker.C {
			select {
			case <-ctx.Done():
				fmt.Println("Cleaning up file polling gracefully")
				consolelogger.InfoLog("Cleaning up file polling gracefully")
				waitgroup.Done()
				return nil
			default:
				startTime := time.Now()
				consolelogger.DebugLog("\n--- Start new Polling Event ---")
		
				// Process failed_files.txt if available
				processFailedFiles(fileURI, moveAfterFailure)
		
				// Get the list of files at the start of this polling event
				files, err := scanDirectoryWithPattern(fileURI, pattern)
				if err != nil {
					consolelogger.ErrorLog(fmt.Sprintf("Error scanning directory %s: %v", fileURI, err))
					continue
				}
		
				// Process each file
				for _, file := range files {
					waitgroup.Add(1)
					//have to test is it safe to make go routines for each file arbitrarily
					// A solution may be put a threshold (eg:- 100 files) and then make go routines for each file.If the number of files is greater than the threshold make only upper limit (threshold) of go routines
					go adapter.ProcessFile(ctx,file)
				}
		
				// Ensure accurate polling interval
				elapsed := time.Since(startTime)
				if elapsed < time.Duration(interval)*time.Second {
					time.Sleep(time.Duration(interval)*time.Second - elapsed)
				}
			}
		}
		waitgroup.Wait()
		return nil
	} else {
		return errors.New("invalid protocol")
	}

}

func GetInstance(inbound artifacts.Inbound) (FileInboundAdapter, error) {
	if inbound.Protocol != "file" {
		return FileInboundAdapter{}, errors.New("invalid protocol")
	}
	return FileInboundAdapter{
		inbound: inbound,
	}, nil
}

func getIntervalParameterValue(inbound artifacts.Inbound) (int, bool) {
	for _, p := range inbound.Parameters {
		if p.Name == "interval" {
			interval, err := strconv.Atoi(p.Value)
			if err != nil {
				return 0, false
			}
			return interval, true
		}
	}
	return 0, false
}

func getFileURIParameterValue(inbound artifacts.Inbound) (string, bool) {
	for _, p := range inbound.Parameters {
		if p.Name == "transport.vfs.FileURI" {
			return p.Value, true
		}
	}
	return "", false
}

func getMoveAfterFailureParameterValue(inbound artifacts.Inbound) (string, bool) {
	for _, p := range inbound.Parameters {
		if p.Name == "transport.vfs.MoveAfterFailure" {
			return p.Value, true
		}
	}
	return "", false
}

func getMoveAfterProcessParameterValue(inbound artifacts.Inbound) (string, bool) {
	for _, p := range inbound.Parameters {
		if p.Name == "transport.vfs.MoveAfterProcess" {
			return p.Value, true
		}
	}
	return "", false
}

func getFileNamePatternParameterValue(inbound artifacts.Inbound) (string, bool) {
	for _, p := range inbound.Parameters {
		if p.Name == "transport.vfs.FileNamePattern" {
			return p.Value, true
		}
	}
	return "", false
}

// Moves failed files from `test/in/` to `test/failed/`. test/failed/failed_files.txt contains the list of failed files.
func processFailedFiles(inDir, failedDir string) {
	// Path to failed_files.txt
	failedFilePath := filepath.Join(failedDir, "failed_files.txt")

	// Open failed_files.txt if it exists
	file, err := os.Open(failedFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			consolelogger.DebugLog("No failed_files.txt found, skipping.")
			return
		}
		consolelogger.ErrorLog(fmt.Sprintf("Error opening %s: %v", failedFilePath, err))
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		failedFile := scanner.Text()
		sourcePath := filepath.Join(inDir, failedFile)
		destPath := filepath.Join(failedDir, failedFile)

		// Move the failed file to the failed directory
		err := os.Rename(sourcePath, destPath)
		if err != nil {
			consolelogger.ErrorLog(fmt.Sprintf("Error moving %s to failed folder: %v", sourcePath, err))
		} else {
			consolelogger.DebugLog(fmt.Sprintf("Moved %s to failed folder\n", failedFile))
		}
	}

	// Handle scanning errors
	if err := scanner.Err(); err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("Error reading failed_files.txt: %v", err))
	}

	// Remove failed_files.txt after processing
	err = os.Remove(failedFilePath)
	if err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("Error deleting failed_files.txt: %v", err))
	}
}

// Scan a directory and return the list of files matching the given pattern
func scanDirectoryWithPattern(folderURI, pattern string) ([]string, error) {
	// Convert file URI to absolute file path
	folderPath, err := ConvertFileURIToPath(folderURI)
	if err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("Error converting file URI to path: %v", err))
		return nil, err
	}
	
	var files []string

	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			matched, err := filepath.Match(pattern, entry.Name()) // Match file pattern
			if err != nil {
				consolelogger.ErrorLog(fmt.Sprint("Error matching pattern %s: %v", pattern, err))
				continue
			}
			if matched {
				files = append(files, filepath.Join(folderPath, entry.Name()))
			}
		}
			files = append(files, filepath.Join(folderPath, entry.Name()))
	}
	return files, nil
}

// ConvertFileURIToPath converts a file:// URI to an absolute file path.
func ConvertFileURIToPath(fileURI string) (string, error) {
	// Parse the file URI
	parsedURI, err := url.Parse(fileURI)
	if err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("invalid file URI: %v", err))
		return "", err
	}

	// Ensure scheme is "file"
	if parsedURI.Scheme != "file" {
		consolelogger.ErrorLog(fmt.Sprintf("unsupported URI scheme: %s", parsedURI.Scheme))
		return "", fmt.Errorf("unsupported URI scheme: %s", parsedURI.Scheme)
	}

	// Get the file path and decode any URL encoding (e.g., spaces as `%20`)
	filePath := parsedURI.Path
	filePath = filepath.Clean(filePath)
	filePath = strings.ReplaceAll(filePath, "%20", " ") // Handle spaces

	return filePath, nil
}

// ReadFile reads a file, extracts metadata, locks it, and returns the extracted data.
func ReadFile(fileURI string) (*synapsecontext.SynapseContext, error) {
	// Convert file URI to absolute file path
	filePath, err := ConvertFileURIToPath(fileURI)
	if err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("Error converting file URI to path: %v", err))
		return nil, err
	}

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("Error opening file %s: %v", filePath, err))
		return nil, err
	}
	defer file.Close()

	// Lock the file to prevent modifications
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX) // Exclusive lock
	if err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("Error locking file %s: %v", filePath, err))
		return nil, err
	}

	// Get file metadata
	info, err := file.Stat()
	if err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("Error getting file info for %s: %v", filePath, err))
		return nil, err
	}

	// Extract metadata into ContextHeader
	header := ContextHeader{
		FILE_LENGTH:   float64(info.Size()),
		LAST_MODIFIED: float64(info.ModTime().Unix()), // Convert to Unix timestamp
		FILE_URI:      fileURI,                        // Keep original FILE_URI
		FILE_PATH:     filePath,                       // Derived file path
		FILE_NAME:     info.Name(),
	}

	properties := Properties{
		isInbound: true,
		ARTIFACT_NAME: "inboundendpointfile",
		inboundEndpointName: "file",
		ClientApiNonBlocking: true,
	}

	// Read file content
	content, err := io.ReadAll(file)
	if err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("Error reading file %s: %v", filePath, err))
		return &synapsecontext.SynapseContext{
			Properties: structToMap(properties),
			Message: synapsecontext.Message{
				RawPayload:  nil,
				ContentType: "text/plain",
			},
			Headers: structToMap(header),
		}, err
	}

	return &synapsecontext.SynapseContext{
		Properties: structToMap(properties),
		Message: synapsecontext.Message{
			RawPayload:  content,
			ContentType: "text/plain",
		},
		Headers: structToMap(header),

	}, nil
}

//In original code this is FileObject
type ExtractedFileDataFromFileAdapter struct {
	ContextHeader
	Context string
}

type ContextHeader struct {
	FILE_LENGTH float64
	LAST_MODIFIED float64
	FILE_URI string
	FILE_PATH string
	FILE_NAME string
}

func (f *FileInboundAdapter) ProcessFile(ctx context.Context,file string) {
	waitgroup := ctx.Value("waitGroup").(*sync.WaitGroup)
	defer waitgroup.Done()
	synapsecontext, err := ReadFile(file)
	if err != nil {
		consolelogger.ErrorLog(fmt.Sprintf("Error reading file %s: %v", file, err))
		return
	}
	
	// creating the mediation engine instance and mediating the sequence
	mediationEngine := mediationengine.GetMediationEngine()
	mediationEngine.MediateNamedSequence("inboundSeq", synapsecontext, ctx)

	//Attention : Here I implemented considering same reading go routine taking care the receiving results and it is needed to reconsider the design. Here the design is simpple but think the situation where some of the processed results of the previous iteration coming and there might be not enough threads.


}

// Function to convert struct to map[string]string
func structToMap(s interface{}) map[string]string {
	result := make(map[string]string)

	// Get the type and value of the struct
	v := reflect.ValueOf(s)
	t := reflect.TypeOf(s)

	// Iterate through struct fields
	for i := 0; i < v.NumField(); i++ {
		fieldName := t.Field(i).Name                       // Get field name
		fieldValue := fmt.Sprintf("%v", v.Field(i).Interface()) // Convert field value to string
		result[fieldName] = fieldValue
	}

	return result
}

type Properties struct {
	isInbound bool
	ARTIFACT_NAME string
	inboundEndpointName string
	ClientApiNonBlocking bool
}