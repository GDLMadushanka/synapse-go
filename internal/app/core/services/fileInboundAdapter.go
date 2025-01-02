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
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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
		var fileContent = "Hello World"
		interval, found := getIntervalParameterValue(adapter.inbound)
		if found {
			consolelogger.InfoLog(fmt.Sprintf("Polling file every %d milliseconds", interval))
		} else {
			consolelogger.ErrorLog("Interval parameter not found")
			return errors.New("interval parameter not found")
		}

		for {
			select {
			case <-ctx.Done():
				waitgroup.Done()
				fmt.Println("Cleaning up file polling gracefully")
				consolelogger.InfoLog("Cleaning up file polling gracefully")
				return nil
			default:
				consolelogger.DebugLog("Polling file")
				// Creating the new message context from file content
				var context = synapsecontext.SynapseContext{
					Properties: make(map[string]string),
					Message: synapsecontext.Message{
						RawPayload:  []byte(fileContent),
						ContentType: "text/plain",
					},
					Headers: make(map[string]string),
				}
				// creating the mediation engine instance and mediating the sequence
				mediationEngine := mediationengine.GetMediationEngine()
				mediationEngine.MediateNamedSequence("inboundSeq", &context, ctx)
			}
			time.Sleep(time.Duration(interval) * time.Millisecond)
		}
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
