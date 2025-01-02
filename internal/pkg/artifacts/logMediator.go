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
package artifacts

import (
	"github.com/apache/synapse-go/internal/pkg/consolelogger"
	"github.com/apache/synapse-go/internal/pkg/synapsecontext"
)

type LogMediator struct {
	Category string `xml:"category,attr"`
	Message  string `xml:"message"`
	LineNo   int
	FileName string
}

func (l *LogMediator) Execute(context *synapsecontext.SynapseContext) bool {
	switch l.Category {
	case "DEBUG":
		go consolelogger.DebugLog(l.Message)
	case "INFO":
		go consolelogger.InfoLog(l.Message)
	default:
		go consolelogger.MediatorErrorLog("Log", l.FileName, l.LineNo, "Invalid log category")
	}
	return true
}

func (l *LogMediator) SetFileName(fileName string) {
	l.FileName = fileName
}
