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
	"net/http"

	"github.com/apache/synapse-go/internal/pkg/synapsecontext"
)

type Resource struct {
	Methods       string   `xml:"methods,attr"`
	URITemplate   string   `xml:"uri-template,attr"`
	InSequence    Sequence `xml:"inSequence"`
	FaultSequence Sequence `xml:"faultSequence"`
}

type API struct {
	Context   string     `xml:"context,attr"`
	Name      string     `xml:"name,attr"`
	Resources []Resource `xml:"resource"`
	FileName  string
}

func (resource *Resource) DispatchResource(w http.ResponseWriter, request *http.Request) {
	// Read transport headers
	var headers = make(map[string]string)
	for name, values := range request.Header {
		headers[name] = values[0]
	}

	// Creating the mssage context
	var context = synapsecontext.SynapseContext{
		Properties: make(map[string]string),
		Message:    synapsecontext.Message{},
		Headers:    headers,
	}

	// Execute the in-sequence
	resource.InSequence.Execute(&context)
}
