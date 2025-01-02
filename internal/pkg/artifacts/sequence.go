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
	"encoding/xml"

	"github.com/apache/synapse-go/internal/pkg/synapsecontext"
)

// example : API insequence
type Sequence struct {
	MediatorList []Mediator
	LineNo       int
	FileName     string
}

// Actual sequence with dedicated XML file
type NamedSequence struct {
	XMLName      xml.Name `xml:"{http://ws.apache.org/ns/synapse}sequence"`
	Name         string   `xml:"name,attr"`
	MediatorList []Mediator
	FileName     string
}

func (v *Sequence) Execute(context *synapsecontext.SynapseContext) bool {
	for _, mediator := range v.MediatorList {
		result := mediator.Execute(context)
		if !result {
			return false
		}
	}
	return true
}

func (v *NamedSequence) Execute(context *synapsecontext.SynapseContext) bool {
	for _, mediator := range v.MediatorList {
		result := mediator.Execute(context)
		if !result {
			return false
		}
	}
	return true
}

func (v *Sequence) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	v.FileName = ""
	mediatorList, err := unmarshalMediators(d)
	if err != nil {
		return err
	}
	v.MediatorList = mediatorList
	return nil
}

func (v *NamedSequence) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	for _, attr := range start.Attr {
		if attr.Name.Local == "name" {
			v.Name = attr.Value
			break
		}
	}
	v.FileName = ""
	mediatorList, err := unmarshalMediators(d)
	if err != nil {
		return err
	}
	v.MediatorList = mediatorList
	return nil
}

func unmarshalMediators(d *xml.Decoder) ([]Mediator, error) {
	var mediatorList []Mediator

	for {
		t, err := d.Token()
		if err != nil {
			break
		}
		line, _ := d.InputPos()
		switch se := t.(type) {
		case xml.StartElement:
			var mediator Mediator
			switch se.Name.Local {
			case "log":
				logMediator := &LogMediator{}
				if err := d.DecodeElement(logMediator, &se); err != nil {
					return nil, err
				}
				logMediator.LineNo = line
				mediator = logMediator
			case "variable":
				variableMediator := &VariableMediator{}
				if err := d.DecodeElement(variableMediator, &se); err != nil {
					return nil, err
				}
				variableMediator.LineNo = line
				mediator = variableMediator
			case "respond":
				respondMediator := &RespondMediator{}
				if err := d.DecodeElement(respondMediator, &se); err != nil {
					return nil, err
				}
				respondMediator.LineNo = line
				mediator = respondMediator
			case "payloadFactory":
				payloadMediator := &PayloadMediator{}
				if err := d.DecodeElement(payloadMediator, &se); err != nil {
					return nil, err
				}
				payloadMediator.LineNo = line
				mediator = payloadMediator
			case "call":
				callMediator := &CallMediator{}
				if err := d.DecodeElement(callMediator, &se); err != nil {
					return nil, err
				}
				callMediator.LineNo = line
				mediator = callMediator
			}

			if mediator != nil {
				mediatorList = append(mediatorList, mediator)
			}
		}
	}
	return mediatorList, nil
}

// Adding the file name to the sequence and mediators - will be used for error logging
func (v *Sequence) SetFileName(fileName string) {
	setFileName(v.MediatorList, fileName)
	v.FileName = fileName
}

func (v *NamedSequence) SetFileName(fileName string) {
	setFileName(v.MediatorList, fileName)
	v.FileName = fileName
}

func setFileName(mediators []Mediator, fileName string) {
	for _, mediator := range mediators {
		mediator.SetFileName(fileName)
	}
}
