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

package ports

import (
	"context"

	"github.com/apache/synapse-go/internal/pkg/artifacts"
	"github.com/apache/synapse-go/internal/pkg/synapsecontext"
)

type FileInboundEndpoint interface {
	GetInstance(inbound artifacts.Inbound) (FileInboundEndpoint, error)
	InitPooling(ctx context.Context) error
}

type InboudReceiver interface {
	ReceiveMessage(context synapsecontext.SynapseContext, inbound string, hasError bool) error
}

type InboundAdapter interface {
	Poll(ctx context.Context) error
	RecieveMessage(ctx context.Context, func ReceiveMessageFunc) error
}

type ReceiveMessageFunc func(context synapsecontext.SynapseContext) error


adapt := File{
	map[string]ReceiveMessageFunc
}

addaptor.register(ctx, func (context synapsecontext.SynapseContext) error {
	// do something
})

addaptor.register(ctx, func (context synapsecontext.SynapseContext) error {
	// do something
})

addaptor.register(ctx, func (context synapsecontext.SynapseContext) error {
	// do something
})

//

adapt.Poll(ctx)


// Use channels to communicate between the adapter and the receiver

type InboundAdapter interface {
	Poll(ctx context.Context) error
	RecieveMessage(ctx context.Context) (<-chan synapsecontext.SynapseContext, error)
}



adaptor := FileInboundAdapter{}

go func() {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			adaptor.Poll(ctx)
		}
	}
}()

channel, err := adaptor.RecieveMessage(ctx)
if err != nil {
	return err
}

for {
	select {
	case message := <-channel:
		// do something with the message
	}
}

