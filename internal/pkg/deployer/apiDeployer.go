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

package deployer

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/synapse-go/internal/pkg/artifacts"
	"github.com/apache/synapse-go/internal/pkg/consolelogger"
	"github.com/apache/synapse-go/internal/pkg/dispatcher"
)

func DeployAPIs(folderPath string, router *dispatcher.Router, confContext *artifacts.ArtifactInfo) {

	files, err := os.ReadDir(folderPath)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	if len(files) == 0 {
		consolelogger.DebugLog("No APIs found. Add your APIs to the artifacts/APIs folder")
	}

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".xml" {
			continue
		}
		xmlFile, err := os.Open(filepath.Join(folderPath, file.Name()))
		if err != nil {
			fmt.Println("Error opening file:", err)
			continue
		}
		defer xmlFile.Close()

		data, err := io.ReadAll(xmlFile)
		if err != nil {
			fmt.Println("Error reading file:", err)
			continue
		}

		consolelogger.InfoLog("Deploying API from " + file.Name())

		var api artifacts.API
		api.FileName = file.Name()
		if err := xml.Unmarshal(data, &api); err != nil {
			fmt.Println("Error unmarshaling XML:", err)
			continue
		}

		// check the api already deployed
		apiAlreadyDeployed := false
		_, ok := confContext.ApiMap[api.Name]
		if ok {
			consolelogger.ErrorLog("API " + api.Name + " already deployed")
			continue
		}
		for _, deployedAPI := range confContext.ApiMap {
			if deployedAPI.Context == api.Context {
				consolelogger.ErrorLog("API " + deployedAPI.Name + " already deployed with same context : " + api.Context)
				apiAlreadyDeployed = true
			}
		}
		if apiAlreadyDeployed {
			continue
		}

		// process the API
		for _, resource := range api.Resources {
			router.AddRoute(resource.Methods, api.Context+resource.URITemplate, resource.DispatchResource)
			resource.InSequence.SetFileName(api.FileName)
		}
		confContext.AddAPI(api)
		consolelogger.InfoLog("API " + api.Name + " deployed successfully")
	}
}
