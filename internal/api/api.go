package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	cl "github.com/razvanmarinn/rcss/internal/client"
)

var rcssClient *cl.RCSSClient

func main() {
	// Initialize router and client
	router := mux.NewRouter()
	rcssClient = cl.NewRCSSClient()

	// Define API endpoints
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Hello, World!")
	})
	router.HandleFunc("/file/{fileName}", getFileFromMaster).Methods("GET")
	router.HandleFunc("/file/{fileName}", setFileToMaster).Methods("POST")

	// Start the server
	log.Println("API is running on port 8000...")
	http.ListenAndServe(":8000", router)
}

// getFileFromMaster handles the GET request to retrieve files from workers
func getFileFromMaster(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileName := vars["fileName"]

	if fileName == "" {
		http.Error(w, "File name is required", http.StatusBadRequest)
		return
	}

	// Retrieve file data from workers via the client
	fileContent, err := rcssClient.GetFileBackFromWorkers(fileName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error retrieving file: %v", err), http.StatusInternalServerError)
		return
	}

	// Set the response headers and send the file content as an attachment
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileName))

	_, err = w.Write(fileContent)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error writing response: %v", err), http.StatusInternalServerError)
		return
	}
}

// setFileToMaster handles the POST request to upload files to the master
func setFileToMaster(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fileName := vars["fileName"]

	if fileName == "" {
		http.Error(w, "File name is required", http.StatusBadRequest)
		return
	}

	// Retrieve the file from the form data
	file, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting file: %v", err), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Read the file content
	fileContent, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading file: %v", err), http.StatusInternalServerError)
		return
	}

	// Use the client to process and send the file to the master
	err = rcssClient.ProcessFileToMaster(fileName, fileContent)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error processing file: %v", err), http.StatusInternalServerError)
		return
	}

	// Respond with success
	fmt.Fprintf(w, "File %s uploaded successfully", fileName)
}
