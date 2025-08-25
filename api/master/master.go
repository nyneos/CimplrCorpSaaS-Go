package master

import (
	"log"
	"net/http"
	"os"
)

func StartMasterService() {
	mux := http.NewServeMux()
	mux.HandleFunc("/master/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Master Service"))
	})
	log.Println("Master Service started on :2143")
	port := os.Getenv("PORT")
	if port == "" {
		port = "2143" // fallback for local dev
	}
	log.Printf("Master Service started on :%s", port)
	err := http.ListenAndServe(":"+port, mux)

	if err != nil {
		log.Fatalf("Master Service failed: %v", err)
	}
}
