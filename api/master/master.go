package master

import (
	"database/sql"
	"log"
	"net/http"
)

func StartMasterService(db *sql.DB) {
	mux := http.NewServeMux()
	mux.HandleFunc("/master/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Master Service"))
	})
	log.Println("Master Service started on :2143")
	err := http.ListenAndServe(":2143", mux)
	if err != nil {
		log.Fatalf("Master Service failed: %v", err)
	}
}
