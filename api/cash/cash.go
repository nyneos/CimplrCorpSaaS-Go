package cash

import (
	"database/sql"
	"log"
	"net/http"
)

func StartCashService(db *sql.DB) {
	mux := http.NewServeMux()
	mux.HandleFunc("/cash/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Cash Service"))
	})
		log.Println("Cash Service started on :6143")
		err := http.ListenAndServe(":6143", mux)
	if err != nil {
		log.Fatalf("Cash Service failed: %v", err)
	}
}
