package cash

import (
	"log"
	"net/http"
	"os"
)

func StartCashService() {
	mux := http.NewServeMux()
	mux.HandleFunc("/cash/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Cash Service"))
	})
	log.Println("Cash Service started on :6143")
	port := os.Getenv("PORT")
	if port == "" {
		port = "6143" // fallback for local dev
	}
	log.Printf("Cash Service started on :%s", port)
	err := http.ListenAndServe(":"+port, mux)

	if err != nil {
		log.Fatalf("Cash Service failed: %v", err)
	}
}
