package dash

import (
	"log"
	"net/http"
	"os"
)

func StartDashService() {
	mux := http.NewServeMux()
	mux.HandleFunc("/dash/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello from Dashboard Service"))
	})
	log.Println("Dashboard Service started on :4143")
	port := os.Getenv("PORT")
	if port == "" {
		port = "4143" // fallback for local dev
	}
	log.Printf("Dashboard Service started on :%s", port)
	err := http.ListenAndServe(":"+port, mux)

	if err != nil {
		log.Fatalf("Dashboard Service failed: %v", err)
	}
}
