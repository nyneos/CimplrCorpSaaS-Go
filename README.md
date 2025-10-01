Go Backend For CimplrCorpSaas


# OWNER (Windows) — Generate & Sign manifest
.\release\service-g-windows-amd64.exe -root . -out manifest.json -privkey private.pem -exclude "*.pem,*.log,*.env,*.exe,README.md,.gitignore"

# CLIENT (Windows) — Verify manifest and files
.\release\service-v-windows-amd64.exe -manifest manifest.json -root .

# OWNER (Linux) — Generate & Sign manifest
./release/service-g-linux-amd64 -root . -out manifest.json -privkey private.pem -exclude '*.pem,*.log,*.env,*.exe,README.md,.gitignore'

# CLIENT (Linux) — Make verifier executable (one-time)
chmod +x release/service-v-linux-amd64

# CLIENT (Linux) — Verify manifest and files
./release/service-v-linux-amd64 -manifest manifest.json -root .

# OWNER (macOS amd64) — Generate & Sign manifest
./release/service-g-darwin-amd64 -root . -out manifest.json -privkey private.pem -exclude '*.pem,*.log,*.env,*.exe,README.md,.gitignore'

# CLIENT (macOS amd64) — Make verifier executable (one-time)
chmod +x release/service-v-darwin-amd64

# CLIENT (macOS amd64) — Verify manifest and files
./release/service-v-darwin-amd64 -manifest manifest.json -root .

# OWNER (macOS arm64) — Generate & Sign manifest
./release/service-g-darwin-arm64 -root . -out manifest.json -privkey private.pem -exclude '*.pem,*.log,*.env,*.exe,README.md,.gitignore'

# CLIENT (macOS arm64) — Make verifier executable (one-time)
chmod +x release/service-v-darwin-arm64

# CLIENT (macOS arm64) — Verify manifest and files
./release/service-v-darwin-arm64 -manifest manifest.json -root .

# OWNER (Linux) — Generate, Sign, and create snapshot (example)
./release/service-g-linux-amd64 -root . -out manifest.json -snapshot snapshot.zip -privkey private.pem -exclude '*.pem,*.log,*.env,*.exe,README.md,.gitignore'
