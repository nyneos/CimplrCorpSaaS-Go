# Go Backend For CimplrCorpSaaS

## Manifest Generation & Verification

Below are platform-specific commands for owners (to generate and sign manifests) and clients (to verify manifests and files).

---

### Windows

**OWNER — Generate & Sign manifest**
```powershell
.\release\service-g-windows-amd64.exe -root . -out manifest.json -privkey private.pem -exclude "*.pem,*.log,*.env,*.exe,README.md,.gitignore"
```

**CLIENT — Verify manifest and files**
```powershell
.\release\service-v-windows-amd64.exe -manifest manifest.json -root .
```

---

### Linux

**OWNER — Generate & Sign manifest**
```bash
./release/service-g-linux-amd64 -root . -out manifest.json -privkey private.pem -exclude '*.pem,*.log,*.env,*.exe,README.md,.gitignore'
```

**CLIENT — Make verifier executable (one-time)**
```bash
chmod +x release/service-v-linux-amd64
```

**CLIENT — Verify manifest and files**
```bash
./release/service-v-linux-amd64 -manifest manifest.json -root .
```

---

### macOS (amd64)

**OWNER — Generate & Sign manifest**
```bash
./release/service-g-darwin-amd64 -root . -out manifest.json -privkey private.pem -exclude '*.pem,*.log,*.env,*.exe,README.md,.gitignore'
```

**CLIENT — Make verifier executable (one-time)**
```bash
chmod +x release/service-v-darwin-amd64
```

**CLIENT — Verify manifest and files**
```bash
./release/service-v-darwin-amd64 -manifest manifest.json -root .
```

---

### macOS (arm64)

**OWNER — Generate & Sign manifest**
```bash
./release/service-g-darwin-arm64 -root . -out manifest.json -privkey private.pem -exclude '*.pem,*.log,*.env,*.exe,README.md,.gitignore'
```

**CLIENT — Make verifier executable (one-time)**
```bash
chmod +x release/service-v-darwin-arm64
```

**CLIENT — Verify manifest and files**
```bash
./release/service-v-darwin-arm64 -manifest manifest.json -root .
```

---

### Linux — Generate, Sign, and Create Snapshot (example)

**OWNER — Generate, Sign, and create snapshot**
```bash
./release/service-g-linux-amd64 -root . -out manifest.json -snapshot snapshot.zip -privkey private.pem -exclude '*.pem,*.log,*.env,*.exe,README.md,.gitignore'
```