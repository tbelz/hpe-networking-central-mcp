# Speaker Notes — MCP Server for HPE Aruba Networking Central

> HP Networking Days 2026 | ~20 minutes | Slides in English, spoken in German

---

## Pre-Talk Checklist

- [ ] Claude Desktop running with MCP server connected
- [ ] Demo environment instance verified (read-only)
- [ ] Lab environment instance verified (mutating)
- [ ] Switch between Central instances tested
- [ ] Backup screenshots taken for each demo
- [ ] Timer visible (aim for 7 min talk + 10 min demos + 3 min closing)

---

## Slide 1: Title

**Timing:** 30 seconds

- Introduce yourself briefly
- "Mein Name ist Till Belz, ich bin Systems Engineer bei HPE"
- "Heute zeige ich euch, wie wir eine API-first Netzwerk-Plattform zu einem
  KI-gesteuerten Operations-Tool machen können — mit dem Model Context Protocol"
- Referenz auf Kollegen: "Wie [Kollege] gerade erklärt hat, ist MCP ein offenes
  Protokoll, das LLMs mit externen Tools verbindet. Ich zeige jetzt die
  konkrete Implementierung für Aruba Central."

---

## Slide 2: Aruba Central — Configuration Model

**Timing:** 90 seconds

**Key points:**
- "Aruba Central hat ein 5-stufiges Konfigurationsmodell"
- Config wird von oben nach unten vererbt
- Die spezifischste Ebene gewinnt — Device überschreibt alles
- DeviceGroups schneiden quer über Sites — das ist ein Cross-Cutting Concern
- "Das ist wichtig, weil unser MCP Server dieses Modell in einer Graph-Datenbank
  abbildet, und der LLM muss den Blast Radius von Config-Änderungen verstehen können"
- Verweis: "Für Details zu Central empfehle ich den Vortrag von [Kollege]:
  'Die neue Aruba Central: Praxis-Check der wichtigsten Features'"

---

## Slide 3: An API-First Platform

**Timing:** 60 seconds

**Key points:**
- "Aruba Central verfolgt eine API-First-Strategie"
- 2100+ Endpoints über 50 API-Kategorien (Central + GreenLake)
- Vollständige OpenAPI-Spezifikationen auf developer.arubanetworks.com
- Monitoring, Konfiguration, Alerting, Firmware, Topologie, Troubleshooting...
- "Das ist die Grundlage, die den MCP Server überhaupt möglich macht"
- Key message: "Alles ist eine API. Und wenn es eine API ist, ist es AI-ready."

---

## Slide 4: The Challenge

**Timing:** 60 seconds

**Key points:**
- "Bei anderen Plattformen mit 20-30 APIs kann man 1:1 mappen — ein Endpoint,
  ein Tool. Bei 2100 Endpoints geht das nicht."
- Naiver Ansatz: Jeder Endpoint wird ein MCP Tool → Millionen von Tokens allein
  für die Tool-Definitionen → sprengt jedes Context Window
- Auch "häufige Endpoints vorladen" ist fragil und unvollständig
- "Wir brauchen einen anderen Ansatz — das Invocation Pattern"

---

## Slide 5: The Invocation Pattern

**Timing:** 90 seconds (IMPORTANT — core concept)

**Key points:**
- Drei Phasen: Search → Discover → Execute
- **Search:** unified_search("vlan") durchsucht einen BM25 Full-Text Index
  über alle 2100+ Endpoints. Liefert eine gerankte Liste: method, path, summary.
- **Discover:** get_api_endpoint_detail() holt das vollständige OpenAPI Schema —
  Parameter, Request Body, Response Format. Jetzt weiß der LLM genau, wie er
  den Endpoint aufrufen muss.
- **Execute:** call_central_api() mit den richtigen Parametern.
- "Der LLM lernt die API on-the-fly — keine vorgeladenen Schemas nötig."
- "Drei generische Tools ersetzen, was ansonsten 2100 einzelne Tools wären."
- Der Suchindex wird beim Start aus den gescrapten OpenAPI-Specs aufgebaut.

---

## Slide 6: Architecture Overview

**Timing:** 2 minutes

**Walk through left to right:**

1. **Links:** Beliebiger MCP-kompatibler Client — Claude Desktop, VS Code, Cursor, etc.
   Standard MCP Protokoll über stdio oder SSE.

2. **Mitte — der MCP Server** hat drei interne Layer:
   - *Knowledge Layer:* API Katalog mit BM25-Suchindex, gebaut aus gescrapten
     OpenAPI-Specs (Central + GreenLake). Enthält auch Docs und Script-Metadaten.
   - *Domain Layer:* Eingebettete Graph-Datenbank (Kùzu) — bildet die Netzwerk-
     Topologie ab: Org → Sites → Devices, LLDP-Nachbarschaft, Device Groups,
     Config Scopes. Automatisch befüllt beim Start durch "Seed Scripts".
   - *Script Engine:* Sandboxed Python-Ausführung für mehrstufige Workflows.
     Scripts bekommen vor-authentifizierte API-Helper — kein OAuth-Boilerplate nötig.

3. **Rechts:** Die echten Aruba Central und GreenLake APIs. Authentifizierung
   (OAuth2 Client Credentials) wird komplett vom Server gehandled. Der LLM sieht
   keine Credentials.

---

## Slide 7: Scripts: Multi-Step Workflows

**Timing:** 60 seconds

**Key points:**
- Einfache Reads: call_central_api() reicht (ein Tool-Call)
- Mehrstufige Workflows: LLM schreibt ein Python-Script → save → execute
- `from central_helpers import api` — vor-authentifiziert, keine OAuth-Logik nötig
- api.paginate() erkennt automatisch Cursor vs. Offset Pagination
- Seed Scripts laufen beim Start: befüllen den Graph mit Live-Daten
- LLM kann NEUE Scripts on-the-fly erstellen für individuelle Workflows
- Scripts sind sandboxed: 5 Minuten Timeout, Output-Truncation, keine Credential-Leaks

---

## Slide 8: The Graph Database

**Timing:** 60 seconds

**Key points:**
- Eingebettete Datenbank (Kùzu) — kein externer Service nötig
- Seeds befüllen automatisch beim Start aus den Live-APIs
- LLM nutzt query_graph() für Topologie-Navigation
- "Wie viele Devices pro Site?" → eine Cypher-Abfrage statt mehrerer API-Calls
- LLDP-Topologie: welcher Switch-Port ist mit welchem verbunden
- CONNECTED_TO Edges tragen Metadaten: Port-Namen, Speed, LAG, STP-Status
- Ermöglicht Blast-Radius-Analyse: "Was ist betroffen, wenn ich diese Config ändere?"
- "Der Graph ist eine STRUKTURELLE KARTE — Live-Betriebsdaten kommen weiterhin via API"

---

## Slide 9: 11 Tools, 4 Categories

**Timing:** 45 seconds

Quick walkthrough:
1. **Discovery:** Wie der LLM die richtige API findet (BM25, Schema)
2. **Direct API:** Die eigentlichen API-Calls (Central + GreenLake)
3. **Graph:** Topologie-Navigation via Cypher
4. **Scripts:** Mehrstufige Automatisierung

"11 Tools geben Zugang zur gesamten Plattform. Das Invocation Pattern heißt:
diese 11 Tools ersetzen, was ansonsten 2100+ einzelne Tools wären."

---

## Slide 10: Demo Overview

**Timing:** 30 seconds

- Zwei Umgebungen: Demo-Environment (read-only) und Lab (mutierbar)
- Demo 1 & 2: Demo-Environment — nur lesend
- Demo 3: Lab — wird tatsächlich eine VLAN Config erstellen
- Alle Demos mit Claude Desktop
- "Ich wechsle jetzt zu Claude Desktop..."

---

## Slide 11: Demo 1 — Root Cause Analysis

**Timing:** ~4 minutes live

**Prompt to type:**
```
Show me devices with problems in the network and diagnose the root causes.
Start by checking the topology graph for device health, then look up alerts
for any unhealthy devices.
```

**Expected agent flow:**
1. `query_graph()` — Alle Devices + Status
2. Findet Devices mit Status != "Up"
3. `unified_search("alerts")` — Alerting API finden
4. `get_api_endpoint_detail()` - Schema lesen
5. `call_central_api()` — Aktive Alerts abrufen
6. LLDP Neighbors checken — sind Upstream-Devices betroffen?
7. Root Cause Diagnose

**Talking points while agent works:**
- "Seht ihr, der Agent startet mit dem Graph — er kennt die Topologie"
- "Jetzt sucht er die richtige API — das ist das Invocation Pattern in Aktion"
- "Er korreliert Daten aus verschiedenen Quellen automatisch"

**Fallback:** Wenn Live-Demo fehlschlägt, den erwarteten Flow schildern.

---

## Slide 12: Demo 2 — Firmware Compliance

**Timing:** ~3 minutes live

**Prompt to type:**
```
Which devices are running outdated firmware? Compare the current firmware
versions against the recommended versions and give me a compliance report
by site.
```

**Expected agent flow:**
1. `unified_search("firmware")` — Firmware-APIs finden
2. Firmware Compliance Endpoint entdecken
3. Schema lesen, API aufrufen
4. Graph für Device-Gruppierung nach Site abfragen
5. Compliance Report: Welche Devices brauchen Updates?

**Key demonstration:** Das Invocation Pattern in reiner Form (Search → Discover → Call)

---

## Slide 13: Demo 3 — VLAN Configuration (Mutating)

**Timing:** ~3 minutes live

**⚠️ SWITCH TO LAB ENVIRONMENT FIRST**

**Prompt to type:**
```
Create VLAN 100 on switch [name], assign it to port 1/1/5.
Make sure that trunk ports also carry it. After creating the config,
verify that it was successfully pushed to the device.
```

**Expected agent flow:**
1. Config Profile APIs suchen
2. VLAN Config Profile via POST erstellen
3. Profile dem Device Scope zuweisen
4. Push-Status prüfen
5. `/effective` mit `detailed=true` prüfen → zeigt Config-Provenance

**Key demonstration:**
- Write Operations, nicht nur Reads
- Config Profile Modell: Create → Assign → Push → Verify
- effective=true&detailed=true zeigt, von welchem Scope die Config kommt

---

## Slide 14: Important (Disclaimer)

**Timing:** 60 seconds

- "Das ist KEIN offizielles HPE-Produkt — es ist ein persönliches Projekt"
- Open Source auf GitHub
- Zeigt die Möglichkeiten der API-First-Strategie
- Für den Produktivbetrieb empfehle ich dringend:
  - Nur GET-Requests zulassen (lesend)
  - Voller API-Zugriff = voller Blast Radius
  - Primär für Monitoring, Diagnostik, Analytics
  - Mutations nur in kontrollierter Lab-Umgebung
- "Mit großem API-Zugriff kommt große Verantwortung"

---

## Slide 15: Try It Out

**Timing:** 30 seconds

- GitHub URL zeigen/nennen
- Docker als einfachster Einstieg
- Funktioniert mit Claude Desktop, VS Code Copilot, Cursor, etc.
- Braucht Aruba Central API Credentials (client_id + client_secret)
- "Ich teile den Link — probiert es mit eurem eigenen Environment aus"

---

## Slide 16: Key Takeaway

**Timing:** 30 seconds

Drive home the message:
- "Die API-First-Strategie von Aruba Central ist nicht nur für Automatisierungs-Scripts"
- "Sie macht die Plattform ready für die KI-Ära"
- "Der MCP Server ist nur eine Brücke — die echte Power kommt von den APIs"
- "Jede Plattform mit umfassenden APIs kann so AI-enabled werden"

---

## Slide 17: Q&A

**Common questions and answers:**

**"Wie geht ihr mit Rate Limiting um?"**
→ Automatisches Retry mit Retry-After Header Parsing, max 60s Wartezeit.

**"Wie sicher ist das?"**
→ OAuth2 Client Credentials, Credentials werden nie dem LLM angezeigt,
Scripts in Sandbox mit Timeout, Path Traversal Protection.

**"Funktioniert das mit anderen Netzwerk-Plattformen?"**
→ Das Invocation Pattern funktioniert für jede Plattform mit OpenAPI Specs.
Man müsste den Spec-Scraper und API-Client austauschen.

**"Wie genau ist der LLM?"**
→ Sehr gut für Monitoring und Diagnostik. Bei Mutations immer prüfen,
was der Agent plant.

**"Performance?"**
→ Graph-Queries sind instant (<10ms). API-Calls hängen von Centrals
Response Time ab. Script-Ausführung hat 5 Minuten Timeout.

---

## Timing Summary

| Section | Slides | Target Time |
|---------|--------|-------------|
| Title + Context | 1–3 | 3 min |
| Architecture | 4–9 | 5 min |
| Demo Intro | 10 | 0.5 min |
| Demo 1: Root Cause | 11 | 4 min |
| Demo 2: Firmware | 12 | 3 min |
| Demo 3: VLAN | 13 | 3 min |
| Closing | 14–17 | 2.5 min |
| **Total** | **17** | **~21 min** |

If running long: cut Demo 2 (Firmware) short or skip it entirely.
If running short: elaborate on architecture or take more questions.
