// ============================================================================
// HPE Aruba Networking Central — MCP Server Presentation
// HP Networking Days 2026
// ============================================================================

#import "@preview/touying:0.7.1": *
#import themes.simple: *

// -- HPE brand colors --------------------------------------------------------
#let hpe-green    = rgb("#01A982")
#let hpe-dark     = rgb("#0D5265")
#let hpe-blue     = rgb("#00739D")
#let hpe-gray     = rgb("#425563")
#let hpe-light    = rgb("#F2F2F2")
#let hpe-accent   = rgb("#FF8D6D")

// -- Theme setup -------------------------------------------------------------
#show: simple-theme.with(
  aspect-ratio: "16-9",
  config-info(
    title: [MCP Server for\ HPE Aruba Networking Central],
    subtitle: [Bridging 3 000+ APIs with AI],
    author: [Till Belz],
    date: [HP Networking Days — 2026],
    institution: [Hewlett Packard Enterprise],
  ),
  config-colors(
    primary: hpe-green,
    secondary: hpe-dark,
    tertiary: hpe-light,
    neutral-lightest: white,
    neutral-darkest: hpe-dark,
  ),
  config-common(
    // show-notes-on-second-screen: right,
  ),
)

// -- Utility helpers ---------------------------------------------------------
#let accent(body) = text(fill: hpe-green, weight: "bold", body)
#let code-block(body) = block(
  fill: hpe-light,
  inset: 12pt,
  radius: 6pt,
  width: 100%,
  text(font: "DejaVu Sans Mono", size: 0.85em, body),
)
#let note(body) = text(fill: hpe-gray, size: 0.8em, style: "italic", body)
#let highlight-box(body) = block(
  fill: hpe-green.lighten(85%),
  stroke: 2pt + hpe-green,
  inset: 16pt,
  radius: 8pt,
  width: 100%,
  body,
)
#let dim-box(title, body) = block(
  width: 100%,
  inset: 12pt,
  radius: 6pt,
  fill: hpe-light,
  [
    #text(weight: "bold", fill: hpe-dark, title) \
    #body
  ],
)

// ============================================================================
// TITLE SLIDE
// ============================================================================

#title-slide[
  = MCP Server for HPE Aruba Networking Central

  #v(1em)
  Bridging 3 000+ APIs with AI

  #v(2em)
  Till Belz #h(2em) HP Networking Days 2026
]

#speaker-note[
  Introduce yourself briefly.
  "My name is Till Belz, I'm a Systems Engineer at HPE, and today I will show
  you how we can turn an API-first networking platform into an AI-powered
  operations tool using the Model Context Protocol."
  Mention that the colleague has already covered MCP fundamentals — tools,
  resources, prompts, and the general protocol architecture. Your part focuses
  on the concrete implementation for Aruba Central.
]

// ============================================================================
// SECTION 1: CONTEXT
// ============================================================================

= Context

// ---------- Slide: Aruba Central in 60s -------------------------------------
== Aruba Central — Configuration Model

#grid(
  columns: (1fr, 1fr),
  gutter: 2em,
  [
    #text(size: 1.1em)[*Five-level configuration hierarchy*]

    Config propagates *top-down*. \
    Most specific scope wins.

    #v(1em)
    #highlight-box[
      *Precedence (highest → lowest):* \
      Device › DeviceGroup › Site › SiteCollection › Global
    ]
  ],
  [
    // Hierarchy diagram
    #align(center)[
      #stack(
        dir: ttb,
        spacing: 4pt,
        block(width: 70%, fill: hpe-dark, inset: 10pt, radius: 4pt,
          align(center, text(fill: white, weight: "bold", size: 0.85em)[Global (Org)])),
        align(center, text(size: 1.5em, fill: hpe-gray)[↓]),
        block(width: 70%, fill: hpe-dark.lighten(20%), inset: 10pt, radius: 4pt,
          align(center, text(fill: white, weight: "bold", size: 0.85em)[SiteCollection])),
        align(center, text(size: 1.5em, fill: hpe-gray)[↓]),
        block(width: 70%, fill: hpe-blue, inset: 10pt, radius: 4pt,
          align(center, text(fill: white, weight: "bold", size: 0.85em)[Site])),
        align(center, text(size: 1.5em, fill: hpe-gray)[↓]),
        block(width: 70%, fill: hpe-green, inset: 10pt, radius: 4pt,
          align(center, text(fill: white, weight: "bold", size: 0.85em)[DeviceGroup])),
        align(center, text(size: 1.5em, fill: hpe-gray)[↓]),
        block(width: 70%, fill: hpe-accent, inset: 10pt, radius: 4pt,
          align(center, text(fill: white, weight: "bold", size: 0.85em)[Device])),
      )
    ]
  ],
)

#note[
  → Deep dive: "Die neue Aruba Central: Praxis-Check der wichtigsten Features"
]

#speaker-note[
  Keep this to 90 seconds max. The audience knows Central but may not be
  familiar with the 5-layer model.

  Key points:
  - Configuration profiles can be assigned at any of the 5 scopes
  - Config is inherited top-down, most specific scope wins
  - DeviceGroups cut across sites — this is a cross-cutting concern
  - This hierarchy is important because the MCP server's graph models it,
    and the LLM needs to understand blast radius of any config change
  - Point to colleague's talk for deeper Central coverage
]


// ---------- Slide: API-First Platform ---------------------------------------
== An API-First Platform

#v(1em)

#grid(
  columns: (1fr, 1fr, 1fr),
  gutter: 1.5em,
  dim-box[~1 500 Central API endpoints][
    Monitoring, Configuration, \
    Alerting, Firmware, Topology, \
    Troubleshooting, ...
  ],
  dim-box[~60 GreenLake endpoints][
    Device onboarding, \
    Subscriptions, Licensing, \
    Locations, Identity, ...
  ],
  dim-box[40+ API categories][
    Full OpenAPI specs available \
    on developer.arubanetworks.com
  ],
)

#v(2em)

#align(center)[
  #text(size: 1.4em, weight: "bold", fill: hpe-green)[
    Everything is an API. If it's an API, it's AI-ready.
  ]
]

#speaker-note[
  ~60 seconds. Emphasize:
  - Aruba Central follows an API-first strategy — every feature is accessible
    via REST APIs
  - Full OpenAPI specifications are publicly documented
  - Both the Central platform AND the GreenLake platform have APIs
  - This is the foundation that makes the MCP server possible — you can't
    build AI tooling on top of a platform that doesn't have APIs
  - "If your platform has APIs, you already have an AI-ready platform —
    you just need to bridge the gap."
]


// ============================================================================
// SECTION 2: DESIGN & ARCHITECTURE
// ============================================================================

= Design & Architecture

// ---------- Slide: The Challenge --------------------------------------------
== The Challenge

#v(1em)

#grid(
  columns: (1fr, auto, 1fr),
  gutter: 2em,
  [
    #dim-box[Naïve approach: 1:1 mapping][
      1 API endpoint = 1 MCP tool \
      \
      ~1 500 tools × full schemas \
      = *millions of tokens* \
      \
      #text(fill: red, weight: "bold")[⛔ Context window explosion]
    ]
  ],
  align(horizon, text(size: 3em, fill: hpe-gray)[→]),
  [
    #block(
      fill: hpe-green.lighten(85%),
      stroke: 2pt + hpe-green,
      inset: 16pt,
      radius: 8pt,
      width: 100%,
    )[
      *Our approach: Invocation Pattern* \
      \
      3 generic tools + search index \
      = *all endpoints accessible* \
      \
      #text(fill: hpe-green, weight: "bold")[✓ Fits any context window]
    ]
  ],
)

#speaker-note[
  ~60 seconds. This is the core design insight.

  - Other MCP servers for simple platforms can map 1 API = 1 tool
  - With 1500+ endpoints, that's impossible — the tool list alone would
    exceed the LLM's context window
  - Even pre-loading schemas for "likely" endpoints is wasteful and fragile
  - The invocation pattern solves this: instead of pre-loading, the LLM
    *discovers* the right API at runtime, reads its schema, then calls it
  - Only 3 tools needed: search, detail, call — universal gateway to all APIs
]


// ---------- Slide: Invocation Pattern Detail --------------------------------
== The Invocation Pattern

#v(0.5em)

#grid(
  columns: (1fr, 0.3fr, 1fr, 0.3fr, 1fr),
  gutter: 0.5em,
  align: horizon,
  // Step 1
  block(
    fill: hpe-dark,
    inset: 14pt,
    radius: 8pt,
    width: 100%,
    [
      #text(fill: hpe-green, weight: "bold", size: 1.1em)[1. Search] \
      #text(fill: white, size: 0.85em)[
        Find endpoints by keyword \
        #v(0.3em)
        `unified_search("vlan")`
      ]
    ],
  ),
  align(center + horizon, text(fill: hpe-green, size: 2em, weight: "bold")[→]),
  // Step 2
  block(
    fill: hpe-dark,
    inset: 14pt,
    radius: 8pt,
    width: 100%,
    [
      #text(fill: hpe-green, weight: "bold", size: 1.1em)[2. Discover] \
      #text(fill: white, size: 0.85em)[
        Read full API schema \
        #v(0.3em)
        `get_api_endpoint_detail(`\
        #h(0.5em)`"GET", "/config/..."`\
        `)`
      ]
    ],
  ),
  align(center + horizon, text(fill: hpe-green, size: 2em, weight: "bold")[→]),
  // Step 3
  block(
    fill: hpe-dark,
    inset: 14pt,
    radius: 8pt,
    width: 100%,
    [
      #text(fill: hpe-green, weight: "bold", size: 1.1em)[3. Execute] \
      #text(fill: white, size: 0.85em)[
        Call the API \
        #v(0.3em)
        `call_central_api(`\
        #h(0.5em)`"/config/...", ...`\
        `)`
      ]
    ],
  ),
)

#v(1.5em)

#align(center)[
  #text(size: 1.1em, fill: hpe-gray)[
    The LLM *learns the API on-the-fly* — zero pre-loaded endpoint schemas.
  ]
]

#speaker-note[
  ~90 seconds. Walk through the three phases:

  1. SEARCH: The LLM doesn't know what APIs exist. It uses unified_search()
     with a keyword like "vlan" or "firmware". This queries a BM25 full-text
     search index over all 1500+ endpoints and returns a ranked list of
     matching endpoints (method, path, summary).

  2. DISCOVER: The LLM picks the most relevant endpoint and calls
     get_api_endpoint_detail() to read its full OpenAPI schema — parameters,
     request body, response format. Now it knows exactly how to call it.

  3. EXECUTE: The LLM calls call_central_api() with the right path, method,
     and parameters. Done.

  Key insight: The search index is built at startup by scraping the OpenAPI
  specs from the developer portal. The LLM never needs to see all 1500
  endpoint schemas — it pulls exactly what it needs, when it needs it.
  This is the "invocation pattern" for MCP servers.
]


// ---------- Slide: Architecture Diagram -------------------------------------
== Architecture Overview

#v(0.5em)

#align(center)[
  #block(width: 95%)[
    #grid(
      columns: (1fr, 0.8fr, 1.5fr, 0.8fr, 1fr),
      gutter: 0.3em,
      align: horizon,
      // LLM
      block(
        fill: hpe-light,
        stroke: 2pt + hpe-gray,
        inset: 12pt,
        radius: 8pt,
        width: 100%,
        align(center)[
          #text(weight: "bold", size: 0.9em)[LLM Client] \
          #text(size: 0.7em, fill: hpe-gray)[Claude Desktop\ VS Code Copilot\ ...]
        ],
      ),
      // Arrow
      align(center, stack(dir: ttb, spacing: 2pt,
        text(size: 0.65em, fill: hpe-gray)[MCP],
        text(size: 1.5em, fill: hpe-green)[⟷],
        text(size: 0.65em, fill: hpe-gray)[stdio / SSE],
      )),
      // MCP Server
      block(
        fill: hpe-green.lighten(90%),
        stroke: 2pt + hpe-green,
        inset: 10pt,
        radius: 8pt,
        width: 100%,
        [
          #align(center, text(weight: "bold", fill: hpe-green, size: 0.9em)[MCP Server])
          #v(4pt)
          #grid(
            columns: (1fr, 1fr),
            gutter: 6pt,
            block(fill: white, inset: 6pt, radius: 4pt, width: 100%,
              text(size: 0.65em)[#text(weight: "bold")[Knowledge Layer]\ API Catalog (BM25)\ Docs • Scripts]),
            block(fill: white, inset: 6pt, radius: 4pt, width: 100%,
              text(size: 0.65em)[#text(weight: "bold")[Domain Layer]\ Graph DB (Kùzu)\ Topology • Config]),
          )
          #v(4pt)
          #block(fill: white, inset: 6pt, radius: 4pt, width: 100%,
            align(center, text(size: 0.65em)[#text(weight: "bold")[Script Engine] — Sandboxed Python execution]))
        ],
      ),
      // Arrow
      align(center, stack(dir: ttb, spacing: 2pt,
        text(size: 0.65em, fill: hpe-gray)[OAuth2],
        text(size: 1.5em, fill: hpe-green)[⟷],
        text(size: 0.65em, fill: hpe-gray)[REST],
      )),
      // Central
      block(
        fill: hpe-dark,
        inset: 12pt,
        radius: 8pt,
        width: 100%,
        align(center)[
          #text(fill: white, weight: "bold", size: 0.9em)[Aruba Central] \
          #text(fill: hpe-green, size: 0.7em)[~1 500 endpoints]
          #v(8pt)
          #text(fill: white, weight: "bold", size: 0.9em)[GreenLake] \
          #text(fill: hpe-green, size: 0.7em)[~60 endpoints]
        ],
      ),
    )
  ]
]

#v(0.5em)
#align(center, note[
  Seeds auto-populate the graph at startup from live API data.\
  BM25 full-text search index over all OpenAPI specifications.
])

#speaker-note[
  ~2 minutes. Walk through the architecture from left to right:

  LEFT: Any MCP-compatible LLM client (Claude Desktop, VS Code with Copilot,
  Cursor, etc.) connects via the standard MCP protocol (stdio or SSE transport).

  CENTER — the MCP server has three internal layers:
  1. Knowledge Layer: API catalog with BM25 search index built from scraped
     OpenAPI specs (both Central and GreenLake). Also holds documentation
     and script metadata.
  2. Domain Layer: Embedded graph database (Kùzu/LadybugDB) that models the
     network topology — Org → SiteCollections → Sites → Devices, LLDP
     adjacency, device groups, config scopes. Auto-populated by "seed scripts"
     that run at startup and call the Central APIs to build the graph.
  3. Script Engine: Sandboxed Python execution for multi-step workflows.
     Scripts get pre-authenticated API helpers injected via environment
     variables — no OAuth boilerplate needed.

  RIGHT: The actual Aruba Central and GreenLake platform APIs. All
  authentication is handled by the server (OAuth2 client credentials flow).
  The LLM never sees credentials.
]

// ---------- Slide: Script Pattern -------------------------------------------
== Scripts: Multi-Step Workflows

#v(0.5em)

#grid(
  columns: (1fr, 1fr),
  gutter: 2em,
  [
    *When to use scripts:*
    - Multiple API calls in sequence
    - Pagination over large datasets
    - Complex logic / conditionals
    - Reusable automation

    #v(1em)
    *Built-in seed scripts:*
    - `populate_base_graph` — Org hierarchy
    - `enrich_topology` — LLDP adjacency
    - `populate_monitoring` — Ports, clients
    - `onboard_device` — Full onboarding flow
  ],
  [
    #code-block[
      ```python
      from central_helpers import api, graph

      # Pre-authenticated — no OAuth needed
      devices = api.paginate(
          "network-monitoring/v1alpha1/devices"
      )

      for d in devices:
          graph.execute(
              "MERGE (d:Device {serial: $s})"
              " ON CREATE SET d.name = $n",
              {"s": d["serial"], "n": d["name"]}
          )
      ```
    ]
  ],
)

#speaker-note[
  ~60 seconds. Explain the script pattern:

  - For simple reads: call_central_api() is enough (one tool call)
  - For multi-step workflows (onboarding, bulk config, audit), the LLM
    writes a Python script, saves it, and executes it
  - Scripts get pre-authenticated helpers: `from central_helpers import api`
  - api.paginate() auto-detects cursor vs offset pagination
  - Scripts can also write to the graph database
  - Seed scripts run at startup: populate the graph with live network data
  - The LLM can also create NEW scripts on-the-fly for custom workflows
  - Scripts are sandboxed: 5-minute timeout, output truncation, no credential leakage
]


// ---------- Slide: Graph Database -------------------------------------------
== The Graph Database

#v(0.5em)

#grid(
  columns: (1.2fr, 1fr),
  gutter: 2em,
  [
    *Embedded graph* (Kùzu / LadybugDB) populated at startup:

    #v(0.5em)
    #code-block[
      ```
      Org ──→ SiteCollection ──→ Site ──→ Device
                                    ↑
               DeviceGroup ────────┘
                                     ↕
                               CONNECTED_TO
                              (LLDP adjacency)
      ```
    ]

    #v(0.5em)
    - Topology navigation via *Cypher* queries
    - LLDP neighbor discovery (incl. unmanaged devices)
    - Config scope tracking for blast-radius analysis
    - BM25 full-text search across all node types
  ],
  [
    *Example queries:*

    #code-block[
      ```
      // All devices at a site
      MATCH (s:Site)-[:HAS_DEVICE]->(d:Device)
      WHERE s.name = "Berlin-HQ"
      RETURN d.name, d.status

      // LLDP neighbors of a switch
      MATCH (d:Device)-[c:CONNECTED_TO]->
            (n:Device)
      WHERE d.name = "SW-Core-01"
      RETURN n.name, c.fromPorts
      ```
    ]
  ],
)

#speaker-note[
  ~60 seconds. Key points:

  - The graph is an embedded database (Kùzu) — no external service needed
  - Seeds auto-populate from live APIs at server startup
  - The LLM uses query_graph() to navigate the topology
  - This is much faster than making API calls for structural questions
  - "How many devices per site?" → one Cypher query vs. multiple API calls
  - The graph also stores LLDP topology — which switch port connects to which
  - CONNECTED_TO edges carry metadata: port names, speed, LAG, STP state
  - Enables blast-radius analysis: "If I change this config, what's affected?"
  - The graph is a STRUCTURAL MAP — live operational data still comes from APIs
]


// ---------- Slide: Tool Overview -------------------------------------------
== 11 Tools, 4 Categories

#v(1em)

#grid(
  columns: (1fr, 1fr),
  gutter: 1.5em,
  // Discovery
  block(
    fill: hpe-green.lighten(90%),
    stroke: 1pt + hpe-green,
    inset: 12pt,
    radius: 6pt,
    width: 100%,
    [
      #text(fill: hpe-green, weight: "bold", size: 1.1em)[🔍 Discovery]
      #v(4pt)
      - `unified_search` — BM25 keyword search
      - `list_api_categories` — Browse API areas
      - `get_api_endpoint_detail` — Full schema
    ],
  ),
  // Direct API
  block(
    fill: hpe-blue.lighten(90%),
    stroke: 1pt + hpe-blue,
    inset: 12pt,
    radius: 6pt,
    width: 100%,
    [
      #text(fill: hpe-blue, weight: "bold", size: 1.1em)[⚡ Direct API]
      #v(4pt)
      - `call_central_api` — Central REST calls
      - `call_greenlake_api` — GreenLake REST calls
    ],
  ),
  // Graph
  block(
    fill: hpe-dark.lighten(85%),
    stroke: 1pt + hpe-dark,
    inset: 12pt,
    radius: 6pt,
    width: 100%,
    [
      #text(fill: hpe-dark, weight: "bold", size: 1.1em)[🔗 Graph]
      #v(4pt)
      - `query_graph` — Read-only Cypher queries
      - `write_graph` — Enrich graph with findings
    ],
  ),
  // Scripts
  block(
    fill: hpe-accent.lighten(85%),
    stroke: 1pt + hpe-accent,
    inset: 12pt,
    radius: 6pt,
    width: 100%,
    [
      #text(fill: hpe-accent.darken(20%), weight: "bold", size: 1.1em)[📜 Scripts]
      #v(4pt)
      - `list_scripts` / `get_script_content`
      - `save_script` — Persist automation
      - `execute_script` — Run in sandbox
    ],
  ),
)

#speaker-note[
  ~45 seconds. Quick walkthrough of the 4 categories:

  1. DISCOVERY: How the LLM finds the right API. BM25 search ranks
     endpoints by relevance. Then it reads the full schema with
     get_api_endpoint_detail.

  2. DIRECT API: The actual API calls. Works for simple reads and
     one-off mutations. Supports both Central and GreenLake platforms.

  3. GRAPH: Structural navigation. query_graph for read-only Cypher.
     write_graph lets the LLM enrich the graph with findings during
     its investigation.

  4. SCRIPTS: For multi-step workflows. The LLM can list existing
     scripts, read their code, write new ones, and execute them.

  Total: 11 tools that give access to the entire platform. The
  invocation pattern means these 11 tools replace what would otherwise
  be 1500+ individual tools.
]


// ============================================================================
// SECTION 3: LIVE DEMOS
// ============================================================================

= Live Demos

// ---------- Slide: Demo Intro -----------------------------------------------
== Demo Overview

#v(2em)

#grid(
  columns: (1fr, 1fr, 1fr),
  gutter: 1.5em,
  block(
    fill: hpe-green.lighten(85%),
    stroke: 2pt + hpe-green,
    inset: 16pt,
    radius: 8pt,
    width: 100%,
    [
      #align(center)[
        #text(size: 2em)[🔍] \
        #text(weight: "bold", size: 1.1em)[Root Cause\ Analysis]
        #v(0.5em)
        #text(size: 0.85em, fill: hpe-gray)[
          Find unhealthy devices, \
          diagnose problems, \
          analyze topology
        ]
        #v(0.5em)
        #text(size: 0.75em, fill: hpe-green)[Demo environment]
      ]
    ],
  ),
  block(
    fill: hpe-blue.lighten(90%),
    stroke: 2pt + hpe-blue,
    inset: 16pt,
    radius: 8pt,
    width: 100%,
    [
      #align(center)[
        #text(size: 2em)[📋] \
        #text(weight: "bold", size: 1.1em)[Firmware\ Compliance]
        #v(0.5em)
        #text(size: 0.85em, fill: hpe-gray)[
          Check firmware versions, \
          find outdated devices, \
          recommend upgrades
        ]
        #v(0.5em)
        #text(size: 0.75em, fill: hpe-blue)[Demo environment]
      ]
    ],
  ),
  block(
    fill: hpe-accent.lighten(85%),
    stroke: 2pt + hpe-accent,
    inset: 16pt,
    radius: 8pt,
    width: 100%,
    [
      #align(center)[
        #text(size: 2em)[⚙️] \
        #text(weight: "bold", size: 1.1em)[VLAN\ Configuration]
        #v(0.5em)
        #text(size: 0.85em, fill: hpe-gray)[
          Create VLAN profile, \
          assign to ports, \
          verify push status
        ]
        #v(0.5em)
        #text(size: 0.75em, fill: hpe-accent.darken(20%))[Lab environment]
      ]
    ],
  ),
)

#speaker-note[
  ~30 seconds intro. Explain:
  - We have two environments: the demo environment (read-only, more complex
    network) and a small lab environment (few devices, can mutate)
  - Demo 1 & 2 run on the demo environment — non-mutating
  - Demo 3 runs on the lab — will actually create a VLAN config profile
  - All demos use Claude Desktop as the MCP client
  - "Let me switch to Claude Desktop now..."
]


// ---------- Slide: Demo 1 ---------------------------------------------------
== Demo 1: Root Cause Analysis

#v(1em)

#align(center)[
  #block(
    fill: hpe-light,
    stroke: 1pt + hpe-gray,
    inset: 24pt,
    radius: 8pt,
    width: 80%,
  )[
    #text(size: 1.2em, style: "italic", fill: hpe-dark)[
      "Show me devices with problems in the network and \
      diagnose the root causes."
    ]
  ]
]

#v(1.5em)

*Expected agent workflow:*

#grid(
  columns: (1fr, 1fr, 1fr, 1fr),
  gutter: 0.8em,
  align: top,
  dim-box[1. Graph query][
    Query topology \
    for device health
  ],
  dim-box[2. Alert API][
    Fetch active alerts \
    and event details
  ],
  dim-box[3. Neighbors][
    Check LLDP topology \
    for upstream issues
  ],
  dim-box[4. Diagnosis][
    Cross-correlate data \
    and report findings
  ],
)

#v(0.5em)
#align(center, note[
  ↑ Replace this slide with a live demo screenshot or video later.
])

#speaker-note[
  ~4 minutes live demo.

  Prompt to type: "Show me devices with problems in the network and diagnose
  the root causes. Start by checking the topology graph for device health,
  then look up alerts for any unhealthy devices."

  Expected flow:
  1. Agent calls query_graph() to get all devices and their status
  2. Finds devices with status != "Up" or health issues
  3. Calls unified_search("alerts") to find the alerting API
  4. Calls get_api_endpoint_detail for the alert endpoint
  5. Calls call_central_api to fetch active alerts for the problematic devices
  6. Checks LLDP neighbors to see if upstream devices are also affected
  7. Provides a root cause diagnosis

  If the demo environment has deliberate problems set up, this should
  produce interesting results. Claude may also generate an SVG topology
  visualization as an artifact.

  FALLBACK: If live demo fails, narrate the expected flow and show the
  tool calls the agent would make.
]


// ---------- Slide: Demo 2 ---------------------------------------------------
== Demo 2: Firmware Compliance

#v(1em)

#align(center)[
  #block(
    fill: hpe-light,
    stroke: 1pt + hpe-gray,
    inset: 24pt,
    radius: 8pt,
    width: 80%,
  )[
    #text(size: 1.2em, style: "italic", fill: hpe-dark)[
      "Which devices are running outdated firmware? \
       Compare against recommended versions."
    ]
  ]
]

#v(1.5em)

*Expected agent workflow:*

#grid(
  columns: (1fr, 1fr, 1fr, 1fr),
  gutter: 0.8em,
  align: top,
  dim-box[1. Search APIs][
    Find firmware & \
    compliance endpoints
  ],
  dim-box[2. Get versions][
    Fetch current vs. \
    recommended firmware
  ],
  dim-box[3. Compare][
    Cross-site comparison \
    per device type
  ],
  dim-box[4. Report][
    Compliance summary \
    with recommendations
  ],
)

#v(0.5em)
#align(center, note[
  ↑ Replace this slide with a live demo screenshot or video later.
])

#speaker-note[
  ~3 minutes live demo.

  Prompt to type: "Which devices are running outdated firmware? Compare the
  current firmware versions against the recommended versions and give me
  a compliance report by site."

  Expected flow:
  1. Agent calls unified_search("firmware") to find firmware-related APIs
  2. Discovers firmware compliance endpoint
  3. Calls get_api_endpoint_detail to read the schema
  4. Calls call_central_api to fetch firmware compliance data
  5. May also query the graph for device grouping by site
  6. Produces a report showing which devices need updates

  This demonstrates:
  - The invocation pattern in action (search → discover → call)
  - The LLM's ability to synthesize data from multiple sources
  - Practical value: firmware compliance is a real daily task
]


// ---------- Slide: Demo 3 ---------------------------------------------------
== Demo 3: VLAN Configuration (Mutating)

#v(1em)

#align(center)[
  #block(
    fill: hpe-accent.lighten(90%),
    stroke: 2pt + hpe-accent,
    inset: 24pt,
    radius: 8pt,
    width: 80%,
  )[
    #text(size: 1.2em, style: "italic", fill: hpe-dark)[
      "Create VLAN 100 on switch \[name\], assign it to port 1/1/5, \
       ensure trunk ports carry it, and verify the config was pushed."
    ]
  ]
]

#v(1.5em)

*Expected agent workflow:*

#grid(
  columns: (1fr, 1fr, 1fr, 1fr),
  gutter: 0.8em,
  align: top,
  dim-box[1. Discover][
    Search config profile \
    APIs for VLANs
  ],
  dim-box[2. Create][
    Create VLAN config \
    profile via API
  ],
  dim-box[3. Assign][
    Assign profile to \
    device scope
  ],
  dim-box[4. Verify][
    Check push status \
    with effective=true
  ],
)

#v(0.5em)
#align(center, note[
  ↑ Replace this slide with a live demo screenshot or video later.
])

#speaker-note[
  ~3 minutes live demo. This is the mutating demo — runs on the lab environment.

  Prompt to type: "Create VLAN 100 on switch [name], assign it to port 1/1/5.
  Make sure that trunk ports also carry it. After creating the config,
  verify that it was successfully pushed to the device."

  Expected flow:
  1. Agent searches for VLAN/config profile APIs
  2. Creates a VLAN config profile via POST call
  3. Assigns the profile to the device scope
  4. Uses the configuration push/audit API to verify
  5. Calls the /effective endpoint with detailed=true to confirm the
     config is active and shows which scope it came from

  This demonstrates:
  - The LLM can make WRITE operations (not just reads)
  - Config profile model: create → assign → push → verify
  - The effective=true&detailed=true pattern shows config provenance
  - Real-world operational task completed autonomously

  IMPORTANT: This is the lab environment with only 2-3 devices.
  Switch to the correct Central instance before this demo.
]


// ============================================================================
// SECTION 4: CLOSING
// ============================================================================

= Closing

// ---------- Slide: Disclaimer -----------------------------------------------
== Important

#v(2em)

#grid(
  columns: (1fr, 1fr),
  gutter: 2em,
  [
    #block(
      fill: hpe-accent.lighten(90%),
      stroke: 2pt + hpe-accent,
      inset: 16pt,
      radius: 8pt,
      width: 100%,
    )[
      #text(weight: "bold", size: 1.1em, fill: hpe-accent.darken(30%))[⚠️ Not an official HPE product]
      #v(0.5em)
      This is a *personal / community project* \
      that demonstrates what API-first \
      platforms enable.
    ]
  ],
  [
    #block(
      fill: hpe-green.lighten(90%),
      stroke: 2pt + hpe-green,
      inset: 16pt,
      radius: 8pt,
      width: 100%,
    )[
      #text(weight: "bold", size: 1.1em, fill: hpe-green)[✓ Production recommendations]
      #v(0.5em)
      - Restrict to *GET requests only*
      - Full API access = full blast radius
      - Use for *monitoring & analytics*
      - Be careful with mutations
    ]
  ],
)

#speaker-note[
  ~60 seconds. Important disclaimer:

  - This MCP server is NOT an official HPE product — it's a personal project
  - It is open source and available on GitHub
  - It demonstrates the capabilities of the Aruba Central API-first strategy
  - For production use, I strongly recommend:
    - Restricting to non-mutating operations only (GET requests)
    - Full API access means the LLM could theoretically change any
      configuration on the entire network
    - Use it primarily for monitoring, diagnostics, analytics, and auditing
    - If you DO enable mutations, use it in a controlled lab environment first
  - "With great API access comes great responsibility"
]


// ---------- Slide: Try It Out -----------------------------------------------
== Try It Out

#v(2em)

#align(center)[
  #block(
    fill: hpe-light,
    inset: 24pt,
    radius: 12pt,
    width: 70%,
  )[
    #text(size: 1.3em, weight: "bold", fill: hpe-dark)[
      github.com/tbelz/hpe-networking-central-mcp
    ]

    #v(1em)

    #grid(
      columns: (1fr, 1fr, 1fr),
      gutter: 1em,
      align(center)[
        #text(weight: "bold")[🖥️ Docker] \
        #text(size: 0.85em, fill: hpe-gray)[One-command setup]
      ],
      align(center)[
        #text(weight: "bold")[🤖 Claude Desktop] \
        #text(size: 0.85em, fill: hpe-gray)[Add to config.json]
      ],
      align(center)[
        #text(weight: "bold")[💻 VS Code] \
        #text(size: 0.85em, fill: hpe-gray)[Copilot MCP support]
      ],
    )
  ]
]

#v(1em)

#align(center)[
  #text(size: 1em, fill: hpe-gray)[
    Works with any MCP-compatible client • Requires Aruba Central API credentials
  ]
]

#speaker-note[
  ~30 seconds. Quick:
  - Show or mention the GitHub URL
  - Mention Docker as the easiest setup path
  - Works with Claude Desktop, VS Code Copilot, Cursor, or any MCP client
  - You need Aruba Central API credentials (client_id + client_secret)
  - The README has full setup instructions
  - "I'll share the link — feel free to try it out with your own environment"
]


// ---------- Slide: Key Takeaway ---------------------------------------------
== Key Takeaway

#v(3em)

#align(center)[
  #block(
    fill: hpe-green.lighten(90%),
    stroke: 3pt + hpe-green,
    inset: 28pt,
    radius: 12pt,
    width: 80%,
  )[
    #text(size: 1.5em, weight: "bold", fill: hpe-dark)[
      An API-first platform is inherently AI-ready.
    ]
    #v(0.5em)
    #text(size: 1.1em, fill: hpe-gray)[
      Point an agent at your network — \
      get autonomous diagnostics, analytics, and operations.
    ]
  ]
]

#speaker-note[
  ~30 seconds. Drive home the key message:

  - Aruba Central's API-first strategy isn't just about automation scripts
  - It's about being ready for the AI era
  - The MCP server is just a bridge — the real power comes from the APIs
  - Any platform with comprehensive APIs can be AI-enabled this way
  - "If your platform has APIs, you already have an AI-ready platform"
]


// ---------- Slide: Q&A ------------------------------------------------------
== Q & A

#v(4em)

#align(center)[
  #text(size: 2em, weight: "bold", fill: hpe-dark)[
    Questions?
  ]

  #v(2em)

  #text(size: 1em, fill: hpe-gray)[
    Till Belz \
    #text(fill: hpe-green)[github.com/tbelz/hpe-networking-central-mcp]
  ]
]

#speaker-note[
  Open for questions. Common questions you might get:

  - "How do you handle rate limiting?" → Built-in retry with Retry-After
    header parsing, max 60s wait
  - "What about security?" → OAuth2 client credentials, no credentials
    exposed to LLM, scripts sandboxed with timeout
  - "Can it work with other networking platforms?" → The invocation pattern
    works for any platform with OpenAPI specs. You'd need to swap the
    spec scraper and API client.
  - "How accurate is the LLM?" → Very good for monitoring and diagnostics.
    For mutations, always review what the agent plans to do.
  - "Performance?" → Graph queries are instant (under 10ms). API calls depend
    on Central's response time. Script execution has 5-minute timeout.
]
