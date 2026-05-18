# ForgeNX Engine

An open-source Stratum V1 mining pool engine written in Go. Multi-coin solo and pool mining.

ForgeNX Engine is provided free of charge under the GPLv3 license. By default, the engine contributes 1% of solved blocks to the development team to help fund ongoing development. This applies to both pool and solo mining modes. The donation can be disabled or adjusted in `config.json` - see the Donation section below.

## Get Started Fast

Install ForgeNX on your node, then add the ForgeNX Engine app from the ForgeNX App Store.

## Features

- **Multi-coin support** - BTC, BC2, BCH, DGB, XEC, plus any SHA256d coin via generic coin definitions
- - **Pool and Solo mining modes**
  - - **Variable Difficulty (VarDiff)**
    - - **Stale share grace period**
      - - **Low-diff share grace period**
        - - **Password-based difficulty** - miners can set difficulty via `d=xxx` in the authorize password field
          - - **Version Rolling** - BIP310 support for ASICBoost-capable miners
            - - **Server-side Ping**
              - - **ZMQ block notifications** - instant new block detection via pure-Go ZMQ (no CGO)
                - - **Address format support** - Bech32 (P2WPKH/P2WSH), Base58 (P2PKH/P2SH), and CashAddr
                  - - **Metrics API** - HTTP endpoints for pool stats, per-worker metrics, and live session info
                    - - **No database** - all state is in-memory for simplicity and performance
                     
                      - ## Building
                     
                      - ```bash
                        git clone https://github.com/ForgeNX/forgenx-engine.git
                        cd forgenx-engine
                        go build -o forgenx-engine ./cmd/forgenx-engine/
                        ```

                        ## Running

                        ```bash
                        ./forgenx-engine --config config.json
                        ```

                        ## Configuration

                        Copy `config.example.json` to `config.json` and edit as needed.

                        ## Donation

                        By default, 1% of solved blocks are donated to fund ongoing development. This can be disabled or adjusted in `config.json`:

                        ```json
                        "donation": {
                          "enabled": false,
                          "percent": 1.0
                        }
                        ```

                        ## License

                        GPL-3.0 - see LICENSE for details.
