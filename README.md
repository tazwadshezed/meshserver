# Wireless Sensor Mesh: MeshServer

This service runs the TCP/UDP gateway and DAQ process for the wireless sensor mesh.

## Features

- TCP gateway server
- UDP autodiscovery (MARCO/POLO)
- NATS message broadcasting
- Emulator support

## Railway Deployment

```bash
railway init
railway up
```

Start command: `python3 run_meshserver.py`
