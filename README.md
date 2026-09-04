# Tunnel-PoC

Ein Agent ohne eingehenden Listener wird über eine selbst aufgebaute
WebSocket-Verbindung per gRPC steuerbar und macht seine lokale Web-UI durch
denselben Kanal erreichbar.

## Starten

    make demo

Danach:

- Hub-UI mit Agent-Liste und Envelope-Inspektor: <http://127.0.0.1:7001/>
- Agent-UI durch den Tunnel: <http://127.0.0.1:7001/a/mac-1/>
- gRPC durch den Tunnel:

      go tool grpcurl -plaintext -import-path proto -proto demo/v1/demo.proto \
        -H 'x-agent-id: mac-1' -d '{"text":"hallo"}' \
        127.0.0.1:7000 demo.v1.Demo/Echo

## Aufbau

`internal/bus` trägt getaggte Envelopes mit `stream_id` über einen
WebSocket und weiß nichts von gRPC oder HTTP. `internal/relay` sind seine
beiden Nutzer. Der gRPC-Relay reicht rohe Message-Bytes durch, deshalb ist
jeder gRPC-Dienst ohne Codeänderung tunnelbar.

Design und bewusste Auslassungen:
`docs/superpowers/specs/2026-09-04-grpc-over-websocket-tunnel-design.md`

## Grenzen

Die Aufrufer-Seite ist nicht authentifiziert: wer den Hub erreicht,
erreicht jeden verbundenen Agent. Beide Hub-Listener binden deshalb auf
`127.0.0.1`. Vor jedem Betrieb auf einer öffentlichen Adresse muss zuerst
eine Authentifizierung der Aufrufer dazu.

Der HTTP-Relay-Handler wartet auf das Ende des Anfragekörpers, bevor er
zurückkehrt. Der Hub setzt dafür zwar `ReadTimeout`/`ReadHeaderTimeout` auf
seinem `http.Server`, das begrenzt aber nur, wie lange ein einzelner
Request insgesamt lesen darf — ein Aufrufer, der viele parallele Requests
offen hält und jeweils knapp unter dem Timeout bleibt, kann trotzdem
mehrere Handler-Goroutinen und Registry-Einträge gleichzeitig belegen.
