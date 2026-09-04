# Tunnel-PoC

Ein Agent ohne eingehenden Listener wird über eine selbst aufgebaute
WebSocket-Verbindung per gRPC steuerbar und macht seine lokale Web-UI durch
denselben Kanal erreichbar.

## Starten

    make demo

Das baut und startet alle drei Prozesse mit den Tokens aus dem Makefile.
Wer `hub` und `agent` stattdessen direkt startet, braucht `HUB_TOKENS`
(Format `id:token,id:token`, z.B. `mac-1:secret1`) beim Hub und den
passenden `AGENT_TOKEN` beim Agent — ohne `HUB_TOKENS` beendet sich der Hub
sofort mit `HUB_TOKENS is empty`.

Danach:

- Hub-UI mit Agent-Liste und Envelope-Inspektor: <http://127.0.0.1:7001/>
- Agent-UI durch den Tunnel: <http://127.0.0.1:7001/a/mac-1/>
- gRPC durch den Tunnel:

      go tool grpcurl -plaintext -import-path proto -proto demo/v1/demo.proto \
        -H 'x-agent-id: mac-1' -d '{"text":"hallo"}' \
        127.0.0.1:7000 demo.v1.Demo/Echo

`buf` und `grpcurl` hängen als go.mod-Tool-Abhängigkeiten im Modul und ziehen
einen großen Satz indirekter Module nach sich. Gebraucht werden sie nur zum
Generieren und für Aufrufe von Hand; der PoC selbst hat keine davon zur
Laufzeit — und kein Docker.

## Aufbau

`internal/bus` trägt getaggte Envelopes mit `stream_id` über einen
WebSocket und weiß nichts von gRPC oder HTTP. `internal/relay` sind seine
beiden Nutzer. Der gRPC-Relay reicht rohe Message-Bytes durch, deshalb ist
jeder gRPC-Dienst ohne Codeänderung tunnelbar.

Design und bewusste Auslassungen:
`docs/superpowers/specs/2026-09-04-grpc-over-websocket-tunnel-design.md`

## Grenzen

Die Aufrufer-Seite ist nicht authentifiziert: wer den Hub erreicht,
erreicht jeden verbundenen Agent. Beide Hub-Listener binden deshalb per
Default auf `127.0.0.1` (überschreibbar über `-grpc-addr`/`-http-addr`).
Vor jedem Betrieb auf einer öffentlichen Adresse muss zuerst eine
Authentifizierung der Aufrufer hinzukommen.

Die Verbindung zwischen Agent und Hub ist unverschlüsselt: `-hub` zeigt per
Default auf ein `ws://`-Ziel, und der Agent schickt sein
`Authorization: Bearer <token>` samt allem Getunnelten im Klartext. Auf einen
entfernten Hub zu zeigen ist ein Flag-Argument weit, also gehört vorher ein
`wss://`-Endpunkt mit TLS davor.

Der HTTP-Relay-Handler wartet auf das Ende des Anfragekörpers, bevor er
zurückkehrt. Der Hub setzt dafür zwar `ReadTimeout`/`ReadHeaderTimeout` auf
seinem `http.Server`, das begrenzt aber nur, wie lange ein einzelner
Request insgesamt lesen darf — ein Aufrufer, der viele parallele Requests
offen hält und jeweils knapp unter dem Timeout bleibt, kann trotzdem
mehrere Handler-Goroutinen und Registry-Einträge gleichzeitig belegen.
Umgekehrt kappt dasselbe `ReadTimeout` jeden getunnelten Upload nach 30 s,
egal wie groß er ist.
