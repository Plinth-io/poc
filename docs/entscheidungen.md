# Entscheidungen während der Umsetzung

Dieses Dokument hält jede Entscheidung fest, die während der Umsetzung des Tunnel-PoC
getroffen wurde, ohne vorher zu fragen — samt Begründung und, wo relevant, den Kosten
falls sie falsch war.

Es existiert, weil Design-Dokument, Implementierungsplan und Fortschritts-Ledger allesamt
in ignorierten Verzeichnissen liegen: ohne diese Datei verschwände die Begründungsspur
mit dem Arbeitsbereich. Wer die Arbeit prüft, sollte hier ansetzen — jede Zeile ist eine
Stelle, an der etwas anders hätte entschieden werden können.

Reihenfolge: chronologisch.

1. Kein git-worktree, Arbeit auf Branch `feat/grpc-tunnel` im Hauptverzeichnis — das Repo war leer und ohne Commit, ein Worktree lässt sich vor dem ersten Commit gar nicht anlegen und es gibt nichts, wovon zu isolieren wäre. Kosten falls falsch: der Nutzer hat laufende Arbeit im selben Verzeichnis, die mit den Commits kollidiert — Repo war nachweislich leer, also nahe null.

2. buf und grpcurl fehlen auf der Maschine und werden NICHT global installiert, sondern als go.mod-Tool-Dependencies aufgenommen (`go get -tool`, Aufruf via `go tool buf` / `go tool grpcurl`). Grund: reproduzierbar aus go.mod, kein Eingriff außerhalb des Repos, kein Homebrew nötig. Kosten falls falsch: `go tool` braucht Go >= 1.24 (hier 1.27) und lädt beim ersten Lauf; falls buf-Remote-Plugins offline nicht erreichbar sind, muss auf lokale Plugins umgestellt werden.

3. Kein `go mod tidy` vor Task 16 — goleak, buf und grpcurl haben bis dahin keinen importierenden Code und würden aus go.mod entfernt. Kosten falls falsch: go.mod trägt bis Task 16 scheinbar unbenutzte Requires.

4. **Task 4** — Important 1 (TestEnvelopeForUnknownStreamIsDropped prüft nur st.ID != 0, was bei SideHub=1 unerreichbar ist — der Test bliebe grün, wenn eine unbekannte stream_id die Verbindung killt) — Befund ist richtig, mein Plantext ist falsch. Spec §11 verlangt Tests, die echtes Verhalten prüfen; ein Test, der den Critical-Fall nicht bemerkt, erfüllt das nicht. Wird gefixt. Kosten falls falsch: minimal, der Fix ist ein echter Round-Trip statt einer toten Assertion.

5. **Task 4** — Important 2 (peerStream := <-ready ohne Timeout-Arm) — richtig, gleicher Ursprung. Ein hängender Test statt einer klaren Fehlermeldung ist inakzeptabel. Wird gefixt.

6. **Task 4** — Minor 3 (detached Close-Goroutine überlebt Run) wird in die Fix-Runde hochgezogen, obwohl Minor: Task 16 lässt goleak laufen, und ein beschränktes Warten entschärft zugleich Minor 4 (verlorener 1002-Frame). Kosten falls falsch: Run verzögert sich um bis zu 250 ms beim Protokollfehler.

7. **Task 4** — Minor 5 (fehlender ponytail:-Kommentar) ist keine Kür — die Global Constraints verlangen ihn für bewusste Vereinfachungen. Wird gefixt.

8. **Task 4** — Minor 6 (Fehler des Write-Loops wird verworfen) wird mitgefixt, weil die nächsten sechs Tasks genau in diesem Code debuggen und ein maskierter Schreibfehler dort teuer wird.

9. **Task 5** — CloseWith aus dem Plantext (`_ = c.ws.Close(code, reason)`) ist nach dem T4-Befund nachweislich falsch — Close() wartet bis zu 5 s auf den Ack. handleConnect ruft old.Conn.CloseWith() inline beim Ersetzen einer Verbindung und würde den Handler der NEUEN Verbindung so lange blockieren; TestSecondConnectionReplacesTheFirst liefe ins Timeout. CloseWith bekommt denselben abgekoppelten Handshake wie closeProtocolError. Kosten falls falsch: der Socket der alten Verbindung wird minimal später freigegeben.

10. **Task 5** — Important (handleConnect schließt den Socket auf dem Normalpfad nie) — plan-mandated Lücke, Befund ist reproduziert und gemessen. Wird gefixt. Kosten falls falsch: keine, CloseNow nach Close ist ein No-op.

11. **Task 5** — Minors 1-3 werden hochgezogen, weil sie die Vertrauensgrenze betreffen: ein Wait ohne Timeout-Arm verstößt gegen die Global Constraints, der Foreign-ID-Test kann nach festem Sleep leer bestehen, und die 401-Pfade von bearerToken sind komplett ungetestet. Auth-Tests, die nichts beweisen, sind schlimmer als keine. Kosten falls falsch: etwas mehr Testcode.

12. **Task 5** — Minor 4 wird hochgezogen — die Rejection-Pfade schließen synchron und binden den Handler bis zu 10 s pro abgelehnter Verbindung; mit vielen Fehlversuchen ist das ein billiger Weg, Handler zu belegen. Gleiche Begründung wie beim CloseWith-Ruling.

13. **Task 5** — Minor 5 wird hochgezogen — Config.Tokens ist ein roher Map-Parameter, an ParseTokens vorbei baubar; ein leerer Key würde auf "Bearer " matchen. Drei Zeilen Guard an einer Auth-Grenze sind billiger als die Annahme, dass nie jemand die Map selbst baut.

14. **Task 5** — Ursache ist geteiltes Eigentum am Socket — zwei unabhängige Schließer derselben Verbindung. Statt den Race zu verengen, wird das Eigentum eindeutig gemacht: ab NewConn gehört der Socket dem bus.Conn, und Run garantiert, dass der fd freigegeben ist, bevor es zurückkehrt. Konkret: Run ruft CloseNow() nur, wenn KEIN abgekoppelter Close gestartet wurde (closeDone == nil); andernfalls übernimmt die Goroutine die Freigabe. Der Hub schließt auf dem Normalpfad gar nicht mehr; die beiden Rejection-Pfade liegen vor NewConn und behalten closeAsync. Damit gibt es nie zwei Schließer, kein Race und kein Leck. Kosten falls falsch: kehrt Run zurück, bevor die Goroutine den Handshake beendet hat, lebt der fd noch kurz weiter — beschränkt durch die Library-Timeouts, nicht unbegrenzt.

15. **Task 5** — die out-of-scope-Beobachtung des Reviewers (agent.go:46 defer ws.CloseNow() hat denselben Defekt) wird NICHT deferred, obwohl sie außerhalb des Fix-Diffs liegt: sie liegt innerhalb des Codes, den Task 5 angelegt hat, das erste Volle Review hat sie nur übersehen. Und sie ist tragend — der Agent verbindet per Design in einer Schleife neu, ein blockierendes ConnectOnce verzögert jeden Reconnect gegen einen nicht antwortenden Hub um bis zu 10 s. Die nächsten elf Tasks bauen darauf auf. Fix in Runde 3. Kosten falls falsch: eine zusätzliche Fix-Runde für einen Pfad, der nur gegen einen langsamen Peer auftritt.

16. **Task 5** — die zwei Doku-Ungenauigkeiten werden mitgefixt, weil sie die gerade etablierte Eigentumsregel falsch beschreiben — und genau diese Regel müssen die folgenden Tasks befolgen.

17. **Task 6** — sämtliche RPC-Aufrufe der Demo-Tests nutzen context.Background() ohne Deadline. Die Global Constraints verlangen ausdrücklich einen Timeout-Arm für jedes Warten im Test. Ein Dienst, der nie antwortet, würde die Suite hängen lassen statt rot zu werden — genau der Fall, den Task 4 schon einmal geliefert hat. Befund ist richtig, mein Plantext ist falsch. Wird gefixt. Kosten falls falsch: keine, es ist reiner Testcode.

18. **Task 7** — Important 1 (nur die Caller-Cancellation schickt RpcCancel, vier andere Ausgänge nicht) wird gefixt. Für unary heilt es in Millisekunden — deshalb sieht es hier kein Test. Ab Task 8 mit Server-Streaming überlebt ein verwaister lokaler Aufruf samt Goroutine, solange der lokale Dienst produziert. Genau die Leck-Klasse, gegen die das ganze Projekt strukturell abgesichert ist. Kosten falls falsch: drei Zeilen plus Helfer.

19. **Task 7** — Important 2 (kein ponytail:-Marker am akzeptierten Teardown-Race) wird gefixt. Der Reviewer hat den Race unabhängig nachverfolgt und als akzeptabel bewertet — der Transport ist dann wirklich tot, Unavailable ist retryable und nicht falsch. Aber die Abwägung darf nicht nur in einer Report-Datei stehen, die niemand mehr liest; die Global Constraints verlangen sie im Code.

20. **Task 7** — Minor 3 wird hochgezogen — sendEnd nutzt den evtl. abgelaufenen Call-Context und funktioniert heute nur, weil die Hub-Deadline zufällig immer zuerst feuert. Eine zufällige Garantie ist keine. Eine Zeile.

21. **Task 7** — Minors 4-6 werden hochgezogen: toter Import-Halter in testenv, verschluckter Connect-Fehler (betrifft die Diagnose ALLER folgenden Tests, testenv trägt noch acht Tasks), und Close() nilt grpcCC, sodass der Agent nach dem Schließen still wiederauflebt — ein Leck-Vektor für den goleak-Check in Task 16.

22. **Task 7** — Abweichung A (kein Cancel auf den RpcEnd-Rückwegen) ist gerechtfertigt — der Reviewer hat am grpc-go-Kontrakt belegt, dass RecvMsg erst terminiert, wenn der Status da ist, also nie ein lebender lokaler Aufruf übrig bleibt. Meine wörtliche Anweisung war hier falsch.

23. **Task 7** — Abweichung B (Panic-Pfad in forwardResponses meldet keinen Cancel) wird vertagt statt blockiert: der Hub-Stream wird per LIFO-defer trotzdem korrekt abgebaut, nur die Meldung an den Agent fehlt, und ein Panic dort setzt einen Fehler an ganz anderer Stelle voraus. Geht als Auflage in Task 8, wo es zugleich testbar wird. Kosten falls falsch: bei einem Panic im Hub-Relay überlebt ein lokaler Aufruf im Agent.

24. **Task 8** — Important (der EINE Test, der einen Abbruch mitten im Stream braucht, ist der einzige ohne Timeout-Arm) wird gefixt — ein Relay, das vor dem ersten Chunk stehenbleibt, ließe genau diesen Test 10 Minuten hängen statt ihn rot zu machen. Eine Zeile.

25. **Task 8** — drei Minors hochgezogen, alle trivial und in denselben Dateien: tunnelCtx dupliziert callCtx (zwei Helfer, die bei der Timeout-Politik synchron bleiben müssen), TailActive trägt keinen ponytail:-Marker obwohl der Report selbst seine Obergrenze benennt, und TestConcurrentLargeChatMessagesDoNotAlias ist nicht concurrent sondern pipelined.

26. **Task 9** — Important 1 (Cleanup-Poll im Abriss-Test ist mit callTimeout=5s begrenzt, während der Agent sich bei ~T0+5s selbst heilt) wird gefixt — die Assertion kann aus dem Grund, für den sie existiert, gar nicht fehlschlagen. Genau die Maskierung, die der Implementer selbst diagnostiziert und in zwei anderen Tests behoben hat.

27. **Task 9** — Important 2 (Deadline-Pfad hat KEINE Cleanup-Abdeckung, nur Statuscode) wird gefixt. Von den drei Pfaden, die der Task benennt, ist damit einer nur halb geprüft; dazu verunreinigt der Test den prozessglobalen Zähler für die beiden folgenden Tests.

28. **Task 9** — Minor 3 wird hochgezogen, obwohl er außerhalb dieses Tasks liegt — der bestehende Abbruch-Test aus Task 8 (grpc_streaming_test.go:175) trägt dieselbe Maskierung. Der Reviewer will ihn ans Abschlussreview verweisen; ich schließe ihn jetzt, weil es dieselbe eine Zeile in einer Datei ist, die ohnehin offen ist. Einen bekannt maskierten Test wegen einer Task-Grenze stehen zu lassen, ist Buchhaltung, kein Urteil.

29. **Task 10** — Important (TestStreamLimitIsEnforced setzt refused=true bei JEDEM Fehler, prüft weder den Statuscode noch wie viele Streams vorher gelangen) wird gefixt. Der Test bestünde auch, wenn man MaxStreams ersatzlos entfernt — solange innerhalb von 300 Versuchen irgendetwas anderes schiefgeht. ResourceExhausted kommt zudem aus einem zweiten, völlig anderen Pfad (Payload zu groß). Genau der Fehlmodus, den der Brief selbst benennt. Aus meinem Plantext übernommen.

30. **Task 10** — beide Minors am Starvation-Test werden hochgezogen. Ohne Synchronisation ist nicht garantiert, dass der große Aufruf überhaupt läuft, während die kleinen laufen — und da nur gegen callTimeout=5s geprüft wird, bestünde ein vollständig serialisierter Entwurf identisch. Der Test behauptet Nicht-Blockieren und beweist es nicht. Kosten falls falsch: eine engere Schranke kann unter Last flackern; dann soll der Implementer das melden, nicht die Schranke aufweichen.

31. **Task 10** — Abweichung A gerechtfertigt — grpc-go legt Streams lazy an, die Ablehnung KANN bei client.Chat() gar nicht auftauchen, nur bei Send/Recv. Meine Anweisung beruhte auf einer falschen Annahme über gRPC.

32. **Task 10** — Abweichung B — die Schranke ist ein schwacher Wächter. Über die lokale Schleife bleibt selbst eine vollständig serialisierte 2-MiB-Übertragung bei ~17,5 ms gegen 500 ms Schranke. Sie fängt grobe Ausfälle, aber nicht das, wonach der Test benannt ist. Akzeptiert als Obergrenze — ABER der Kommentar, der das Gegenteil behauptet, muss weg. Ein Kommentar, der eine Garantie vorgibt, die es nicht gibt, ist schlimmer als keiner: er hält den nächsten Leser davon ab, den Test zu schreiben, der es wirklich prüfen würde.

33. **Task 11** — Important 1 (Fehler der Request-Body-Pumpe cancelt die laufende Antwort, und sendResponseEnd nutzt dann genau den gecancelten Context — Conn.Send wählt zwischen out und ctx.Done() zufällig; verliert es, kommt gar kein Abschluss-Envelope und der Hub-Handler parkt unbegrenzt) wird gefixt. Der gRPC-Relay hat diese Lektion bereits gelernt und nutzt dort bewusst context.Background(). Plan-mandated. Reviewer konnte es in-process nicht auslösen (Ziel antwortet in 2-7 ms), aber das Fenster öffnet sich genau bei langsam streamenden Antworten — also in Task 12.

34. **Task 11** — Important 2 (forwardHTTPRequestBody liest und schließt r.Body auf einer Goroutine, die den Handler überleben kann; net/http erklärt den Request danach für ungültig) wird gefixt. Plan-mandated.

35. **Task 11** — Important 3 (WriteHeader mit einem vom Agent gelieferten Status paniert außerhalb 100..999, und ein ungesetztes Feld ist 0) wird gefixt, plus wroteHead-Guard. Der gRPC-Hub hält diese Klasse für wichtig genug für ein recover().

36. **Task 11** — Minors 4,6,7,8,9 hochgezogen — alle klein, und drei davon sind Aussagen im Code, die nicht stimmen: die Doku von errClosed gilt nur noch für einen Zweig, die Begründung zum Prefix-Trimmen ist sachlich falsch (der Auslöser ist client- nicht id-kontrolliert), und hopByHop ist von KEINEM Test gedeckt — die Drop-Liste ersatzlos zu löschen lässt die Suite grün.

37. **Task 11** — Abweichung (Schleife statt return) ist eine VERBESSERUNG, am Code belegt — st.In fasst 16 Envelopes, läuft es voll, blockiert Conn.dispatch den einzigen readLoop der Verbindung für alle Streams. Meine Anweisung hätte einen schlimmeren Fehler eingebaut.

38. **Task 12** — der Minor wird hochgezogen, obwohl der Reviewer ihn nicht als Blocker sieht — bei einer echten Regression schlägt der Test zwar korrekt fehl, aber t.Cleanup(srv.Close) blockiert danach unbegrenzt, weil httptest auf den nie endenden /sse-Handler wartet. Ergebnis: ein hängendes CI statt eines lesbaren Fehlschlags. Genau die Klasse "hängen statt fehlschlagen", für die in diesem Plan schon sieben Tests korrigiert wurden — nur eine Ebene höher, im Aufräumen. Kosten falls falsch: der SSE-Handler bekommt eine obere Schranke, die im Normalbetrieb nie greift.

39. **Task 13** — der ungedeckte hello-write-failure-Zweig bleibt ungedeckt. Der Implementer hat es 20-mal versucht; der lokale Schreibvorgang gelingt immer, weil der Kernel-Puffer ihn annimmt, auch wenn die Gegenseite schon weg ist. Erzwingen bräuchte Kontrolle über rohe Sockets — unverhältnismäßig für zwei reine Log-Zeilen. Genau das hatte ich als zulässige Antwort vorgegeben.

40. **Task 13** — Minor 1 (Ping-Fehlerpfad des keepalive hat KEINEN Test) wird hochgezogen, obwohl der Reviewer davon abrät. Sein Gegenargument — Testbarkeits-Knöpfe sind spekulative Fläche — ist ernst, aber hier steht die Hälfte des Tasknamens ohne jede Abdeckung da. Das keepalive existiert genau für Ausfälle, die sonst KEIN Symptom erzeugen; bleibt es ungeprüft, merkt niemand etwas, wenn es aufhört zu funktionieren. Zwei unexportierte var statt const ist eine sehr kleine Fläche gegen ein Verhalten, das sonst nur durch Lesen belegt ist. Kosten falls falsch: zwei Konstanten sind jetzt Variablen.

41. **Task 13** — Minors 3 und 4 hochgezogen — max<min bleibt nach der Normalisierung möglich (60s/0 ergibt min=60s, max=30s), und die letzte Assertion im Reconnect-Test ist unerreichbar, liest sich aber wie eine echte Prüfung. Gleiche Klasse wie der überbehauptende Kommentar in T10.

42. **Task 13** — dieser Minor wird hochgezogen, obwohl der Reviewer ihn als nicht-blockierend einstuft — er konnte nicht wissen, dass Task 16 genau für internal/agent einen goleak-Check einführt. Eine dauerhaft geparkte Handler-Goroutine würde diesen Check reißen und dort wie ein Produktionsleck aussehen. Jetzt eine Zeile, in Task 16 eine verwirrende Fehlersuche.

43. **Task 14** — KEINE Fix-Runde für die drei Minors. Anders als bei den bisherigen Hochstufungen hat keiner davon eine konkrete Folgewirkung — kein Leck-Check, den sie reißen, keine falsche Aussage im Code, keine Vertrauensgrenze. Das Muster "Template erst in einen Puffer rendern" geht stattdessen als Vorgabe in den T15-Dispatch, wo die zweite UI entsteht. Billiger und genauso wirksam.

44. **Task 15** — Important 1+2 werden gefixt (ein Test schließt beide). Man könnte conn.SetTap() ersatzlos löschen, ohne dass ein Test fehlschlägt — der Inspektor ist der Zweck dieses Tasks und seine Anbindung an den Bus ist ungeprüft. Zugleich wurde der heiße Pfad des SSE-Handlers nie durchlaufen, wodurch die "kein Leck"-Prüfung weniger belegt als der Report behauptet. Ein Integrationstest (Agent verbinden, Aufruf auslösen, Envelope im SSE-Strom erwarten) schließt beides.

45. **Task 15** — Important 3 (commit 5bc914f heißt "wip:", kein Conventional-Commit-Typ) wird NICHT gefixt. Die Nachricht ist falsch, aber History-Rewrite würde die Commit-SHAs ungültig machen, die das Ledger als Wiederherstellungskarte nutzt. Das ist der schlechtere Tausch für ein Präfix. Vermerkt fürs Abschlussreview und für ein etwaiges Squash beim Merge.

46. **Task 16** — Important 1 (make demo macht den dokumentierten Abnahmeschritt zerstörerisch — `go run` startet ein Kind, dessen Kommandozeile NICHT "cmd/agent" enthält, während die Wrapper-Shell des Makefiles sie enthält; pkill killt also Hub und Demo-Dienst und lässt den Agent laufen) wird gefixt. Der Reviewer weist die "Plattform-Eigenheit" zurecht zurück: go run verhält sich überall so. Wer der Anleitung folgt, schließt auf einen Defekt, den es nicht gibt.

47. **Task 16** — Important 2 (der Kommentar, der zwei goleak-Ignores rechtfertigt, behauptet eine Garantie, die grpc-go nicht gibt) wird gefixt. Die Einträge dürfen bleiben — die vom Implementer im Report genannte Begründung ist die richtige und gehört in die Datei. Gleiche Klasse wie die überbehauptenden Kommentare in T10 und T11.

48. **Task 16** — Important 3 (Kriterium 4 als bestanden gemeldet, Beleg zeigt nur einen offenen, chunked Kanal — nie ein durchlaufendes Byte) wird gefixt. Es ist genau das Kriterium, das Flush, Tunnel und SSE verbindet. Die genannte Begründung für die Lücke stimmt nicht; ein Ein-Kommando- Auslöser existiert.

49. **Task 16** — Kriterium 5 wird MIT gefixt, obwohl der Reviewer es nur als Warnung führt — drei sequenziell abgeschlossene Aufrufe erzeugen dieselben stream_ids wie drei gleichzeitige, der Beleg kann beide nicht unterscheiden. Gleiche Klasse wie Kriterium 4: eine Abnahme, deren Nachweis die Behauptung nicht trägt. Die Abnahme ist das eine Artefakt, das sagt "das funktioniert".

50. Der Reviewer widerspricht meinem T13-RESTRISIKO-Ruling — ZU RECHT. Ich hatte den STALE-Fall analysiert (Handler schreibt auf die Nachfolgeverbindung, Envelope wird verworfen) und den NIL-Fall übersehen: clearConn() nilt busConn in dem Moment, in dem Run zurückkehrt, also kann session() nil liefern. Der Reviewer hat den Panic reproduziert — und er ist unrettbar, weil der recover in ServeRPC selbst wieder auf demselben nil-Pointer paniert. Das ist der Blocker.

51. Der Reviewer widerspricht meinem T15-Ruling zum wip:-Commit — zu Recht FÜR DEN MERGE-ZEITPUNKT. Der Tausch war richtig, solange das Ledger lebende Wiederherstellungskarte war; beim Merge ist er es nicht mehr. Squash-Merge löst beides.

52. Der Reviewer widerspricht meinem T14-Ruling halb — zu Recht. Ich hatte das Puffer-Muster nach T15 weitergereicht statt es zu fixen; T15 hat es bekommen, die Agent-UI nicht. Derselbe Defekt an zwei Stellen, an einer behoben. Wird jetzt angeglichen.

53. Important 3 (Hub erkennt einen still gestorbenen Agent nicht; Spec §7 verspricht eine "letzter Pong"-Spalte, Spec §10 verspricht UNAVAILABLE) — ich entscheide für den CODE, nicht für eine Spec-Änderung: der Hub pingt künftig ebenfalls. Grund: ohne das hängt ein Aufrufer OHNE Deadline unbegrenzt, was Spec §10 ausdrücklich ausschließt. Fünf Zeilen spiegeln agent.keepalive.

54. Important 4 (500-Einträge-Ringpuffer, den nichts liest) — ich entscheide für RENDERN statt Löschen, gegen die Empfehlung des Reviewers. Der Inspektor existiert, um Multiplexing sichtbar zu machen; eine Seite, die bis zum nächsten Verkehr leer bleibt, untergräbt genau das. Die Daten werden bereits gesammelt. Kosten falls falsch: sechs Zeilen Template statt fünfzehn Zeilen löschen.

55. Minor 13 (Spec §4 behauptet "genau eine sendende Goroutine pro Stream und Richtung", der Code hat zwei) — hier hat die SPEC unrecht, nicht der Code: die zweite Goroutine sendet nur terminale Envelopes, und ein Cancel vor einem Body-Chunk ist das gewünschte Ergebnis. Spec wird präzisiert.

56. Restpunkt 1 (Keepalive loggt bei sauberem Verbindungsende eine irreführende Warnung) wird GEPARKT. Reine Log-Kosmetik: Remove ist identitätsgesichert, CloseWith ist dann ein No-op.

57. Restpunkt 2 (die verkürzten Ping-Konstanten gelten für das ganze hub-Testbinary, 100 ms Pong-Budget) wird GEPARKT. -count=5 lief grün; es ist eine Robustheitsmarge, kein beobachtetes Flackern. 500 ms wären der ruhigere Wert.

58. Restpunkt 3 (der recover in pumpBusToLocal endet mit CANCELED, Spec §10 nennt INTERNAL) wird GEPARKT. Die tragende Hälfte von §10 hält: Stream geschlossen, terminales Envelope gesendet, Prozess lebt. Der Code weicht im Statuscode ab, nicht im Verhalten.

59. Restpunkt 4 (der Doku-Kommentar am Hub-Keepalive überzeichnet — der Pong braucht auch den eigenen Read-Loop) wird GEPARKT, obwohl überzeichnende Kommentare in diesem Plan dreimal ein echter Befund waren. Grund: der Sachverhalt ist bereits über bus/stream.go:12 als ponytail dokumentiert, und die Prozessregel lässt keine zweite Fix-Welle zu.

60. Restpunkt 5 (http_hub.go:61 startet eine nackte Goroutine ohne recover; ein Panic dort tötet den Hub — dieselbe Klasse wie Important 2, auf dem HTTP-Pfad) wird GEPARKT und dem Nutzer vorgelegt. Es ist ein echter Defekt, aber kein abhängiger Task baut darauf auf, der ganze Plan ist fertig, und der Reviewer empfiehlt ein eigenes Ticket statt einer Last-Minute-Änderung. Kosten falls falsch: ein Panic in diesem einen Pfad beendet den Hub-Prozess.

