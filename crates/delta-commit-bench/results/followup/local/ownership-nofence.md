| scenario | G1 | G2 | G3 | G4 | accepted | missing | dups | pending | appIds | drain ms | switch lag ms | events |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q1-crash-after-tenure-nox | pass | pass | pass | pass | 37 | 0 | 0 | 0 | 4 | 10 | inst-b +80 | x1:claim=1 x2:claim=1,drain_done=1 y:claim=1 |
| q1-crash-before-tenure-nox | pass | pass | pass | pass | 37 | 0 | 0 | 0 | 5 | 25 | inst-b +238 | x1:claim=1 x2:claim=1,drain_done=1 y:claim=1 |
| q1-drain-load-1.2-tenure-nox | pass | pass | pass | pass | 87 | 0 | 0 | 0 | 4 | 2 | inst-b +1482 | x1:claim=1 x2:claim=1,drain_done=1 y:claim=1 |
| q1-drain-load-12-tenure-nox | pass | pass | pass | pass | 519 | 0 | 0 | 0 | 4 | 2 | inst-b +131 | x1:claim=1 x2:claim=1,drain_done=1 y:claim=1 |
| q1-pause-inflight-tenure-nox | pass | pass | pass | pass | 143 | 0 | 0 | 0 | 4 | - | inst-b +103 | x:claim=1 y:claim=1 |
| q1-two-holders-0-tenure-nox | pass | pass | pass | pass | 150 | 0 | 0 | 0 | 6 | 1399 | inst-b +901 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:claim=1 |
| q1-two-holders-100-tenure-nox | pass | pass | pass | pass | 150 | 0 | 0 | 0 | 6 | 2660 | inst-b +1145 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:claim=1 |
| q1-two-holders-300-tenure-nox | pass | pass | pass | pass | 150 | 0 | 0 | 0 | 6 | 2307 | inst-b +58 | a:claim=2,drain_done=1 x:claim=2,lost=1,drain_done=1 y:claim=1 |
| q2-flip-tenure-nox | pass | pass | pass | pass | 1000 | 0 | 0 | 0 | 4 | - | 60 switches, p50 145 max 228 | x:claim=1 y:claim=1 |
| q2-misroute-120-tenure-nox | pass | pass | pass | pass | 1600 | 0 | 0 | 0 | 4 | - | inst-b +91, inst-a +631 | x:claim=1 y:claim=1 |
| q2-misroute-30-tenure-nox | pass | pass | pass | pass | 700 | 0 | 0 | 0 | 4 | - | inst-b +179, inst-a +267 | x:claim=1 y:claim=1 |
| q2-misroute-5-tenure-nox | pass | pass | pass | pass | 450 | 0 | 0 | 0 | 4 | - | inst-b +136, inst-a +71 | x:claim=1 y:claim=1 |
