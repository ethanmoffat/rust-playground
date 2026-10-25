| scenario | G1 | G2 | G3 | G4 | accepted | missing | dups | pending | appIds | drain ms | switch lag ms | events |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q1-crash-after-tenure-nox | pass | pass | pass | pass | 38 | 0 | 0 | 0 | 4 | 2 | inst-b +917 | x1:claim=1 x2:claim=1,drain_done=1 y:claim=1 |
| q1-crash-before-tenure-nox | pass | pass | pass | pass | 38 | 0 | 0 | 0 | 5 | 237 | inst-b +1044 | x1:claim=1 x2:claim=1,drain_done=1 y:claim=1 |
| q1-drain-load-1.2-tenure-nox | pass | pass | pass | pass | 87 | 0 | 0 | 0 | 4 | 2 | inst-b +720 | x1:claim=1 x2:claim=1,drain_done=1 y:claim=1 |
| q1-drain-load-12-tenure-nox | pass | pass | pass | pass | 516 | 0 | 0 | 0 | 4 | 2 | inst-b +283 | x1:claim=1 x2:claim=1,drain_done=1 y:claim=1 |
| q1-pause-inflight-tenure-nox | pass | pass | pass | pass | 137 | 0 | 0 | 0 | 4 | - | inst-b +590 | x:claim=1 y:claim=1 |
| q1-two-holders-0-tenure-nox | pass | pass | pass | pass | 144 | 0 | 0 | 0 | 6 | 3462 | inst-b +605 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:claim=1 |
| q1-two-holders-100-tenure-nox | pass | pass | pass | pass | 148 | 0 | 0 | 0 | 6 | 1689 | inst-b +537 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:claim=1 |
| q1-two-holders-300-tenure-nox | pass | pass | pass | pass | 144 | 0 | 0 | 0 | 4 | 0 | inst-b +1454 | a:claim=1,drain_done=1 x:claim=1 y:claim=1 |
| q2-flip-tenure-nox | pass | pass | pass | pass | 996 | 0 | 0 | 0 | 4 | - | 60 switches, p50 227 max 1340 | x:claim=1 y:claim=1 |
| q2-misroute-120-tenure-nox | pass | pass | pass | pass | 1600 | 0 | 0 | 0 | 4 | - | inst-b +1081, inst-a +7007 | x:claim=1 y:claim=1 |
| q2-misroute-30-tenure-nox | pass | pass | pass | pass | 700 | 0 | 0 | 0 | 4 | - | inst-b +1286, inst-a +1729 | x:claim=1 y:claim=1 |
| q2-misroute-5-tenure-nox | pass | pass | pass | pass | 450 | 0 | 0 | 0 | 4 | - | inst-b +1018, inst-a +516 | x:claim=1 y:claim=1 |
