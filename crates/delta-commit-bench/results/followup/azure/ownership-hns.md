| scenario | G1 | G2 | G3 | G4 | accepted | missing | dups | pending | appIds | drain ms | switch lag ms | events |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q1-crash-after-drain | pass | pass | pass | pass | 38 | 0 | 0 | 0 | 3 | 2 | inst-b +1271 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-crash-after-tenure | pass | pass | pass | pass | 37 | 0 | 0 | 0 | 4 | 2 | inst-b +679 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-crash-before-drain | pass | pass | pass | pass | 40 | 0 | 0 | 0 | 4 | 103 | inst-b +1686 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-crash-before-tenure | pass | pass | pass | pass | 40 | 0 | 0 | 0 | 5 | 114 | inst-b +1553 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-drain-load-1.2-drain | pass | pass | pass | pass | 88 | 0 | 0 | 0 | 3 | 2 | inst-b +537 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-drain-load-1.2-tenure | pass | pass | pass | pass | 87 | 0 | 0 | 0 | 4 | 2 | inst-b +951 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-drain-load-12-drain | pass | pass | pass | pass | 520 | 0 | 0 | 0 | 3 | 1 | inst-b +611 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-drain-load-12-tenure | pass | pass | pass | pass | 519 | 0 | 0 | 0 | 4 | 1 | inst-b +787 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-pause-inflight-drain | pass | pass | pass | pass | 140 | 0 | 0 | 0 | 4 | 3001 | inst-b +464 | x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-pause-inflight-tenure | pass | pass | pass | pass | 140 | 0 | 0 | 0 | 5 | 2941 | inst-b +1127 | x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q1-two-holders-0-drain | pass | pass | pass | pass | 145 | 0 | 0 | 0 | 5 | 3416 | inst-b +441 | a:claim=1,drain_done=1 x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-two-holders-0-tenure | pass | pass | pass | pass | 145 | 0 | 0 | 0 | 6 | 3333 | inst-b +1336 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q1-two-holders-100-drain | pass | pass | pass | pass | 145 | 0 | 0 | 0 | 5 | 4741 | inst-b +595 | a:claim=1,drain_done=1 x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-two-holders-100-tenure | pass | pass | pass | pass | 146 | 0 | 0 | 0 | 6 | 4660 | inst-b +489 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q1-two-holders-300-drain | pass | pass | pass | pass | 146 | 0 | 0 | 0 | 4 | 4736 | inst-b +1013 | a:claim=1,drain_done=1 x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-two-holders-300-tenure | pass | pass | pass | pass | 146 | 0 | 0 | 0 | 5 | 6550 | inst-b +638 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q2-flip-drain | FAIL | pass | pass | pass | 1000 | 501 | 0 | 463 | 4 | 239 | 60 switches, p50 168 max 694 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-flip-tenure | pass | pass | pass | pass | 1001 | 0 | 0 | 0 | 10 | 311 | 60 switches, p50 314 max 908 | x:fence=1,claim=5,acquire=3,lost=2,drain_done=2 y:fence=2,claim=3,acquire=2,lost=1,drain_done=1 |
| q2-misroute-120-drain | FAIL | pass | pass | pass | 1600 | 195 | 0 | 190 | 4 | 221 | inst-b +739, inst-a +826 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-misroute-120-tenure | pass | pass | pass | pass | 1600 | 0 | 0 | 0 | 6 | 212 | inst-b +1079, inst-a +919 | x:fence=1,claim=3,acquire=2,lost=1,drain_done=1 y:fence=1,claim=1,acquire=1 |
| q2-misroute-30-drain | FAIL | pass | pass | pass | 700 | 199 | 0 | 193 | 4 | 239 | inst-b +757, inst-a +374 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-misroute-30-tenure | pass | pass | pass | pass | 700 | 0 | 0 | 0 | 6 | 227 | inst-b +1039, inst-a +373 | x:fence=1,claim=3,acquire=2,lost=1,drain_done=1 y:fence=1,claim=1,acquire=1 |
| q2-misroute-5-drain | FAIL | pass | pass | pass | 450 | 199 | 0 | 188 | 4 | 235 | inst-b +797, inst-a +344 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-misroute-5-tenure | pass | pass | pass | pass | 450 | 0 | 0 | 0 | 6 | 212 | inst-b +797, inst-a +399 | x:fence=1,claim=3,acquire=2,lost=1,drain_done=1 y:fence=1,claim=1,acquire=1 |
