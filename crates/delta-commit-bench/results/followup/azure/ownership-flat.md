| scenario | G1 | G2 | G3 | G4 | accepted | missing | dups | pending | appIds | drain ms | switch lag ms | events |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q1-crash-after-drain | pass | pass | pass | pass | 38 | 0 | 0 | 0 | 3 | 2 | inst-b +557 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-crash-after-tenure | pass | pass | pass | pass | 37 | 0 | 0 | 0 | 4 | 1 | inst-b +1403 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-crash-before-drain | pass | pass | pass | pass | 39 | 0 | 0 | 0 | 4 | 62 | inst-b +1093 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-crash-before-tenure | pass | pass | pass | pass | 37 | 0 | 0 | 0 | 5 | 62 | inst-b +552 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-drain-load-1.2-drain | pass | pass | pass | pass | 88 | 0 | 0 | 0 | 3 | 2 | inst-b +1367 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-drain-load-1.2-tenure | pass | pass | pass | pass | 87 | 0 | 0 | 0 | 4 | 1 | inst-b +597 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-drain-load-12-drain | pass | pass | pass | pass | 520 | 0 | 0 | 0 | 3 | 3 | inst-b +408 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-drain-load-12-tenure | pass | pass | pass | pass | 519 | 0 | 0 | 0 | 4 | 2 | inst-b +452 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-pause-inflight-drain | pass | pass | pass | pass | 141 | 0 | 0 | 0 | 4 | 3325 | inst-b +359 | x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-pause-inflight-tenure | pass | pass | pass | pass | 142 | 0 | 0 | 0 | 5 | 4572 | inst-b +982 | x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q1-two-holders-0-drain | pass | pass | pass | pass | 148 | 0 | 0 | 0 | 5 | 3352 | inst-b +1116 | a:claim=1,drain_done=1 x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-two-holders-0-tenure | pass | pass | pass | pass | 148 | 0 | 0 | 0 | 6 | 5042 | inst-b +426 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q1-two-holders-100-drain | pass | pass | pass | pass | 148 | 0 | 0 | 0 | 5 | 5090 | inst-b +707 | a:claim=1,drain_done=1 x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-two-holders-100-tenure | pass | pass | pass | pass | 148 | 0 | 0 | 0 | 6 | 5697 | inst-b +292 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q1-two-holders-300-drain | pass | pass | pass | pass | 148 | 0 | 0 | 0 | 4 | 3348 | inst-b +498 | a:claim=1,drain_done=1 x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-two-holders-300-tenure | pass | pass | pass | pass | 148 | 0 | 0 | 0 | 5 | 5022 | inst-b +502 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q2-flip-drain | FAIL | pass | pass | pass | 1000 | 499 | 0 | 492 | 4 | 135 | 60 switches, p50 109 max 532 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-flip-tenure | pass | pass | pass | pass | 1001 | 0 | 0 | 0 | 90 | 437 | 60 switches, p50 191 max 567 | x:fence=21,claim=45,acquire=23,lost=22,drain_done=22 y:fence=22,claim=43,acquire=22,lost=21,drain_done=21 |
| q2-misroute-120-drain | FAIL | pass | pass | pass | 1600 | 197 | 0 | 194 | 4 | 171 | inst-b +604, inst-a +424 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-misroute-120-tenure | pass | pass | pass | pass | 1600 | 0 | 0 | 0 | 6 | 140 | inst-b +658, inst-a +449 | x:fence=1,claim=3,acquire=2,lost=1,drain_done=1 y:fence=1,claim=1,acquire=1 |
| q2-misroute-30-drain | FAIL | pass | pass | pass | 700 | 198 | 0 | 194 | 4 | 150 | inst-b +551, inst-a +298 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-misroute-30-tenure | pass | pass | pass | pass | 700 | 0 | 0 | 0 | 6 | 166 | inst-b +583, inst-a +315 | x:fence=1,claim=3,acquire=2,lost=1,drain_done=1 y:fence=1,claim=1,acquire=1 |
| q2-misroute-5-drain | FAIL | pass | pass | pass | 450 | 200 | 0 | 193 | 4 | 140 | inst-b +646, inst-a +169 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-misroute-5-tenure | pass | pass | pass | pass | 450 | 0 | 0 | 0 | 6 | 161 | inst-b +711, inst-a +202 | x:fence=1,claim=3,acquire=2,lost=1,drain_done=1 y:fence=1,claim=1,acquire=1 |
