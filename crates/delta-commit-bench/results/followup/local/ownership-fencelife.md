| scenario | G1 | G2 | G3 | G4 | accepted | missing | dups | pending | appIds | drain ms | switch lag ms | events |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q1-crash-after-drain | pass | pass | pass | pass | 38 | 0 | 0 | 0 | 3 | 7 | inst-b +1079 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-crash-after-tenure | pass | pass | pass | pass | 37 | 0 | 0 | 0 | 4 | 2 | inst-b +994 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-crash-before-drain | pass | pass | pass | pass | 38 | 0 | 0 | 0 | 4 | 25 | inst-b +897 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-crash-before-tenure | pass | pass | pass | pass | 37 | 0 | 0 | 0 | 5 | 20 | inst-b +93 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-drain-load-1.2-drain | pass | pass | pass | pass | 88 | 0 | 0 | 0 | 3 | 9 | inst-b +158 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-drain-load-1.2-tenure | pass | pass | pass | pass | 87 | 0 | 0 | 0 | 4 | 10 | inst-b +834 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-drain-load-12-drain | pass | pass | pass | pass | 520 | 0 | 0 | 0 | 3 | 2 | inst-b +94 | x2:claim=1,drain_done=1 y:fence=1 |
| q1-drain-load-12-tenure | pass | pass | pass | pass | 519 | 0 | 0 | 0 | 4 | 2 | inst-b +121 | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q1-pause-inflight-drain | pass | pass | pass | pass | 144 | 0 | 0 | 0 | 4 | 2630 | inst-b +267 | x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-pause-inflight-tenure | pass | pass | pass | pass | 144 | 0 | 0 | 0 | 5 | 4799 | inst-b +267 | x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q1-two-holders-0-drain | pass | pass | pass | pass | 149 | 0 | 0 | 0 | 5 | 1398 | inst-b +1323 | a:claim=1,drain_done=1 x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-two-holders-0-tenure | pass | pass | pass | pass | 150 | 0 | 0 | 0 | 6 | 1390 | inst-b +1272 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q1-two-holders-100-drain | pass | pass | pass | pass | 150 | 0 | 0 | 0 | 5 | 1395 | inst-b +814 | a:claim=1,drain_done=1 x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-two-holders-100-tenure | pass | pass | pass | pass | 150 | 0 | 0 | 0 | 6 | 1390 | inst-b +771 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q1-two-holders-300-drain | pass | pass | pass | pass | 150 | 0 | 0 | 0 | 4 | 4174 | inst-b +344 | a:claim=1,drain_done=1 x:claim=1,lost=1,drain_done=1 y:fence=1 |
| q1-two-holders-300-tenure | pass | pass | pass | pass | 150 | 0 | 0 | 0 | 5 | 4807 | inst-b +462 | a:claim=1,drain_done=1 x:claim=2,lost=1,drain_done=1 y:fence=1,claim=1 |
| q2-flip-drain | FAIL | pass | pass | pass | 1000 | 497 | 0 | 497 | 4 | 12 | 60 switches, p50 75 max 170 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-flip-tenure | pass | pass | pass | pass | 1000 | 0 | 0 | 0 | 122 | 125 | 60 switches, p50 102 max 218 | x:fence=30,claim=61,acquire=31,lost=30,drain_done=30 y:fence=30,claim=59,acquire=30,lost=29,drain_done=29 |
| q2-misroute-120-drain | FAIL | pass | pass | pass | 1600 | 198 | 0 | 198 | 4 | 24 | inst-b +135, inst-a +272 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-misroute-120-tenure | pass | pass | pass | pass | 1600 | 0 | 0 | 0 | 6 | 21 | inst-b +87, inst-a +301 | x:fence=1,claim=3,acquire=2,lost=1,drain_done=1 y:fence=1,claim=1,acquire=1 |
| q2-misroute-30-drain | FAIL | pass | pass | pass | 700 | 198 | 0 | 198 | 4 | 17 | inst-b +129, inst-a +190 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-misroute-30-tenure | pass | pass | pass | pass | 701 | 0 | 0 | 0 | 6 | 14 | inst-b +51, inst-a +81 | x:fence=1,claim=3,acquire=2,lost=1,drain_done=1 y:fence=1,claim=1,acquire=1 |
| q2-misroute-5-drain | FAIL | pass | pass | pass | 450 | 198 | 0 | 190 | 4 | 46 | inst-b +122, inst-a +113 | x:claim=1,acquire=1,lost=1,drain_done=1 y:fence=1,acquire=1 |
| q2-misroute-5-tenure | pass | pass | pass | pass | 450 | 0 | 0 | 0 | 6 | 9 | inst-b +74, inst-a +150 | x:fence=1,claim=3,acquire=2,lost=1,drain_done=1 y:fence=1,claim=1,acquire=1 |
| q3-inproc-ret-off | FAIL | pass | pass | pass | 130 | 1 | 0 | 1 | 2 | - |  | y:fence=1 |
| q3-inproc-ret-tenure | pass | pass | pass | pass | 248 | 0 | 0 | 0 | 5 | 104 |  | x1:claim=3,acquire=1,lost=1,drain_done=1 y:fence=1,claim=1 |
| q3-restart-cleanup-off | pass | pass | pass | pass | 70 | 0 | 0 | 1 | 1 | - |  | y:fence=1 |
| q3-restart-cleanup-ret-off | pass | pass | pass | pass | 72 | 0 | 0 | 0 | 2 | - |  | y:fence=1 |
| q3-restart-cleanup-ret-tenure | pass | pass | pass | pass | 69 | 0 | 0 | 0 | 3 | 14 |  | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q3-restart-cleanup-tenure | pass | pass | pass | pass | 69 | 0 | 0 | 0 | 2 | 11 |  | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q3-restart-optimize-off | pass | pass | pass | pass | 70 | 0 | 0 | 1 | 2 | - |  | y:fence=1 |
| q3-restart-optimize-tenure | pass | pass | pass | pass | 69 | 0 | 0 | 0 | 4 | 9 |  | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q3-restart-ret-off | pass | FAIL | pass | pass | 74 | 0 | 1 | 0 | 2 | - |  | y:fence=1 |
| q3-restart-ret-optimize-off | pass | FAIL | pass | pass | 74 | 0 | 1 | 0 | 2 | - |  | y:fence=1 |
| q3-restart-ret-optimize-tenure | pass | FAIL | pass | pass | 69 | 0 | 1 | 0 | 4 | 24 |  | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q3-restart-ret-tenure | pass | FAIL | pass | pass | 69 | 0 | 1 | 0 | 4 | 32 |  | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
| q3-restart-ret-vacuum-off | pass | FAIL | pass | FAIL | 74 | 0 | 1 | 0 | 2 | - |  | y:fence=1 |
| q3-restart-ret-vacuum-tenure | pass | FAIL | pass | FAIL | 69 | 0 | 1 | 0 | 4 | 26 |  | x1:claim=1 x2:claim=1,drain_done=1 y:fence=1,claim=1 |
q3-recreate-off 4 ok commits by restarted X into the new table; files now visible: 4
q3-recreate-tenure 3 ok commits by restarted X into the new table; files now visible: 1
