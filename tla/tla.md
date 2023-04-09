
We have developed a [TLA+ specification](tdbb.tla) to demonstrate the correctness of our protocol. 
While there are some differences between this specification and protocol in our paper, we have implemented several optimizations to improve its functionality.

To start, we have introduced a caching system for committed data in the CCB, eliminating the need to replay corresponding data before calling ReportToCCB. 

Additionally, we now maintain the Commit Transaction Sequence Number(CSN) instead of the Log Sequence Number(LSN) when replaying data between the RLB and DSB.
The CSN is used to indicate the Nth committed transaction on this RLB, resulting in a continuous space for serial numbers. 
This simplifies our design and increases efficiency of our system.