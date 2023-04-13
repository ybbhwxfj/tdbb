------------------------------ MODULE tdbb ------------------------------
EXTENDS Integers,  FiniteSets, Sequences, TLC

CONSTANTS 
    INVALID_ID,
    NODE_CCB,
    NODE_DSB,
    NODE_RLB,
    KEY,
    XID,
    CNO_MAX,
    COMBINATION


VARIABLE
    \* variables `pc' to keep track of the control 
    \* state and `stack' to hold the procedure-calling
    pc,
    \* the history of global schedule
    history, 
    \* the serializable schedule of CCB
    schedule,
    \* All logs have been appended if less or equal to this position of schedule
    position, 
    
    \* CCB variables
    ccb_tx,
    ccb_cno,
    ccb_tuple,
    ccb_last_csn,
    
    \* RLB variables
    rlb_log,
    rlb_commit_lsn,
    rlb_cno,
    rlb_dsb,
    rlb_ccb,
    rlb_last_csn,

    \* DSB variables
    dsb_tuple,
    dsb_tuple_cache,
    dsb_last_csn,
    dsb_cno

ccb_vars == <<
    ccb_tx,
    ccb_cno,
    ccb_tuple,
    ccb_last_csn
>>

rlb_vars == <<
    rlb_log,
    rlb_commit_lsn,
    rlb_cno,
    rlb_dsb,
    rlb_ccb,
    rlb_last_csn
>>

dsb_vars == <<
    dsb_tuple,
    dsb_tuple_cache,
    dsb_last_csn,
    dsb_cno
>>

aux_vars == <<
    history,
    schedule,
    position
>>

variables == <<
    pc,
    aux_vars,
    ccb_vars,
    rlb_vars,
    dsb_vars
>>

BLOCK_CCB == "ccb"
BLOCK_DSB == "dsb"
BLOCK_RLB == "rlb"

PC_INIT == "init"
PC_IDLE == "idle"


OP_READ == "read"
OP_WRITE == "write"
OP_ABORT == "abort"
OP_COMMIT == "commit"

TS_COMMITTED == "committed"
TS_ABORTED == "aborted"
TS_IDLE == "idle"

LOG_OP_SET == {OP_WRITE, OP_COMMIT, OP_ABORT}
WRITE_OP_SET == {OP_WRITE}
READ_WRITE_OP_SET == {OP_WRITE, OP_READ}
END_OP_SET == {OP_ABORT, OP_COMMIT}

NODE == NODE_CCB \cup NODE_DSB \cup NODE_RLB

BLOCK_COMBINATION == {
    c \in [
            ccb : NODE_CCB,
            rlb : NODE_RLB,
            dsb : NODE_DSB,
            keys : SUBSET KEY
        ]   
        : 
        /\ KEY \cap NODE = {}
        /\ Cardinality(COMBINATION) > 0
        /\ \E s \in COMBINATION:
            /\ c.ccb \in s
            /\ c.rlb \in s
            /\ c.dsb \in s
            /\ c.keys = s \ {c.ccb, c.rlb, c.dsb}
            /\ ~(\E r \in COMBINATION:
                    /\ r # s
                    /\ r.keys \cap s.keys # {}
                )
}       


NodeIdOfKey(_key, _block) ==
    LET ids == 
        {
            id \in NODE:
                \E c \in BLOCK_COMBINATION: 
                    /\ _key \in c.keys
                    /\ c[_block] = id
       }
    IN ids

NodeIdOfBlock(_in_id, _input_block, _ouput_block) ==
    LET ids == 
        { 
            _out_id \in NODE:
                /\ \E c \in BLOCK_COMBINATION: 
                    /\ _in_id = c[_input_block]
                    /\ _out_id = c[_ouput_block]
        }
    IN ids


InitCCB ==
    /\ ccb_tx = [n \in NODE |-> {} ]
    /\ ccb_cno = [n \in NODE |-> 0]
    /\ ccb_tuple = [n \in NODE |-> [key \in KEY |-> <<>>]]
    /\ ccb_last_csn = [n \in NODE |-> 0]

    
InitRLB ==
    /\ rlb_log = [n \in NODE |-> <<>>]
    /\ rlb_commit_lsn = [n \in NODE |-> 0]
    /\ rlb_cno = [n \in NODE |-> 0]
    /\ rlb_dsb = [n \in NODE |-> {}]
    /\ rlb_ccb = [n \in NODE |-> {}]
    /\ rlb_last_csn = [n \in NODE |-> 0]
    
InitDSB ==
    /\ rlb_last_csn = [n \in NODE |-> 0]
    /\ dsb_tuple = [n \in NODE |-> [key \in KEY |-> <<>>]]
    /\ dsb_tuple_cache = [n \in NODE |-> [key \in KEY |-> <<>>]]
    /\ dsb_last_csn = [n \in NODE |-> 0]
    /\ dsb_cno = [n \in NODE |-> 0]

Init ==
    /\ pc = [n \in NODE |-> [state |-> PC_INIT]]
    /\ history = <<>>
    /\ schedule = <<>>
    /\ position = 0
    /\ InitCCB
    /\ InitRLB
    /\ InitDSB
    
Max(s) ==
    CHOOSE i \in s : (\A j \in s : j <= i)
    
LogEntry(otype, xid, lsn, csn, payload) == 
    [
        otype |-> otype,
        xid |-> xid,
        lsn |-> lsn,
        csn |-> csn,
        payload |-> payload
    ]

Tuple(xid, csn) ==
    [
        xid |-> xid,
        csn |-> csn
    ]

_NewTupleVersion(_tuple_version, _xid, _csn) ==
    IF Len(_tuple_version) > 0 THEN
        IF _tuple_version[Len(_tuple_version)].csn < _csn THEN 
            \* only when current CSN are less than the replayed log's CSN
            <<Tuple(_xid, _csn)>>
        ELSE
            _tuple_version
    ELSE
        <<Tuple(_xid, _csn)>>

_ReadTupleLatest(_versions) ==
    IF Len(_versions) = 0 THEN 
        <<>>
    ELSE
        <<_versions[Len(_versions)]>>

_ReadTupleLatestCommit(_versions, _tx_state) ==
    IF Len(_versions) = 0 THEN 
        <<>>
    ELSE
        \* read the latest committed
        LET last_i == {
                i \in 1..Len(_versions): 
                    /\ _tx_state[_versions  [i].xid] = TS_COMMITTED
                    /\ \E j \in 1..i: _tx_state[_versions  [i].xid] = TS_COMMITTED
            }
        IN  IF Cardinality(last_i) = 0 THEN 
                \* no committed
                <<>> 
            ELSE
                LET index == CHOOSE  i \in last_i :TRUE
                IN <<_versions[index]>>


\* define Action of history\schedule
_ActionEndCommand(xid, type) ==
    [
        otype |-> type,
        xid |-> xid
    ]
    
_ActionRead(xid, key, tuple) ==
    [
        otype |-> OP_READ,
        xid |-> xid,
        key |-> key,
        tuple |-> tuple
    ]

_ActionWrite(xid, key, tuple) ==
    [
        otype |-> OP_WRITE,
        xid |-> xid,
        key |-> key,
        tuple |-> tuple
    ]


\* utility for select froma history/schedule/log
_IsLogOp(_e) ==
    _e.otype \in LOG_OP_SET

_IsEndOp(_e) ==
    _e.otype \in END_OP_SET

_IsCommitOp(_e) ==
    _e.otype = OP_COMMIT

_IsReadOp(_e) ==
    _e.otype = OP_READ
    
IsWriteLog(_e) ==
    _e.otype \in WRITE_OP_SET

_IsReadWriteOp(_e) ==
    _e.otype \in READ_WRITE_OP_SET
    
_SelectLogOp(_op_sequence) ==
    SelectSeq(_op_sequence, _IsLogOp)

_SelectEnd(_log) ==
    SelectSeq(_log, _IsEndOp)

_SelectCommit(_seq) ==
    SelectSeq(_seq, _IsCommitOp)

_SelectRead(_seq) ==
    SelectSeq(_seq, _IsReadOp)
        
_SelectWrite(_log, _is_committed, _xids) ==
    LET F[i \in 0..Len(_log)] == 
            IF i = 0 \/ _xids = {} THEN 
                << >>
            ELSE IF /\ _log[i].xid \in _xids
                    /\ IsWriteLog(_log[i]) 
                    /\(\/ (/\ _is_committed 
                            /\ \E ci \in i..Len(_log) : 
                                _log[ci].otype = OP_COMMIT
                           )
                       \/ (/\ ~_is_committed 
                            /\ ~(\E ci \in i..Len(_log) : 
                                    _log[ci].otype = OP_COMMIT
                                )
                           )
                       )
                THEN 
                    Append(F[i-1], _log[i])
            ELSE F[i-1]
    IN F[Len(_log)]

_SelectCommittedWriteOfTx(_log, _xids) ==
    _SelectWrite(_log, TRUE, _xids)
    
_SelectCommittedWrite(_log) ==
    _SelectWrite(_log, TRUE, XID)
    
_SelectUncommittedWrite(_log) ==
    _SelectWrite(_log, FALSE, XID)


\* define message between blocks
_MessageAppendLog(_cno, _entries) ==
    [
        cno |-> _cno,
        entries |-> _entries
    ] 
    
_MessageReport(_cno, _last_csn, _end_log) ==
    [
        cno |-> _cno,
        entries |-> _end_log,
        last_csn |-> _last_csn
    ]

_MessageRead(_xid, _cno, _key) ==
    [
        xid |-> _xid,
        cno |-> _cno,
        key |-> _key
    ]

_MessageReadResponse(_xid, _is_ok, _cno, _key, _tuple) ==
    [
        xid |-> _xid,
        ok |-> _is_ok,
        cno |-> _cno,
        key |-> _key,
        tuple |-> _tuple
    ]

_MessageReplayToDSBRequest(_cno, _entries) ==
    [
        cno |->  _cno,
        entries |-> _entries
    ]

_MessageUpdateLastCSN(_cno, _last_csn) ==
    [
        cno |->  _cno,
        last_csn |-> _last_csn
    ]

_IsConflictOType(_t1, _t2) ==
    \/ (/\ _t1 = OP_READ
        /\ _t2 = OP_WRITE)
    \/ (/\ _t1 = OP_WRITE
        /\ _t2 = OP_READ)
    \/ (/\ _t1 = OP_WRITE
        /\ _t2 = OP_WRITE)
        
_IsConflict(_op1, _op2) ==
    /\ _IsReadWriteOp(_op1)
    /\ _IsReadWriteOp(_op2)
    /\ _op1.key = _op2.key
    /\ _IsConflictOType(_op1.otype, _op2.otype)

_RegisteredRLB(_cno_info_set) ==
    LET info == CHOOSE x \in _cno_info_set:
                    ~(\E y \in _cno_info_set:
                        /\ x # y
                        /\ x.cno < y.cno
                      )
    IN info.node_id
    
_RegisterUpdate(_cno_info_set, _cno_info) ==
    IF \E info \in _cno_info_set:
        /\ info.node_id = _cno_info.node_id
    THEN
        LET info == CHOOSE info \in _cno_info_set:
                        /\ info.node_id = _cno_info.node_id
        IN IF info.cno < _cno_info.cno THEN
                (_cno_info_set \ {info}) \cup {_cno_info}
           ELSE
                _cno_info_set
    ELSE
        _cno_info_set \cup {_cno_info}

_RegisterCNOEqual(_cno_info_set, _cno) ==
    \A i \in _cno_info_set:
        i.cno = _cno

_RegisterIds(_cno_info_set) ==
    {node_id \in NODE: \E info \in _cno_info_set : info.node_id = node_id}

_RegisterOK(_cno_info_set, _set, _cno) ==
    /\ _RegisterCNOEqual(_cno_info_set, _cno)
    /\ _RegisterIds(_cno_info_set) = _set

_SelectKeyOfLogSeq(_log, _k) == 
    LET F[i \in 0..Len(_log)] == 
            IF i = 0 THEN 
                << >>
            ELSE 
                IF _log[i].key = _k THEN 
                    Append(F[i-1], _log[i])
                ELSE F[i-1]
    IN F[Len(_log)]

RECURSIVE _UpdateNewVersion(_,  _)
_UpdateNewVersion(_versions,  _log_seq) ==
    IF Len(_log_seq) = 0 THEN
        _versions
    ELSE 
        LET v == _versions 
            l == _log_seq[1]
            nv == _NewTupleVersion(v, l.xid, l.csn)
        IN _UpdateNewVersion(nv, SubSeq(_log_seq, 2, Len(_log_seq)))

_LogTxCSN(_log, _xid) ==
    LET index_set == {i \in DOMAIN _log:
        _log[i].xid = _xid}
    IN
        IF Cardinality(index_set) = 0 THEN
            0
        ELSE
            LET i == CHOOSE i \in index_set : TRUE
            IN _log[i].csn

\* update the log entry's CSN by the commit log entry's            
_LogUpdateCSN(_log_write, _log_commit) ==
    [
        i \in DOMAIN _log_write |->
            IF _log_write[i].otype = OP_WRITE THEN 
                LET xid == _log_write[i].xid
                    csn == _LogTxCSN(_log_commit, xid)
                IN IF csn = 0 THEN
                        _log_write[i]  
                   ELSE
                        [_log_write[i] EXCEPT !.tuple[1].csn = csn]
            ELSE
                _log_write[i]   
    ]


_InvalidCCBCache(_tuple, _last_csn) ==
    [
        key \in KEY |-> 
            IF Len(_tuple[key]) > 0 THEN
                IF Len(_tuple[key]) = 1 THEN
                    IF _tuple[key][1].csn < _last_csn THEN
                        <<>>
                    ELSE
                        _tuple[key]
                ELSE
                   _tuple.length_error
            ELSE 
               <<>>
    ]
    
\* replay log with 2 parameters    
_ReplayLog2(_tuple_c, _log) ==
    [
        key \in KEY |-> 
            LET ls == _SelectKeyOfLogSeq(_log, key)
            IN 
            IF Len(ls) > 0 THEN
                LET versions == _tuple_c[key]
                    version_updated == _UpdateNewVersion(versions , ls)
                IN 
                    version_updated
            ELSE 
                _tuple_c[key]
    ]


\* replay log with 3 parameters          
_ReplayLog3(_tuple_c, _tuple, _log) ==
    [
        key \in KEY |-> 
            LET ls == _SelectKeyOfLogSeq(_log, key)
            IN 
            IF Len(ls) > 0 THEN
                LET versions == _tuple[key] \o _tuple_c[key]
                    version_updated == _UpdateNewVersion(versions, ls)
                IN 
                    version_updated
            ELSE 
                _tuple_c[key]
    ]


_Op2Entry(_op, _lsn, _csn) ==
    CASE _op.otype \in READ_WRITE_OP_SET -> (
        [
            otype |-> _op.otype,
            xid |-> _op.xid,
            key |-> _op.key,
            tuple |-> _op.tuple,
            lsn |-> _lsn,
            csn |-> 0
        ]
    )
    [] _op.otype \in END_OP_SET -> (
        [
            otype |-> _op.otype,
            xid |-> _op.xid,
            lsn |-> _lsn,
            csn |-> _csn
        ]
    )
    [] OTHER -> (
        _op.error_op_type
    )
RECURSIVE _LogEntriesAppend(_, _)
              
_LogEntriesAppend(_log, _op_seq) ==
    IF Len(_op_seq) = 0 THEN
        _log
    ELSE
        LET end_tx_log_index == {i \in 1..Len(_log) : _log[i].otype \in END_OP_SET}
            csn == Cardinality(end_tx_log_index)
            lsn == Len(_log)
            op == _op_seq[1]
            entry == _Op2Entry(op, lsn + 1, csn + 1)
        IN _LogEntriesAppend(_log \o <<entry>>, SubSeq(_op_seq, 2, Len(_op_seq)))

        
_CSN2LSN(_log, n) ==
    IF n = 0 \/ Len(_log) = 0 THEN
        0
    ELSE  
        IF \E i \in 1..Len(_log) : _log[i].csn = n THEN
            LET i == CHOOSE i \in 1..Len(_log): _log[i].csn = n
                l == _log[i]
            IN l.lsn
        ELSE
            0 

_IsSerializable(_schedule) ==
    ~(\E i, j, m, n \in DOMAIN _schedule:
        /\ i < j
        /\ m > n
        /\ LET xi == _schedule[i].xid
               xj == _schedule[j].xid
           IN   /\ xi = _schedule[m].xid
                /\ xj = _schedule[n].xid
                /\ _IsConflict(_schedule[i], _schedule[j]) 
                /\ _IsConflict(_schedule[m], _schedule[n]) 
                /\ (\E c_xi \in  i..Len(_schedule):
                        /\ _schedule[c_xi].xid =  xi
                        /\ _schedule[c_xi].otype = OP_COMMIT
                   )
                /\ (\E c_xj \in  j..Len(_schedule):
                        /\ _schedule[c_xj].xid =  xj
                        /\ _schedule[c_xj].otype = OP_COMMIT
                   )
     )

_IsSerializableEqual(_schedule1, _schedule2) ==
    ~(\E i, j \in DOMAIN _schedule1:
        \E m, n \in DOMAIN _schedule2:
            /\ i < j
            /\ m > n
            /\ LET xi == _schedule1[i].xid
                   xj == _schedule1[j].xid
               IN   /\ xi = _schedule2[m].xid
                    /\ xj = _schedule2[n].xid
                    /\ _IsConflict(_schedule1[i], _schedule1[j]) 
                    /\ _IsConflict(_schedule2[m], _schedule2[n]) 
                    /\ (\E c_xi \in  i..Len(_schedule1):
                            /\ _schedule1[c_xi].xid =  xi
                            /\ _schedule1[c_xi].otype = OP_COMMIT
                       )
                    /\ (\E c_xj \in  j..Len(_schedule1):
                            /\ _schedule1[c_xj].xid =  xj
                            /\ _schedule1[c_xj].otype = OP_COMMIT
                       )
                    /\ (\E c_xm \in  m..Len(_schedule2):
                            /\ _schedule1[c_xm].xid =  xi
                            /\ _schedule1[c_xm].otype = OP_COMMIT
                       )
                    /\ (\E c_xn \in  n..Len(_schedule2):
                            /\ _schedule1[c_xn].xid =  xj
                            /\ _schedule1[c_xn].otype = OP_COMMIT
                       )
     )
         
_CanSchedule(_schedule, _new_op) ==
    \* no such COMMIT/ABORT command
    /\ ~(\E i \in 1..Len(_schedule): 
            /\ _schedule[i].otype \in END_OP_SET
            /\ _schedule[i].xid = _new_op.xid
         )
    /\  IF _new_op.otype \in READ_WRITE_OP_SET THEN
            \* no such READ/WRITE operations accessing this key
            ~(\E i \in 1..Len(_schedule):
                /\ _schedule[i].otype \in READ_WRITE_OP_SET
                /\ _schedule[i].xid = _new_op.xid
                /\ _schedule[i].key = _new_op.key
            )
        ELSE \* prevent no accesinng transaction
            \E i \in 1..Len(_schedule):
                /\ _schedule[i].otype \in READ_WRITE_OP_SET
                /\ _schedule[i].xid = _new_op.xid
    /\ _IsSerializable(_schedule \o <<_new_op>>)
                         
CCBAppendLog(i) ==
    /\ pc[i].state = PC_IDLE
    /\ Len(schedule) # 0
    /\ Len(schedule) > position
    /\ LET rlb_id == CHOOSE _n \in NodeIdOfBlock(i, BLOCK_CCB, BLOCK_RLB) : TRUE
           to_append_s == SubSeq(schedule, position + 1, Len(schedule))
       IN (/\ ccb_cno[i] = rlb_cno[rlb_id]
           /\ position' = Len(schedule)
           /\ LET message == _MessageAppendLog(ccb_cno[i], _SelectLogOp(to_append_s))
                IN pc' = [pc EXCEPT ![rlb_id] = [state |-> "HandleAppendLogRequest", message |-> message]]
          )
    /\ UNCHANGED <<
            schedule,
            history,
            ccb_tx,
            ccb_cno,
            ccb_tuple,
            ccb_last_csn,
            rlb_vars,
            dsb_vars
        >>



RLBHandleAppendLogRequest(i) ==
    /\ pc[i].state = "HandleAppendLogRequest"
    /\ LET message == pc[i].message
           entries == message.entries
       IN  IF rlb_cno[i] = message.cno THEN
                rlb_log' = [rlb_log EXCEPT ![i] = _LogEntriesAppend(rlb_log[i], entries)]
           ELSE
                UNCHANGED <<rlb_log>>
    /\ pc' = [pc EXCEPT ![i] = [state |-> PC_IDLE]]
    /\ UNCHANGED <<
            aux_vars,
            rlb_commit_lsn,
            rlb_cno,
            rlb_dsb,
            rlb_ccb,
            rlb_last_csn,
            ccb_vars,
            dsb_vars
        >>       

RLBLogCommit(i) ==
    \E _index \in 1..Len(rlb_log[i]):
        /\ _index <= Len(rlb_log[i])
        /\ rlb_commit_lsn[i] < _index
        /\ rlb_commit_lsn' = [rlb_commit_lsn EXCEPT ![i] = _index ]
        /\ LET seq == SubSeq(rlb_log[i], rlb_commit_lsn[i] + 1, _index)
               commit_seq == _SelectCommit(seq)
           IN history' = history \o commit_seq
        /\ UNCHANGED <<ccb_vars, dsb_vars, 
                pc,
                schedule,
                position,
                rlb_log,
                rlb_cno,
                rlb_dsb,
                rlb_ccb,
                rlb_last_csn,
                ccb_vars,
                dsb_vars
            >>


_LogType2TxState(_otype) ==
    CASE _otype = OP_ABORT -> (
        TS_ABORTED
    )
    [] _otype = OP_COMMIT -> (
        TS_COMMITTED
    )
    [] OTHER -> (
        _otype.error_log_type
    )


_TxStateUpdate(_old_state, _new_state) ==
    CASE _old_state = TS_IDLE -> (
        _new_state
    )
    [] OTHER -> (
        _old_state
    )
    
RECURSIVE _CCBUpdateTxState(_, _)
_CCBUpdateTxState(_ccb_tx, _log_set) ==
    IF Cardinality(_log_set) = 0 THEN
        _ccb_tx
    ELSE
        LET log == CHOOSE log \in _log_set : TRUE
            state == _LogType2TxState(log.otype)
            xid == log.xid
            new_ccb_tx == IF \E tx \in _ccb_tx : tx.xid = xid THEN
                            LET tx == CHOOSE tx \in _ccb_tx : tx.xid = log.xid
                            IN (_ccb_tx \ {tx}) \cup {[ xid |-> tx.xid, state |-> state]}
                      ELSE
                            _ccb_tx \cup {[ xid |-> xid, state |-> state]}
        IN _CCBUpdateTxState(new_ccb_tx, _log_set \ {log})


    
RLBReportToCCB(i) ==
    /\ rlb_dsb[i] # {}
    /\ rlb_ccb[i] # {}
    /\ pc[i].state = PC_IDLE
    /\ LET ccb == CHOOSE rlb \in NodeIdOfBlock(i, BLOCK_RLB, BLOCK_CCB) : TRUE
       IN
        /\ rlb_cno[i] = ccb_cno[ccb]    
        /\
            LET commit_index == rlb_commit_lsn[i]
                commit_prefix_seq == SubSeq(rlb_log[i], 1, commit_index)
                end_log_seq == _SelectEnd(commit_prefix_seq)
                message == _MessageReport(rlb_cno[i], rlb_last_csn[i], end_log_seq)
            IN 
               /\ Len(end_log_seq) > 0
               /\ pc' = [pc EXCEPT ![i] = [state |-> "HandleReportToCCB", message |-> message]]
    /\ UNCHANGED <<
        aux_vars,
        rlb_vars,
        ccb_vars,
        dsb_vars
      >>    


CCBHandleReportToCCB(i) ==
    /\ pc[i].state = "HandleReportToCCB"
    /\ LET message == pc[i].message
            cno == message.cno
            entries ==  message.entries
            last_csn == IF ccb_last_csn[i] < message.last_csn THEN 
                            message.last_csn 
                        ELSE
                            ccb_last_csn[i]
            end_log_set == { entries[j] : j \in DOMAIN entries}
            committed == {x \in XID : 
                            \E j \in 1..Len(entries) :
                                /\ x = entries[j].xid
                                /\ entries[j].otype = OP_COMMIT
                         } 
       IN IF ccb_cno[i] = cno THEN
            /\ ccb_tx' = [ccb_tx EXCEPT ![i] = _CCBUpdateTxState(ccb_tx[i], end_log_set) ]
            /\ ccb_last_csn' = [ccb_last_csn EXCEPT ![i] = last_csn]
            /\ LET rlb_node == CHOOSE _n \in NodeIdOfBlock(i, BLOCK_CCB, BLOCK_RLB) : TRUE
                   write_log1 == _SelectCommittedWriteOfTx(rlb_log[i], committed) 
                   write_log2 ==  _LogUpdateCSN(write_log1, entries)
                   tuple_set1 == _ReplayLog2(ccb_tuple[i], write_log2)
                   tuple_set2 == _InvalidCCBCache(tuple_set1, last_csn)
               IN  ccb_tuple' = [ccb_tuple EXCEPT ![i] = tuple_set2]
          ELSE
            UNCHANGED <<ccb_tx, ccb_last_csn>>
    /\ pc' = [pc EXCEPT ![i] = [state |-> PC_IDLE]]
    /\ UNCHANGED <<
            aux_vars,
            ccb_cno,
            ccb_last_csn,
            rlb_vars,
            dsb_vars
        >>

CCBWrite(i, _xid, _key) ==
    /\ pc[i].state = PC_IDLE
    /\ LET action == _ActionWrite(_xid, _key, <<Tuple(_xid, 0)>>)
       IN /\ _CanSchedule(schedule, action)
          /\ schedule' = schedule \o <<action>>
          /\ history' = schedule \o <<action>>
    /\ UNCHANGED <<
            pc,
            position,
            ccb_vars,
            rlb_vars,
            dsb_vars
        >> 

CCBRead(i, _xid, _key) ==
    /\ pc[i].state = PC_IDLE
    /\ LET action == _ActionRead(_xid, _key, <<>>)
       IN   /\ _CanSchedule(schedule, action)
            /\ schedule' = schedule \o <<action>>
    /\ LET tuple == _ReadTupleLatest(ccb_tuple[i][_key])
        IN IF Len(tuple) = 0 THEN
            \* read from DSB
                /\ LET message == _MessageRead(_xid, ccb_cno[i], _key)
                       dsb_node == CHOOSE _n \in NodeIdOfKey(_key, BLOCK_DSB): TRUE
                   IN pc' = [
                            pc EXCEPT  ![dsb_node] = 
                                [
                                    state |-> "HandleReadFromDSBRequest", 
                                    message |-> message
                                ]
                            ]
                /\ UNCHANGED <<history>>
            ELSE \* direct read from CCB cached
                /\ history' = history \o <<_ActionRead(_xid, _key, tuple)>>
                /\ UNCHANGED <<pc>>
    /\ UNCHANGED <<
            position,
            ccb_vars,
            rlb_vars,
            dsb_vars
        >>

CCBAbort(i, _xid) ==
    /\ pc[i].state = PC_IDLE
    /\ LET action == _ActionEndCommand(_xid, OP_ABORT)
       IN /\ _CanSchedule(schedule, action)
          /\ schedule' = schedule \o <<action>>
    /\ UNCHANGED <<
            pc,
            history,
            position,
            ccb_vars,
            rlb_vars,
            dsb_vars
        >>   

CCBCommit(i, _xid) ==
    /\ pc[i].state = PC_IDLE
    /\ LET action == _ActionEndCommand(_xid, OP_COMMIT)
       IN /\ _CanSchedule(schedule, action)
          /\ schedule' = schedule \o <<action>>
    /\ UNCHANGED <<
            pc, 
            history,
            position,
            ccb_vars,
            rlb_vars,
            dsb_vars
        >> 
        
DSBHandleReadFromDSBRequest(i) ==
    /\ pc[i].state = "HandleReadFromDSBRequest"
    /\  LET message == pc[i].message
            cno == message.cno
            key == message.key
            xid == message.xid
            tuple == _ReadTupleLatest(dsb_tuple[i][key] \o dsb_tuple_cache[i][key])
            response ==             
                IF dsb_cno[i] = cno THEN 
                    _MessageReadResponse(xid, TRUE, dsb_cno[i], key, tuple)
                ELSE 
                    _MessageReadResponse(xid, TRUE, dsb_cno[i], key, tuple)
        IN pc' = [pc EXCEPT  ![i] = 
                [
                    state |-> "HandleReadFromDSBResponse", 
                    message |-> response
                ]
            ]
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars, 
            rlb_vars, 
            dsb_vars
        >>

CCBHandleReadFromDSBResponse(i) ==
    /\ pc[i].state = "HandleReadFromDSBResponse"
    /\  LET message == pc[i].message
            xid == message.xid
            cno == message.cno
            key == message.key
            tuple == message.tuple
            read_action ==  _ActionRead(xid, key, tuple)
        IN history' = history \o <<read_action>>
    /\ pc' = [pc EXCEPT ![i] = [state |-> PC_IDLE]]
    /\ UNCHANGED <<
            schedule,
            position,
            ccb_vars,
            rlb_vars,
            dsb_vars
        >>

_CommittedWriteLog(_log_sequence, _last_lsn, _commit_lsn) ==
    LET last_lsn == IF _last_lsn = 0 THEN 1 ELSE _last_lsn
        commit_seq == SubSeq(_log_sequence, last_lsn, _commit_lsn)
        write_seq == _SelectCommittedWrite(commit_seq)
    IN write_seq

_UnCommittedWriteLog(_log_sequence, _last_lsn, _commit_lsn) ==
    LET last_lsn == IF _last_lsn = 0 THEN 1 ELSE _last_lsn
        commit_seq == SubSeq(_log_sequence, last_lsn, _commit_lsn)
        write_seq == _SelectUncommittedWrite(commit_seq)
    IN write_seq
        
RLBReplayToDSB(i) ==
    /\ pc[i].state = PC_IDLE
    /\  LET last_lsn == _CSN2LSN(rlb_log, rlb_last_csn[i])
            commit_lsn == rlb_commit_lsn[i]
            commit_write == _CommittedWriteLog(rlb_log[i], last_lsn + 1, commit_lsn)
            message == _MessageReplayToDSBRequest(rlb_cno[i], commit_write)
        IN  /\ Len(commit_write) > 0
            /\ pc' = [ 
                    pc EXCEPT ![i] = 
                    [
                        state |-> "HandleReplayToDSBRequest", 
                        message |-> message
                    ]
                ]
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars,
            rlb_vars,
            dsb_vars
        >>



DSBHandleReplayToDSBRequest(i) ==
    /\ pc[i].state = "HandleReplayToDSBRequest"
    /\ LET message == pc[i].message
            cno == message.cno
            sync == message.sync
            entries == message.entries

        IN 
            LET new_tuple_cache == _ReplayLog3(dsb_tuple_cache[i], dsb_tuple[i], entries)
            IN
            IF cno = dsb_cno[i] THEN 
                UNCHANGED  <<dsb_tuple_cache, pc>>
            ELSE 
                /\ dsb_tuple_cache' = [dsb_tuple_cache EXCEPT ![i] = new_tuple_cache]
                /\ pc' = [pc EXCEPT ![i] =
                                [
                                    state |-> PC_IDLE
                                ]
                        ]
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars,
            rlb_vars,
            dsb_tuple,
            dsb_last_csn,
            dsb_cno
        >>
    
_AdvanceCSN(_last_csn, _tuple_cache) ==
    LET csn_set == {   
            (IF Len(_tuple_cache[k]) = 0 THEN
                0
             ELSE
                _tuple_cache[k][Len(_tuple_cache[k])].csn
            )
                : k \in DOMAIN _tuple_cache    
        }
       max_csn == IF Cardinality(csn_set) = 0 THEN 0 ELSE Max(csn_set)
    IN  IF max_csn <= _last_csn THEN
            _last_csn
        ELSE
            max_csn
        
DSBFlush(i) ==
    /\ pc[i].state = PC_IDLE
    /\ dsb_cno[i] # 0
    /\ dsb_tuple' = [dsb_tuple EXCEPT ![i] = dsb_tuple_cache[i]]
    /\ LET csn == _AdvanceCSN(dsb_last_csn[i], dsb_tuple_cache[i])
       IN dsb_last_csn' = [dsb_last_csn EXCEPT ![i] =  csn]
    /\ LET message == _MessageUpdateLastCSN(dsb_cno[i], dsb_last_csn[i])
           rlb_node == CHOOSE _n \in NodeIdOfBlock(i, BLOCK_DSB, BLOCK_RLB) : TRUE
       IN  pc' = [pc EXCEPT ![rlb_node] = [
                        state |-> "HandleUpdateLastCSN",
                        message |-> message 
                      ]
                 ]
    /\ UNCHANGED <<
            dsb_tuple_cache,
            dsb_last_csn,
            dsb_cno
        >>
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars,
            rlb_vars
        >>
    
RLBHandleHandleUpdateLastCSN(i) ==
    /\ pc[i].state = "HandleUpdateLastCSN"
    /\ pc' = [pc EXCEPT ![i] = [
                        state |-> PC_IDLE
                      ]
                 ]
    /\ LET message == pc[i].message
            last_csn == message.last_csn
            cno == message.cno
            csn == IF rlb_last_csn[i] < last_csn THEN last_csn ELSE rlb_last_csn[i]
        IN IF cno = rlb_cno[i] THEN
                rlb_last_csn' = [rlb_last_csn EXCEPT ![i] = csn]
           ELSE
                UNCHANGED <<rlb_last_csn>>
    /\ UNCHANGED <<
            rlb_log,
            rlb_commit_lsn,
            rlb_cno,
            rlb_dsb,
            rlb_ccb
        >>
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars,
            dsb_vars 
        >>


_RegisterCCBRequest(_node_id) ==
    [
        node_id |-> _node_id
    ]

_RegisterCCBResponse(_node_id, _success, _cno, _uncommitted) ==
    [
        node_id |-> _node_id,
        success |-> _success,
        cno |-> _cno,
        uncommitted |-> _uncommitted
    ]
    
CCBRegisterCCB(i) ==
    /\ pc[i].state = PC_INIT
    /\ LET request == _RegisterCCBRequest(i)
           id_set == NodeIdOfBlock(i, BLOCK_CCB, BLOCK_RLB)
           rlb_id == CHOOSE n \in id_set : TRUE
       IN /\ pc' = [pc EXCEPT ![rlb_id] = [
                                    state |-> "HandleRegisterCCBRequest",
                                    message |-> request
                            ]]
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars,
            rlb_vars, 
            dsb_vars 
        >>
            
RLBHandleRegisterCCBRequest(i) ==
    /\ pc[i].state = "HandleRegisterCCBRequest"
    /\ LET  message == pc[i].message
            node_id == message.node_id
            dsb_ids == NodeIdOfBlock(i, BLOCK_RLB, BLOCK_DSB)
            ccb_ids == NodeIdOfBlock(i, BLOCK_RLB, BLOCK_CCB)
            cno == rlb_cno[i]
       IN /\ LET ccb_info == [
                        cno |-> cno,
                        node_id |-> node_id
                    ] 
              reg_ccb == _RegisterUpdate(rlb_ccb[i], ccb_info)
              IN 
                 /\ IF _RegisterOK(rlb_dsb[i], dsb_ids, cno) THEN
                        /\ rlb_ccb' = [rlb_ccb EXCEPT ![i] = reg_ccb]
                        /\ LET  log_seq == _UnCommittedWriteLog(rlb_log[i], 1, rlb_commit_lsn[i])
                                uncommitted == {x \in XID: \E _li \in 1..Len(log_seq): log_seq[_li].xid = x}
                                response == _RegisterCCBResponse(i, TRUE, cno, uncommitted)
                                pc_ccb == 
                                    [
                                        state |-> "HandleRegisterCCBResponse",
                                        message |-> response
                                    ]
                                pc_rlb == 
                                    [
                                        state |-> PC_IDLE
                                    ] 
                           IN  IF i # node_id THEN
                                    pc' = [pc EXCEPT ![node_id] = pc_ccb,  ![i] = pc_rlb]
                               ELSE
                                    pc' = [pc EXCEPT ![node_id] = pc_ccb]
                                 
                    ELSE
                        /\ LET response == _RegisterCCBResponse(i, FALSE, 0, {})
                               pc_ccb == 
                                    [
                                        state |-> "HandleRegisterCCBResponse",
                                        message |-> response
                                    ]
                               pc_rlb == 
                                    [
                                        state |-> PC_IDLE
                                    ] 
                           IN  IF i # node_id THEN
                                    pc' = [pc EXCEPT ![node_id] = pc_ccb,  ![i] = pc_rlb]
                               ELSE
                                    pc' = [pc EXCEPT ![node_id] = pc_ccb]
                        /\ UNCHANGED <<rlb_ccb>>

    /\ UNCHANGED <<
            rlb_log,
            rlb_commit_lsn,
            rlb_cno,
            rlb_dsb,
            rlb_last_csn
        >>
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars,
            dsb_vars 
        >>

RECURSIVE _ScheduleAbort(_, _)   
_ScheduleAbort(_schedule, _to_abort) == 
    IF _to_abort = {} THEN
        _schedule
    ELSE
        LET x == CHOOSE x \in _to_abort : TRUE
            action == _ActionEndCommand(x, OP_ABORT)
            seq == IF _CanSchedule(schedule, action) THEN <<action>> ELSE <<>>
            schedule_seq == _schedule \o seq
        IN  _ScheduleAbort(schedule_seq, _to_abort \ {x})
  
CCBHandleRegisterCCBResponse(i) ==
    /\ pc[i].state = "HandleRegisterCCBResponse"
    /\ LET message == pc[i].message
            success == message.success
            uncommitted == message.uncommitted 
            cno == message.cno
       IN IF success THEN
             /\ LET s == _ScheduleAbort(schedule, uncommitted)
                IN  schedule' = s
             /\ ccb_cno' = [ccb_cno EXCEPT ![i] = cno]
          ELSE
            UNCHANGED <<ccb_cno, schedule>>
    /\ pc' = [pc EXCEPT ![i] = [state |-> PC_IDLE]]
    /\ UNCHANGED <<
            ccb_tx,
            ccb_tuple,
            ccb_last_csn
        >>
    /\ UNCHANGED <<
            history,
            position,
            rlb_vars,
            dsb_vars 
        >>

_RegisterDSBRequest(_last_csn, _dsb_id) ==
    [
        last_csn |-> _last_csn,
        node_id |-> _dsb_id
    ]

_RegisterDSBResponse(_node_id, _success, _cno, _entries) ==
    [
        node_id |-> _node_id, 
        success |-> _success,
        cno |-> _cno,
        entries |-> _entries
    ]

                 

               
DSBRegisterDSB(i) ==
    /\ pc[i].state = PC_INIT
    /\ LET message == _RegisterDSBRequest(dsb_last_csn[i], i)
            rlb_id == CHOOSE n \in NodeIdOfBlock(i, BLOCK_DSB, BLOCK_RLB) : TRUE
       IN  /\ rlb_cno[rlb_id] < CNO_MAX
           /\ pc' = [pc EXCEPT ![rlb_id] = [
                            state |-> "HandleRegisterDSBRequest",
                            message |-> message
                    ]]
          
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars,
            rlb_vars,
            dsb_vars 
        >>

        
RLBHandleRegisterDSBRequest(i) ==
    /\ pc[i].state = "HandleRegisterDSBRequest"
    /\ LET message == pc[i].message
           dsb_ids == NodeIdOfBlock(i, BLOCK_RLB, BLOCK_DSB)
           node_id == message.node_id
       IN IF message.last_csn < rlb_last_csn[i] THEN
            LET last_csn == rlb_last_csn[i]
                last_lsn == _CSN2LSN(rlb_log[i], last_csn)
                commit_lsn == rlb_commit_lsn[i]
                commit_write == _CommittedWriteLog(rlb_log[i], last_lsn + 1, commit_lsn)
                response == _RegisterDSBResponse(i, FALSE, 0, commit_write)
            IN 
                /\ LET pc_dsb == 
                        [
                            state |-> "HandleRegisterDSBResponse",
                            message |-> response
                        ]
                       pc_rlb == 
                        [
                            state |-> PC_INIT
                        ]
                   IN IF i # node_id THEN
                        pc' = [pc EXCEPT ![node_id] = pc_dsb,  ![i] = pc_rlb]
                      ELSE
                        pc' = [pc EXCEPT ![node_id] = pc_dsb]
                /\ UNCHANGED <<rlb_cno, rlb_dsb>>
          ELSE
            LET cno == rlb_cno[i] + 1
                response == _RegisterDSBResponse(i, TRUE, cno, <<>>)
            IN  /\ LET pc_dsb == 
                        [
                            state |-> "HandleRegisterDSBResponse",
                            message |-> response
                        ]
                       pc_rlb == 
                        [
                            state |-> PC_INIT
                        ]
                   IN IF i # node_id THEN
                        pc' = [pc EXCEPT ![node_id] = pc_dsb,  ![i] = pc_rlb]
                      ELSE
                        pc' = [pc EXCEPT ![node_id] = pc_dsb]
                /\ LET dsb_info == [
                                    cno |-> cno,
                                    node_id |-> node_id
                                 ]
                       reg_dsb == _RegisterUpdate(rlb_dsb[i], dsb_info)
                   IN /\ rlb_dsb' = [rlb_dsb EXCEPT ![i] = reg_dsb]
                      /\ IF _RegisterOK(reg_dsb, dsb_ids, cno) THEN
                            rlb_cno' = [rlb_cno EXCEPT ![i] = cno]
                         ELSE
                            UNCHANGED <<rlb_cno>>
    /\ UNCHANGED <<
            rlb_log,
            rlb_commit_lsn,
            rlb_ccb,
            rlb_last_csn
        >>
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars,
            dsb_vars 
        >>

DSBHandleRegisterDSBResponse(i) ==
    /\ pc[i].state = "HandleRegisterDSBResponse"
    /\ LET message == pc[i].message
           cno == message.cno
           success == message.success
           entries == message.entries
           node_id == message.node_id 
       IN IF message.success THEN
            /\ dsb_cno' = [dsb_cno EXCEPT ![i] = cno]
            /\ UNCHANGED <<dsb_tuple_cache>>
          ELSE
            /\ LET tuple_cache == _ReplayLog3(dsb_tuple_cache[i], dsb_tuple[i], entries)
               IN dsb_tuple_cache' = [dsb_tuple_cache EXCEPT ![i] = tuple_cache]
            /\ UNCHANGED <<dsb_cno>>
    /\ pc' = [pc EXCEPT ![i] = [state |-> PC_INIT]]
    /\ UNCHANGED <<
            dsb_tuple,
            dsb_last_csn
        >>    
    /\ UNCHANGED <<
            aux_vars,
            ccb_vars,
            rlb_vars
        >>

_RestartCCB(i) ==
    /\ ccb_tx' = [ccb_tx EXCEPT ![i] = {}]
    /\ ccb_cno' = [ccb_cno EXCEPT ![i] = 0]
    /\ ccb_tuple' = [ccb_tuple EXCEPT ![i] = [key \in KEY |-> <<>>]]
    /\ ccb_last_csn' = [ccb_last_csn EXCEPT ![i] = 0]
    /\ ccb_cno' = [ccb_cno EXCEPT ![i] = 0]

    
_RestartRLB(i) ==
    /\ rlb_dsb' = [rlb_dsb EXCEPT ![i] = {}]
    /\ rlb_ccb' = [rlb_ccb EXCEPT ![i] = {}]
    /\ rlb_log' = [rlb_log EXCEPT ![i] = SubSeq(rlb_log[i], 1, rlb_commit_lsn[i])]
    /\ UNCHANGED <<rlb_last_csn, rlb_commit_lsn, rlb_cno>>
    
_RestartDSB(i) ==
    /\ dsb_tuple_cache' = [dsb_tuple_cache EXCEPT ![i] = [key \in KEY |-> <<>>]]
    /\ dsb_cno' = [dsb_cno EXCEPT ![i] = 0]
    /\ UNCHANGED <<rlb_last_csn, dsb_tuple, dsb_last_csn>>  


Restart(i) ==
    /\ pc' = [pc EXCEPT ![i] = [state |-> PC_INIT]]
    /\ (\/ /\ i \in NODE_CCB
           /\ _RestartCCB(i)
        \/ UNCHANGED <<ccb_vars>>
       )
    /\ (\/ /\ i \in NODE_RLB   
           /\ _RestartRLB(i)
        \/ UNCHANGED <<rlb_vars>>
       )
    /\ (\/ /\ i \in NODE_DSB
           /\ _RestartDSB(i)
        \/ UNCHANGED <<dsb_vars>>
       )
    /\ UNCHANGED <<aux_vars>>
    
                    
\* Defines how the variables may transition.  
Next == 
    \/ \E i \in NODE_CCB, x \in XID, k \in KEY : CCBRead(i, x, k) 
    \/ \E i \in NODE_CCB, x \in XID, k \in KEY: CCBWrite(i, x, k)
    \/ \E i \in NODE_CCB, x \in XID: CCBAbort(i, x)
    \/ \E i \in NODE_CCB, x \in XID: CCBCommit(i, x)
    \/ \E i \in NODE_CCB : CCBAppendLog(i)
    \/ \E i \in NODE_RLB : RLBHandleAppendLogRequest(i)
    \/ \E i \in NODE_RLB : RLBLogCommit(i)
    \/ \E i \in NODE_CCB : CCBHandleReadFromDSBResponse(i)
    \/ \E i \in NODE_DSB : DSBHandleReadFromDSBRequest(i)
    \/ \E i \in NODE_RLB : RLBReportToCCB(i)
    \/ \E i \in NODE_CCB : CCBHandleReportToCCB(i)
    \/ \E i \in NODE_RLB : RLBReplayToDSB(i)
    \/ \E i \in NODE_DSB : DSBHandleReplayToDSBRequest(i)
    \/ \E i \in NODE_DSB : DSBFlush(i)
    \/ \E i \in NODE_RLB : RLBHandleHandleUpdateLastCSN(i)
    \/ \E i \in NODE_CCB : CCBRegisterCCB(i)
    \/ \E i \in NODE_RLB : RLBHandleRegisterCCBRequest(i)
    \/ \E i \in NODE_CCB : CCBHandleRegisterCCBResponse(i)
    \/ \E i \in NODE_DSB : DSBRegisterDSB(i)
    \/ \E i \in NODE_RLB : RLBHandleRegisterDSBRequest(i)
    \/ \E i \in NODE_DSB : DSBHandleRegisterDSBResponse(i)
    \/ \E i \in NODE : Restart(i)
    
    
\* The specification must start with the initial state and transition according to Next.
Spec == Init /\ [][Next]_variables


\* The specification must start with the initial state and transition according to Next.
NoStuttering ==
    WF_variables(Next)
    
LivenessSpec == Init /\ [][Next]_variables /\ NoStuttering

_ExistingCommit(_history, xid) ==
    \E i \in DOMAIN _history:
        /\ _history[i].otype = OP_COMMIT
        /\ _history[i].xid = xid

\* Write before Read dependency, read from empty value (<<>>)        
_ExistingWRFirstDependency(_schedule, _out, _key) ==
    \E i \in DOMAIN _schedule:
        /\ LET history_sub == SubSeq(_schedule, i + 1, Len(_schedule)) 
           IN _ExistingCommit(history_sub, _out)
        /\ _schedule[i].xid = _out
        /\ _schedule[i].otype = OP_READ
        /\ ~(\E k \in 1..i: \* these is no another write ahead of i
                /\ k # i
                /\ _schedule[k].otype = OP_WRITE
                /\ _schedule[k].key = _key
                /\ LET history_sub == SubSeq(_schedule, k+1, i)
                   IN _ExistingCommit(history_sub, _out)
            )
            
\* Write before Read dependency, read from previous write
_ExistingWRDependency(_schedule, _in, _out, _key) ==
    \E i, j \in DOMAIN _schedule:
        /\ i < j
        /\ _schedule[i].xid = _in
        /\ _schedule[j].xid = _out
        /\ _schedule[i].otype = OP_WRITE
        /\ _schedule[j].otype = OP_READ
        /\ _schedule[i].key = _key
        /\ _schedule[j].key = _key
        /\ LET history_sub == SubSeq(_schedule, i, j) 
           IN _ExistingCommit(history_sub, _in)
        /\ LET history_sub == SubSeq(_schedule, j + 1, Len(_schedule))
           IN _ExistingCommit(history_sub, _out)
        /\ \* these is no another committed write between i and j
           ~(\E k \in i..j: 
                /\ k # i
                /\ k # j
                /\ _schedule[k].otype = OP_WRITE
                /\ _schedule[k].key = _key
                /\ LET history_sub == SubSeq(_schedule, k+1, j)
                   IN _ExistingCommit(history_sub, _out)
            )
            
_ReadHistoryConsistent(_history) ==
    LET read_seq == _SelectRead(_history)
    IN \A i \in DOMAIN read_seq:
        CASE Len(read_seq[i].tuple) = 0 -> (
           LET out_x == read_seq[i].xid
               key == read_seq[i].key
           IN (
              LET history_sub == SubSeq(_history, i+1, Len(_history))
              IN _ExistingCommit(history_sub, out_x)
             ) 
                => _ExistingWRFirstDependency(_history, out_x, key)
        )
        [] Len(read_seq[i].tuple) = 1 -> (
           LET in_x == read_seq[i].tuple[1].xid
               out_x == read_seq[i].xid
               key == read_seq[i].key
           IN (
              /\ in_x # out_x
              /\ LET history_prefix == SubSeq(_history, 1, i) 
                 IN _ExistingCommit(history_prefix, in_x)
              /\ _ExistingCommit(_history, out_x)
             ) 
                => _ExistingWRDependency(_history, in_x, out_x, key)
        )
        [] OTHER -> (
            FALSE
        )
        

HistorySerializable ==
    /\ _IsSerializable(history)
    /\ _IsSerializableEqual(history, schedule)
    
ReadConsistency ==
    _ReadHistoryConsistent(history)

Corectness ==
    /\ HistorySerializable
    /\ ReadConsistency

=============================================================================


